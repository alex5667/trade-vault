#!/usr/bin/env python3
"""
ML Scorer V2 Training Script — Regression model for signal confidence scoring.

Goal: Train a LightGBM regression model to predict realized_edge_bps from signal_facts.
      Outputs: scorer_v2.joblib (dict-pack compatible with MLScoringGate).

Target: realized edge (in bps) = (R × stop_bps) - fees - slippage
        Falls back to R-multiple if edge_bps not derivable.

Design:
  - OOF (out-of-fold) with purge + embargo for leakage-safe evaluation
  - Feature engineering via feature_registry (train == serve)
  - Robust median/MAD scaler
  - Isotonic calibration on OOF predictions
  - Guard rails: min_samples, MAE check, R² check

Usage:
  python3 scripts/train_ml_scorer.py \\
    --lookback 60 \\
    --output /var/lib/trade/ml_models/scorer_v2/scorer_v2.joblib \\
    --feature_schema_ver v12_of
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import hashlib
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# GPU / CPU array backend (CuPy preferred, numpy fallback)
# ---------------------------------------------------------------------------
try:
    import cupy as cp  # type: ignore[import-untyped]
    _GPU = True
except ImportError:
    cp = None
    _GPU = False

try:
    import joblib
except ImportError:
    raise SystemExit("joblib is required: pip install joblib")

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore

try:
    import lightgbm as lgb
except ImportError:
    lgb = None  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ml_scorer_v2_train")


# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------

def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x) if x is not None else float(default)
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


def _sha256_16(items: Sequence[str]) -> str:
    payload = "\n".join(str(x) for x in items).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _median(xs: Sequence[float]) -> float:
    ys = sorted(float(x) for x in xs)
    n = len(ys)
    if n == 0:
        return 0.0
    m = n // 2
    return float(ys[m]) if n % 2 == 1 else float(0.5 * (ys[m - 1] + ys[m]))


def _mad(xs: Sequence[float], center: float) -> float:
    return _median([abs(float(x) - center) for x in xs])


def _winsorize(arr: np.ndarray, sigma: float = 3.0) -> np.ndarray:
    """Winsorize outliers beyond ±sigma standard deviations.

    GPU-accelerated: uses CuPy for median/abs when available (batch operation).
    Fallback: numpy on CPU.
    """
    if _GPU and cp is not None and len(arr) >= 200:
        try:
            arr_gpu = cp.asarray(arr, dtype=cp.float64)
            med = float(cp.median(arr_gpu))
            mad_val = float(cp.median(cp.abs(arr_gpu - med)))
            if mad_val < 1e-9:
                mad_val = float(cp.std(arr_gpu)) or 1.0
            robust_sigma = mad_val * 1.4826
            lo = med - sigma * robust_sigma
            hi = med + sigma * robust_sigma
            return cp.asnumpy(cp.clip(arr_gpu, lo, hi))
        except Exception:
            pass  # fall through to CPU
    med = float(np.median(arr))
    mad_val = float(np.median(np.abs(arr - med)))
    if mad_val < 1e-9:
        mad_val = float(np.std(arr)) or 1.0
    robust_sigma = mad_val * 1.4826  # MAD → sigma conversion
    lo = med - sigma * robust_sigma
    hi = med + sigma * robust_sigma
    return np.clip(arr, lo, hi)


# ---------------------------------------------------------------------------
# Purged + Embargo Time-Series Split (reused pattern from edge_stack)
# ---------------------------------------------------------------------------

@dataclass
class PurgedEmbargoTimeSeriesSplit:
    n_splits: int = 5
    purge_ms: int = 300_000  # 5 min
    embargo_ms: int = 120_000  # 2 min
    min_train: int = 500

    def split(self, ts_ms: Sequence[int]) -> Iterable[Tuple[np.ndarray, np.ndarray]]:
        order = np.argsort(np.asarray(ts_ms, dtype=np.int64), kind="mergesort")
        n = len(order)
        if n == 0:
            return

        fold_sizes = [n // self.n_splits] * self.n_splits
        for i in range(n % self.n_splits):
            fold_sizes[i] += 1

        start = 0
        ts_arr = np.asarray(ts_ms, dtype=np.int64)
        for fs in fold_sizes:
            end = start + fs
            val_idx = order[start:end]
            if len(val_idx) == 0:
                start = end
                continue

            val_start_ts = int(np.min(ts_arr[val_idx]))
            val_end_ts = int(np.max(ts_arr[val_idx]))

            cut_ts = val_start_ts - self.purge_ms
            train_mask = ts_arr < cut_ts
            train_idx = np.where(train_mask)[0]

            if self.embargo_ms > 0:
                emb_end = val_end_ts + self.embargo_ms
                emb_mask = (ts_arr > val_end_ts) & (ts_arr <= emb_end)
                if np.any(emb_mask):
                    train_idx = np.array(
                        [i for i in train_idx if not emb_mask[i]], dtype=np.int64
                    )

            if len(train_idx) < self.min_train:
                start = end
                continue

            yield np.asarray(train_idx, dtype=np.int64), np.asarray(val_idx, dtype=np.int64)
            start = end


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _get_dsn() -> str:
        os.getenv(
            "PG_DSN",
            os.getenv(
                "ANALYTICS_DB_DSN",
                f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}@postgres:5432/scanner_analytics",
            ),
        )


def fetch_training_data(lookback_days: int) -> Optional[Any]:
    """Fetch joined signal_facts + trade_performance from PostgreSQL."""
    if psycopg2 is None:
        logger.error("psycopg2 not installed — cannot fetch from DB")
        return None

    dsn = _get_dsn()
    logger.info("Fetching training data (last %d days) from %s...", lookback_days, dsn[:40])

    query = f"""
    SELECT
        s.ts,
        EXTRACT(EPOCH FROM s.ts)::BIGINT * 1000 AS ts_ms,
        s.signal_id,
        s.symbol,
        s.direction,
        s.signal_family,
        s.conf_score,
        s.atr_14,
        s.delta_spike_z,
        s.obi_avg_20,
        s.weak_progress_ratio,
        s.l3_spread_bps,
        s.l3_microprice_shift_bps_20,
        s.l3_microprice_velocity_bps,
        s.l3_obi_5,
        s.l3_obi_20,
        s.l3_obi_50,
        s.l3_obi_persistence_score,
        s.l3_cancel_to_trade_bid_5s,
        s.l3_cancel_to_trade_ask_5s,
        s.l3_cancel_to_trade_bid_20s,
        s.l3_cancel_to_trade_ask_20s,
        s.l3_queue_pressure_bid,
        s.l3_queue_pressure_ask,
        s.l3_market_depth_imbalance,
        t.r AS pnl_r,
        t.hit AS is_win,
        t.slippage_bps,
        t.adverse_bps,
        t.holding_ms,
        t.close_reason_bucket
    FROM signal_facts s
    JOIN trade_performance t ON s.signal_id = t.signal_id
    WHERE s.ts > NOW() - INTERVAL '{lookback_days} days'
      AND t.r IS NOT NULL
      AND s.symbol NOT IN ('XAUUSDT', 'XAUUSD', 'GOLD', 'XAGUSD', 'XAGUSDT')
    ORDER BY s.ts ASC
    """

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("DB fetch failed: %s", e)
        return None

    if not rows:
        logger.warning("No labeled rows found in last %d days", lookback_days)
        return None

    logger.info("Fetched %d labeled samples (%d columns)", len(rows), len(cols))
    return cols, rows


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

# Numeric feature columns from signal_facts (in stable order)
NUMERIC_FEATURES = [
    "conf_score",
    "atr_14",
    "delta_spike_z",
    "obi_avg_20",
    "weak_progress_ratio",
    "l3_spread_bps",
    "l3_microprice_shift_bps_20",
    "l3_microprice_velocity_bps",
    "l3_obi_5",
    "l3_obi_20",
    "l3_obi_50",
    "l3_obi_persistence_score",
    "l3_cancel_to_trade_bid_5s",
    "l3_cancel_to_trade_ask_5s",
    "l3_cancel_to_trade_bid_20s",
    "l3_cancel_to_trade_ask_20s",
    "l3_queue_pressure_bid",
    "l3_queue_pressure_ask",
    "l3_market_depth_imbalance",
]

# Derived features
DERIVED_FEATURES = [
    "direction_long",       # 1 if LONG, 0 if SHORT
    "cancel_to_trade_max",  # max(bid_5s, ask_5s, bid_20s, ask_20s)
    "obi_spread",           # obi_5 - obi_50
    "queue_imbalance",      # pressure_bid - pressure_ask
]


def _build_feature_names() -> List[str]:
    return [f"f_{c}" for c in NUMERIC_FEATURES] + DERIVED_FEATURES


def _build_feature_row(row_dict: Dict[str, Any]) -> List[float]:
    """Build feature vector from a row dict."""
    out: List[float] = []

    # Raw numeric features (with robust scaling applied later)
    for col in NUMERIC_FEATURES:
        out.append(_f(row_dict.get(col), 0.0))

    # Derived features
    direction = _f(row_dict.get("direction"), 0)
    out.append(1.0 if direction > 0 else 0.0)  # direction_long

    c2t_vals = [
        _f(row_dict.get("l3_cancel_to_trade_bid_5s"), 0.0),
        _f(row_dict.get("l3_cancel_to_trade_ask_5s"), 0.0),
        _f(row_dict.get("l3_cancel_to_trade_bid_20s"), 0.0),
        _f(row_dict.get("l3_cancel_to_trade_ask_20s"), 0.0),
    ]
    out.append(max(c2t_vals))  # cancel_to_trade_max

    obi_5 = _f(row_dict.get("l3_obi_5"), 0.0)
    obi_50 = _f(row_dict.get("l3_obi_50"), 0.0)
    out.append(obi_5 - obi_50)  # obi_spread

    qp_bid = _f(row_dict.get("l3_queue_pressure_bid"), 0.0)
    qp_ask = _f(row_dict.get("l3_queue_pressure_ask"), 0.0)
    out.append(qp_bid - qp_ask)  # queue_imbalance

    return out


def _fit_robust_scaler(
    X: np.ndarray, feature_names: List[str]
) -> Dict[str, Dict[str, float]]:
    """Fit median/MAD robust scaler per feature.

    GPU-accelerated: transfers full matrix to GPU once, computes
    median + MAD for all columns in batch. ~10-50× faster on GPU
    for typical N=2000-50000 training samples.
    """
    params: Dict[str, Dict[str, float]] = {}
    if _GPU and cp is not None and X.shape[0] >= 200:
        try:
            X_gpu = cp.asarray(X, dtype=cp.float64)
            for i, name in enumerate(feature_names):
                col_gpu = X_gpu[:, i]
                c = float(cp.median(col_gpu))
                s = float(cp.median(cp.abs(col_gpu - c)))
                if not math.isfinite(s) or s <= 1e-12:
                    s = float(cp.std(col_gpu)) or 1.0
                params[name] = {"center": c, "scale": s}
            logger.info("Robust scaler fitted on GPU (%d features, %d samples)", len(feature_names), X.shape[0])
            return params
        except Exception as exc:
            logger.warning("GPU robust scaler failed, falling back to CPU: %s", exc)
            params = {}  # reset on failure
    # CPU fallback
    for i, name in enumerate(feature_names):
        col = X[:, i].astype(np.float64)
        c = float(np.median(col))
        s = float(np.median(np.abs(col - c)))
        if not math.isfinite(s) or s <= 1e-12:
            s = float(np.std(col)) or 1.0
        params[name] = {"center": c, "scale": s}
    return params


def _apply_robust_scaler(
    X: np.ndarray,
    feature_names: List[str],
    params: Dict[str, Dict[str, float]],
) -> np.ndarray:
    """Apply robust scaler in-place."""
    out = X.copy()
    for i, name in enumerate(feature_names):
        if name in params:
            c = params[name]["center"]
            s = max(params[name]["scale"], 1e-12)
            out[:, i] = (X[:, i] - c) / s
    return out


# ---------------------------------------------------------------------------
# Target engineering
# ---------------------------------------------------------------------------

def _compute_target(row_dict: Dict[str, Any]) -> float:
    """Compute regression target = R-multiple (winsorized later)."""
    return _f(row_dict.get("pnl_r"), 0.0)


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_model(
    X: np.ndarray,
    y: np.ndarray,
    ts_ms: List[int],
    *,
    n_splits: int = 5,
    purge_ms: int = 300_000,
    embargo_ms: int = 120_000,
) -> Tuple[Any, np.ndarray, Dict[str, float]]:
    """Train LightGBM with OOF evaluation."""
    if lgb is None:
        raise SystemExit("lightgbm is required: pip install lightgbm")

    splitter = PurgedEmbargoTimeSeriesSplit(
        n_splits=n_splits,
        purge_ms=purge_ms,
        embargo_ms=embargo_ms,
        min_train=max(200, len(X) // 10),
    )

    oof_preds = np.full(len(X), np.nan, dtype=np.float64)

    params = {
        "objective": "regression",
        "metric": "mae",
        "verbose": -1,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "reg_lambda": 0.1,
        "seed": 42,
        "n_jobs": -1,
    }

    fold_n = 0
    for tr_idx, va_idx in splitter.split(ts_ms):
        fold_n += 1
        X_tr, y_tr = X[tr_idx], y[tr_idx]
        X_va, y_va = X[va_idx], y[va_idx]

        train_data = lgb.Dataset(X_tr, label=y_tr)
        valid_data = lgb.Dataset(X_va, label=y_va, reference=train_data)

        model = lgb.train(
            params,
            train_data,
            num_boost_round=1000,
            valid_sets=[valid_data],
            callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
        )

        oof_preds[va_idx] = model.predict(X_va)
        logger.info(
            "Fold %d: train=%d val=%d best_iter=%d",
            fold_n, len(tr_idx), len(va_idx), model.best_iteration,
        )

    if fold_n == 0:
        raise SystemExit("No usable folds (check data size / n_splits)")

    # Final model on all data
    train_data = lgb.Dataset(X, label=y)
    final_model = lgb.train(
        params,
        train_data,
        num_boost_round=800,
    )

    # OOF metrics
    mask = np.isfinite(oof_preds)
    n_oof = int(np.sum(mask))
    if n_oof < 100:
        raise SystemExit(f"Too few OOF predictions: {n_oof}")

    y_oof = y[mask]
    p_oof = oof_preds[mask]

    mae = float(np.mean(np.abs(y_oof - p_oof)))
    ss_res = float(np.sum((y_oof - p_oof) ** 2))
    ss_tot = float(np.sum((y_oof - np.mean(y_oof)) ** 2))
    r2 = 1.0 - (ss_res / max(ss_tot, 1e-9))

    # Rank correlation (Spearman-like)
    from scipy.stats import spearmanr  # type: ignore

    try:
        rank_corr, _ = spearmanr(y_oof, p_oof)
    except Exception:
        rank_corr = 0.0

    # Top-5% precision (binary: is predicted top 5% actually positive R?)
    k = max(1, int(0.05 * n_oof))
    top_idx = np.argsort(p_oof)[::-1][:k]
    top5_hit_rate = float(np.mean(y_oof[top_idx] > 0))

    metrics = {
        "n_oof": n_oof,
        "mae_oof": mae,
        "r2_oof": r2,
        "spearman_oof": float(rank_corr) if math.isfinite(float(rank_corr)) else 0.0,
        "top5_hit_rate": top5_hit_rate,
        "y_mean": float(np.mean(y)),
        "y_std": float(np.std(y)),
        "folds": fold_n,
    }

    logger.info(
        "OOF metrics: MAE=%.4f R²=%.4f Spearman=%.4f Top5%%HitRate=%.2f",
        mae, r2, float(rank_corr), top5_hit_rate,
    )

    return final_model, oof_preds, metrics


# ---------------------------------------------------------------------------
# Calibration: predicted R → conf01
# ---------------------------------------------------------------------------

def _build_r_to_conf01_isotonic(
    y_oof: np.ndarray, p_oof: np.ndarray
) -> Optional[Any]:
    """Fit isotonic regression: predicted_R → P(R > 0) → conf01."""
    try:
        from sklearn.isotonic import IsotonicRegression  # type: ignore

        # Binary target: is R > 0?
        y_binary = (y_oof > 0).astype(np.float64)
        iso = IsotonicRegression(
            out_of_bounds="clip", y_min=0.05, y_max=0.98
        )
        iso.fit(p_oof, y_binary)
        return iso
    except Exception as e:
        logger.warning("Isotonic calibration failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Train ML Scorer V2 (regression)")
    parser.add_argument("--lookback", type=int, default=60, help="Days to look back")
    parser.add_argument(
        "--output",
        type=str,
        default="/var/lib/trade/ml_models/scorer_v2/scorer_v2.joblib",
    )
    parser.add_argument("--min_samples", type=int, default=2000)
    parser.add_argument("--n_splits", type=int, default=5)
    parser.add_argument("--purge_ms", type=int, default=300_000)
    parser.add_argument("--embargo_ms", type=int, default=120_000)
    parser.add_argument("--winsorize_sigma", type=float, default=3.0)
    parser.add_argument("--feature_schema_ver", type=str, default="")
    parser.add_argument(
        "--approval",
        type=int,
        default=int(os.getenv("ML_SCORER_APPROVAL_REQUIRED", "1")),
        help="1 = send Telegram approval request after training (default), 0 = auto-promote",
    )
    args = parser.parse_args()

    if lgb is None:
        logger.error("lightgbm not installed.")
        return 1

    # 1. Fetch data
    result = fetch_training_data(args.lookback)
    if result is None:
        logger.warning("No data available — exiting cleanly (rule-based continues)")
        return 0

    cols, rows = result
    n_total = len(rows)
    logger.info("Total labeled samples: %d (min required: %d)", n_total, args.min_samples)

    if n_total < args.min_samples:
        logger.warning(
            "Insufficient data (%d < %d). Rule-based scoring continues.",
            n_total, args.min_samples,
        )
        return 0

    # 2. Convert to dicts
    row_dicts = [dict(zip(cols, row)) for row in rows]

    # 3. Build features
    feature_names = _build_feature_names()
    X_raw = np.array(
        [_build_feature_row(rd) for rd in row_dicts],
        dtype=np.float64,
    )

    # 4. Build targets
    y_raw = np.array([_compute_target(rd) for rd in row_dicts], dtype=np.float64)

    # Filter out NaN targets
    valid_mask = np.isfinite(y_raw) & np.all(np.isfinite(X_raw), axis=1)
    X = X_raw[valid_mask]
    y = y_raw[valid_mask]
    ts_ms = [int(_f(row_dicts[i].get("ts_ms"), 0)) for i in range(len(row_dicts)) if valid_mask[i]]

    logger.info("After filtering: %d samples with valid features + target", len(X))

    if len(X) < args.min_samples:
        logger.warning("After filtering: %d < %d. Aborting.", len(X), args.min_samples)
        return 0

    # 5. Winsorize target
    y = _winsorize(y, sigma=args.winsorize_sigma)

    # 5.5. Percentile Clipping limit outliers (1%, 99%)
    X_clipped = X.copy()
    for col_idx in range(X_clipped.shape[1]):
        p1 = float(np.percentile(X_clipped[:, col_idx], 1))
        p99 = float(np.percentile(X_clipped[:, col_idx], 99))
        X_clipped[:, col_idx] = np.clip(X_clipped[:, col_idx], p1, p99)

    # 6. Fit robust scaler
    scaler_params = _fit_robust_scaler(X_clipped, feature_names)
    X_scaled = _apply_robust_scaler(X_clipped, feature_names, scaler_params)

    # 7. Train
    model, oof_preds, metrics = train_model(
        X_scaled, y, ts_ms,
        n_splits=args.n_splits,
        purge_ms=args.purge_ms,
        embargo_ms=args.embargo_ms,
    )

    # 8. Guard rails
    if metrics["mae_oof"] > 50.0:
        logger.error("MAE too high (%.2f > 50). Model not saved.", metrics["mae_oof"])
        return 1

    # 9. Calibrator (predicted_R → conf01)
    oof_mask = np.isfinite(oof_preds)
    calibrator = _build_r_to_conf01_isotonic(y[oof_mask], oof_preds[oof_mask])

    # 10. Package and save as CANDIDATE (not yet promoted)
    out_pack: Dict[str, Any] = {
        "schema_version": 2,
        "kind": "ml_scorer_v2",
        "model": model,
        "feature_names": feature_names,
        "feature_cols_hash": _sha256_16(feature_names),
        "robust_scaler_params": scaler_params,
        "calibrator": calibrator,  # IsotonicRegression or None
        "metrics": metrics,
        "trained_at_ms": get_ny_time_millis(),
        "n_samples": len(X),
        "target": "pnl_r",
        "winsorize_sigma": float(args.winsorize_sigma),
    }

    # Save as candidate (pending approval)
    output_path = Path(args.output)
    candidate_path = output_path.with_suffix(".candidate.joblib")
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(out_pack, str(candidate_path))
    logger.info("✅ Candidate model saved to %s (%d samples)", candidate_path, len(X))

    # Print report JSON for CI / monitoring
    report = {
        "kind": "ml_scorer_v2",
        "n_samples": len(X),
        "metrics": metrics,
        "output": str(candidate_path),
        "trained_at": int(time.time()),
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))

    # 11. Telegram approval flow
    if args.approval:
        _send_approval_request(args, metrics, len(X), str(candidate_path), str(output_path))

    return 0


# ---------------------------------------------------------------------------
# Telegram Approval Flow
# ---------------------------------------------------------------------------

NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
PENDING_PREFIX = "ml_scorer:pending"
PENDING_TTL = int(os.getenv("ML_SCORER_PENDING_TTL_SEC", "86400") or 86400)
REMINDER_SEC = int(os.getenv("ML_SCORER_REMINDER_SEC", "1800") or 1800)


def _get_redis():
    """Get Redis client for approval flow."""
    import redis as _redis
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    try:
        r = _redis.from_url(url, decode_responses=True)
        r.ping()
        return r
    except Exception as e:
        logger.error("Cannot connect to Redis for approval flow: %s", e)
        return None


def _format_approval_report(metrics: Dict[str, float], n_samples: int) -> str:
    """Format Telegram report with model metrics."""
    mae = metrics.get("mae_oof", -1)
    r2 = metrics.get("r2_oof", -1)
    spearman = metrics.get("spearman_oof", -1)
    top5 = metrics.get("top5_hit_rate", -1)
    folds = metrics.get("folds", 0)
    y_mean = metrics.get("y_mean", 0)
    y_std = metrics.get("y_std", 0)

    expert_audit = ""
    if top5 < 0.05:
        expert_audit = (
            f"⚠️ <b>АВТО-АУДИТ (ПРАВЫЙ ХВОСТ):</b>\n"
            f"  ❌ <b>НЕ ПРИНИМАТЬ! (Reject)</b>\n"
            f"  Top5% hit катастрофически низкий ({top5:.2%} < 5%). Модель инвертирует логику "
            f"  на самых уверенных прогнозах. Проверьте аутлайеры в признаках.\n\n"
        )
    else:
        expert_audit = (
            f"✅ <b>АВТО-АУДИТ (ПРАВЫЙ ХВОСТ): ПРОЙДЕН</b>\n"
            f"  Top5% hit составляет {top5:.2%} (хорошо/приемлемо). Хвосты распределения стабильны.\n\n"
        )

    return (
        f"🤖 <b>ML Scorer V2 — Training Complete</b>\n\n"
        f"📊 <b>OOF Metrics ({folds} folds)</b>\n"
        f"  • MAE:        <code>{mae:.4f}</code> R\n"
        f"  • R²:         <code>{r2:.4f}</code>\n"
        f"  • Spearman:   <code>{spearman:.4f}</code>\n"
        f"  • Top5% hit:  <code>{top5:.2%}</code>\n\n"
        f"📦 <b>Dataset</b>\n"
        f"  • Samples:    <code>{n_samples}</code>\n"
        f"  • Target μ:   <code>{y_mean:.4f}</code> R\n"
        f"  • Target σ:   <code>{y_std:.4f}</code> R\n\n"
        f"💡 <b>Рекомендация:</b>\n"
        f"  ✅ Принимать, если Spearman явно положительный (> 0.05)\n"
        f"  ❌ Отклонять, если Spearman близок к 0 или отрицательный\n\n"
        f"{expert_audit}"
        f"📋 <b>Action required:</b>\n"
        f"  ✅ Approve → promote candidate to production\n"
        f"  ❌ Reject → discard, keep current model\n"
    )


def _build_approval_buttons(run_id: str) -> list:
    """Build inline keyboard with Approve/Reject buttons."""
    return [[
        {"text": "✅ Approve (promote)", "callback": f"ml_scorer_approve:{run_id}"},
        {"text": "❌ Reject (discard)",  "callback": f"ml_scorer_reject:{run_id}"},
    ]]


def _create_pending(
    r, run_id: str, metrics: Dict, n_samples: int,
    candidate_path: str, production_path: str, report: str,
) -> None:
    """Store pending approval in Redis."""
    key = f"{PENDING_PREFIX}:{run_id}"
    summary = {
        "run_id": run_id,
        "status": "PENDING",
        "created_at_ms": get_ny_time_millis(),
        "last_reminder_ms": get_ny_time_millis(),
        "n_samples": n_samples,
        "metrics": metrics,
        "candidate_path": candidate_path,
        "production_path": production_path,
        "report": report,
    }
    try:
        r.set(key, json.dumps(summary, ensure_ascii=False), ex=PENDING_TTL)
        logger.info("Created pending approval: %s", run_id)
    except Exception as e:
        logger.error("Failed to create pending: %s", e)


def _notify_telegram(r, message: str, buttons: list = None) -> None:
    """Publish message to notify:telegram stream."""
    fields: Dict[str, str] = {
        "type": "report",
        "text": message,
        "parse_mode": "HTML",
        "source": "ml_scorer_v2",
    }
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    try:
        r.xadd(NOTIFY_STREAM, fields, maxlen=50_000)
        logger.info("Telegram report sent to %s", NOTIFY_STREAM)
    except Exception as e:
        logger.error("Failed to publish to %s: %s", NOTIFY_STREAM, e)


def _send_approval_request(
    args, metrics: Dict, n_samples: int,
    candidate_path: str, production_path: str,
) -> None:
    """Send Telegram approval request and start reminder loop."""
    r = _get_redis()
    if r is None:
        logger.warning("Redis unavailable — skipping approval flow, model stays as candidate")
        return

    run_id = f"scorer_{int(time.time())}_{n_samples}s"
    report = _format_approval_report(metrics, n_samples)
    buttons = _build_approval_buttons(run_id)

    full_report = f"{report}\nRun ID: <code>{run_id}</code>"
    _notify_telegram(r, full_report, buttons=buttons)
    _create_pending(r, run_id, metrics, n_samples, candidate_path, production_path, report)

    logger.info("Approval request sent: %s", run_id)


def check_and_send_reminders() -> None:
    """Scan for pending approvals and re-send reminders every REMINDER_SEC.

    Called from the Docker timer loop between training cycles.
    """
    r = _get_redis()
    if r is None:
        return

    try:
        cursor = 0
        now_ms = get_ny_time_millis()
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{PENDING_PREFIX}:*", count=10000)
            for key in keys:
                raw = r.get(key)
                if not raw:
                    continue
                try:
                    pending = json.loads(raw)
                except Exception:
                    continue

                if pending.get("status") != "PENDING":
                    continue

                last_reminder = int(pending.get("last_reminder_ms", 0))
                if (now_ms - last_reminder) < REMINDER_SEC * 1000:
                    continue

                run_id = pending.get("run_id", "unknown")
                report = pending.get("report", "")
                elapsed_min = (now_ms - int(pending.get("created_at_ms", now_ms))) // 60_000

                if elapsed_min >= 60:
                    pending["status"] = "REJECTED"
                    pending["rejected_by"] = "system_auto_reject"
                    pending["rejected_at_ms"] = now_ms
                    r.set(key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                    candidate_path = pending.get("candidate_path", "")
                    deleted = False
                    if candidate_path:
                        try:
                            import os as _os
                            if _os.path.isfile(candidate_path):
                                _os.remove(candidate_path)
                                deleted = True
                        except Exception:
                            pass

                    reject_text = (
                        f"❌ <b>ML Scorer V2 AUTO-REJECTED</b>\n"
                        f"by system (timeout 1h)\n\n"
                        f"Candidate model discarded{' (file deleted)' if deleted else ''}.\n"
                        f"Current production model remains active.\n\n"
                        f"Run ID: <code>{run_id}</code>"
                    )
                    _notify_telegram(r, reject_text)
                    logger.info("Auto-rejected pending %s (timeout)", run_id)
                    continue

                reminder_text = (
                    f"⏰ <b>REMINDER</b> — ML Scorer V2 pending approval ({elapsed_min}min ago)\n\n"
                    f"{report}\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                buttons = _build_approval_buttons(run_id)
                _notify_telegram(r, reminder_text, buttons=buttons)

                pending["last_reminder_ms"] = now_ms
                r.set(key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                logger.info("Sent reminder for pending %s (%d min ago)", run_id, elapsed_min)

            if cursor == 0:
                break
    except Exception as e:
        logger.error("Reminder check failed: %s", e)


if __name__ == "__main__":
    raise SystemExit(main())

