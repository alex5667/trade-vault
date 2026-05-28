"""Train LGBM LONG-gate v1 (P2.A, 2026-05-27).

Goal
----
Bayesian classification head, оценивающий P(LONG_win | features) per signal,
интегрированный в EntryPolicyGate как дополнительный fail-CLOSED фильтр.

Cohort
------
  scanner_analytics.trades_closed
    WHERE direction='LONG' AND r_multiple IS NOT NULL
    AND exit_ts >= now() - interval '{COHORT_DAYS} days'
    AND COALESCE(is_virtual, FALSE) IN (TRUE, FALSE)  -- both, IPS-weighted later

Label: y = 1 if r_multiple > 0 else 0

Features (joined from config_json snapshot at entry):
  base:    entry_regime, vol_regime, kind, time_of_day_hour
  trend:   ema21_slope_15m, higher_low_30m, vwap_z_15m, ema21_gt_ema55
  breadth: market_breadth_ret_5m, cg_rel_strength_btc_1h,
           symbol_rel_strength_vs_btc_1m
  htf:     btc_ret_5m, btc_ret_1m, htf_proximity_*
  micro:   spread_bps, lob_obi_5

Pipeline
--------
1) fetch_cohort()  → DataFrame
2) feature_engineer()  → numeric + one-hot
3) train_test_split TIME-aware (last 7d test, rest train)
4) LGBMClassifier with class_weight='balanced'
5) IsotonicCalibrator on validation fold
6) write {model, calibrator, schema_version, train_meta} to
   /var/lib/trade/of_reports/models/lgbm_long_gate_v1_{ts}.joblib
7) promote: HSET cfg:lgbm_long_gate {path, p_min, run_id} (if PROMOTE=1)

Gates (preflight before promote):
  - n_train ≥ MIN_TRAIN (default 1000)
  - n_positives ≥ MIN_POS (default 200)
  - ROC-AUC (val) ≥ MIN_AUC (default 0.55)
  - ECE_cal ≤ MAX_ECE (default 0.05)
Если хоть один gate fail → промоут не происходит, exit code != 0.

ENV:
  LGBM_LONG_GATE_TRAINER_ENABLED   default 1
  LGBM_LONG_GATE_COHORT_DAYS       default 30
  LGBM_LONG_GATE_PG_DSN            fallback ANALYTICS_DB_DSN
  LGBM_LONG_GATE_MIN_TRAIN         default 1000
  LGBM_LONG_GATE_MIN_POS           default 200
  LGBM_LONG_GATE_MIN_AUC           default 0.55
  LGBM_LONG_GATE_MAX_ECE           default 0.05
  LGBM_LONG_GATE_MODEL_DIR         default /var/lib/trade/of_reports/models
  LGBM_LONG_GATE_PROMOTE           default 0 (preflight-only)
  LGBM_LONG_GATE_REDIS_URL         fallback REDIS_URL
  LGBM_LONG_GATE_CFG_KEY           default cfg:lgbm_long_gate
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

logger = logging.getLogger("train_lgbm_long_gate")


def _env_int(k: str, d: int) -> int:
    try:
        return int(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = os.environ.get(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def fetch_cohort(dsn: str, cohort_days: int) -> Any:
    """Returns pandas DataFrame; caller validates n_train/n_pos."""
    try:
        import pandas as pd  # type: ignore
        import psycopg2  # type: ignore
    except Exception as e:
        logger.error("missing pandas/psycopg2: %s", e)
        return None
    sql = f"""
    SELECT
        sid AS signal_id,
        direction,
        symbol,
        entry_tag        AS kind,
        entry_regime,
        config_json,
        r_multiple,
        EXTRACT(EPOCH FROM entry_ts) * 1000 AS opened_ms,
        EXTRACT(EPOCH FROM exit_ts)  * 1000 AS exit_ms,
        EXTRACT(HOUR FROM entry_ts AT TIME ZONE 'UTC') AS opened_hour_utc,
        COALESCE(is_virtual, FALSE) AS is_virtual
    FROM trades_closed
    WHERE direction = 'LONG'
      AND r_multiple IS NOT NULL
      AND exit_ts >= now() - interval '{int(cohort_days)} days'
    ORDER BY exit_ts
    """
    with psycopg2.connect(dsn) as conn:
        df = pd.read_sql(sql, conn)
    logger.info("fetched cohort: n=%d positives=%d", len(df), int((df["r_multiple"] > 0).sum()))
    return df


def feature_engineer(df: Any) -> tuple[Any, Any, list[str], Any]:
    """Returns (X, y, feature_names, sample_weights). NaN preserved."""
    import pandas as pd  # type: ignore
    import numpy as np  # type: ignore

    feat_keys = [
        "ema21_slope_15m", "higher_low_30m", "vwap_z_15m",
        "market_breadth_ret_5m", "cg_rel_strength_btc_1h",
        "symbol_rel_strength_vs_btc_1m",
        "btc_ret_5m", "btc_ret_1m", "btc_ret_1h",
        "spread_bps", "lob_obi_5",
        "delta_z", "confidence_pct",
        "vol_regime_code",  # numeric mapping if exists
    ]

    def _extract(row_json: Any, key: str) -> float:
        if row_json is None:
            return float("nan")
        try:
            if isinstance(row_json, (dict,)):
                cj = row_json
            elif isinstance(row_json, str):
                cj = json.loads(row_json)
            else:
                return float("nan")
            ind = cj.get("indicators") or cj
            v = ind.get(key)
            if v is None:
                return float("nan")
            return float(v)
        except Exception:
            return float("nan")

    X = pd.DataFrame()
    for k in feat_keys:
        X[k] = df["config_json"].apply(lambda j: _extract(j, k))

    X["opened_hour_utc"] = df["opened_hour_utc"].astype(float)
    # One-hot kind + entry_regime (compact)
    kinds = ["iceberg", "delta_spike", "absorption", "of", "ok"]
    for k in kinds:
        X[f"kind_{k}"] = (df["kind"].str.lower() == k).astype(int)
    regimes = ["trending_bull", "trending_bear", "range", "squeeze", "expansion", "mixed", "na"]
    for r in regimes:
        X[f"regime_{r}"] = (df["entry_regime"].astype(str).str.lower() == r).astype(int)

    # IPS weight: down-weight virtual rows
    w_virt = float(os.environ.get("LGBM_LONG_GATE_VIRTUAL_WEIGHT", "0.7") or 0.7)
    sample_w = np.where(df["is_virtual"].fillna(False).astype(bool), w_virt, 1.0)

    y = (df["r_multiple"] > 0).astype(int).values
    feature_names = list(X.columns)
    return X, y, feature_names, sample_w


def expected_calibration_error(probs: Any, labels: Any, n_bins: int = 10) -> float:
    import numpy as np  # type: ignore
    probs = np.asarray(probs)
    labels = np.asarray(labels)
    if len(probs) == 0:
        return 0.0
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        mask = (probs >= edges[i]) & (probs <= edges[i + 1] if i == n_bins - 1 else probs < edges[i + 1])
        if not mask.any():
            continue
        bin_conf = probs[mask].mean()
        bin_acc = labels[mask].mean()
        ece += (mask.sum() / len(probs)) * abs(bin_acc - bin_conf)
    return float(ece)


def train_and_calibrate(X_train, y_train, X_val, y_val, sample_w_train) -> dict[str, Any]:
    import lightgbm as lgb  # type: ignore
    from sklearn.isotonic import IsotonicRegression  # type: ignore
    from sklearn.metrics import roc_auc_score, brier_score_loss  # type: ignore

    model = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        min_data_in_leaf=20,
        reg_lambda=0.1,
        objective="binary",
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(X_train, y_train, sample_weight=sample_w_train)

    val_proba_raw = model.predict_proba(X_val)[:, 1]
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(val_proba_raw, y_val)
    val_proba_cal = iso.transform(val_proba_raw)

    auc_raw = float(roc_auc_score(y_val, val_proba_raw)) if len(set(y_val)) > 1 else 0.5
    brier_raw = float(brier_score_loss(y_val, val_proba_raw))
    brier_cal = float(brier_score_loss(y_val, val_proba_cal))
    ece_raw = expected_calibration_error(val_proba_raw, y_val)
    ece_cal = expected_calibration_error(val_proba_cal, y_val)

    base_wr = float(y_val.mean()) if len(y_val) else 0.0
    return {
        "model": model,
        "isotonic": iso,
        "metrics": {
            "auc_raw": auc_raw,
            "brier_raw": brier_raw,
            "brier_cal": brier_cal,
            "ece_raw": ece_raw,
            "ece_cal": ece_cal,
            "base_wr": base_wr,
            "n_train": int(len(X_train)),
            "n_val": int(len(X_val)),
            "n_pos_train": int(sum(y_train)),
            "n_pos_val": int(sum(y_val)),
        },
    }


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    if not _env_bool("LGBM_LONG_GATE_TRAINER_ENABLED", True):
        logger.info("trainer disabled — exit 0")
        return 0

    dsn = os.environ.get("LGBM_LONG_GATE_PG_DSN") or os.environ.get("ANALYTICS_DB_DSN")
    if not dsn:
        logger.error("ANALYTICS_DB_DSN missing — abort")
        return 2

    cohort_days = _env_int("LGBM_LONG_GATE_COHORT_DAYS", 30)
    min_train = _env_int("LGBM_LONG_GATE_MIN_TRAIN", 1000)
    min_pos = _env_int("LGBM_LONG_GATE_MIN_POS", 200)
    min_auc = _env_float("LGBM_LONG_GATE_MIN_AUC", 0.55)
    max_ece = _env_float("LGBM_LONG_GATE_MAX_ECE", 0.05)
    model_dir = os.environ.get("LGBM_LONG_GATE_MODEL_DIR", "/var/lib/trade/of_reports/models")
    promote = _env_bool("LGBM_LONG_GATE_PROMOTE", False)

    df = fetch_cohort(dsn, cohort_days)
    if df is None or len(df) < min_train:
        logger.error("cohort size %s < min_train=%d — abort", len(df) if df is not None else 0, min_train)
        return 3

    X, y, feature_names, sample_w = feature_engineer(df)
    # Time-aware split: last 25% as validation.
    n = len(X)
    split = int(n * 0.75)
    X_train, X_val = X.iloc[:split], X.iloc[split:]
    y_train, y_val = y[:split], y[split:]
    sw_train = sample_w[:split]

    n_pos = int(y_train.sum())
    if n_pos < min_pos:
        logger.error("n_pos_train=%d < min_pos=%d — abort", n_pos, min_pos)
        return 4

    result = train_and_calibrate(X_train, y_train, X_val, y_val, sw_train)
    m = result["metrics"]
    logger.info("metrics: %s", json.dumps(m, indent=2))

    # Preflight gates
    if m["auc_raw"] < min_auc:
        logger.error("AUC %.4f < %.4f — gate FAIL", m["auc_raw"], min_auc)
        return 5
    if m["ece_cal"] > max_ece:
        logger.error("ECE_cal %.4f > %.4f — gate FAIL", m["ece_cal"], max_ece)
        return 6

    # Persist
    os.makedirs(model_dir, exist_ok=True)
    ts = int(time.time())
    run_id = f"lgbm_long_gate_v1_{ts}"
    path = os.path.join(model_dir, f"{run_id}.joblib")
    try:
        import joblib  # type: ignore
        joblib.dump(
            {
                "model": result["model"],
                "isotonic": result["isotonic"],
                "feature_names": feature_names,
                "metrics": m,
                "schema_version": 1,
                "run_id": run_id,
                "created_ms": int(time.time() * 1000),
            },
            path,
            compress=3,
        )
        logger.info("✅ model written: %s", path)
    except Exception as e:
        logger.error("persist fail: %s", e)
        return 7

    if not promote:
        logger.info("PROMOTE=0 — preflight-only mode, skipping Redis publish")
        return 0

    # Publish to Redis
    try:
        import redis  # type: ignore
        url = os.environ.get("LGBM_LONG_GATE_REDIS_URL") or os.environ.get("REDIS_URL") or "redis://redis-worker-1:6379/0"
        rc = redis.from_url(url, decode_responses=True, socket_timeout=2.0)
        cfg_key = os.environ.get("LGBM_LONG_GATE_CFG_KEY", "cfg:lgbm_long_gate")
        snapshot = {
            "schema_version": 1,
            "run_id": run_id,
            "model_path": path,
            "p_min": float(os.environ.get("LGBM_LONG_GATE_P_MIN", "0.50") or 0.50),
            "feature_names": feature_names,
            "metrics": m,
            "promoted_ms": int(time.time() * 1000),
            "mode": os.environ.get("LGBM_LONG_GATE_MODE", "SHADOW"),
        }
        rc.set(cfg_key, json.dumps(snapshot))
        logger.info("✅ promoted: HSET %s run_id=%s", cfg_key, run_id)
    except Exception as e:
        logger.error("redis promote fail: %s", e)
        return 8

    return 0


if __name__ == "__main__":
    sys.exit(main())
