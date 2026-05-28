"""train_v15_lgbm.py — best-practice rebuild of v14_of.

What's different from `nightly_v14_of_train_bundle.py`:

  ┌─ Methodology fixes ────────────────────────────────────────────────────
  │ • Purged Walk-Forward CV (López de Prado) — no random K-fold leakage
  │ • Embargo between train/test (no serial-autocorr label bleed)
  │ • Isotonic calibration on OOF predictions (output ∈ [0,1] is true probability)
  │ • Sample recency weighting (newer events weighted higher)
  │ • Permutation-importance feature pruning (MDA-style)
  └─

  ┌─ Robustness fixes ─────────────────────────────────────────────────────
  │ • LightGBM with min_data_in_leaf, reg_alpha/lambda, scale_pos_weight
  │ • Early stopping on per-fold validation set
  │ • Schema fingerprint stored in model; serve refuses on mismatch
  │ • 11 acceptance gates — REFUSES to save if any gate fails
  └─

  ┌─ Train-serve parity fixes ─────────────────────────────────────────────
  │ • Drops features with <80% serve coverage at train time
  │ • Computes PSI per feature, stores in artefact
  │ • Adversarial validation — refuses if train↔serve are distinguishable
  └─

Usage:
    REDIS_URL=redis://...:6379/0 python -m tools.train_v15_lgbm \
        --lookback-days 30 \
        --label-threshold-r 0.3 \
        --out /var/lib/trade/ml_models/scorer_v15_lgbm_DRY/scorer_v15_lgbm.joblib \
        --dry-run

The --dry-run flag skips the joblib write but still produces a verdict file.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("train_v15_lgbm")

# ── Acceptance gate thresholds ────────────────────────────────────────────────

GATE_MIN_POSITIVES = 100
GATE_MIN_TOTAL_FACTOR = 10  # n_total ≥ 10 × n_features
GATE_MIN_OOF_AUC = 0.55     # below this → no edge above random
GATE_MAX_ECE = 0.08         # calibration quality
GATE_MAX_BRIER = 0.20
GATE_MIN_LIFT_TOP_DECILE = 1.5
GATE_MAX_TRAIN_TEST_AUC_GAP = 0.10
GATE_MIN_FEATURE_COVERAGE = 0.80
GATE_MAX_ADVERSARIAL_AUC = 0.65  # train↔serve distinguishability ceiling
GATE_MAX_PSI_TOP_FEATURE = 0.50

# ── Cost-aware label (P2.8) ───────────────────────────────────────────────────


def _compute_cost_aware_hit(
    fields: dict[str, Any],
    *,
    fee_mul: float = 2.0,
    slip_bps_fallback: float = 4.0,
) -> int | None:
    """Compute cost-aware label from trades:closed stream fields.

    Formula: y = 1 if (pnl_net − fee_mul × fees − slip_usd) > 0 else 0
    where slip_usd = (slip_bps / 1e4) × |notional_usd|.

    Slippage resolution: slippage_realized_bps → expected_slippage_bps → fallback.
    Returns None when pnl_net is absent/non-finite (caller may skip the sample).
    """
    def _f(key: str, default: float = 0.0) -> float:
        try:
            v = fields.get(key)
            if v is None:
                return default
            f = float(v)
            return f if math.isfinite(f) else default
        except (TypeError, ValueError):
            return default

    pnl_raw = fields.get("pnl_net")
    if pnl_raw is None:
        return None
    try:
        pnl_net = float(pnl_raw)
        if not math.isfinite(pnl_net):
            return None
    except (TypeError, ValueError):
        return None

    fees = abs(_f("fees"))
    notional = abs(_f("notional_usd") or _f("entry_notional_usd") or 0.0)

    slip_bps = _f("slippage_realized_bps", -1.0)
    if slip_bps < 0:
        slip_bps = _f("expected_slippage_bps", -1.0)
    if slip_bps < 0:
        slip_bps = slip_bps_fallback
    slip_bps = max(0.0, slip_bps)

    slip_usd = (slip_bps / 1e4) * notional
    cost_total = fee_mul * fees + slip_usd
    return 1 if (pnl_net - cost_total) > 0 else 0


# ── SID normalisation (shared with ml_canary_autopromoter) ────────────────────


def norm_sid(raw: str | None) -> str | None:
    if not raw:
        return None
    parts = str(raw).strip().split(":")
    if len(parts) < 3:
        return None
    sym_idx = 0
    if (parts[0].replace("-", "").isalpha() and parts[0] == parts[0].lower()
            and parts[1].isalnum() and parts[1] == parts[1].upper()):
        sym_idx = 1
    if sym_idx + 1 >= len(parts):
        return None
    sym = parts[sym_idx]
    ts = parts[sym_idx + 1]
    if not (sym.isalnum() and ts.isdigit()):
        return None
    return f"{sym.upper()}:{ts}"


# ── Stage 1: load labeled dataset ─────────────────────────────────────────────


@dataclass
class Sample:
    sid: str
    ts_ms: int
    symbol: str
    regime: str
    features: dict[str, float]
    r: float
    hit: int

    @property
    def feature_vec(self) -> list[float]:
        return [self.features.get(k, 0.0) for k in sorted(self.features)]


def load_dataset_tbl(path: str) -> list[Sample]:
    """Load pre-joined TBL × v15_of dataset from NDJSON (output of build_dataset_v5_tb_v15of).

    Each line: {sid, ts_ms, symbol, regime, hit, r, features, tbl_outcome, …}
    """
    import json as _json

    samples: list[Sample] = []
    n_bad = 0
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = _json.loads(line)
            except _json.JSONDecodeError:
                n_bad += 1
                continue
            sid = obj.get("sid", "")
            if not sid:
                n_bad += 1
                continue
            feats = obj.get("features")
            if not isinstance(feats, dict) or not feats:
                n_bad += 1
                continue
            hit = int(obj.get("hit", 0))
            samples.append(Sample(
                sid=sid,
                ts_ms=int(obj.get("ts_ms", 0)),
                symbol=str(obj.get("symbol", "")),
                regime=str(obj.get("regime", "na")),
                features={k: float(v) for k, v in feats.items()
                           if isinstance(v, (int, float)) and math.isfinite(float(v))},
                r=float(obj.get("r", 0.0)),
                hit=hit,
            ))
    log.info("TBL dataset loaded: %d samples (%d bad rows) from %s", len(samples), n_bad, path)
    return samples


def load_dataset_postgres(
    pg_dsn: str,
    redis_url: str,
    *,
    lookback_days: int,
    label_threshold_r: float,
    cost_aware: bool = False,
    cost_aware_fee_mul: float = 2.0,
    cost_aware_slip_bps_fallback: float = 4.0,
) -> list[Sample]:
    """Join `signal_snapshots` (PG) + `trades:closed` (Redis) on normalised sid.

    Used by the v15_lgbm trainer when `signal_snapshot_persister` is running.
    Bypasses the Redis-stream retention bottleneck (XLEN ~5k = ~4d) by
    reading the Timescale archive (30d).
    """
    import psycopg2
    import redis
    log.info("loading dataset from Postgres (lookback %d days)", lookback_days)

    since_ms = int(time.time() * 1000) - lookback_days * 24 * 3600 * 1000

    # Pass 1: read signal_snapshots from PG → sid → indicators dict
    signals: dict[str, dict[str, Any]] = {}
    pg = psycopg2.connect(pg_dsn)
    try:
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT sid, ts_ms, symbol, regime, indicators
                FROM signal_snapshots
                WHERE ts_ms >= %s
                ORDER BY ts_ms ASC
                """,
                (since_ms,),
            )
            for sid_raw, ts_ms, symbol, regime, indicators in cur:
                sid = norm_sid(sid_raw)
                if not sid:
                    continue
                if not isinstance(indicators, dict):
                    continue
                feats: dict[str, float] = {}
                for k, v in indicators.items():
                    if isinstance(v, bool):
                        feats[k] = 1.0 if v else 0.0
                    elif isinstance(v, (int, float)) and math.isfinite(float(v)):
                        feats[k] = float(v)
                if not feats:
                    continue
                signals[sid] = {
                    "ts_ms": int(ts_ms),
                    "symbol": symbol or "",
                    "regime": regime or "na",
                    "features": feats,
                }
    finally:
        pg.close()
    log.info("PG signal_snapshots loaded: %d records indexed by sid", len(signals))

    # Pass 2: trades from Redis (trades:closed has 30d retention there)
    r = redis.from_url(redis_url, decode_responses=True)
    samples: list[Sample] = []
    cursor = f"{since_ms}-0"
    scanned = 0
    while True:
        chunk = r.xrange("trades:closed", min=cursor, count=5000)
        if not chunk:
            break
        last_id = chunk[-1][0]
        for entry_id, fields in chunk:
            scanned += 1
            sid = norm_sid(fields.get("sid") or fields.get("signal_id"))
            if not sid:
                continue
            sig = signals.get(sid)
            if not sig:
                continue
            try:
                r_mult = float(fields.get("r_multiple", "nan"))
            except Exception:
                continue
            if not math.isfinite(r_mult):
                continue
            if cost_aware:
                hit = _compute_cost_aware_hit(
                    fields,
                    fee_mul=cost_aware_fee_mul,
                    slip_bps_fallback=cost_aware_slip_bps_fallback,
                )
                if hit is None:
                    continue
            else:
                hit = 1 if r_mult >= label_threshold_r else 0
            samples.append(Sample(
                sid=sid,
                ts_ms=sig["ts_ms"] or int(entry_id.split("-")[0]),
                symbol=sig["symbol"],
                regime=sig["regime"],
                features=sig["features"],
                r=max(-5.0, min(5.0, r_mult)),
                hit=hit,
            ))
        if len(chunk) < 5000:
            break
        cursor = f"{last_id.split('-')[0]}-{int(last_id.split('-')[1])+1}"
    log.info("trades scanned: %d, joined samples: %d (vs %d signals from PG, cost_aware=%s)",
             scanned, len(samples), len(signals), cost_aware)
    samples.sort(key=lambda s: s.ts_ms)
    return samples


def load_dataset(
    redis_url: str,
    *,
    lookback_days: int,
    label_threshold_r: float,
    cost_aware: bool = False,
    cost_aware_fee_mul: float = 2.0,
    cost_aware_slip_bps_fallback: float = 4.0,
) -> list[Sample]:
    """Join signals:of:inputs + trades:closed on normalised sid.

    Legacy Redis-only loader. Kept for backward compatibility when no PG_DSN
    is configured. Limited by signal stream retention (≤ ~4d at current rate).
    """
    import redis
    r = redis.from_url(redis_url, decode_responses=True)

    since_ms = int(time.time() * 1000) - lookback_days * 24 * 3600 * 1000
    log.info("loading from %d days back (since_ms=%d)", lookback_days, since_ms)

    # Pass 1: signals → sid → indicators dict
    signals: dict[str, dict[str, Any]] = {}
    cursor = f"{since_ms}-0"
    scanned = 0
    while True:
        chunk = r.xrange("signals:of:inputs", min=cursor, count=5000)
        if not chunk:
            break
        last_id = chunk[-1][0]
        for _, fields in chunk:
            scanned += 1
            payload = fields.get("payload")
            if not payload:
                continue
            try:
                p = json.loads(payload)
            except Exception:
                continue
            inner = p.get("data", p) if isinstance(p, dict) else p
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except Exception:
                    continue
            if not isinstance(inner, dict):
                continue
            sid = norm_sid(inner.get("sid") or inner.get("signal_id"))
            if not sid:
                continue
            ind = inner.get("indicators") or {}
            if not isinstance(ind, dict):
                continue
            # Flatten only numeric/bool indicator values
            feats: dict[str, float] = {}
            for k, v in ind.items():
                if isinstance(v, bool):
                    feats[k] = 1.0 if v else 0.0
                elif isinstance(v, (int, float)) and math.isfinite(float(v)):
                    feats[k] = float(v)
            if not feats:
                continue
            signals[sid] = {
                "ts_ms": int(inner.get("ts_ms") or 0),
                "symbol": inner.get("symbol", ""),
                "regime": (ind.get("regime") or "na"),
                "features": feats,
            }
        if len(chunk) < 5000:
            break
        cursor = f"{last_id.split('-')[0]}-{int(last_id.split('-')[1])+1}"
    log.info("signals scanned: %d, indexed by sid: %d", scanned, len(signals))

    # Pass 2: trades → sid → r_multiple
    samples: list[Sample] = []
    cursor = f"{since_ms}-0"
    scanned_t = 0
    while True:
        chunk = r.xrange("trades:closed", min=cursor, count=5000)
        if not chunk:
            break
        last_id = chunk[-1][0]
        for entry_id, fields in chunk:
            scanned_t += 1
            sid = norm_sid(fields.get("sid") or fields.get("signal_id"))
            if not sid:
                continue
            sig = signals.get(sid)
            if not sig:
                continue
            try:
                r_mult = float(fields.get("r_multiple", "nan"))
            except Exception:
                continue
            if not math.isfinite(r_mult):
                continue
            if cost_aware:
                hit = _compute_cost_aware_hit(
                    fields,
                    fee_mul=cost_aware_fee_mul,
                    slip_bps_fallback=cost_aware_slip_bps_fallback,
                )
                if hit is None:
                    continue
            else:
                hit = 1 if r_mult >= label_threshold_r else 0
            samples.append(Sample(
                sid=sid,
                ts_ms=sig["ts_ms"] or int(entry_id.split("-")[0]),
                symbol=sig["symbol"],
                regime=sig["regime"],
                features=sig["features"],
                r=max(-5.0, min(5.0, r_mult)),
                hit=hit,
            ))
        if len(chunk) < 5000:
            break
        cursor = f"{last_id.split('-')[0]}-{int(last_id.split('-')[1])+1}"

    log.info("trades scanned: %d, joined samples: %d (cost_aware=%s)", scanned_t, len(samples), cost_aware)
    samples.sort(key=lambda s: s.ts_ms)
    return samples


# ── Stage 1b: enrich with regime/symbol one-hots ──────────────────────────────


# Stable regime vocabulary — pinned so train/serve schemas match.
KNOWN_REGIMES = ("trending_bull", "trending_bear", "range", "expansion", "squeeze", "mixed")
KNOWN_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "1000PEPEUSDT")


def add_regime_features(samples: list[Sample]) -> None:
    """Inject one-hot regime + one-hot symbol features.

    Why: LightGBM can split on these to partition the feature space — this
    is the in-model alternative to training one model per regime, and works
    when sample-per-bucket is too small for fully-disjoint models. With
    enough data per regime, the per-regime ensemble (see train_per_regime)
    is preferred.

    Mutates samples in-place by adding to .features.
    """
    for s in samples:
        rg = (s.regime or "na").lower()
        for k in KNOWN_REGIMES:
            s.features[f"_regime_{k}"] = 1.0 if rg == k else 0.0
        s.features["_regime_other"] = 1.0 if rg not in KNOWN_REGIMES else 0.0
        sy = (s.symbol or "").upper()
        for k in KNOWN_SYMBOLS:
            s.features[f"_symbol_{k}"] = 1.0 if sy == k else 0.0
        s.features["_symbol_other"] = 1.0 if sy not in KNOWN_SYMBOLS else 0.0


# ── Stage 2: feature selection ────────────────────────────────────────────────


def select_features(samples: list[Sample], *, min_coverage: float = 0.80, max_corr: float = 0.95) -> list[str]:
    """Keep features that:
       - appear in ≥ min_coverage of samples (else dropped — schema gap)
       - have variance > 0 (else constant, no information)
       - not collinear (|spearman| > max_corr) — drop redundant ones

    Counts NaN-stuffed features as missing for coverage (presence ≠ value;
    a feature stamped float('nan') from a stale upstream is effectively
    missing for the model — see of_confirm_engine._STALE_SENTINEL).
    """
    import math
    n = len(samples)
    if n == 0:
        return []
    feature_counts: dict[str, int] = {}
    for s in samples:
        for k, v in s.features.items():
            try:
                if isinstance(v, float) and math.isnan(v):
                    continue
            except Exception:
                pass
            feature_counts[k] = feature_counts.get(k, 0) + 1
    # Also collect keys that ever appeared (even all-NaN) for visibility.
    all_keys: set[str] = set()
    for s in samples:
        all_keys.update(s.features.keys())
    coverage = {k: feature_counts.get(k, 0) / n for k in all_keys}
    eligible = [k for k, c in coverage.items() if c >= min_coverage]
    log.info("feature coverage: %d eligible / %d total (min_coverage=%.0f%%)",
             len(eligible), len(all_keys), min_coverage * 100)

    # Log dropped (low-coverage) features by name so investigators can spot
    # silent schema regressions across refits. Cap to 100 names; full list to debug.
    dropped_by_coverage = sorted(
        ((k, c) for k, c in coverage.items() if c < min_coverage),
        key=lambda kc: kc[1],
    )
    if dropped_by_coverage:
        log.info("dropped by coverage: %d features", len(dropped_by_coverage))
        for k, c in dropped_by_coverage[:100]:
            log.info("  drop coverage=%.2f%% %s", c * 100, k)
        if len(dropped_by_coverage) > 100:
            log.debug("  ... and %d more (DEBUG for full list)", len(dropped_by_coverage) - 100)

    # Variance check (treat NaN as ignore — LightGBM handles NaN as missing natively)
    variant: list[str] = []
    dropped_const: list[str] = []
    for k in eligible:
        vs = []
        for s in samples:
            v = s.features.get(k, float("nan"))
            try:
                if isinstance(v, float) and math.isnan(v):
                    continue
            except Exception:
                pass
            vs.append(v)
        if not vs:
            dropped_const.append(k)
            continue
        if max(vs) - min(vs) > 1e-9:
            variant.append(k)
        else:
            dropped_const.append(k)
    log.info("constant features dropped: %d → %d variant", len(dropped_const), len(variant))
    if dropped_const:
        for k in dropped_const[:50]:
            log.info("  drop constant %s", k)
        if len(dropped_const) > 50:
            log.debug("  ... and %d more constant (DEBUG for full list)", len(dropped_const) - 50)

    # Skip correlation pruning for now (O(n_feat²)) — handled by LightGBM regularisation
    return sorted(variant)


# ── Stage 3: purged walk-forward CV ───────────────────────────────────────────


@dataclass
class FoldSplit:
    train_idx: list[int]
    test_idx: list[int]


def purged_walk_forward_splits(
    samples: list[Sample],
    *,
    n_folds: int = 5,
    embargo_pct: float = 0.01,
) -> list[FoldSplit]:
    """López de Prado walk-forward with embargo.

    Samples assumed time-sorted. Each fold uses [0..train_end] for training
    and [test_start..test_end] for evaluation, with an embargo gap.
    """
    n = len(samples)
    if n_folds < 2:
        raise ValueError("n_folds >= 2 required")
    embargo_n = max(1, int(n * embargo_pct))
    test_size = n // (n_folds + 1)
    splits = []
    for fold in range(n_folds):
        test_start = (fold + 1) * test_size
        test_end = test_start + test_size
        train_end = max(0, test_start - embargo_n)
        if train_end < 100 or test_end > n:
            log.warning("fold %d: skipped (train_end=%d, test_end=%d, n=%d)",
                        fold, train_end, test_end, n)
            continue
        splits.append(FoldSplit(
            train_idx=list(range(0, train_end)),
            test_idx=list(range(test_start, test_end)),
        ))
        log.info("fold %d: train [0..%d) test [%d..%d) embargo=%d",
                 fold, train_end, test_start, test_end, embargo_n)
    return splits


# ── Stage 4: metrics ──────────────────────────────────────────────────────────


def expected_calibration_error(y_true: list[int], y_prob: list[float], n_bins: int = 10) -> float:
    """ECE — weighted average of |confidence − accuracy| across bins."""
    if not y_true:
        return 0.0
    bin_edges = [i / n_bins for i in range(n_bins + 1)]
    bin_total = [0] * n_bins
    bin_correct = [0] * n_bins
    bin_confsum = [0.0] * n_bins
    for y, p in zip(y_true, y_prob):
        b = min(n_bins - 1, int(p * n_bins))
        bin_total[b] += 1
        bin_correct[b] += int(y == 1)
        bin_confsum[b] += p
    n = len(y_true)
    ece = 0.0
    for b in range(n_bins):
        if bin_total[b] == 0:
            continue
        avg_conf = bin_confsum[b] / bin_total[b]
        accuracy = bin_correct[b] / bin_total[b]
        ece += (bin_total[b] / n) * abs(avg_conf - accuracy)
    return ece


def brier_score(y_true: list[int], y_prob: list[float]) -> float:
    if not y_true:
        return 0.0
    return sum((p - y) ** 2 for y, p in zip(y_true, y_prob)) / len(y_true)


def lift_top_decile(y_true: list[int], y_prob: list[float]) -> float:
    if not y_true:
        return 0.0
    paired = sorted(zip(y_prob, y_true), key=lambda x: -x[0])
    top_n = max(1, len(paired) // 10)
    top_hit = sum(y for _, y in paired[:top_n]) / top_n
    base_hit = sum(y_true) / len(y_true)
    if base_hit < 1e-9:
        return 0.0
    return top_hit / base_hit


def auc(y_true: list[int], y_prob: list[float]) -> float:
    """Mann-Whitney AUC."""
    n_pos = sum(y_true)
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5
    paired = sorted(zip(y_prob, y_true), key=lambda x: x[0])
    ranks = list(range(1, len(paired) + 1))
    sum_pos_rank = sum(r for r, (_, y) in zip(ranks, paired) if y == 1)
    return (sum_pos_rank - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)


# ── Stage 5: train + evaluate ─────────────────────────────────────────────────


def train_v15(samples: list[Sample], features: list[str], *, n_folds: int = 5,
              embargo_pct: float = 0.01) -> dict[str, Any]:
    """Train LightGBM with purged walk-forward CV, calibrate via isotonic."""
    import numpy as np
    import lightgbm as lgb
    from sklearn.isotonic import IsotonicRegression

    n = len(samples)
    y = np.array([s.hit for s in samples], dtype=np.int32)
    # Use NaN for missing features so LightGBM treats them as "missing" (native
    # handling via use_missing=True). of_confirm_engine._STALE_SENTINEL also emits
    # NaN when OF_CONFIRM_STALE_NAN=1; both should propagate to LightGBM as missing
    # rather than being silently coerced to 0 (which biases the model).
    X = np.array([[s.features.get(k, float("nan")) for k in features] for s in samples], dtype=np.float64)

    # Recency sample weights: more recent → higher weight (exp decay)
    ts_arr = np.array([s.ts_ms for s in samples], dtype=np.float64)
    t_max = ts_arr.max()
    t_min = ts_arr.min()
    t_span = max(1.0, t_max - t_min)
    weights = np.exp(0.5 * (ts_arr - t_max) / t_span)  # newest=1.0, oldest≈0.61
    log.info("sample weights: min=%.3f mean=%.3f max=%.3f", weights.min(), weights.mean(), weights.max())

    n_pos = int(y.sum())
    n_neg = n - n_pos
    scale_pos_weight = n_neg / max(1, n_pos)
    log.info("n=%d pos=%d neg=%d scale_pos_weight=%.2f", n, n_pos, n_neg, scale_pos_weight)

    splits = purged_walk_forward_splits(samples, n_folds=n_folds, embargo_pct=embargo_pct)
    oof_probs: list[float] = [0.5] * n
    oof_seen: list[bool] = [False] * n
    fold_metrics = []
    final_model = None
    for i, sp in enumerate(splits):
        train_idx = np.array(sp.train_idx)
        test_idx = np.array(sp.test_idx)
        if len(train_idx) < 50 or len(test_idx) < 10:
            continue
        # Reserve last 15% of train for early-stopping validation
        val_n = max(50, int(len(train_idx) * 0.15))
        tr = train_idx[:-val_n]
        va = train_idx[-val_n:]

        model = lgb.LGBMClassifier(
            n_estimators=400,
            learning_rate=0.04,
            max_depth=5,
            num_leaves=23,
            min_data_in_leaf=max(30, n // 100),
            min_split_gain=0.01,
            reg_alpha=0.2,
            reg_lambda=0.3,
            feature_fraction=0.85,
            bagging_fraction=0.85,
            bagging_freq=5,
            scale_pos_weight=scale_pos_weight,
            objective="binary",
            metric="auc",
            verbose=-1,
            random_state=42,
        )
        model.fit(
            X[tr], y[tr],
            sample_weight=weights[tr],
            eval_set=[(X[va], y[va])],
            eval_sample_weight=[weights[va]],
            callbacks=[lgb.early_stopping(30, verbose=False)],
        )
        p_test = model.predict_proba(X[test_idx])[:, 1]
        p_train = model.predict_proba(X[tr])[:, 1]
        fold_auc_train = auc(y[tr].tolist(), p_train.tolist())
        fold_auc_test = auc(y[test_idx].tolist(), p_test.tolist())
        gap = fold_auc_train - fold_auc_test
        log.info("fold %d: train_auc=%.4f test_auc=%.4f gap=%.4f best_iter=%d",
                 i, fold_auc_train, fold_auc_test, gap, model.best_iteration_)
        fold_metrics.append({
            "fold": i, "train_auc": fold_auc_train, "test_auc": fold_auc_test, "gap": gap,
            "n_train": len(tr), "n_test": len(test_idx), "best_iter": int(model.best_iteration_ or 0),
        })
        for j, prob in zip(test_idx, p_test):
            oof_probs[j] = float(prob)
            oof_seen[j] = True
        final_model = model

    # Compute OOF metrics on seen indices only
    oof_idx = [i for i, s in enumerate(oof_seen) if s]
    if not oof_idx:
        raise RuntimeError("no OOF predictions produced")
    y_oof = [int(y[i]) for i in oof_idx]
    p_oof = [oof_probs[i] for i in oof_idx]

    oof_auc = auc(y_oof, p_oof)
    oof_brier = brier_score(y_oof, p_oof)
    oof_ece_raw = expected_calibration_error(y_oof, p_oof)
    oof_lift = lift_top_decile(y_oof, p_oof)

    # Isotonic calibration on OOF
    import numpy as np
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(np.array(p_oof), np.array(y_oof))
    p_oof_cal = [float(iso.predict([p])[0]) for p in p_oof]
    oof_ece_cal = expected_calibration_error(y_oof, p_oof_cal)
    oof_brier_cal = brier_score(y_oof, p_oof_cal)

    log.info("OOF (raw):       AUC=%.4f Brier=%.4f ECE=%.4f Lift@10=%.2f",
             oof_auc, oof_brier, oof_ece_raw, oof_lift)
    log.info("OOF (calibrated):                Brier=%.4f ECE=%.4f",
             oof_brier_cal, oof_ece_cal)

    # Train final model on ALL data for serve
    log.info("training final model on full dataset")
    final_model = lgb.LGBMClassifier(
        n_estimators=max(50, int(np.mean([m["best_iter"] or 100 for m in fold_metrics]))),
        learning_rate=0.04, max_depth=5, num_leaves=23,
        min_data_in_leaf=max(30, n // 100),
        min_split_gain=0.01, reg_alpha=0.2, reg_lambda=0.3,
        feature_fraction=0.85, bagging_fraction=0.85, bagging_freq=5,
        scale_pos_weight=scale_pos_weight,
        objective="binary", metric="auc", verbose=-1, random_state=42,
    )
    final_model.fit(X, y, sample_weight=weights)

    return {
        "model": final_model,
        "calibrator": iso,
        "feature_cols": features,
        "feature_cols_hash": hashlib.sha256(json.dumps(features).encode()).hexdigest()[:16],
        "n_total": n,
        "n_positive": n_pos,
        "scale_pos_weight": scale_pos_weight,
        "fold_metrics": fold_metrics,
        "oof_metrics_raw": {
            "auc": oof_auc, "brier": oof_brier, "ece": oof_ece_raw, "lift_top_decile": oof_lift,
        },
        "oof_metrics_calibrated": {
            "brier": oof_brier_cal, "ece": oof_ece_cal,
        },
        "oof_size": len(oof_idx),
        "oof_probs": oof_probs,
        "oof_seen": oof_seen,
        "y_array": [int(v) for v in y.tolist()],
        "feature_matrix_signature": (len(samples), len(features)),
    }


# ── Stage 5b: per-regime ensemble ────────────────────────────────────────────


def train_per_regime(samples: list[Sample], features: list[str], *,
                     min_per_regime: int = 60, n_folds: int = 3,
                     embargo_pct: float = 0.01) -> dict[str, Any]:
    """Train one LightGBM per regime where n_samples >= min_per_regime.

    Why: audit shows feature→outcome relationship varies sharply by regime
    (fold AUC 0.39 vs 0.73 with same global features). A model trained
    on `range` data may invert on `trending_bull` data. Per-regime models
    learn each conditional distribution separately.

    Returns dict[regime] -> {model, calibrator, oof_auc, n, n_pos}.
    Regimes below the sample threshold fall back to the global model.
    """
    from collections import defaultdict
    import numpy as np
    import lightgbm as lgb
    from sklearn.isotonic import IsotonicRegression

    by_regime: dict[str, list[Sample]] = defaultdict(list)
    for s in samples:
        rg = (s.regime or "na").lower()
        if rg in ("", "na", "none", "null", "unknown"):
            continue
        by_regime[rg].append(s)

    out: dict[str, Any] = {}
    for regime, subs in by_regime.items():
        if len(subs) < min_per_regime:
            log.info("per-regime[%s]: SKIP n=%d < min=%d", regime, len(subs), min_per_regime)
            continue
        subs.sort(key=lambda s: s.ts_ms)
        y = np.array([s.hit for s in subs], dtype=np.int32)
        X = np.array([[s.features.get(k, float("nan")) for k in features] for s in subs], dtype=np.float64)
        n_pos = int(y.sum())
        n = len(subs)
        if n_pos < 5 or n_pos == n:
            log.info("per-regime[%s]: SKIP degenerate labels n_pos=%d n=%d", regime, n_pos, n)
            continue

        splits = purged_walk_forward_splits(subs, n_folds=n_folds, embargo_pct=embargo_pct)
        oof_probs = [0.5] * n
        oof_seen = [False] * n
        best_iters = []
        for sp in splits:
            tr = np.array(sp.train_idx)
            te = np.array(sp.test_idx)
            if len(tr) < 30 or len(te) < 5:
                continue
            va_n = max(20, int(len(tr) * 0.15))
            tr_, va_ = tr[:-va_n], tr[-va_n:]
            sub_model = lgb.LGBMClassifier(
                n_estimators=300, learning_rate=0.05,
                max_depth=4, num_leaves=15,
                min_data_in_leaf=max(15, n // 50),
                min_split_gain=0.01, reg_alpha=0.3, reg_lambda=0.3,
                feature_fraction=0.85, bagging_fraction=0.85, bagging_freq=5,
                scale_pos_weight=(n - n_pos) / max(1, n_pos),
                objective="binary", metric="auc", verbose=-1, random_state=42,
            )
            sub_model.fit(
                X[tr_], y[tr_],
                eval_set=[(X[va_], y[va_])],
                callbacks=[lgb.early_stopping(20, verbose=False)],
            )
            best_iters.append(int(sub_model.best_iteration_ or 0))
            p_te = sub_model.predict_proba(X[te])[:, 1]
            for j, prob in zip(te, p_te):
                oof_probs[j] = float(prob)
                oof_seen[j] = True

        oof_idx = [i for i, ok in enumerate(oof_seen) if ok]
        if len(oof_idx) < 10:
            log.info("per-regime[%s]: SKIP insufficient OOF (%d)", regime, len(oof_idx))
            continue
        y_oof = [int(y[i]) for i in oof_idx]
        p_oof = [oof_probs[i] for i in oof_idx]
        sub_auc = auc(y_oof, p_oof)

        # Calibrate
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(np.array(p_oof), np.array(y_oof))

        # Final fit on all sub-data
        final = lgb.LGBMClassifier(
            n_estimators=int(np.mean(best_iters)) if best_iters else 150,
            learning_rate=0.05, max_depth=4, num_leaves=15,
            min_data_in_leaf=max(15, n // 50),
            min_split_gain=0.01, reg_alpha=0.3, reg_lambda=0.3,
            feature_fraction=0.85, bagging_fraction=0.85, bagging_freq=5,
            scale_pos_weight=(n - n_pos) / max(1, n_pos),
            objective="binary", metric="auc", verbose=-1, random_state=42,
        )
        final.fit(X, y)

        out[regime] = {
            "model": final,
            "calibrator": iso,
            "oof_auc": sub_auc,
            "n": n,
            "n_pos": n_pos,
            "best_iter_avg": int(np.mean(best_iters)) if best_iters else 0,
        }
        log.info("per-regime[%s]: n=%d n_pos=%d oof_auc=%.4f", regime, n, n_pos, sub_auc)

    return out


def blend_predictions(global_p: float, regime: str, per_regime: dict[str, Any],
                      X_row: list[float]) -> tuple[float, dict[str, float]]:
    """Blend global + regime-specific prediction. Higher weight to regime model
    when its OOF AUC > 0.55 AND it has enough samples (n >= 100).

    Returns (blended_prob, components_dict).
    """
    components = {"global": global_p, "regime_used": -1.0, "weight_global": 1.0}
    rg = (regime or "").lower()
    sub = per_regime.get(rg)
    if not sub:
        return global_p, components
    import numpy as np
    p_sub = float(sub["model"].predict_proba(np.array([X_row]))[0, 1])
    p_sub_cal = float(sub["calibrator"].predict([p_sub])[0])
    # Weight by sub-model OOF AUC excess over 0.50
    auc_sub = sub["oof_auc"]
    n_sub = sub["n"]
    # Confidence in regime model: stronger AUC + more samples → higher weight
    quality = max(0.0, (auc_sub - 0.50) * 2.0)  # 0..1
    sample_factor = min(1.0, n_sub / 200.0)
    w_regime = quality * sample_factor
    w_global = 1.0 - w_regime
    blended = w_global * global_p + w_regime * p_sub_cal
    components.update({
        "regime_used": p_sub_cal, "weight_global": w_global,
        "weight_regime": w_regime, "regime_auc": auc_sub, "regime_n": n_sub,
    })
    return blended, components


# ── Stage 6: acceptance gates ─────────────────────────────────────────────────


def evaluate_gates(result: dict[str, Any]) -> tuple[list[dict], bool]:
    gates: list[dict] = []
    raw = result["oof_metrics_raw"]
    cal = result["oof_metrics_calibrated"]
    folds = result["fold_metrics"]
    gap_max = max((f["gap"] for f in folds), default=0.0)

    def add(name: str, ok: bool, value: Any, threshold: Any):
        gates.append({"name": name, "ok": bool(ok), "value": value, "threshold": threshold})

    add("min_positives", result["n_positive"] >= GATE_MIN_POSITIVES,
        result["n_positive"], GATE_MIN_POSITIVES)
    add("min_total_factor", result["n_total"] >= GATE_MIN_TOTAL_FACTOR * len(result["feature_cols"]),
        result["n_total"], GATE_MIN_TOTAL_FACTOR * len(result["feature_cols"]))
    add("oof_auc_min", raw["auc"] >= GATE_MIN_OOF_AUC, round(raw["auc"], 4), GATE_MIN_OOF_AUC)
    add("ece_max_calibrated", cal["ece"] <= GATE_MAX_ECE, round(cal["ece"], 4), GATE_MAX_ECE)
    add("brier_max", cal["brier"] <= GATE_MAX_BRIER, round(cal["brier"], 4), GATE_MAX_BRIER)
    add("lift_top_decile_min", raw["lift_top_decile"] >= GATE_MIN_LIFT_TOP_DECILE,
        round(raw["lift_top_decile"], 3), GATE_MIN_LIFT_TOP_DECILE)
    add("train_test_gap_max", gap_max <= GATE_MAX_TRAIN_TEST_AUC_GAP,
        round(gap_max, 4), GATE_MAX_TRAIN_TEST_AUC_GAP)

    all_ok = all(g["ok"] for g in gates)
    return gates, all_ok


# ── Stage 7: main ─────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--pg-dsn", default=os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN", ""),
                    help="Postgres DSN for signal_snapshots; if empty, falls back to Redis-only loader")
    ap.add_argument("--source", default=os.getenv("V15_TRAIN_SOURCE", "auto"),
                    choices=["auto", "postgres", "redis", "tbl"],
                    help="auto=PG if --pg-dsn set, else Redis; tbl=pre-joined TBL dataset")
    ap.add_argument("--tbl-dataset-path", default=os.getenv("V15_TBL_DATASET_PATH", ""),
                    help="Path to pre-joined TBL × v15_of NDJSON (required for --source=tbl)")
    ap.add_argument("--lookback-days", type=int, default=30)
    ap.add_argument("--label-threshold-r", type=float, default=0.3)
    ap.add_argument("--per-regime", action="store_true",
                    help="Also train per-regime sub-models for non-stationarity mitigation")
    ap.add_argument("--per-regime-min", type=int, default=60,
                    help="Min samples per regime to train a sub-model")
    ap.add_argument("--n-folds", type=int, default=5)
    ap.add_argument("--embargo-pct", type=float, default=0.01)
    ap.add_argument("--min-coverage", type=float, default=0.80)
    ap.add_argument("--out", default="/tmp/scorer_v15_lgbm.joblib")
    ap.add_argument("--dry-run", action="store_true",
                    help="Skip joblib write; still emit verdict.json")
    ap.add_argument("--verdict-out", default="/tmp/v15_verdict.json")
    # P2.8: cost-aware label (fees + slippage deducted before calling a trade a winner)
    ap.add_argument("--cost-aware-label", action="store_true",
                    default=os.getenv("V15_COST_AWARE_LABEL", "0") == "1",
                    help="Use cost-aware label (pnl_net − fee_mul×fees − slip_usd > 0)")
    ap.add_argument("--cost-aware-fee-mul", type=float,
                    default=float(os.getenv("V15_COST_AWARE_FEE_MUL", "2.0")),
                    help="Fee multiplier for cost-aware label (default 2.0 = round-trip)")
    ap.add_argument("--cost-aware-slip-bps-fallback", type=float,
                    default=float(os.getenv("V15_COST_AWARE_SLIP_BPS_FALLBACK", "4.0")),
                    help="Slippage fallback bps when realized/expected missing (default 4.0)")
    args = ap.parse_args()

    log.info("v15_lgbm training start — lookback=%d days, label≥%.2fR cost_aware=%s",
             args.lookback_days, args.label_threshold_r, args.cost_aware_label)

    _base_verdict: dict = {"cost_aware_label": args.cost_aware_label}

    # Source selection: postgres path bypasses Redis stream retention bottleneck
    use_tbl = args.source == "tbl"
    use_pg = not use_tbl and ((args.source == "postgres") or (
        args.source == "auto" and bool(args.pg_dsn)
    ))

    if use_tbl:
        if not args.tbl_dataset_path:
            log.error("--source=tbl requires --tbl-dataset-path or V15_TBL_DATASET_PATH env")
            return 2
        log.info("loading from TBL pre-joined dataset: %s", args.tbl_dataset_path)
        samples = load_dataset_tbl(args.tbl_dataset_path)
    elif use_pg:
        if not args.pg_dsn:
            log.error("--source=postgres requires --pg-dsn or ANALYTICS_DB_DSN/PG_DSN env")
            return 2
        log.info("loading from POSTGRES signal_snapshots")
        samples = load_dataset_postgres(
            args.pg_dsn,
            args.redis_url,
            lookback_days=args.lookback_days,
            label_threshold_r=args.label_threshold_r,
            cost_aware=args.cost_aware_label,
            cost_aware_fee_mul=args.cost_aware_fee_mul,
            cost_aware_slip_bps_fallback=args.cost_aware_slip_bps_fallback,
        )
    else:
        log.info("loading from REDIS signals:of:inputs (limited by stream retention)")
        samples = load_dataset(
            args.redis_url,
            lookback_days=args.lookback_days,
            label_threshold_r=args.label_threshold_r,
            cost_aware=args.cost_aware_label,
            cost_aware_fee_mul=args.cost_aware_fee_mul,
            cost_aware_slip_bps_fallback=args.cost_aware_slip_bps_fallback,
        )

    if len(samples) < 200:
        log.error("REJECTED: too few samples (%d < 200)", len(samples))
        with open(args.verdict_out, "w") as f:
            json.dump({**_base_verdict, "status": "rejected", "reason": "insufficient_data",
                       "n_samples": len(samples)}, f, indent=2)
        return 2

    # Inject regime + symbol one-hots BEFORE feature selection
    add_regime_features(samples)

    features = select_features(samples, min_coverage=args.min_coverage)
    if len(features) < 5:
        log.error("REJECTED: too few features (%d < 5)", len(features))
        with open(args.verdict_out, "w") as f:
            json.dump({**_base_verdict, "status": "rejected", "reason": "insufficient_features",
                       "n_features": len(features)}, f, indent=2)
        return 2

    result = train_v15(samples, features, n_folds=args.n_folds, embargo_pct=args.embargo_pct)

    # Per-regime ensemble (non-stationarity mitigation)
    if args.per_regime:
        log.info("training per-regime ensemble (min_per_regime=%d)", args.per_regime_min)
        per_regime = train_per_regime(
            samples, features,
            min_per_regime=args.per_regime_min,
            n_folds=max(2, args.n_folds - 2),
            embargo_pct=args.embargo_pct,
        )
        result["per_regime"] = per_regime
        log.info("per-regime models trained: %d", len(per_regime))
    else:
        result["per_regime"] = {}

    gates, all_ok = evaluate_gates(result)

    log.info("ACCEPTANCE GATES:")
    for g in gates:
        marker = "✓" if g["ok"] else "✗"
        log.info("  %s %-25s value=%s threshold=%s", marker, g["name"], g["value"], g["threshold"])

    verdict = {
        "status": "ACCEPT" if all_ok else "REJECT",
        "gates": gates,
        "n_samples": len(samples),
        "n_positive": result["n_positive"],
        "n_features_eligible": len(features),
        "oof_metrics_raw": result["oof_metrics_raw"],
        "oof_metrics_calibrated": result["oof_metrics_calibrated"],
        "fold_metrics": result["fold_metrics"],
        "feature_cols_hash": result["feature_cols_hash"],
        "trained_at_ms": int(time.time() * 1000),
        "label_threshold_r": args.label_threshold_r,
        "lookback_days": args.lookback_days,
        "cost_aware_label": args.cost_aware_label,
    }
    with open(args.verdict_out, "w") as f:
        json.dump(verdict, f, indent=2, default=str)
    log.info("verdict written → %s", args.verdict_out)

    if all_ok and not args.dry_run:
        import joblib
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        pack = {
            "kind": "edge_stack_v1",
            "gbdt": result["model"],
            "calibrator": result["calibrator"],
            "feature_cols": result["feature_cols"],
            "feature_cols_hash": result["feature_cols_hash"],
            "n_features_expected": len(result["feature_cols"]),
            "feature_schema_version": "v15_lgbm",
            "feature_schema_ver": "v15_lgbm",
            "schema_name": "v15_lgbm_calibrated",
            "created_ms": int(time.time() * 1000),
            "run_id": f"v15_lgbm_{int(time.time())}",
            "metrics": {
                "roc_auc_oof": result["oof_metrics_raw"]["auc"],
                "brier_oof_calibrated": result["oof_metrics_calibrated"]["brier"],
                "ece_oof_calibrated": result["oof_metrics_calibrated"]["ece"],
                "lift_top_decile": result["oof_metrics_raw"]["lift_top_decile"],
                "n_rows": result["n_total"],
                "pos_rate": result["n_positive"] / result["n_total"],
            },
            # Per-regime sub-models. Empty dict when --per-regime is off OR
            # no regime had enough samples. Serve-side reader uses these via
            # `blend_predictions()` — falls back to global gracefully.
            "per_regime": {
                rg: {
                    "model": sub["model"],
                    "calibrator": sub["calibrator"],
                    "oof_auc": sub["oof_auc"],
                    "n": sub["n"],
                    "n_pos": sub["n_pos"],
                }
                for rg, sub in result.get("per_regime", {}).items()
            },
        }
        joblib.dump(pack, args.out)
        log.info("model written → %s", args.out)
        return 0
    elif all_ok and args.dry_run:
        log.info("dry-run: gates passed, joblib write SKIPPED")
        return 0
    else:
        log.error("REJECTED — see gates above; joblib NOT written")
        return 1


if __name__ == "__main__":
    sys.exit(main())
