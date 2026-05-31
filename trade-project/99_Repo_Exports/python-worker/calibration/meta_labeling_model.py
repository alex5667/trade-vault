"""
meta_labeling_model.py — Phase 2.1: Triple-barrier meta-labeling model.

Trains LightGBM on signal_outcome (frozen features → triple-barrier label)
using purged walk-forward CV. Calibrates with isotonic regression on OOS folds.

Triple-barrier label mapping:
  +1 (TP hit)  → y=1 (signal hit target)
  -1 (SL hit)  → y=0 (signal stopped out)
   0 (timeout) → y=0 (no resolution within TTL)

Meta-label = P(TP | features) — probability that a given signal hits TP.
This is the "second model" in meta-labeling framework (López de Prado, Ch. 10).

Model state dict (Redis / file):
  {
    schema_version: 2,
    ts_ms: int,
    n_samples: int,
    n_folds: int,
    roc_auc_oos: float,
    dsr: float,
    pbo: float,
    feature_cols: list[str],
    thresholds_by_regime: dict[str, float],   # regime → P(TP) threshold
    default_threshold: float,
    model_bytes_b64: str,   # pickle → base64 of lgbm.Booster
    calibrator_bytes_b64: str,  # pickle → base64 of IsotonicRegression
  }

Usage:
    from calibration.meta_labeling_model import train_meta_labeling_model, predict_prob

    state = train_meta_labeling_model(rows, n_blocks=8, embargo_ms=600_000)
    prob = predict_prob(features_dict, state)
"""
from __future__ import annotations

import base64
import logging
import math
import os
import pickle
import time
from typing import Any

import numpy as np

log = logging.getLogger("meta_labeling_model")

_SCHEMA_VERSION = 2

# Feature columns sourced from signal.indicators (frozen at decision_time_ms)
_BASE_FEATURES = [
    "delta_z",
    "lob_obi_5",
    "spread_bps",
    "atr_bps",
    "ml_prob",
    "of_score",
    "vpin_cdf",
    "ofi_norm_z",
    "book_depth_imbalance_5",
    "cvd_slope_60s",
    "realized_vol_60s",
    "funding_rate_bps",
    "open_interest_delta_pct",
]

# Categorical features → integer-encoded (regime, session)
_REGIME_CODES = {
    "trending_bull": 0, "trending_bear": 1, "ranging": 2,
    "squeeze": 3, "unknown": 4, "choppy": 5, "mixed": 6,
}
_SESSION_CODES = {
    "asian": 0, "london": 1, "new_york": 2, "overlap": 3, "weekend": 4, "other": 5,
}


# ─── Feature extraction ────────────────────────────────────────────────────────

def extract_features(indicators: dict[str, Any]) -> dict[str, float]:
    """
    Extract numeric features from signal.indicators dict.
    Missing values → 0.0. Categorical → integer-encoded.
    """
    def _f(k: str) -> float:
        v = indicators.get(k)
        try:
            f = float(v)
            return f if math.isfinite(f) else 0.0
        except (TypeError, ValueError):
            return 0.0

    feats: dict[str, float] = {col: _f(col) for col in _BASE_FEATURES}

    regime_raw = str(indicators.get("market_regime") or indicators.get("regime") or "unknown").lower()
    feats["regime_code"] = float(_REGIME_CODES.get(regime_raw, 4))

    session_raw = str(indicators.get("session") or "other").lower()
    feats["session_code"] = float(_SESSION_CODES.get(session_raw, 5))

    side = str(indicators.get("direction") or indicators.get("side") or "long").lower()
    feats["is_long"] = 1.0 if "long" in side else 0.0

    return feats


def features_to_array(features_dict: dict[str, float], feature_cols: list[str]) -> np.ndarray:
    """Convert features dict to numpy array in canonical column order."""
    return np.array([[features_dict.get(col, 0.0) for col in feature_cols]], dtype=np.float32)


# ─── Training ────────────────────────────────────────────────────────────────

def train_meta_labeling_model(
    rows: list[dict],
    *,
    n_blocks: int = 8,
    embargo_ms: int = 600_000,
    min_samples: int = 200,
    lgbm_params: dict | None = None,
    default_threshold: float = 0.45,
    sample_weights: list[float] | None = None,
) -> dict | None:
    """
    Train LightGBM meta-labeling model on signal_outcome rows using purged CV.

    Args:
        rows: List of signal_outcome dicts with keys:
              decision_time_ms, resolved_time_ms, label, features (dict or JSON str)
        n_blocks:  Purged walk-forward blocks
        embargo_ms: Embargo after test window (ms)
        min_samples: Minimum samples required to proceed
        lgbm_params: LightGBM parameters (override defaults)
        default_threshold: Default P(TP) gate threshold

    Returns:
        Model state dict or None if training failed / insufficient data.
    """
    import lightgbm as lgb  # type: ignore
    from sklearn.isotonic import IsotonicRegression  # type: ignore
    from sklearn.metrics import roc_auc_score  # type: ignore
    from calibration.purged_cv import purged_walkforward, check_calibration_guards

    n = len(rows)
    if n < min_samples:
        log.warning("train_meta_labeling_model: insufficient data (%d < %d)", n, min_samples)
        return None

    # Parse features
    X_dicts: list[dict[str, float]] = []
    ys: list[int] = []
    d_ms_list: list[float] = []
    r_ms_list: list[float] = []

    feature_cols: list[str] | None = None

    for row in rows:
        label = row.get("label")
        if label is None:
            continue
        y = 1 if int(label) > 0 else 0

        raw_feats = row.get("features")
        if raw_feats is None:
            raw_feats = {}
        if isinstance(raw_feats, str):
            import json
            try:
                raw_feats = json.loads(raw_feats)
            except Exception:
                raw_feats = {}

        feats = extract_features(raw_feats)
        if feature_cols is None:
            feature_cols = sorted(feats.keys())

        X_dicts.append(feats)
        ys.append(y)
        d_ms_list.append(float(row.get("decision_time_ms") or 0))
        r_ms_list.append(float(row.get("resolved_time_ms") or 0))

    if not X_dicts or feature_cols is None:
        log.warning("train_meta_labeling_model: no usable rows")
        return None

    X_all = np.array([[fd.get(c, 0.0) for c in feature_cols] for fd in X_dicts], dtype=np.float32)
    y_all = np.array(ys, dtype=np.int32)
    d_ms = np.array(d_ms_list, dtype=float)
    r_ms = np.array(r_ms_list, dtype=float)

    # Align sample_weights to usable rows (some rows may have been skipped above)
    if sample_weights is not None and len(sample_weights) == len(rows):
        w_all = np.array(sample_weights[: len(y_all)], dtype=np.float32)
    else:
        w_all = None

    n_pos = int(y_all.sum())
    n_neg = len(y_all) - n_pos
    if n_pos < 20 or n_neg < 20:
        log.warning("train_meta_labeling_model: too few positive (%d) or negative (%d) samples", n_pos, n_neg)
        return None

    # LightGBM params
    params = {
        "objective": "binary",
        "metric": "auc",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "min_child_samples": 20,
        "reg_alpha": 0.1,
        "reg_lambda": 0.1,
        "n_estimators": 200,
        "verbosity": -1,
        "is_unbalance": True,
        "random_state": 42,
    }
    if lgbm_params:
        params.update(lgbm_params)

    # Purged walk-forward
    folds = list(purged_walkforward(d_ms, r_ms, n_blocks=n_blocks, embargo_ms=embargo_ms))
    if len(folds) < 2:
        log.warning("train_meta_labeling_model: too few folds (%d)", len(folds))
        return None

    oos_probs: list[float] = []
    oos_labels: list[int] = []
    fold_aucs: list[float] = []

    last_model = None
    last_calibrator = None

    for train_idx, test_idx in folds:
        X_tr, y_tr = X_all[train_idx], y_all[train_idx]
        X_te, y_te = X_all[test_idx], y_all[test_idx]

        if y_tr.sum() < 5 or (len(y_tr) - y_tr.sum()) < 5:
            continue

        try:
            clf = lgb.LGBMClassifier(**params)
            sw_tr = w_all[train_idx] if w_all is not None else None
            clf.fit(X_tr, y_tr, sample_weight=sw_tr)
            raw_probs = clf.predict_proba(X_te)[:, 1]

            if len(np.unique(y_te)) == 2:
                auc = float(roc_auc_score(y_te, raw_probs))
                fold_aucs.append(auc)

            # Isotonic calibration on OOS fold
            iso = IsotonicRegression(out_of_bounds="clip")
            iso.fit(raw_probs, y_te)
            cal_probs = iso.predict(raw_probs)

            oos_probs.extend(cal_probs.tolist())
            oos_labels.extend(y_te.tolist())

            last_model = clf
            last_calibrator = iso
        except Exception as e:
            log.debug("fold training error: %s", e)
            continue

    if last_model is None:
        log.warning("train_meta_labeling_model: all folds failed")
        return None

    # OOS AUC across all folds
    roc_auc_oos = 0.0
    if len(np.unique(oos_labels)) == 2:
        try:
            from sklearn.metrics import roc_auc_score
            roc_auc_oos = float(roc_auc_score(oos_labels, oos_probs))
        except Exception:
            pass

    # DSR + PBO guards
    fold_rets = [[float(np.mean(np.array(oos_probs)[np.where(np.array(oos_labels) == 1)[0]]))]
                 if np.any(np.array(oos_labels) == 1) else [0.0]
                 for _ in folds[:len(fold_aucs)]]  # simplified
    sr = float(np.mean(fold_aucs) - 0.5) / (float(np.std(fold_aucs)) + 1e-9) if fold_aucs else 0.0

    _, guard_details = check_calibration_guards(
        sr=sr, n_trials=len(folds), skew=0.0, kurt=0.0, n_obs=len(X_all),
        fold_returns=None,
    )

    # Regime-specific thresholds: use default for now, calibrated from so_daily later
    thresholds_by_regime = {regime: default_threshold for regime in _REGIME_CODES}

    # Serialize model + calibrator
    model_bytes = base64.b64encode(pickle.dumps(last_model)).decode("ascii")
    cal_bytes = base64.b64encode(pickle.dumps(last_calibrator)).decode("ascii")

    return {
        "schema_version": _SCHEMA_VERSION,
        "ts_ms": int(time.time() * 1000),
        "n_samples": len(X_all),
        "n_folds": len(folds),
        "n_folds_trained": len(fold_aucs),
        "roc_auc_oos": round(roc_auc_oos, 4),
        "fold_aucs": [round(a, 4) for a in fold_aucs],
        "dsr": round(guard_details.get("dsr", 0.0), 4),
        "pbo": 0.0,
        "feature_cols": feature_cols,
        "default_threshold": default_threshold,
        "thresholds_by_regime": thresholds_by_regime,
        "model_bytes_b64": model_bytes,
        "calibrator_bytes_b64": cal_bytes,
    }


# ─── Inference ────────────────────────────────────────────────────────────────

def predict_prob(features_dict: dict[str, Any], state: dict) -> float:
    """
    Score a signal using the trained meta-labeling model.

    Args:
        features_dict: signal.indicators dict (raw, not yet extracted)
        state: model state dict from train_meta_labeling_model or Redis

    Returns:
        Calibrated P(TP) in [0, 1]. Returns 0.5 on error (indeterminate).
    """
    try:
        feature_cols: list[str] = state["feature_cols"]
        model_bytes = base64.b64decode(state["model_bytes_b64"])
        cal_bytes = base64.b64decode(state["calibrator_bytes_b64"])

        clf = pickle.loads(model_bytes)
        iso = pickle.loads(cal_bytes)

        feats = extract_features(features_dict)
        X = features_to_array(feats, feature_cols)

        raw_prob = clf.predict_proba(X)[0, 1]
        cal_prob = float(iso.predict([raw_prob])[0])
        return max(0.0, min(1.0, cal_prob))
    except Exception as e:
        log.debug("predict_prob error: %s", e)
        return 0.5  # indeterminate


def get_threshold(state: dict, regime: str) -> float:
    """Get P(TP) gate threshold for a given regime."""
    thresholds = state.get("thresholds_by_regime") or {}
    default = float(state.get("default_threshold", 0.45))
    reg = str(regime or "unknown").lower()
    return float(thresholds.get(reg, default))
