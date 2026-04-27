#!/usr/bin/env python3
"""Train MetaModelLR (portable JSON LR) from a Parquet dataset.

Expected input (flexible):
- A column with indicators dict/json: 'indicators' (preferred) or 'meta' or 'row'
- A binary label column: 'y' (preferred) or 'label' or 'win'
- Optional timestamp column: 'ts_ms' or 'ts'

The output is a JSON file compatible with core.meta_model_lr.MetaModelLR.load().
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

from core.feature_engineering import apply_transform


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        if isinstance(x, bool):
            return float(int(x))
        return float(x)
    except Exception:
        return d


def _loads_maybe_json(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            return json.loads(s)
        except Exception:
            return {}
    return {}


def _ece(y: np.ndarray, p: np.ndarray, n_bins: int = 10) -> float:
    # Expected Calibration Error (ECE)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        m = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(m):
            continue
        acc = float(np.mean(y[m]))
        conf = float(np.mean(p[m]))
        ece += float(np.mean(m)) * abs(acc - conf)
    return float(ece)


def _scenario_buckets(s: str) -> Dict[str, float]:
    s = (s or "").lower()
    from common.market_mode import is_range_regime
    return {
        "scn_is_news": 1.0 if "news" in s else 0.0,
        "scn_is_trend": 1.0 if "trend" in s else 0.0,
        "scn_is_range": 1.0 if is_range_regime(s) else 0.0,
        "scn_is_chop": 1.0 if ("chop" in s or "saw" in s) else 0.0,
    }


# Feature list must match what OFConfirmEngine produces in 'feat' dict.
FEATURES_V1: List[str] = [
    # rule confidence
    "base_score",
    "score_final_raw",
    "score_final_01",
    "exec_pen",
    "have",
    "need",
    "have_need_ratio",
    "ok_soft",
    "exec_risk_norm",
    "exec_risk_bps",
    "exec_risk_ref_bps",
    "agg_is_sum",
    "agg_is_avg",
    # microstructure
    "delta_z",
    "obi",
    "obi_stable",
    "obi_stable_secs",
    "iceberg_strict",
    "iceberg_refresh",
    "iceberg_duration",
    "absorption",
    "absorption_volume",
    "abs_lvl_ok",
    "abs_lvl_score",
    "fp_edge_absorb",
    "fp_edge_strength",
    "fp_edge_range_expansion",
    "ofi",
    "ofi_z",
    "ofi_stable",
    "ofi_stable_secs",
    "ofi_stability_score",
    # health / staleness
    "data_health",
    "book_health_ok",
    "cvd_quarantine_active",
    "obi_age_ms",
    "iceberg_age_ms",
    "ofi_age_ms",
    "sweep_age_ms",
    "reclaim_age_ms",
    "fp_edge_age_ms",
    # scenario buckets
    "scn_is_news",
    "scn_is_trend",
    "scn_is_range",
    "scn_is_chop",
    # legs
    "leg_ofi_leg",
    "leg_fp_edge_absorb",
    "leg_obi_stable",
    "leg_iceberg_strict",
    "leg_abs_lvl_ok",
    "leg_reclaim_recent",
    "leg_weak_progress",
    "leg_sweep_recent",
]


def build_meta_features(ind: Dict[str, Any]) -> Dict[str, float]:
    # score breakdown (preferred source)
    sb = ind.get("score_breakdown_small") or ind.get("score_breakdown") or {}
    if not isinstance(sb, dict):
        sb = {}

    base_score = _f(ind.get("of_base_score"), _f(sb.get("base_score"), _f(ind.get("base_score"), 0.0)))
    score_raw = _f(ind.get("of_score_final_raw"), _f(sb.get("final_score_raw"), _f(sb.get("final_score"), _f(ind.get("score_final_raw"), 0.0))))
    score_01 = _f(ind.get("of_score_final"), _f(sb.get("final_score_01"), _f(ind.get("score_final_01"), 0.0)))
    exec_pen = _f(ind.get("exec_pen"), _f(sb.get("exec_pen"), 0.0))

    have = _f(ind.get("have"), _f(ind.get("rule_have"), 0.0))
    need = _f(ind.get("need"), _f(ind.get("rule_need"), 1.0))
    ok_soft = _f(ind.get("ok_soft"), 0.0)

    agg = str(sb.get("agg", "") or "")
    agg_is_sum = 1.0 if agg == "sum" else 0.0
    agg_is_avg = 1.0 if agg != "sum" else 0.0

    scn = str(ind.get("scenario_v4", "") or "")
    scn_b = _scenario_buckets(scn)

    # legs (prefer explicit)
    def _leg(name: str, fallback_key: str) -> float:
        return _f(ind.get(name), _f(ind.get(fallback_key), 0.0))

    out: Dict[str, float] = {
        "base_score": float(base_score),
        "score_final_raw": float(score_raw),
        "score_final_01": float(score_01),
        "exec_pen": float(exec_pen),
        "have": float(have),
        "need": float(need),
        "have_need_ratio": float(have) / max(1.0, float(need)),
        "ok_soft": float(ok_soft),
        "exec_risk_norm": _f(ind.get("exec_risk_norm"), 0.0),
        "exec_risk_bps": _f(ind.get("exec_risk_bps"), 0.0),
        "exec_risk_ref_bps": _f(ind.get("exec_risk_ref_bps"), 0.0),
        "agg_is_sum": float(agg_is_sum),
        "agg_is_avg": float(agg_is_avg),
        "delta_z": _f(ind.get("delta_z"), 0.0),
        "obi": _f(ind.get("obi"), 0.0),
        "obi_stable": _f(ind.get("obi_stable"), 0.0),
        "obi_stable_secs": _f(ind.get("obi_stable_secs"), 0.0),
        "iceberg_strict": _f(ind.get("iceberg_strict"), 0.0),
        "iceberg_refresh": _f(ind.get("iceberg_refresh"), 0.0),
        "iceberg_duration": _f(ind.get("iceberg_duration"), 0.0),
        "absorption": _f(ind.get("absorption"), 0.0),
        "absorption_volume": _f(ind.get("absorption_volume"), 0.0),
        "abs_lvl_ok": _f(ind.get("abs_lvl_ok"), 0.0),
        "abs_lvl_score": _f(ind.get("abs_lvl_score"), 0.0),
        "fp_edge_absorb": _f(ind.get("fp_edge_absorb"), 0.0),
        "fp_edge_strength": _f(ind.get("fp_edge_strength"), 0.0),
        "fp_edge_range_expansion": _f(ind.get("fp_edge_range_expansion"), 0.0),
        "ofi": _f(ind.get("ofi"), 0.0),
        "ofi_z": _f(ind.get("ofi_z"), 0.0),
        "ofi_stable": _f(ind.get("ofi_stable"), 0.0),
        "ofi_stable_secs": _f(ind.get("ofi_stable_secs"), 0.0),
        "ofi_stability_score": _f(ind.get("ofi_stability_score"), 0.0),
        "data_health": _f(ind.get("data_health"), 1.0),
        "book_health_ok": _f(ind.get("book_health_ok"), 1.0),
        "cvd_quarantine_active": _f(ind.get("cvd_quarantine_active"), 0.0),
        "obi_age_ms": _f(ind.get("obi_age_ms"), -1.0),
        "iceberg_age_ms": _f(ind.get("iceberg_age_ms"), -1.0),
        "ofi_age_ms": _f(ind.get("ofi_age_ms"), -1.0),
        "sweep_age_ms": _f(ind.get("sweep_age_ms"), -1.0),
        "reclaim_age_ms": _f(ind.get("reclaim_age_ms"), -1.0),
        "fp_edge_age_ms": _f(ind.get("fp_edge_age_ms"), -1.0),
        # scenario buckets
        **scn_b,
        # legs
        "leg_ofi_leg": _leg("leg_ofi_leg", "ofi_leg"),
        "leg_fp_edge_absorb": _leg("leg_fp_edge_absorb", "fp_edge_absorb"),
        "leg_obi_stable": _leg("leg_obi_stable", "obi_stable"),
        "leg_iceberg_strict": _leg("leg_iceberg_strict", "iceberg_strict"),
        "leg_abs_lvl_ok": _leg("leg_abs_lvl_ok", "abs_lvl_ok"),
        "leg_reclaim_recent": _leg("leg_reclaim_recent", "reclaim"),
        "leg_weak_progress": _leg("leg_weak_progress", "weak_progress"),
        "leg_sweep_recent": _leg("leg_sweep_recent", "sweep"),
    }
    # Ensure all features exist
    for k in FEATURES_V1:
        out.setdefault(k, 0.0)
    return out


def choose_transforms() -> Dict[str, Any]:
    tf: Dict[str, Any] = {}
    for f in FEATURES_V1:
        if f.endswith("_age_ms") or f.endswith("_duration") or f.endswith("_secs") or f.endswith("_bps") or f.endswith("_volume") or f.endswith("_refresh"):
            tf[f] = {"type": "log1p"}
        if f.endswith("_z") or f in ("delta_z",):
            tf[f] = {"type": "clip", "lo": -8.0, "hi": 8.0}
        if f in ("score_final_raw", "base_score", "score_final_01"):
            tf[f] = {"type": "clip", "lo": -1.0, "hi": 2.0}
        if f in ("exec_pen", "exec_risk_norm"):
            tf[f] = {"type": "clip", "lo": 0.0, "hi": 5.0}
        if f in ("data_health",):
            tf[f] = {"type": "clip", "lo": 0.0, "hi": 1.0}
    return tf


def robust_params(x: np.ndarray) -> Tuple[float, float]:
    # median / MAD; fall back to IQR; then 1.0
    x = x[np.isfinite(x)]
    if x.size == 0:
        return 0.0, 1.0
    med = float(np.median(x))
    mad = float(np.median(np.abs(x - med)))
    scale = mad * 1.4826
    if not math.isfinite(scale) or scale <= 1e-12:
        q25, q75 = np.percentile(x, [25, 75])
        iqr = float(q75 - q25)
        scale = iqr / 1.349 if iqr > 1e-12 else 1.0
    return med, float(max(scale, 1e-6))


def transform_matrix(feats: List[Dict[str, float]], tf: Dict[str, Any]) -> Tuple[np.ndarray, Dict[str, Dict[str, float]]]:
    # Apply transforms; compute robust scaler on transformed values; return scaled matrix + params
    n = len(feats)
    m = len(FEATURES_V1)
    X = np.zeros((n, m), dtype=np.float64)
    raw = np.zeros((n, m), dtype=np.float64)

    for i, d in enumerate(feats):
        for j, name in enumerate(FEATURES_V1):
            v = float(d.get(name, 0.0))
            v = float(apply_transform(v, tf.get(name)))
            raw[i, j] = v

    rs: Dict[str, Dict[str, float]] = {}
    for j, name in enumerate(FEATURES_V1):
        center, scale = robust_params(raw[:, j])
        rs[name] = {"center": float(center), "scale": float(scale)}
        X[:, j] = (raw[:, j] - center) / scale
    return X, rs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True, help="Input parquet path")
    ap.add_argument("--out", required=True, help="Output JSON path (MetaModelLR format)")
    ap.add_argument("--label-col", default="y", help="Label column (default: y)")
    ap.add_argument("--ts-col", default="ts_ms", help="Timestamp column for time splits")
    ap.add_argument("--threshold", type=float, default=0.55, help="Decision threshold stored in model (default: 0.55)")
    ap.add_argument("--c-grid", default="0.2,0.5,1,2,5", help="Comma-separated C candidates")
    ap.add_argument("--splits", type=int, default=5, help="TimeSeries splits (default 5)")
    args = ap.parse_args()

    df = pd.read_parquet(args.parquet)
    if args.label_col not in df.columns:
        for c in ("label", "win", "y"):
            if c in df.columns:
                args.label_col = c
                break
    if args.ts_col not in df.columns:
        for c in ("ts", "ts_ms", "t"):
            if c in df.columns:
                args.ts_col = c
                break

    ind_col = None
    for c in ("indicators", "meta", "row"):
        if c in df.columns:
            ind_col = c
            break
    if ind_col is None:
        raise SystemExit("No indicators-like column found (expected: indicators/meta/row).")

    # sort chronologically
    df = df.sort_values(args.ts_col).reset_index(drop=True)

    indicators = [_loads_maybe_json(v) for v in df[ind_col].tolist()]
    feats = [build_meta_features(d if isinstance(d, dict) else {}) for d in indicators]

    y = df[args.label_col].astype(float).values
    y = (y > 0.5).astype(int)

    tf = choose_transforms()
    X, rs = transform_matrix(feats, tf)

    c_grid = [float(x.strip()) for x in str(args.c_grid).split(",") if x.strip()]
    tscv = TimeSeriesSplit(n_splits=int(args.splits))
    best = None

    for C in c_grid:
        ll = []
        auc = []
        for tr, te in tscv.split(X):
            # fit
            m = LogisticRegression(C=float(C), penalty="l2", solver="lbfgs", max_iter=500)
            m.fit(X[tr], y[tr])
            p = m.predict_proba(X[te])[:, 1]
            ll.append(log_loss(y[te], p, labels=[0, 1]))
            try:
                auc.append(roc_auc_score(y[te], p))
            except Exception:
                pass
        score = float(np.mean(ll))
        auc_m = float(np.mean(auc)) if auc else float("nan")
        if best is None or score < best[0]:
            best = (score, auc_m, C)

    assert best is not None
    _, _, best_C = best

    # fit on full data
    m = LogisticRegression(C=float(best_C), penalty="l2", solver="lbfgs", max_iter=500)
    m.fit(X, y)
    p = m.predict_proba(X)[:, 1]

    report = {
        "n": int(len(y)),
        "pos_rate": float(np.mean(y)),
        "best_C": float(best_C),
        "logloss": float(log_loss(y, p, labels=[0, 1])),
        "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) > 1 else float("nan"),
        "brier": float(brier_score_loss(y, p)),
        "ece10": float(_ece(y, p, n_bins=10)),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))

    out = {
        "features": FEATURES_V1,
        "intercept": float(m.intercept_[0]),
        "coef": [float(x) for x in m.coef_[0].tolist()],
        "threshold": float(args.threshold),
        "transforms": tf,
        "robust_scaler": rs,
        "report": report,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
