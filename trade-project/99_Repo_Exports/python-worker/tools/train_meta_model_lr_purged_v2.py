#!/usr/bin/env python3
import argparse
import json
import math
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

from core.feature_engineering import apply_transform
from ml_core.purged_cv import purged_kfold_time_series


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


def _scenario_buckets(s: str) -> dict[str, float]:
    s = (s or "").lower()
    from common.market_mode import is_range_regime
    return {
        "scn_is_news": 1.0 if "news" in s else 0.0,
        "scn_is_trend": 1.0 if "trend" in s else 0.0,
        "scn_is_range": 1.0 if is_range_regime(s) else 0.0,
        "scn_is_chop": 1.0 if ("chop" in s or "saw" in s) else 0.0,
    }


# Feature list aligned with OFConfirmEngine / train_meta_model_lr_v1.py
FEATURES_V1: list[str] = [
    "base_score", "score_final_raw", "score_final_01", "exec_pen", "have", "need", "have_need_ratio", "ok_soft",
    "exec_risk_norm", "exec_risk_bps", "exec_risk_ref_bps", "agg_is_sum", "agg_is_avg",
    "delta_z", "obi", "obi_stable", "obi_stable_secs", "iceberg_strict", "iceberg_refresh", "iceberg_duration",
    "absorption", "absorption_volume", "abs_lvl_ok", "abs_lvl_score", "fp_edge_absorb", "fp_edge_strength",
    "fp_edge_range_expansion", "ofi", "ofi_z", "ofi_stable", "ofi_stable_secs", "ofi_stability_score",
    "data_health", "book_health_ok", "cvd_quarantine_active", "obi_age_ms", "iceberg_age_ms", "ofi_age_ms",
    "sweep_age_ms", "reclaim_age_ms", "fp_edge_age_ms", "scn_is_news", "scn_is_trend", "scn_is_range", "scn_is_chop",
    "leg_ofi_leg", "leg_fp_edge_absorb", "leg_obi_stable", "leg_iceberg_strict", "leg_abs_lvl_ok", "leg_reclaim_recent",
    "leg_weak_progress", "leg_sweep_recent",
]


def build_meta_features(ind: dict[str, Any]) -> dict[str, float]:
    sb = ind.get("score_breakdown_small") or ind.get("score_breakdown") or {}
    if not isinstance(sb, dict): sb = {}

    base_score = _f(ind.get("of_base_score"), _f(sb.get("base_score"), _f(ind.get("base_score"), 0.0)))
    score_raw = _f(ind.get("of_score_final_raw"), _f(sb.get("final_score_raw"), _f(sb.get("final_score"), _f(ind.get("score_final_raw"), 0.0))))
    score_01 = _f(ind.get("of_score_final"), _f(sb.get("final_score_01"), _f(ind.get("score_final_01"), 0.0)))
    exec_pen = _f(ind.get("exec_pen"), _f(sb.get("exec_pen"), 0.0))
    have = _f(ind.get("have"), _f(ind.get("rule_have"), 0.0))
    need = _f(ind.get("need"), _f(ind.get("rule_need"), 1.0))
    ok_soft = _f(ind.get("ok_soft"), 0.0)

    agg = (sb.get("agg", "") or "")
    agg_is_sum = 1.0 if agg == "sum" else 0.0
    agg_is_avg = 1.0 if agg != "sum" else 0.0

    scn = (ind.get("scenario_v4", "") or "")
    scn_b = _scenario_buckets(scn)

    def _leg(name: str, fallback_key: str) -> float:
        return _f(ind.get(name), _f(ind.get(fallback_key), 0.0))

    out: dict[str, float] = {
        "base_score": float(base_score), "score_final_raw": float(score_raw), "score_final_01": float(score_01),
        "exec_pen": float(exec_pen), "have": float(have), "need": float(need),
        "have_need_ratio": float(have) / max(1.0, float(need)), "ok_soft": float(ok_soft),
        "exec_risk_norm": _f(ind.get("exec_risk_norm"), 0.0), "exec_risk_bps": _f(ind.get("exec_risk_bps"), 0.0),
        "exec_risk_ref_bps": _f(ind.get("exec_risk_ref_bps"), 0.0), "agg_is_sum": float(agg_is_sum),
        "agg_is_avg": float(agg_is_avg), "delta_z": _f(ind.get("delta_z"), 0.0), "obi": _f(ind.get("obi"), 0.0),
        "obi_stable": _f(ind.get("obi_stable"), 0.0), "obi_stable_secs": _f(ind.get("obi_stable_secs"), 0.0),
        "iceberg_strict": _f(ind.get("iceberg_strict"), 0.0), "iceberg_refresh": _f(ind.get("iceberg_refresh"), 0.0),
        "iceberg_duration": _f(ind.get("iceberg_duration"), 0.0), "absorption": _f(ind.get("absorption"), 0.0),
        "absorption_volume": _f(ind.get("absorption_volume"), 0.0), "abs_lvl_ok": _f(ind.get("abs_lvl_ok"), 0.0),
        "abs_lvl_score": _f(ind.get("abs_lvl_score"), 0.0), "fp_edge_absorb": _f(ind.get("fp_edge_absorb"), 0.0),
        "fp_edge_strength": _f(ind.get("fp_edge_strength"), 0.0), "fp_edge_range_expansion": _f(ind.get("fp_edge_range_expansion"), 0.0),
        "ofi": _f(ind.get("ofi"), 0.0), "ofi_z": _f(ind.get("ofi_z"), 0.0), "ofi_stable": _f(ind.get("ofi_stable"), 0.0),
        "ofi_stable_secs": _f(ind.get("ofi_stable_secs"), 0.0), "ofi_stability_score": _f(ind.get("ofi_stability_score"), 0.0),
        "data_health": _f(ind.get("data_health"), 1.0), "book_health_ok": _f(ind.get("book_health_ok"), 1.0),
        "cvd_quarantine_active": _f(ind.get("cvd_quarantine_active"), 0.0), "obi_age_ms": _f(ind.get("obi_age_ms"), -1.0),
        "iceberg_age_ms": _f(ind.get("iceberg_age_ms"), -1.0), "ofi_age_ms": _f(ind.get("ofi_age_ms"), -1.0),
        "sweep_age_ms": _f(ind.get("sweep_age_ms"), -1.0), "reclaim_age_ms": _f(ind.get("reclaim_age_ms"), -1.0),
        "fp_edge_age_ms": _f(ind.get("fp_edge_age_ms"), -1.0), **scn_b,
        "leg_ofi_leg": _leg("leg_ofi_leg", "ofi_leg"), "leg_fp_edge_absorb": _leg("leg_fp_edge_absorb", "fp_edge_absorb"),
        "leg_obi_stable": _leg("leg_obi_stable", "obi_stable"), "leg_iceberg_strict": _leg("leg_iceberg_strict", "iceberg_strict"),
        "leg_abs_lvl_ok": _leg("leg_abs_lvl_ok", "abs_lvl_ok"), "leg_reclaim_recent": _leg("leg_reclaim_recent", "reclaim"),
        "leg_weak_progress": _leg("leg_weak_progress", "weak_progress"), "leg_sweep_recent": _leg("leg_sweep_recent", "sweep"),
    }
    for k in FEATURES_V1: out.setdefault(k, 0.0)
    return out


def choose_transforms() -> dict[str, Any]:
    tf: dict[str, Any] = {}
    for f in FEATURES_V1:
        if any(f.endswith(suffix) for suffix in ["_age_ms", "_duration", "_secs", "_bps", "_volume", "_refresh"]):
            tf[f] = {"type": "log1p"}
        elif f.endswith("_z") or f == "delta_z":
            tf[f] = {"type": "clip", "lo": -8.0, "hi": 8.0}
        elif f in ["score_final_raw", "base_score", "score_final_01"]:
            tf[f] = {"type": "clip", "lo": -1.0, "hi": 2.0}
        elif f in ["exec_pen", "exec_risk_norm"]:
            tf[f] = {"type": "clip", "lo": 0.0, "hi": 5.0}
        elif f == "data_health":
            tf[f] = {"type": "clip", "lo": 0.0, "hi": 1.0}
    return tf


def robust_params(x: np.ndarray) -> tuple[float, float]:
    x = x[np.isfinite(x)]
    if x.size == 0: return 0.0, 1.0
    med = float(np.median(x))
    mad = float(np.median(np.abs(x - med)))
    scale = mad * 1.4826
    if not math.isfinite(scale) or scale <= 1e-12:
        q25, q75 = np.percentile(x, [25, 75])
        iqr = float(q75 - q25)
        scale = iqr / 1.349 if iqr > 1e-12 else 1.0
    return med, float(max(scale, 1e-6))


def transform_matrix(feats: list[dict[str, float]], tf: dict[str, Any]) -> tuple[np.ndarray, dict[str, dict[str, float]]]:
    n, m = len(feats), len(FEATURES_V1)
    X = np.zeros((n, m), dtype=np.float64)
    raw = np.zeros((n, m), dtype=np.float64)
    for i, d in enumerate(feats):
        for j, name in enumerate(FEATURES_V1):
            v = float(apply_transform(float(d.get(name, 0.0)), tf.get(name)))
            raw[i, j] = v
    rs: dict[str, dict[str, float]] = {}
    for j, name in enumerate(FEATURES_V1):
        center, scale = robust_params(raw[:, j])
        rs[name] = {"center": float(center), "scale": float(scale)}
        X[:, j] = (raw[:, j] - center) / scale
    return X, rs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out_json", required=True)
    ap.add_argument("--out_report_json", required=True)
    ap.add_argument("--label_col", default="y")
    ap.add_argument("--time_col", default="ts_ms")
    ap.add_argument("--purge_ms", type=int, default=300000)
    ap.add_argument("--embargo_ms", type=int, default=60000)
    ap.add_argument("--feature_prefix", default="")
    ap.add_argument("--threshold", type=float, default=0.55)
    ap.add_argument("--splits", type=int, default=5)
    args = ap.parse_args()

    rows = []
    with open(args.in_jsonl, encoding="utf-8") as f:
        for line in f:
            if line.strip(): rows.append(json.loads(line))

    df = pd.DataFrame(rows).sort_values(args.time_col).reset_index(drop=True)
    ind_col = next((c for c in ["indicators", "meta", "row"] if c in df.columns), None)
    if not ind_col: raise SystemExit("No indicators column found")

    indicators = [_loads_maybe_json(v) for v in df[ind_col].tolist()]
    feats = [build_meta_features(d if isinstance(d, dict) else {}) for d in indicators]
    y = (df[args.label_col].astype(float).values > 0.5).astype(int)
    ts = df[args.time_col].astype(int).values

    # Simple T1 estimate: ts + some offset if not present
    t1 = df["tb_t_hit_ms"].astype(int).values if "tb_t_hit_ms" in df.columns else ts + 300000

    tf = choose_transforms()
    X, rs = transform_matrix(feats, tf)

    folds = purged_kfold_time_series(ts_ms=ts, t1_ms=t1, n_splits=args.splits, embargo_ms=args.embargo_ms)

    metrics = []
    for i, f in enumerate(folds):
        m = LogisticRegression(C=1.0, penalty="l2", solver="lbfgs", max_iter=500, class_weight="balanced")
        m.fit(X[f.train_idx], y[f.train_idx])
        p = m.predict_proba(X[f.test_idx])[:, 1]
        metrics.append({
            "fold": i + 1, "n": len(f.test_idx), "logloss": float(log_loss(y[f.test_idx], p, labels=[0, 1])),
            "auc": float(roc_auc_score(y[f.test_idx], p)) if len(np.unique(y[f.test_idx])) > 1 else 0.5,
            "brier": float(brier_score_loss(y[f.test_idx], p)), "ece10": float(_ece(y[f.test_idx], p))
        })

    m_final = LogisticRegression(C=1.0, penalty="l2", solver="lbfgs", max_iter=500, class_weight="balanced")
    m_final.fit(X, y)
    p_final = m_final.predict_proba(X)[:, 1]

    report = {
        "n": len(y), "pos_rate": float(np.mean(y)), "logloss": float(log_loss(y, p_final)),
        "auc": float(roc_auc_score(y, p_final)), "brier": float(brier_score_loss(y, p_final)),
        "ece10": float(_ece(y, p_final)), "folds": metrics,
    }
    with open(args.out_report_json, "w", encoding="utf-8") as f: json.dump(report, f, indent=2)

    model = {
        "features": FEATURES_V1, "intercept": float(m_final.intercept_[0]), "coef": m_final.coef_[0].tolist(),
        "threshold": args.threshold, "transforms": tf, "robust_scaler": rs, "report": report,
    }
    with open(args.out_json, "w", encoding="utf-8") as f: json.dump(model, f, indent=2)


if __name__ == "__main__":
    main()
