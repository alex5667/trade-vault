#!/usr/bin/env python3
"""SHAP TreeExplainer feature importance report — Phase P3 (4.13 explainability).

Computes global mean |SHAP| importance, optional per-bucket breakdown
(symbol_family / session), and week-over-week SHAP drift to flag unstable features.

Usage:
    # Global SHAP importance (top 30 features)
    python -m tools.feature_shap_report \\
        --model /var/lib/trade/ml_models/scorer_v5.joblib \\
        --lookback_days 7 \\
        --top_n 30

    # With week-over-week drift (flags features whose importance shifted > 30%)
    python -m tools.feature_shap_report \\
        --model /var/lib/trade/ml_models/scorer_v5.joblib \\
        --lookback_days 28 \\
        --by_week \\
        --drift_threshold 0.30

    # Per-bucket breakdown by symbol_family
    python -m tools.feature_shap_report \\
        --model /var/lib/trade/ml_models/scorer_v5.joblib \\
        --lookback_days 14 \\
        --by_symbol_family \\
        --out_json /tmp/shap_report.json

Requires: shap>=0.42  (pip install shap)

SHAP interpretation:
    mean_abs_shap  — global feature impact (higher = more influential)
    drift_rel      — relative change vs prior week (> drift_threshold → unstable)
    denylist_suggestions — features with high drift AND low recent importance
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from typing import Any

import numpy as np

logger = logging.getLogger("feature_shap_report")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

_SYMBOL_FAMILIES = {
    "BTCUSDT": "btc", "ETHUSDT": "eth",
    "SOLUSDT": "major", "BNBUSDT": "major", "XRPUSDT": "major",
    "DOGEUSDT": "meme", "PEPEUSDT": "meme", "SHIBUSDT": "meme",
    "SUIUSDT": "alt", "AVAXUSDT": "alt", "LINKUSDT": "alt",
}
_SESSIONS = [
    ("asia",    0, 8),
    ("europe",  7, 16),
    ("us",      13, 22),
]


# ---------------------------------------------------------------------------
# Data loader (signals:of:inputs Redis stream)
# ---------------------------------------------------------------------------

def _load_signals(
    lookback_days: int,
    feature_names: Sequence[str],
) -> tuple[np.ndarray | None, list[dict[str, str]]]:
    """Return (feature_matrix, meta_list).  meta_list has 'symbol', 'hour_utc', 'ts_ms'."""
    try:
        import redis as _redis
        r = _redis.from_url(
            os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
            decode_responses=True,
        )
        stream_key = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
        now_ms = int(__import__("time").time() * 1000)
        from_ms = now_ms - lookback_days * 86_400_000
        entries = r.xrange(stream_key, min=f"{from_ms}-0", max="+", count=100_000)
    except Exception as exc:
        logger.error("Redis load failed: %s", exc)
        return None, []

    if not entries:
        logger.warning("No signals in last %d days", lookback_days)
        return None, []

    rows: list[list[float]] = []
    metas: list[dict[str, str]] = []
    for eid, fields in entries:
        try:
            raw = fields.get("indicators") or fields.get("indicators_json") or ""
            if not raw:
                continue
            ind = json.loads(raw)
            rows.append([float(ind.get(k) or 0.0) for k in feature_names])
            ts_ms = int(eid.split("-")[0])
            hour_utc = str(int((ts_ms // 3_600_000) % 24))
            metas.append({
                "symbol": fields.get("symbol", ""),
                "hour_utc": hour_utc,
                "ts_ms": str(ts_ms),
                "week": str(ts_ms // (7 * 86_400_000)),
            })
        except Exception:
            continue

    if not rows:
        return None, []
    logger.info("Loaded %d signal rows from Redis (%d-day lookback)", len(rows), lookback_days)
    return np.array(rows, dtype=np.float64), metas


def _apply_scaler(X: np.ndarray, feature_names: Sequence[str], scaler_params: dict) -> np.ndarray:
    X = X.copy()
    for j, name in enumerate(feature_names):
        sp = scaler_params.get(name)
        if sp:
            med = float(sp.get("median", 0.0) or 0.0)
            iqr = float(sp.get("iqr", 1.0) or 1.0)
            X[:, j] = (X[:, j] - med) / max(iqr, 1e-9)
    return X


# ---------------------------------------------------------------------------
# SHAP computation
# ---------------------------------------------------------------------------

def _shap_global(
    model: Any,
    X: np.ndarray,
    feature_names: Sequence[str],
    max_samples: int,
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    """Return (shap_values_2d, ranked_importance_list)."""
    try:
        import shap  # type: ignore
    except ImportError:
        raise SystemExit("shap not installed — run: pip install shap")

    if len(X) > max_samples:
        idx = np.random.default_rng(42).choice(len(X), size=max_samples, replace=False)
        X = X[idx]
    logger.info("Running SHAP TreeExplainer on %d samples × %d features", *X.shape)

    explainer = shap.TreeExplainer(model)
    sv = explainer.shap_values(X)
    # For binary classifiers some libraries return list [neg_class, pos_class]
    if isinstance(sv, list):
        sv = sv[1] if len(sv) == 2 else sv[0]

    mean_abs = np.abs(sv).mean(axis=0)
    total = mean_abs.sum() or 1.0
    rows = [
        {
            "feature": name,
            "mean_abs_shap": float(mean_abs[i]),
            "pct_of_total": float(mean_abs[i] / total * 100),
        }
        for i, name in enumerate(feature_names)
    ]
    rows.sort(key=lambda r: -r["mean_abs_shap"])
    return sv, rows


def _shap_by_bucket(
    model: Any,
    X: np.ndarray,
    metas: list[dict[str, str]],
    feature_names: Sequence[str],
    bucket_key: str,
    max_samples_per_bucket: int = 2000,
) -> dict[str, list[dict[str, Any]]]:
    """Compute mean |SHAP| per bucket value (e.g., symbol_family or session)."""
    try:
        import shap  # type: ignore
    except ImportError:
        raise SystemExit("shap not installed — run: pip install shap")

    def _bucket_label(meta: dict[str, str]) -> str:
        if bucket_key == "symbol_family":
            return _SYMBOL_FAMILIES.get(meta["symbol"], "other")
        if bucket_key == "session":
            h = int(meta["hour_utc"])
            for name, lo, hi in _SESSIONS:
                if lo <= h < hi:
                    return name
            return "off_hours"
        return meta.get(bucket_key, "unknown")

    from collections import defaultdict
    buckets: dict[str, list[int]] = defaultdict(list)
    for i, m in enumerate(metas):
        buckets[_bucket_label(m)].append(i)

    result: dict[str, list[dict[str, Any]]] = {}
    explainer = shap.TreeExplainer(model)
    for bname, idxs in sorted(buckets.items()):
        if len(idxs) < 10:
            continue
        if len(idxs) > max_samples_per_bucket:
            rng = np.random.default_rng(42)
            idxs = list(rng.choice(idxs, size=max_samples_per_bucket, replace=False))
        Xb = X[idxs]
        sv = explainer.shap_values(Xb)
        if isinstance(sv, list):
            sv = sv[1] if len(sv) == 2 else sv[0]
        mean_abs = np.abs(sv).mean(axis=0)
        total = mean_abs.sum() or 1.0
        rows = [
            {
                "feature": name,
                "mean_abs_shap": float(mean_abs[i]),
                "pct_of_total": float(mean_abs[i] / total * 100),
            }
            for i, name in enumerate(feature_names)
        ]
        rows.sort(key=lambda r: -r["mean_abs_shap"])
        result[bname] = rows
        logger.info("SHAP bucket '%s': %d samples, top feature='%s'", bname, len(idxs), rows[0]["feature"])
    return result


def _shap_week_drift(
    model: Any,
    X: np.ndarray,
    metas: list[dict[str, str]],
    feature_names: Sequence[str],
    drift_threshold: float,
    max_samples_per_week: int = 1500,
) -> dict[str, Any]:
    """Per-week mean |SHAP|; detect features with high week-over-week drift."""
    try:
        import shap  # type: ignore
    except ImportError:
        raise SystemExit("shap not installed — run: pip install shap")

    from collections import defaultdict
    weeks: dict[str, list[int]] = defaultdict(list)
    for i, m in enumerate(metas):
        weeks[m["week"]].append(i)

    sorted_weeks = sorted(weeks.keys(), key=int)
    if len(sorted_weeks) < 2:
        return {"error": "need >= 2 weeks of data for drift analysis", "weeks": sorted_weeks}

    explainer = shap.TreeExplainer(model)
    week_means: dict[str, np.ndarray] = {}
    for wk in sorted_weeks:
        idxs = weeks[wk]
        if len(idxs) < 5:
            continue
        if len(idxs) > max_samples_per_week:
            rng = np.random.default_rng(42)
            idxs = list(rng.choice(idxs, size=max_samples_per_week, replace=False))
        sv = explainer.shap_values(X[idxs])
        if isinstance(sv, list):
            sv = sv[1] if len(sv) == 2 else sv[0]
        week_means[wk] = np.abs(sv).mean(axis=0)

    if len(week_means) < 2:
        return {"error": "insufficient weeks after filtering", "weeks_found": list(week_means.keys())}

    wk_list = sorted(week_means.keys(), key=int)
    ref_mean = week_means[wk_list[0]]
    cur_mean = week_means[wk_list[-1]]

    drifted: list[dict[str, Any]] = []
    for i, name in enumerate(feature_names):
        ref_v = float(ref_mean[i])
        cur_v = float(cur_mean[i])
        drift_rel = abs(cur_v - ref_v) / max(ref_v, 1e-9)
        if drift_rel > drift_threshold:
            drifted.append({
                "feature": name,
                "ref_week_mean_abs_shap": round(ref_v, 8),
                "cur_week_mean_abs_shap": round(cur_v, 8),
                "drift_rel": round(drift_rel, 4),
            })
    drifted.sort(key=lambda r: -r["drift_rel"])

    week_summary = [
        {
            "week": wk,
            "n_samples": len(weeks[wk]),
            "top_feature": str(feature_names[int(week_means[wk].argmax())]),
            "top_mean_abs_shap": float(week_means[wk].max()),
        }
        for wk in wk_list if wk in week_means
    ]

    # Denylist: high drift AND current importance below median
    median_cur = float(np.median(cur_mean))
    denylist_suggestions = [
        d["feature"] for d in drifted if d["cur_week_mean_abs_shap"] < median_cur
    ]

    return {
        "weeks_analyzed": wk_list,
        "n_drifted": len(drifted),
        "drift_threshold": drift_threshold,
        "drifted_features": drifted,
        "denylist_suggestions": denylist_suggestions,
        "week_summary": week_summary,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="SHAP TreeExplainer feature importance report")
    parser.add_argument("--model", required=True, help="Path to .joblib model artifact")
    parser.add_argument("--lookback_days", type=int, default=7)
    parser.add_argument("--top_n", type=int, default=30, help="Top N features to include in output")
    parser.add_argument("--max_samples", type=int, default=5000, help="Max rows for global SHAP")
    parser.add_argument("--by_week", action="store_true", help="Week-over-week drift analysis")
    parser.add_argument("--by_symbol_family", action="store_true", help="Breakdown by symbol family")
    parser.add_argument("--by_session", action="store_true", help="Breakdown by trading session")
    parser.add_argument("--drift_threshold", type=float, default=0.30,
                        help="Relative SHAP drift threshold (>threshold → flagged)")
    parser.add_argument("--out_json", type=str, default="", help="Write JSON to file (default: stdout)")
    args = parser.parse_args()

    try:
        import joblib
        pack = joblib.load(args.model)
    except Exception as exc:
        logger.error("Cannot load model: %s", exc)
        return 1

    feature_names: list[str] = list(pack.get("feature_names") or [])
    if not feature_names:
        logger.error("model artifact has no feature_names")
        return 1

    model = pack.get("model")
    if model is None:
        logger.error("model artifact has no 'model' key")
        return 1

    schema_hash = pack.get("schema_hash", "unknown")
    feature_cols_hash = pack.get("feature_cols_hash", "unknown")
    feature_schema_ver = pack.get("feature_schema_ver", "unknown")
    scaler_params = pack.get("robust_scaler_params") or {}

    logger.info(
        "Model: schema_ver=%s schema_hash=%s n_features=%d",
        feature_schema_ver, schema_hash, len(feature_names),
    )

    X_raw, metas = _load_signals(args.lookback_days, feature_names)
    if X_raw is None:
        logger.error("No data loaded — aborting")
        return 1

    X = _apply_scaler(X_raw, feature_names, scaler_params)

    report: dict[str, Any] = {
        "model_path": args.model,
        "feature_schema_ver": feature_schema_ver,
        "schema_hash": schema_hash,
        "feature_cols_hash": feature_cols_hash,
        "n_features": len(feature_names),
        "n_samples": len(X),
        "lookback_days": args.lookback_days,
    }

    # Global SHAP
    try:
        _, global_rows = _shap_global(model, X, feature_names, args.max_samples)
        report["shap_global_top"] = global_rows[:args.top_n]
        report["shap_global_bottom"] = global_rows[-10:]  # least important features
    except Exception as exc:
        logger.error("Global SHAP failed: %s", exc)
        report["shap_global_error"] = str(exc)
        return 1

    # Per-bucket breakdowns
    for flag, bkey in ((args.by_symbol_family, "symbol_family"), (args.by_session, "session")):
        if not flag:
            continue
        try:
            bucket_data = _shap_by_bucket(model, X, metas, feature_names, bkey)
            report[f"shap_by_{bkey}"] = {
                bname: rows[:args.top_n] for bname, rows in bucket_data.items()
            }
        except Exception as exc:
            logger.warning("SHAP by_%s failed: %s", bkey, exc)
            report[f"shap_by_{bkey}_error"] = str(exc)

    # Week-over-week drift
    if args.by_week:
        try:
            drift = _shap_week_drift(model, X, metas, feature_names, args.drift_threshold)
            report["shap_drift"] = drift
            if drift.get("denylist_suggestions"):
                logger.warning(
                    "SHAP drift denylist suggestions (%d): %s",
                    len(drift["denylist_suggestions"]),
                    drift["denylist_suggestions"][:10],
                )
        except Exception as exc:
            logger.warning("SHAP drift analysis failed: %s", exc)
            report["shap_drift_error"] = str(exc)

    report_json = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.out_json:
        from pathlib import Path
        Path(args.out_json).write_text(report_json, encoding="utf-8")
        logger.info("Report written to %s", args.out_json)
    else:
        print(report_json)

    return 0


if __name__ == "__main__":
    sys.exit(main())
