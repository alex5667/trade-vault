#!/usr/bin/env python3
"""Feature PSI drift report + permutation importance (4.13 Model Explainability).

Usage:
    # PSI drift: compare feature distributions in model artifact vs recent signals
    python -m tools.feature_psi_report \\
        --model /var/lib/trade/ml_models/scorer_v3.joblib \\
        --lookback_days 7 \\
        --psi_threshold 0.2

    # Permutation importance on recent data (requires OOF or a test set)
    python -m tools.feature_psi_report \\
        --model /var/lib/trade/ml_models/scorer_v3.joblib \\
        --lookback_days 14 \\
        --permutation_importance

    Output: JSON report to stdout + optional --out_json path.

PSI interpretation:
    < 0.10  — negligible shift (safe)
    0.10–0.20 — moderate shift (monitor)
    > 0.20  — significant shift → candidate for denylist
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from collections.abc import Sequence
from typing import Any

import numpy as np

logger = logging.getLogger("feature_psi_report")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")


# ---------------------------------------------------------------------------
# PSI core
# ---------------------------------------------------------------------------

def _psi_buckets(ref: np.ndarray, cur: np.ndarray, n_bins: int = 10) -> float:
    """Population Stability Index between two 1-D arrays."""
    ref = ref[np.isfinite(ref)]
    cur = cur[np.isfinite(cur)]
    if len(ref) < 5 or len(cur) < 5:
        return float("nan")
    # Use quantile-based bin edges from reference distribution
    quantiles = np.linspace(0, 100, n_bins + 1)
    edges = np.percentile(ref, quantiles)
    edges[0] -= 1e-9
    edges[-1] += 1e-9
    # Handle degenerate case: all reference values identical
    if edges[-1] <= edges[0]:
        return 0.0
    ref_counts, _ = np.histogram(ref, bins=edges)
    cur_counts, _ = np.histogram(cur, bins=edges)
    ref_frac = ref_counts / max(len(ref), 1)
    cur_frac = cur_counts / max(len(cur), 1)
    eps = 1e-6
    psi = float(np.sum((cur_frac - ref_frac) * np.log((cur_frac + eps) / (ref_frac + eps))))
    return psi


def compute_psi_report(
    ref_matrix: np.ndarray,
    cur_matrix: np.ndarray,
    feature_names: Sequence[str],
    threshold: float = 0.2,
) -> dict[str, Any]:
    """Return PSI per feature with denylist suggestions for PSI > threshold."""
    assert ref_matrix.shape[1] == len(feature_names), "column count mismatch"
    assert cur_matrix.shape[1] == len(feature_names), "column count mismatch"
    results: list[dict[str, Any]] = []
    for i, name in enumerate(feature_names):
        psi = _psi_buckets(ref_matrix[:, i], cur_matrix[:, i])
        results.append({"feature": name, "psi": round(psi, 6) if math.isfinite(psi) else None})
    results.sort(key=lambda r: -(r["psi"] or 0.0))
    flagged = [r["feature"] for r in results if (r["psi"] or 0.0) > threshold]
    return {
        "n_ref": int(ref_matrix.shape[0]),
        "n_cur": int(cur_matrix.shape[0]),
        "n_features": len(feature_names),
        "psi_threshold": threshold,
        "features": results,
        "denylist_suggestions": flagged,
        "n_flagged": len(flagged),
    }


# ---------------------------------------------------------------------------
# Permutation importance (sklearn, no extra deps)
# ---------------------------------------------------------------------------

def compute_permutation_importance(
    model: Any,
    X: np.ndarray,
    y: np.ndarray,
    feature_names: Sequence[str],
    n_repeats: int = 5,
    scoring: str = "roc_auc",
) -> list[dict[str, Any]]:
    """Run sklearn permutation_importance and return ranked feature list."""
    from sklearn.inspection import permutation_importance  # type: ignore
    logger.info("Running permutation importance: %d samples, %d features, %d repeats", len(X), X.shape[1], n_repeats)
    result = permutation_importance(model, X, (y > 0).astype(int), n_repeats=n_repeats, scoring=scoring, n_jobs=-1)
    rows = [
        {
            "feature": name,
            "importance_mean": float(result.importances_mean[i]),
            "importance_std": float(result.importances_std[i]),
        }
        for i, name in enumerate(feature_names)
    ]
    rows.sort(key=lambda r: -r["importance_mean"])
    return rows


# ---------------------------------------------------------------------------
# Data loader (signals:of:inputs Redis stream)
# ---------------------------------------------------------------------------

def _load_recent_signals(lookback_days: int, feature_names: Sequence[str]) -> np.ndarray | None:
    """Load recent signals from Redis and build feature matrix."""
    try:
        import redis

        r_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        r = redis.from_url(r_url, decode_responses=True)
        stream_key = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
        now_ms = int(__import__("time").time() * 1000)
        from_ms = now_ms - lookback_days * 86_400_000
        entries = r.xrange(stream_key, min=f"{from_ms}-0", max="+", count=50_000)
    except Exception as exc:
        logger.error("Failed to load signals from Redis: %s", exc)
        return None

    if not entries:
        logger.warning("No signals found in last %d days", lookback_days)
        return None

    rows: list[list[float]] = []
    for _eid, fields in entries:
        try:
            indicators_raw = fields.get("indicators") or fields.get("indicators_json")
            if not indicators_raw:
                continue
            ind = json.loads(indicators_raw)
            row = [float(ind.get(k) or 0.0) for k in feature_names]
            rows.append(row)
        except Exception:
            continue

    if not rows:
        return None
    logger.info("Loaded %d signal rows from Redis (%d-day lookback)", len(rows), lookback_days)
    return np.array(rows, dtype=np.float64)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Feature PSI drift + permutation importance report")
    parser.add_argument("--model", required=True, help="Path to .joblib model artifact")
    parser.add_argument("--lookback_days", type=int, default=7, help="Days of recent signals for current distribution")
    parser.add_argument("--ref_lookback_days", type=int, default=30, help="Days of signals as reference (training proxy)")
    parser.add_argument("--psi_threshold", type=float, default=0.2, help="PSI > threshold → denylist suggestion")
    parser.add_argument("--permutation_importance", action="store_true", help="Also compute permutation importance")
    parser.add_argument("--out_json", type=str, default="", help="Write report to this JSON file (default: stdout)")
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
    schema_hash = pack.get("schema_hash", "unknown")
    feature_cols_hash = pack.get("feature_cols_hash", "unknown")
    feature_schema_ver = pack.get("feature_schema_ver", "unknown")

    logger.info("Model: schema_ver=%s schema_hash=%s feature_cols_hash=%s n_features=%d",
                feature_schema_ver, schema_hash, feature_cols_hash, len(feature_names))

    # Load distributions from Redis
    cur_matrix = _load_recent_signals(args.lookback_days, feature_names)
    ref_matrix = _load_recent_signals(args.ref_lookback_days, feature_names)

    report: dict[str, Any] = {
        "model_path": args.model,
        "feature_schema_ver": feature_schema_ver,
        "schema_hash": schema_hash,
        "feature_cols_hash": feature_cols_hash,
        "n_features": len(feature_names),
        "lookback_days": args.lookback_days,
        "ref_lookback_days": args.ref_lookback_days,
    }

    # PSI
    if cur_matrix is not None and ref_matrix is not None:
        psi_report = compute_psi_report(ref_matrix, cur_matrix, feature_names, args.psi_threshold)
        report["psi"] = psi_report
        logger.info("PSI: %d/%d features flagged (PSI > %.2f)", psi_report["n_flagged"], len(feature_names), args.psi_threshold)
        if psi_report["denylist_suggestions"]:
            logger.warning("Denylist suggestions (high PSI): %s", psi_report["denylist_suggestions"][:10])
    else:
        report["psi"] = {"error": "insufficient data for PSI computation"}

    # Feature importance from artifact (gain)
    if "feature_importance_gain" in pack:
        report["feature_importance_gain_top20"] = list(pack["feature_importance_gain"].items())[:20]

    # Permutation importance
    if args.permutation_importance and model is not None and cur_matrix is not None:
        try:
            from sklearn.preprocessing import RobustScaler  # type: ignore
            # Apply robust scaler params if available
            scaler_params = pack.get("robust_scaler_params") or {}
            X_eval = cur_matrix.copy()
            for j, name in enumerate(feature_names):
                sp = scaler_params.get(name)
                if sp:
                    med = float(sp.get("median", 0.0) or 0.0)
                    iqr = float(sp.get("iqr", 1.0) or 1.0)
                    X_eval[:, j] = (X_eval[:, j] - med) / max(iqr, 1e-9)

            # For permutation importance we need labels — use a simple proxy:
            # predict with model to get pseudo-labels (not ideal but informative for ranking)
            y_proxy = (model.predict(X_eval) > 0.5).astype(int)
            if y_proxy.mean() > 0.01:
                pi = compute_permutation_importance(model, X_eval, y_proxy, feature_names)
                report["permutation_importance_top20"] = pi[:20]
        except Exception as exc:
            logger.warning("Permutation importance failed: %s", exc)
            report["permutation_importance_error"] = str(exc)

    # Output
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
