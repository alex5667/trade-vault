#!/usr/bin/env python3
from __future__ import annotations

"""Export a trained UtilMH joblib model to a portable FASTLINEAR JSON.

This is meant to satisfy the online "one function" requirement:
  u = b + dot(w, x_row)

Usage:
  python3 -m tools.export_util_mh_fastlinear \
    --in /var/lib/trade/ml_models/model.joblib \
    --out /var/lib/trade/ml_models/model_fastlinear.json

Notes:
- Works best if your trained model contains per-horizon linear estimators (Ridge/LinearRegression/SGDRegressor).
- If your current util_mh is an ensemble (Ridge + GBDT), export will:
    * prefer linear head if present
    * otherwise you should distill (train a linear student on teacher predictions)
"""


import argparse
import json
import os
from typing import Any
import contextlib

try:
    import joblib  # type: ignore
except Exception:
    joblib = None  # type: ignore


def _get_attr_any(obj: Any, names: list[str]) -> Any:
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return None


def _extract_linear_estimator(model: Any) -> Any | None:
    """Best-effort: unwrap pipelines and return an estimator that has coef_ & intercept_."""
    if model is None:
        return None
    # sklearn Pipeline
    if hasattr(model, "named_steps") and isinstance(model.named_steps, dict):
        # prefer 'model' or last step
        ns = model.named_steps
        if "model" in ns:
            model = ns["model"]
        else:
            with contextlib.suppress(Exception):
                model = list(ns.values())[-1]
    if hasattr(model, "coef_") and hasattr(model, "intercept_"):
        return model
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--kind", default="util_mh_fastlinear_v1")
    args = ap.parse_args()

    if joblib is None:
        raise SystemExit("joblib is required for reading the input model")

    m = joblib.load(args.in_path)

    feature_cols = list(getattr(m, "feature_cols", []) or [])
    horizons_ms = [int(x) for x in (getattr(m, "horizons_ms", []) or [])]

    # try common containers for per-horizon linear heads
    by_h = _get_attr_any(m, [
        "ridge_by_horizon",
        "linear_by_horizon",
        "models_by_horizon",
        "estimators_by_horizon",
        "by_horizon",
    ])

    weights: dict[str, dict[str, Any]] = {}

    if isinstance(by_h, dict) and by_h:
        for h, est in by_h.items():
            h_i = int(h)
            lin = _extract_linear_estimator(est)
            if not lin:
                continue
            coef = [float(x) for x in list(lin.coef_)]
            intercept = float(lin.intercept_)
            weights[str(h_i)] = {"intercept": intercept, "coef": coef, "unc": 0.0}
    else:
        # fallback: maybe the whole model is already linear
        lin = _extract_linear_estimator(m)
        if lin:
            coef = [float(x) for x in list(lin.coef_)]
            intercept = float(lin.intercept_)
            # single horizon unknown -> 0
            weights["0"] = {"intercept": intercept, "coef": coef, "unc": 0.0}

    out = {
        "kind": str(args.kind),
        "feature_cols": feature_cols,
        "horizons_ms": horizons_ms,
        "weights": weights,
        # optional pass-through if your joblib model already stores them:
        "feature_transforms": getattr(m, "feature_transforms", {}) if hasattr(m, "feature_transforms") else {},
        "robust_scaler": getattr(m, "robust_scaler", {}) if hasattr(m, "robust_scaler") else {},
        "spread_bucket_edges": getattr(m, "spread_bucket_edges", None) if hasattr(m, "spread_bucket_edges") else None,
        "session_cfg": getattr(m, "session_cfg", None) if hasattr(m, "session_cfg") else None,
        "liq_cfg": getattr(m, "liq_cfg", None) if hasattr(m, "liq_cfg") else None,
        "source_model": os.path.abspath(args.in_path),
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.out_path)), exist_ok=True)
    with open(args.out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote {args.out_path} (horizons={len(horizons_ms)} weights={len(weights)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())










