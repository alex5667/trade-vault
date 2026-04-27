from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingRegressor

from services.ml_calibration import fit_platt_logit, brier_score, ece_score, PlattLogitCalibrator
import math
from core.ml_model_types import UtilMHModelV1

# ---------------------------------------------------------------------------
# GPU backend detection (lazy — no hard dependency)
# Priority: torch.cuda > cupy for XGBoost; cupy mandatory for cuML/Ridge
# ---------------------------------------------------------------------------
_GPU_XGBOOST = False
_GPU_CUML = False

# XGBoost GPU via torch.cuda (primary) or cupy (secondary)
try:
    import xgboost as _xgb  # noqa: F401
    # Prefer torch.cuda — always available when GPU_ENABLED=1
    try:
        import torch as _torch
        if _torch.cuda.is_available():
            _GPU_XGBOOST = True
    except Exception:
        pass
    # Fallback: cupy
    if not _GPU_XGBOOST:
        try:
            import cupy as _cp
            if _cp.cuda.is_available():
                _GPU_XGBOOST = True
        except Exception:
            pass
except Exception:
    pass

# cuML (RAPIDS) GPU Ridge/Scaler — requires cupy mandatory
try:
    import cuml  # noqa: F401  # RAPIDS cuML — GPU Ridge/Scaler
    import cupy as _cp
    if _cp.cuda.is_available():
        _GPU_CUML = True
except Exception:
    pass

_GPU_ENABLED = bool(int(os.getenv("GPU_ENABLED", "1") or "1")) and (_GPU_XGBOOST or _GPU_CUML)

import logging as _logging
_log = _logging.getLogger("train_ml_gpu")
_log.info(f"GPU training: xgboost={_GPU_XGBOOST}, cuml={_GPU_CUML}, enabled={_GPU_ENABLED}")


def _fit_ridge(X: np.ndarray, y: np.ndarray) -> Any:
    """Fit Ridge regression with standardization.

    Uses cuML GPU Ridge when available (20-50x faster for N>50k),
    falls back to sklearn transparently.
    """
    if _GPU_ENABLED and _GPU_CUML and len(X) > 5_000:
        try:
            from cuml.linear_model import Ridge as CuRidge
            from cuml.preprocessing import StandardScaler as CuScaler
            import cupy as cp
            X_gpu = cp.asarray(X, dtype=cp.float32)
            y_gpu = cp.asarray(y, dtype=cp.float32)
            scaler = CuScaler()
            X_scaled = scaler.fit_transform(X_gpu)
            model = CuRidge(alpha=3.0).fit(X_scaled, y_gpu)
            # Wrap in a CPU-compatible sklearn Pipeline for serving
            # cuML models export to numpy-compatible predict()
            # We keep a shim that applies GPU scaler at predict time
            class _CuRidgeWrapper:
                def __init__(self, sc, m):
                    self._sc, self._m = sc, m
                def predict(self, X):
                    import cupy as cp
                    X_g = cp.asarray(np.asarray(X), dtype=cp.float32)
                    return cp.asnumpy(self._m.predict(self._sc.transform(X_g))).astype(np.float64)
            _log.info(f"Ridge: cuML GPU fit on N={len(X)}")
            return _CuRidgeWrapper(scaler, model)
        except Exception as e:
            _log.warning(f"cuML Ridge failed, falling back to sklearn: {e}")

    return Pipeline([("scaler", StandardScaler()), ("ridge", Ridge(alpha=3.0))]).fit(X, y)


def _fit_gbdt(X: np.ndarray, y: np.ndarray) -> Any:
    """Fit GBDT with robust params for noisy utility targets.

    Uses XGBoost GPU (device='cuda') when available (10-100x faster),
    falls back to sklearn HistGradientBoosting transparently.
    Model exports to joblib-compatible predict() in both cases.
    """
    if _GPU_ENABLED and _GPU_XGBOOST and len(X) > 1_000:
        try:
            import xgboost as xgb
            model = xgb.XGBRegressor(
                n_estimators=450,
                max_leaves=63,
                learning_rate=0.05,
                reg_lambda=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                tree_method="hist",
                device="cuda",      # GPU acceleration
                verbosity=0,
            )
            model.fit(X, y)
            _log.info(f"GBDT: XGBoost GPU fit on N={len(X)}, features={X.shape[1]}")
            return model
        except Exception as e:
            _log.warning(f"XGBoost GPU failed, falling back to sklearn: {e}")

    return HistGradientBoostingRegressor(
        max_leaf_nodes=63,
        learning_rate=0.05,
        max_iter=450,
        max_depth=None,
        l2_regularization=0.1,
    ).fit(X, y)


def _sigmoid(x: float) -> float:
    """Stable sigmoid matching MLConfirmGate."""
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _scale_p_edge(score: float) -> float:
    """Replicate MLConfirmGate scaling logic."""
    base_scale = 2.5
    if score < -5.0:
        scale_factor = base_scale * 1.6
    elif score > 5.0:
        scale_factor = base_scale * 0.8
    else:
        scale_factor = base_scale
    
    scaled = float(score) * scale_factor
    p = _sigmoid(scaled)
    if p == 0.0 and score > -1e17:
        p = max(1e-6, _sigmoid(scaled * 1.1))
    return p


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, help="Input parquet dataset")
    ap.add_argument("--out-dir", required=True, help="Output directory for model and meta")
    ap.add_argument("--time-col", default="ts_ms", help="Time column name")
    ap.add_argument("--horizons", default=os.getenv("TB_HORIZONS_MS", "60000,180000,300000"), help="Comma-separated horizons in ms")
    ap.add_argument("--unc-k", type=float, default=float(os.getenv("UTIL_UNC_K", "0.5")), help="Uncertainty penalty coefficient")
    ap.add_argument("--splits", type=int, default=int(os.getenv("ML_SPLITS", "5")), help="Number of CV splits")
    ap.add_argument("--purge-ms", type=int, default=int(os.getenv("ML_PURGE_MS", "180000")), help="Purge window in ms")
    ap.add_argument("--embargo-ms", type=int, default=int(os.getenv("ML_EMBARGO_MS", "60000")), help="Embargo window in ms")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    df = pd.read_parquet(args.dataset)
    if df.empty:
        print(f"ERROR: Dataset {args.dataset} is empty. No training possible.")
        return
    df = df.sort_values(args.time_col).reset_index(drop=True)

    horizons = [int(x) for x in args.horizons.split(",") if x.strip().isdigit()]
    if not horizons:
        raise SystemExit("no horizons")

    # Feature columns (drop labels/targets + raw text cols)
    drop_cols = {"sid", "symbol", args.time_col, "scenario_v4"}
    for c in df.columns:
        if c.startswith("util_r_") or c.startswith("y_"):
            drop_cols.add(c)
    # Keep scenario one-hots if present
    feature_cols = [c for c in df.columns if c not in drop_cols]

    X = df[feature_cols].to_numpy(dtype=np.float32)
    ts = df[args.time_col].astype("int64").to_numpy()

    split = PurgedEmbargoTimeSeriesSplitV2(n_splits=args.splits, purge_ms=args.purge_ms, embargo_ms=args.embargo_ms)

    ridge: Dict[int, Any] = {}
    gbdt: Dict[int, Any] = {}

    metrics: Dict[str, Any] = {"eval_last_split": {}, "horizons": horizons, "reliability": {}}
    calibrators: Dict[int, PlattLogitCalibrator] = {}

    # OOF accumulation for calibration
    # dict[h] -> (list_pred, list_y)
    oof_data: Dict[int, Dict[str, List[float]]] = {h: {"pred": [], "y": []} for h in horizons}

    for tr, te in split.split(ts):
        if len(te) < 10:
             continue
        
        for h in horizons:
            ycol = f"util_r_{h}"
            if ycol not in df.columns:
                 continue
            y = df[ycol].astype(float).to_numpy()
            
            # Fit temp models on tr
            r_tmp = _fit_ridge(X[tr], y[tr])
            g_tmp = _fit_gbdt(X[tr], y[tr])
            
            # Predict on te
            p_te = 0.5 * (r_tmp.predict(X[te]) + g_tmp.predict(X[te]))
            
            metrics["eval_last_split"][str(h)] = {"mae_util": float(np.mean(np.abs(p_te - y[te]))), "n": int(len(te))}
            
            # Store for calibration
            oof_data[h]["pred"].extend([float(x) for x in p_te])
            oof_data[h]["y"].extend([float(x) for x in y[te]])

    # Fit calibration on OOF
    for h in horizons:
        preds = oof_data[h]["pred"]
        targets = oof_data[h]["y"]
        if not preds:
            continue
            
        # 1. Scale scores to p_raw
        p_raw = [_scale_p_edge(s) for s in preds]
        
        # 2. Binary target (util > 0)
        y_bin = [1 if t > 0 else 0 for t in targets]
        
        # 3. Fit Platt
        cal = fit_platt_logit(p_raw, y_bin, l2=0.01)
        calibrators[h] = cal
        
        # 4. Metrics
        p_cal = cal.apply(p_raw)
        brier = brier_score(p_cal, y_bin)
        ece, _ = ece_score(p_cal, y_bin)
        
        metrics["reliability"][str(h)] = {
            "brier": float(brier),
            "ece": float(ece),
            "n": len(preds),
            "a": float(cal.a),
            "b": float(cal.b)
        }
        print(f"Horizon {h}ms: Brier={brier:.4f}, ECE={ece:.4f}, a={cal.a:.3f}, b={cal.b:.3f}")

    # Final fit on all data

    # Final fit on all data
    for h in horizons:
        ycol = f"util_r_{h}"
        y = df[ycol].astype(float).to_numpy()
        ridge[h] = _fit_ridge(X, y)
        gbdt[h] = _fit_gbdt(X, y)

    model = UtilMHModelV1(feature_cols=feature_cols, horizons=horizons, unc_k=float(args.unc_k), ridge=ridge, gbdt=gbdt)
    joblib.dump(model, os.path.join(args.out_dir, "model.joblib"))

    meta = {
        "kind": "util_mh_v1",
        "created_ms": int(__import__("time").time() * 1000),
        "horizons": horizons,
        "unc_k": float(args.unc_k),
        "time_col": args.time_col,
        "feature_cols": feature_cols,
        "metrics": metrics,
        # Save PRIMARY calibrator (use 180s or first horizon as default for gate if kind=util_mh_v1)
        "calibrator": calibrators.get(horizons[0]).to_dict() if calibrators.get(horizons[0]) else None,
        # Save all just in case
        "calibrators_by_horizon": {h: c.to_dict() for h, c in calibrators.items()},
    }
    with open(os.path.join(args.out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

