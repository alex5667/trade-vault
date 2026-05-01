#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
train_edge_stack_v1.py

Train OOF stacking model (LR + GBDT -> meta LR) for MLConfirmGate kind=edge_stack_v1.

Input:
  NDJSON dataset with numeric features + labels.
  Рекомендуется использовать ваш dataset.ndjson (после build_of_dataset / tb_labeling),
  либо расширенный экспорт closed_trades с подготовленными фичами.

Output:
  model.joblib: dict-pack
    {
      "schema_version": 1,
      "kind": "edge_stack_v1",
      "feature_cols": [...],
      "lr": <sklearn LR>,
      "gbdt": <CatBoost|HGBDT>,
      "meta": <sklearn LR>
    }

OOF rule:
  meta обучается только на OOF предиктах base моделей.
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import math
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, brier_score_loss, average_precision_score, log_loss
from sklearn.preprocessing import RobustScaler
from sklearn.pipeline import Pipeline

try:
    from sklearn.ensemble import HistGradientBoostingClassifier
except Exception:
    HistGradientBoostingClassifier = None

try:
    from catboost import CatBoostClassifier
except Exception:
    CatBoostClassifier = None

# Import calibration utilities
from services.ml_calibration import fit_platt_logit, PlattLogitCalibrator


def read_ndjson(path: str) -> pd.DataFrame:
    """Читает NDJSON файл в DataFrame."""
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return pd.DataFrame(rows)


def pick_feature_cols(df: pd.DataFrame, drop: set) -> List[str]:
    """Выбирает числовые колонки, исключая drop."""
    cols = []
    for c in df.columns:
        if c in drop:
            continue
        if df[c].dtype == object:
            continue
        cols.append(c)
    return cols


def day_group(ts_ms: np.ndarray) -> np.ndarray:
    """Группирует по UTC дню."""
    return (ts_ms // 86_400_000).astype(np.int64)


def walk_forward_splits(groups: np.ndarray, n_splits: int) -> List[Tuple[np.ndarray, np.ndarray]]:
    """Walk-forward split по дням."""
    # unique days sorted
    ug = np.unique(groups)
    ug.sort()
    if len(ug) < max(5, n_splits + 1):
        # fallback: single split 80/20
        cut = int(len(groups) * 0.8)
        idx = np.arange(len(groups))
        return [(idx[:cut], idx[cut:])]

    # split by days
    folds = np.array_split(ug, n_splits + 1)  # last fold used as validation in each step
    splits = []
    for i in range(1, len(folds)):
        train_days = np.concatenate(folds[:i])
        val_days = folds[i]
        tr = np.where(np.isin(groups, train_days))[0]
        va = np.where(np.isin(groups, val_days))[0]
        if len(tr) == 0 or len(va) == 0:
            continue
        splits.append((tr, va))
    return splits


def ece_score(y_true: np.ndarray, p: np.ndarray, n_bins: int = 10) -> float:
    """Expected Calibration Error."""
    p = np.clip(p, 0.0, 1.0)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        m = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if m.sum() == 0:
            continue
        acc = float(y_true[m].mean())
        conf = float(p[m].mean())
        w = float(m.mean())
        ece += w * abs(acc - conf)
    return float(ece)


def make_lr() -> Pipeline:
    """Создает Pipeline с LR и RobustScaler."""
    return Pipeline([
        ("scaler", RobustScaler(with_centering=True, with_scaling=True, quantile_range=(25.0, 75.0))),
        ("lr", LogisticRegression(
            C=1.0,
            solver="lbfgs",
            max_iter=500,
            class_weight="balanced",
            random_state=42
        ))
    ])


def _detect_xgboost_device() -> str:
    """Detect if GPU is available for XGBoost; return 'cuda' or 'cpu'."""
    try:
        import torch as _t
        if _t.cuda.is_available():
            return "cuda"
    except Exception:
        pass
    try:
        import cupy as _cp
        if _cp.cuda.is_available():
            return "cuda"
    except Exception:
        pass
    return "cpu"


def make_gbdt():
    """Создает GBDT (XGBoost с авто-определением GPU, иначе CatBoost или HistGradientBoostingClassifier)."""
    try:
        import xgboost as xgb
        return xgb.XGBClassifier(
            n_estimators=800,
            max_depth=6,
            learning_rate=0.05,
            tree_method="hist",
            device=_detect_xgboost_device(),
            eval_metric="logloss",
            random_state=42
        )
    except Exception:
        pass

    if CatBoostClassifier is not None:
        return CatBoostClassifier(
            depth=6,
            learning_rate=0.05,
            iterations=800,
            loss_function="Logloss",
            eval_metric="AUC",
            verbose=False,
            random_seed=42
        )
    if HistGradientBoostingClassifier is None:
        raise RuntimeError("No XGBoost, CatBoost and no HistGradientBoostingClassifier available")
    return HistGradientBoostingClassifier(
        max_depth=6,
        learning_rate=0.05,
        max_iter=800,
        random_state=42
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="NDJSON input file")
    ap.add_argument("--out", dest="out_dir", required=True, help="Output directory")
    ap.add_argument("--label-col", default="y_edge", help="binary label column (0/1)")
    ap.add_argument("--time-col", default="ts_ms", help="timestamp column (ms)")
    ap.add_argument("--n-splits", type=int, default=5, help="number of time-based splits for OOF")
    ap.add_argument("--seed", type=int, default=42, help="random seed")
    args = ap.parse_args()
    
    # Set random seed
    np.random.seed(args.seed)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = read_ndjson(args.in_path)
    if args.label_col not in df.columns:
        raise RuntimeError(f"label col {args.label_col} not found")

    df = df.dropna(subset=[args.label_col, args.time_col]).copy()
    y = df[args.label_col].astype(int).values
    ts = df[args.time_col].astype(np.int64).values

    drop = {"sid", "symbol", "scenario_v4", args.label_col, args.time_col}
    feature_cols = pick_feature_cols(df, drop=drop)
    if not feature_cols:
        raise RuntimeError("no numeric feature cols detected")

    X = df[feature_cols].astype(float).values
    groups = day_group(ts)
    splits = walk_forward_splits(groups, n_splits=args.n_splits)

    # OOF preds
    oof_lr = np.full(len(df), np.nan, dtype=float)
    oof_gb = np.full(len(df), np.nan, dtype=float)

    for k, (tr, va) in enumerate(splits):
        lr = make_lr()
        gb = make_gbdt()
        lr.fit(X[tr], y[tr])
        gb.fit(X[tr], y[tr])
        oof_lr[va] = lr.predict_proba(X[va])[:, 1]
        # CatBoost uses predict_proba, HGBDT uses predict_proba
        oof_gb[va] = gb.predict_proba(X[va])[:, 1]

    m = np.isfinite(oof_lr) & np.isfinite(oof_gb)
    if m.sum() < 50:
        raise RuntimeError(f"too few OOF rows: {int(m.sum())}")

    Z = np.stack([oof_lr[m], oof_gb[m]], axis=1)
    y_m = y[m]

    meta = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=500,
        class_weight="balanced",
        random_state=42
    )
    meta.fit(Z, y_m)

    p_meta_oof = meta.predict_proba(Z)[:, 1]

    # Fit calibrator on OOF meta probabilities
    cal = fit_platt_logit(p_meta_oof.tolist(), y_m.tolist(), l2=1e-3, max_iter=50)
    p_meta_cal_oof = np.array([cal.apply_one(float(p)) for p in p_meta_oof], dtype=np.float32)

    # Metrics helper
    def _safe_auc(y_true, p):
        try:
            if len(np.unique(y_true)) > 1:
                return float(roc_auc_score(y_true, p))
            return None
        except Exception:
            return None

    report = {
        "n": int(m.sum()),
        "pos_rate": float(np.mean(y_m)),
        "auc_lr_oof": _safe_auc(y_m, oof_lr[m]),
        "auc_gbdt_oof": _safe_auc(y_m, oof_gb[m]),
        "auc_meta_oof": _safe_auc(y_m, p_meta_oof),
        "auc_cal_oof": _safe_auc(y_m, p_meta_cal_oof),
        "prauc_cal_oof": float(average_precision_score(y_m, p_meta_cal_oof)) if len(np.unique(y_m)) > 1 else None,
        "brier_cal_oof": float(brier_score_loss(y_m, np.clip(p_meta_cal_oof, 0, 1))),
        "logloss_cal_oof": float(log_loss(y_m, np.clip(p_meta_cal_oof, 1e-6, 1 - 1e-6))),
        "ece10_meta_oof": float(ece_score(y_m, p_meta_oof, n_bins=10)),
        "ece10_cal_oof": float(ece_score(y_m, p_meta_cal_oof, n_bins=10)),
        "calibrator": cal.to_dict(),
        "folds": int(len(splits)),
        "seed": 42,
        "gbdt_type": "catboost" if CatBoostClassifier is not None else "hgbdt"
    }

    # Fit final models on full data
    lr_final = make_lr()
    gb_final = make_gbdt()
    lr_final.fit(X, y)
    gb_final.fit(X, y)

    pack = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "feature_cols": feature_cols,
        "lr": lr_final,
        "gbdt": gb_final,
        "meta": meta,
        # Embed calibrator in pack so runtime can load from model pack
        "calibrator": cal.to_dict(),
        "train_meta": {
            "input": str(args.in_path),
            "seed": 42,
            "n_splits": int(args.n_splits),
            "label_col": args.label_col,
            "time_col": args.time_col,
        },
        "report": report,
    }

    import joblib
    import time
    
    model_path = out_dir / "model.joblib"
    joblib.dump(pack, model_path, compress=3)
    (out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    
    # Generate suggested cfg snippet
    cfg_suggested = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "run_id": out_dir.name,
        "created_ms": get_ny_time_millis(),
        "model_path": str(model_path),
        "mode": "SHADOW",
        "enforce_share": 0.0,
        "p_min": 0.55,
        "p_min_by_bucket": {"trend": 0.55, "range": 0.60, "other": 0.52, "news": 0.65},
        "hard_p_min_floor": 0.50,
        # Calibrator is embedded in model pack, but can also be set explicitly:
        # "calibrator": cal.to_dict(),
    }
    
    print(json.dumps({
        "model_path": str(model_path),
        "metrics": report,
        "suggested_cfg": cfg_suggested
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()




