from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, log_loss, brier_score_loss
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import HistGradientBoostingClassifier


def _now_ms() -> int:
    """Get current timestamp in milliseconds."""
    import time
    return get_ny_time_millis()


def ece_score(y_true: np.ndarray, p: np.ndarray, n_bins: int = 10) -> float:
    """Calculate Expected Calibration Error (ECE)."""
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        m = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(m):
            continue
        acc = float(np.mean(y_true[m]))
        conf = float(np.mean(p[m]))
        w = float(np.mean(m))
        ece += w * abs(acc - conf)
    return float(ece)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--time-col", default="ts_ms")
    ap.add_argument("--label-col", default="y_edge")  # y_edge or y_util_pos
    ap.add_argument("--splits", type=int, default=int(os.getenv("ML_SPLITS", "5")))
    ap.add_argument("--purge-ms", type=int, default=int(os.getenv("ML_PURGE_MS", "180000")))
    ap.add_argument("--embargo-ms", type=int, default=int(os.getenv("ML_EMBARGO_MS", "60000")))
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    df = pd.read_parquet(args.dataset)
    
    if len(df) == 0:
        raise SystemExit(f"❌ Dataset is empty. Check data export and join process.")
    
    if args.time_col not in df.columns:
        available_cols = ", ".join(df.columns.tolist()[:10])
        raise SystemExit(f"❌ Missing time col '{args.time_col}' in dataset. Available columns: {available_cols}... (total: {len(df.columns)})")
    if args.label_col not in df.columns:
        available_cols = ", ".join(df.columns.tolist()[:10])
        raise SystemExit(f"❌ Missing label col '{args.label_col}' in dataset. Available columns: {available_cols}... (total: {len(df.columns)})")
    
    print(f"📊 Dataset loaded: {len(df)} rows, {len(df.columns)} columns")

    # sort by time
    df = df.sort_values(args.time_col).reset_index(drop=True)

    # Drop non-feature cols
    non_feat = {"sid", "symbol", args.time_col, "tb_label", "tb_r_mult", "tb_ret_bps", "tb_mae_bps", "tb_mfe_bps", "tb_adverse_proxy", "util_r", "y_edge", "y_util_pos"}
    y = df[args.label_col].astype(int).to_numpy()
    X = df[[c for c in df.columns if c not in non_feat]].to_numpy(dtype=float)

    # Time splits
    tss = TimeSeriesSplit(n_splits=max(2, int(args.splits)))
    oof_lr = np.zeros(len(df), dtype=float)
    oof_gbdt = np.zeros(len(df), dtype=float)
    oof_meta = np.zeros(len(df), dtype=float)

    fold_metrics: List[Dict[str, Any]] = []

    # Purge/embargo by time (simple index-based approximation with time gaps)
    ts = df[args.time_col].to_numpy()

    def mask_purge_embargo(train_idx: np.ndarray, test_idx: np.ndarray) -> np.ndarray:
        """Remove train samples within [test_start - purge, test_end + embargo]."""
        t0 = ts[test_idx[0]]
        t1 = ts[test_idx[-1]]
        lo = t0 - int(args.purge_ms)
        hi = t1 + int(args.embargo_ms)
        keep = (ts[train_idx] < lo) | (ts[train_idx] > hi)
        return train_idx[keep]

    for fold, (train_idx, test_idx) in enumerate(tss.split(X), start=1):
        train_idx2 = mask_purge_embargo(train_idx, test_idx)
        if len(train_idx2) < 100:
            continue

        X_tr, y_tr = X[train_idx2], y[train_idx2]
        X_te, y_te = X[test_idx], y[test_idx]

        # Base LR (calibrated)
        lr = LogisticRegression(max_iter=200, n_jobs=1)
        lr_cal = CalibratedClassifierCV(lr, method="sigmoid", cv=3)
        lr_cal.fit(X_tr, y_tr)
        p_lr = lr_cal.predict_proba(X_te)[:, 1]

        # Base GBDT (calibrated)
        gbdt = HistGradientBoostingClassifier(max_depth=6, learning_rate=0.06, max_iter=250)
        gbdt.fit(X_tr, y_tr)
        p_g = gbdt.predict_proba(X_te)[:, 1]
        # Platt for gbdt
        g_platt = LogisticRegression(max_iter=200)
        g_platt.fit(p_g.reshape(-1, 1), y_te)
        p_g_cal = g_platt.predict_proba(p_g.reshape(-1, 1))[:, 1]

        # Meta LR on OOF base preds (train meta on train_idx2 using their own in-fold preds)
        # Approximation: fit meta on train_idx2 using base models' in-sample preds (acceptable for MVP),
        # but for strict OOF stacking you'd run nested OOF. Here we keep it simple and deterministic.
        meta = LogisticRegression(max_iter=200)
        meta.fit(np.column_stack([lr_cal.predict_proba(X_tr)[:, 1], gbdt.predict_proba(X_tr)[:, 1]]), y_tr)
        p_meta = meta.predict_proba(np.column_stack([p_lr, p_g_cal]))[:, 1]

        oof_lr[test_idx] = p_lr
        oof_gbdt[test_idx] = p_g_cal
        oof_meta[test_idx] = p_meta

        fold_metrics.append({
            "fold": fold,
            "n_train": int(len(train_idx2)),
            "n_test": int(len(test_idx)),
            "pr_auc": float(average_precision_score(y_te, p_meta)) if len(np.unique(y_te)) > 1 else 0.0,
            "logloss": float(log_loss(y_te, np.clip(p_meta, 1e-6, 1 - 1e-6))),
            "brier": float(brier_score_loss(y_te, p_meta)),
            "ece": float(ece_score(y_te, p_meta)),
        })

    # Train final models on full data
    lr = LogisticRegression(max_iter=300, n_jobs=1)
    lr_cal = CalibratedClassifierCV(lr, method="sigmoid", cv=5)
    lr_cal.fit(X, y)

    gbdt = HistGradientBoostingClassifier(max_depth=6, learning_rate=0.06, max_iter=300)
    gbdt.fit(X, y)
    p_g_full = gbdt.predict_proba(X)[:, 1]
    g_platt = LogisticRegression(max_iter=300)
    g_platt.fit(p_g_full.reshape(-1, 1), y)

    p_lr_full = lr_cal.predict_proba(X)[:, 1]
    p_g_cal_full = g_platt.predict_proba(p_g_full.reshape(-1, 1))[:, 1]
    meta = LogisticRegression(max_iter=300)
    meta.fit(np.column_stack([p_lr_full, p_g_cal_full]), y)

    p_meta_full = meta.predict_proba(np.column_stack([p_lr_full, p_g_cal_full]))[:, 1]

    metrics = {
        "n": int(len(df)),
        "pos_rate": float(np.mean(y)),
        "pr_auc": float(average_precision_score(y, p_meta_full)) if len(np.unique(y)) > 1 else 0.0,
        "logloss": float(log_loss(y, np.clip(p_meta_full, 1e-6, 1 - 1e-6))),
        "brier": float(brier_score_loss(y, p_meta_full)),
        "ece": float(ece_score(y, p_meta_full)),
        "folds": fold_metrics,
    }

    meta_json = {
        "created_ms": _now_ms(),
        "label_col": args.label_col,
        "time_col": args.time_col,
        "columns_used": [c for c in df.columns if c not in {"sid", "symbol"}],
        "metrics": metrics,
        "version": "tb_stack_v1",
    }

    joblib.dump({"lr_cal": lr_cal, "gbdt": gbdt, "g_platt": g_platt, "meta": meta}, os.path.join(args.out_dir, "model.joblib"))
    with open(os.path.join(args.out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta_json, f, ensure_ascii=False, indent=2)

    # Save OOF preds for audit
    oof_df = pd.DataFrame({
        "ts_ms": df[args.time_col],
        "y": y,
        "p_lr": oof_lr,
        "p_gbdt": oof_gbdt,
        "p_meta": oof_meta,
    })
    oof_df.to_parquet(os.path.join(args.out_dir, "oof_preds.parquet"), index=False)


if __name__ == "__main__":
    main()

