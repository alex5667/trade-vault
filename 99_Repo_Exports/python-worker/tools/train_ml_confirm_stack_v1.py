from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, log_loss, brier_score_loss
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingClassifier

from core.ml_feature_schema_v3 import MLFeatureSchemaV3
from core.purged_embargo_split import PurgedEmbargoTimeSeriesSplit


def ece(p: np.ndarray, y: np.ndarray, n_bins: int = 10) -> float:
    bins = np.minimum(n_bins - 1, np.maximum(0, (p * n_bins).astype(int)))
    e = 0.0
    n = len(y)
    for b in range(n_bins):
        idx = (bins == b)
        if not idx.any():
            continue
        avg_p = float(p[idx].mean())
        avg_y = float(y[idx].mean())
        e += (idx.sum() / n) * abs(avg_p - avg_y)
    return float(e)


def _fit_base_lr(X: np.ndarray, y: np.ndarray) -> Any:
    base = Pipeline([
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(C=1.0, max_iter=500, n_jobs=1, class_weight="balanced")),
    ])
    return CalibratedClassifierCV(base, method="sigmoid", cv=3).fit(X, y)


def _fit_base_gbdt(X: np.ndarray, y: np.ndarray) -> Any:
    base = HistGradientBoostingClassifier(max_leaf_nodes=31, learning_rate=0.05, max_iter=300, l2_regularization=1e-4)
    return CalibratedClassifierCV(base, method="sigmoid", cv=3).fit(X, y)


@dataclass
class StackingModel:
    schema: str
    feature_names: List[str]
    base_lr: Any
    base_gbdt: Any
    meta_lr: Any
    calibrator: Any

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        p1 = self.base_lr.predict_proba(X)[:, 1]
        p2 = self.base_gbdt.predict_proba(X)[:, 1]
        Z = np.stack([p1, p2], axis=1)
        raw = self.meta_lr.predict_proba(Z)[:, 1]
        p = self.calibrator.predict_proba(raw.reshape(-1, 1))[:, 1]
        return np.stack([1.0 - p, p], axis=1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--time-col", default="ts_ms")
    ap.add_argument("--label", default="y_edge")
    ap.add_argument("--splits", type=int, default=int(os.getenv("ML_SPLITS", "5") or 5))
    ap.add_argument("--purge-ms", type=int, default=int(os.getenv("ML_PURGE_MS", "180000") or 180000))
    ap.add_argument("--embargo-ms", type=int, default=int(os.getenv("ML_EMBARGO_MS", "60000") or 60000))
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    df = pd.read_parquet(args.dataset).sort_values(args.time_col).reset_index(drop=True)
    schema = MLFeatureSchemaV3()
    X = np.array([schema.vectorize_row(r) for r in df.to_dict(orient="records")], dtype=np.float32)
    ts = df[args.time_col].astype("int64").to_numpy()
    y = df[args.label].astype(int).to_numpy()

    splitter = PurgedEmbargoTimeSeriesSplit(n_splits=args.splits, purge_ms=args.purge_ms, embargo_ms=args.embargo_ms)

    oof_p_lr = np.full(len(df), np.nan, dtype=np.float64)
    oof_p_gb = np.full(len(df), np.nan, dtype=np.float64)
    oof_y = np.full(len(df), np.nan, dtype=np.float64)

    fold_metrics: List[Dict[str, Any]] = []
    last_tr = None
    last_te = None

    fold = 0
    for tr_idx, te_idx in splitter.split(ts):
        fold += 1
        last_tr, last_te = tr_idx, te_idx

        lr = _fit_base_lr(X[tr_idx], y[tr_idx])
        gb = _fit_base_gbdt(X[tr_idx], y[tr_idx])

        p_lr = lr.predict_proba(X[te_idx])[:, 1]
        p_gb = gb.predict_proba(X[te_idx])[:, 1]
        oof_p_lr[te_idx] = p_lr
        oof_p_gb[te_idx] = p_gb
        oof_y[te_idx] = y[te_idx]

        p_avg = 0.5 * (p_lr + p_gb)
        fold_metrics.append({
            "fold": fold,
            "n": int(len(te_idx)),
            "pr_auc_avg": float(average_precision_score(y[te_idx], p_avg)),
            "logloss_avg": float(log_loss(y[te_idx], p_avg, labels=[0, 1])),
            "brier_avg": float(brier_score_loss(y[te_idx], p_avg)),
            "ece_avg": float(ece(p_avg, y[te_idx])),
        })

    mask = ~np.isnan(oof_p_lr) & ~np.isnan(oof_p_gb) & ~np.isnan(oof_y)
    if mask.sum() < 200:
        raise RuntimeError("not_enough_oof_rows_for_meta")
    Z_oof = np.stack([oof_p_lr[mask], oof_p_gb[mask]], axis=1)
    y_oof = oof_y[mask].astype(int)

    meta_lr = LogisticRegression(C=1.0, max_iter=500, n_jobs=1, class_weight="balanced")
    meta_lr.fit(Z_oof, y_oof)

    if last_tr is None or last_te is None:
        raise RuntimeError("not_enough_splits")

    # Calibrate meta on last split (freshest)
    lr_last = _fit_base_lr(X[last_tr], y[last_tr])
    gb_last = _fit_base_gbdt(X[last_tr], y[last_tr])
    p_lr = lr_last.predict_proba(X[last_te])[:, 1]
    p_gb = gb_last.predict_proba(X[last_te])[:, 1]
    raw = meta_lr.predict_proba(np.stack([p_lr, p_gb], axis=1))[:, 1]
    calib = LogisticRegression(C=1.0, max_iter=300)
    calib.fit(raw.reshape(-1, 1), y[last_te])

    # Fit final base models on full dataset
    base_lr_full = _fit_base_lr(X, y)
    base_gb_full = _fit_base_gbdt(X, y)

    model = StackingModel(
        schema="MLFeatureSchemaV3",
        feature_names=schema.feature_names(),
        base_lr=base_lr_full,
        base_gbdt=base_gb_full,
        meta_lr=meta_lr,
        calibrator=calib,
    )

    # Evaluate stacked on last split
    p_lr = lr_last.predict_proba(X[last_te])[:, 1]
    p_gb = gb_last.predict_proba(X[last_te])[:, 1]
    raw = meta_lr.predict_proba(np.stack([p_lr, p_gb], axis=1))[:, 1]
    p_stack = calib.predict_proba(raw.reshape(-1, 1))[:, 1]

    metrics = {
        "n": int(len(last_te)),
        "pr_auc": float(average_precision_score(y[last_te], p_stack)),
        "logloss": float(log_loss(y[last_te], p_stack, labels=[0, 1])),
        "brier": float(brier_score_loss(y[last_te], p_stack)),
        "ece": float(ece(p_stack, y[last_te])),
    }

    model_path = os.path.join(args.out_dir, "model.joblib")
    meta_path = os.path.join(args.out_dir, "meta.json")
    joblib.dump(model, model_path)

    meta = {
        "kind": "stacking_oof_lr_gbdt_platt",
        "schema": "MLFeatureSchemaV3",
        "feature_names": schema.feature_names(),
        "label": args.label,
        "n_rows": int(len(df)),
        "pos_rate": float(df[args.label].mean()) if len(df) else 0.0,
        "split": {"type": "PurgedEmbargoTimeSeriesSplit", "splits": int(args.splits), "purge_ms": int(args.purge_ms), "embargo_ms": int(args.embargo_ms)},
        "fold_metrics_avg": fold_metrics,
        "stack_eval_last_split": metrics,
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps({"model": model_path, "meta": meta_path}, ensure_ascii=False))


if __name__ == "__main__":
    main()

