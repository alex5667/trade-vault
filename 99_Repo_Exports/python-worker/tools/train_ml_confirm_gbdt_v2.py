from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, log_loss, brier_score_loss
from sklearn.model_selection import TimeSeriesSplit

from core.ml_feature_schema_v2 import MLFeatureSchemaV2
from ml_core.purged_cv import purged_kfold_time_series


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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--time-col", default="ts_ms")
    ap.add_argument("--splits", type=int, default=5)
    ap.add_argument("--max-leaf-nodes", type=int, default=31)
    ap.add_argument("--learning-rate", type=float, default=0.05)
    ap.add_argument("--max-depth", type=int, default=None)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    df = pd.read_parquet(args.dataset).sort_values(args.time_col)
    schema = MLFeatureSchemaV2()
    X = np.array([schema.vectorize_row(r) for r in df.to_dict(orient="records")], dtype=np.float32)
    y = df["y"].astype(int).to_numpy()

    base = HistGradientBoostingClassifier(
        max_leaf_nodes=args.max_leaf_nodes,
        learning_rate=args.learning_rate,
        max_depth=args.max_depth,
        max_iter=300,
        l2_regularization=1e-4,
    )

    clf = CalibratedClassifierCV(base, method="sigmoid", cv=3)
    clf.fit(X, y)

    use_purged = os.getenv("ML_PURGED_CV_ENABLE", "1").strip().lower() in {"1", "true", "yes"}
    embargo_ms = int(os.getenv("ML_PURGED_CV_EMBARGO_MS", "60000"))
    has_t1 = "tb_t_hit_ms" in df.columns

    folds = None
    if use_purged and has_t1:
        folds = purged_kfold_time_series(
            ts_ms=df[args.time_col].astype("int64").to_numpy(),
            t1_ms=df["tb_t_hit_ms"].astype("int64").to_numpy(),
            n_splits=int(args.splits),
            embargo_ms=int(embargo_ms),
        )
    else:
        tscv = TimeSeriesSplit(n_splits=args.splits)

    fold = 0
    metrics: List[Dict[str, Any]] = []
    if folds is not None:
        it = [(f.train_idx, f.test_idx) for f in folds]
        cv_meta = {"type": "PurgedKFold", "splits": int(args.splits), "embargo_ms": int(embargo_ms), "t1_col": "tb_t_hit_ms"}
    else:
        it = tscv.split(X)
        cv_meta = {"type": "TimeSeriesSplit", "splits": int(args.splits)}

    for tr, te in it:
        fold += 1
        base_f = HistGradientBoostingClassifier(
            max_leaf_nodes=args.max_leaf_nodes,
            learning_rate=args.learning_rate,
            max_depth=args.max_depth,
            max_iter=300,
            l2_regularization=1e-4,
        )
        cal = CalibratedClassifierCV(base_f, method="sigmoid", cv=3)
        cal.fit(X[tr], y[tr])
        p = cal.predict_proba(X[te])[:, 1]
        m = {
            "fold": fold,
            "n": int(len(te)),
            "pr_auc": float(average_precision_score(y[te], p)),
            "logloss": float(log_loss(y[te], p, labels=[0, 1])),
            "brier": float(brier_score_loss(y[te], p)),
            "ece": float(ece(p, y[te])),
        }
        metrics.append(m)

    model_path = os.path.join(args.out_dir, "model.joblib")
    meta_path = os.path.join(args.out_dir, "meta.json")

    joblib.dump(clf, model_path)

    meta = {
        "kind": "hgbdt_calibrated_sigmoid",
        "schema": "MLFeatureSchemaV2",
        "feature_names": schema.feature_names(),
        "n_rows": int(len(df)),
        "pos_rate": float(df["y"].mean()) if len(df) else 0.0,
        "time_col": args.time_col,
        "params": {
            "max_leaf_nodes": int(args.max_leaf_nodes),
            "learning_rate": float(args.learning_rate),
            "max_depth": (int(args.max_depth) if args.max_depth is not None else None),
            "max_iter": 300,
        },
        "cv": cv_meta,
        "fold_metrics": metrics,
        "mean": {
            "pr_auc": float(np.mean([m["pr_auc"] for m in metrics])) if metrics else 0.0,
            "logloss": float(np.mean([m["logloss"] for m in metrics])) if metrics else 0.0,
            "brier": float(np.mean([m["brier"] for m in metrics])) if metrics else 0.0,
            "ece": float(np.mean([m["ece"] for m in metrics])) if metrics else 0.0,
        },
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps({"model": model_path, "meta": meta_path}, ensure_ascii=False))


if __name__ == "__main__":
    main()
