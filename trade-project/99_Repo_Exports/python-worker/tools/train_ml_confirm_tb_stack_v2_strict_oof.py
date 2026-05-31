from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from core.purged_embargo_split_v2 import PurgedEmbargoTimeSeriesSplitV2


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


@dataclass
class TBStackModelV2:
    feature_cols: list[str]
    base_lr: Any
    base_gbdt: Any
    meta: Any
    calibrator: Any

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        p1 = self.base_lr.predict_proba(X)[:, 1]
        p2 = self.base_gbdt.predict_proba(X)[:, 1]
        Z = np.column_stack([p1, p2])
        raw = self.meta.predict_proba(Z)[:, 1]
        p = self.calibrator.predict_proba(raw.reshape(-1, 1))[:, 1]
        return np.column_stack([1.0 - p, p])


def _fit_lr(X: np.ndarray, y: np.ndarray) -> Any:
    pipe = Pipeline([
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(C=1.0, max_iter=800, n_jobs=1, class_weight="balanced")),
    ])
    return CalibratedClassifierCV(pipe, method="sigmoid", cv=3).fit(X, y)


def _fit_gbdt(X: np.ndarray, y: np.ndarray) -> Any:
    base = HistGradientBoostingClassifier(max_leaf_nodes=31, learning_rate=0.05, max_iter=350, max_depth=None)
    # Wrap in CV calibrator for stability
    return CalibratedClassifierCV(base, method="sigmoid", cv=3).fit(X, y)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--time-col", default="ts_ms")
    ap.add_argument("--label-col", default="y_util_pos")
    ap.add_argument("--splits", type=int, default=int(os.getenv("ML_SPLITS", "5") or 5))
    ap.add_argument("--purge-ms", type=int, default=int(os.getenv("ML_PURGE_MS", "180000") or 180000))
    ap.add_argument("--embargo-ms", type=int, default=int(os.getenv("ML_EMBARGO_MS", "60000") or 60000))
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    df = pd.read_parquet(args.dataset)
    df = df.sort_values(args.time_col).reset_index(drop=True)

    if args.label_col not in df.columns:
        raise SystemExit(f"missing label: {args.label_col}")

    # Feature columns: drop non-features & all labels/targets
    drop_cols = set([
        "sid","symbol",args.time_col,
        "tb_label","tb_r_mult","tb_ret_bps","tb_mae_bps","tb_mfe_bps","tb_adverse_proxy",
        "util_r","y_edge","y_util_pos",
        "y_edge_60000","y_edge_180000","y_edge_300000",
    ])
    feature_cols = [c for c in df.columns if c not in drop_cols]

    X = df[feature_cols].to_numpy(dtype=np.float32)
    y = df[args.label_col].astype(int).to_numpy()
    ts = df[args.time_col].astype("int64").to_numpy()

    splitter = PurgedEmbargoTimeSeriesSplitV2(n_splits=args.splits, purge_ms=args.purge_ms, embargo_ms=args.embargo_ms)

    oof_p1 = np.full(len(df), np.nan)
    oof_p2 = np.full(len(df), np.nan)
    oof_y = np.full(len(df), np.nan)

    folds: list[dict[str, Any]] = []
    last_tr = None
    last_te = None

    for fold, (tr, te) in enumerate(splitter.split(ts), start=1):
        last_tr, last_te = tr, te
        lr = _fit_lr(X[tr], y[tr])
        gb = _fit_gbdt(X[tr], y[tr])

        p1 = lr.predict_proba(X[te])[:, 1]
        p2 = gb.predict_proba(X[te])[:, 1]

        oof_p1[te] = p1
        oof_p2[te] = p2
        oof_y[te] = y[te]

        p_avg = 0.5 * (p1 + p2)
        folds.append({
            "fold": fold,
            "n_test": int(len(te)),
            "pr_auc_avg": float(average_precision_score(y[te], p_avg)) if len(np.unique(y[te])) > 1 else 0.0,
            "logloss_avg": float(log_loss(y[te], np.clip(p_avg, 1e-6, 1-1e-6))),
            "brier_avg": float(brier_score_loss(y[te], p_avg)),
            "ece_avg": float(ece(p_avg, y[te])),
        })

    mask = ~np.isnan(oof_p1) & ~np.isnan(oof_p2) & ~np.isnan(oof_y)
    if mask.sum() < 500:
        raise SystemExit("not_enough_oof_rows (need >=500)")

    Z = np.column_stack([oof_p1[mask], oof_p2[mask]])
    y_oof = oof_y[mask].astype(int)

    meta = LogisticRegression(C=1.0, max_iter=800, n_jobs=1, class_weight="balanced")
    meta.fit(Z, y_oof)

    # Calibrate meta on the freshest split (last_te) to match live distribution
    if last_tr is None or last_te is None:
        raise SystemExit("not_enough_splits")

    lr_last = _fit_lr(X[last_tr], y[last_tr])
    gb_last = _fit_gbdt(X[last_tr], y[last_tr])
    p1_last = lr_last.predict_proba(X[last_te])[:, 1]
    p2_last = gb_last.predict_proba(X[last_te])[:, 1]
    raw_last = meta.predict_proba(np.column_stack([p1_last, p2_last]))[:, 1]

    calib = LogisticRegression(C=1.0, max_iter=500)
    calib.fit(raw_last.reshape(-1, 1), y[last_te])

    # Fit final base models on full dataset
    base_lr = _fit_lr(X, y)
    base_gb = _fit_gbdt(X, y)

    model = TBStackModelV2(feature_cols=feature_cols, base_lr=base_lr, base_gbdt=base_gb, meta=meta, calibrator=calib)

    # Evaluate on last_te
    p_final = calib.predict_proba(raw_last.reshape(-1, 1))[:, 1]
    metrics = {
        "n_eval": int(len(last_te)),
        "pr_auc": float(average_precision_score(y[last_te], p_final)) if len(np.unique(y[last_te])) > 1 else 0.0,
        "logloss": float(log_loss(y[last_te], np.clip(p_final, 1e-6, 1-1e-6))),
        "brier": float(brier_score_loss(y[last_te], p_final)),
        "ece": float(ece(p_final, y[last_te])),
    }

    joblib.dump(model, os.path.join(args.out_dir, "model.joblib"))

    meta_json = {
        "kind": "tb_stack_v2_strict_oof",
        "label_col": args.label_col,
        "time_col": args.time_col,
        "feature_cols": feature_cols,
        "split": {"type": "PurgedEmbargoTimeSeriesSplitV2", "splits": int(args.splits), "purge_ms": int(args.purge_ms), "embargo_ms": int(args.embargo_ms)},
        "folds": folds,
        "eval_last_split": metrics,
        "created_ms": int(__import__("time").time() * 1000),
    }
    with open(os.path.join(args.out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta_json, f, ensure_ascii=False, indent=2)

    # Save oof for audit
    pd.DataFrame({"ts_ms": ts, "y": y, "p1": oof_p1, "p2": oof_p2}).to_parquet(os.path.join(args.out_dir, "oof_base_preds.parquet"), index=False)


if __name__ == "__main__":
    main()

