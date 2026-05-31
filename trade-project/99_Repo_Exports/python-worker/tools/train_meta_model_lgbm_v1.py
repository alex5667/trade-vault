#!/usr/bin/env python3
"""LightGBM challenger trainer for MetaModel.

Uses the same production builder (Train==Serve parity) as train_meta_model_lr_v4.py
via get_schema_builder() from the central registry. Outputs a JSON model artifact
compatible with downstream comparison scripts.

Usage:
    python -m tools.train_meta_model_lgbm_v1 \
        --in-parquet /tmp/ml_dataset_tb.parquet \
        --out-json /tmp/lgbm_meta_v13of.json \
        --schema meta_feat_v13_of \
        --label-col y_edge_cost_aware \
        --purge-cv 1 \
        --out-cv-report /tmp/lgbm_cv_report.json

Requirements:
    pip install lightgbm
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any

import numpy as np
import pandas as pd

try:
    import lightgbm as lgb
    _HAS_LGB = True
except ImportError:
    _HAS_LGB = False

from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

from core.meta_features_v1 import META_FEAT_V1_NAME
from core.meta_schema_registry import (
    META_SCHEMA_REGISTRY,
    META_SCHEMA_TRANSFORMS,
    get_schema_builder,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("train_lgbm")

SCHEMAS = {
    name: {
        "version": spec.version,
        "cols": list(spec.cols),
        "hash": spec.hash,
        "transforms": META_SCHEMA_TRANSFORMS[name],
        "builder": spec.builder,
    }
    for name, spec in META_SCHEMA_REGISTRY.items()
}


def _ece(y: np.ndarray, p: np.ndarray, n_bins: int = 10) -> float:
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        mask = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(mask):
            continue
        ece += float(np.mean(mask)) * abs(float(np.mean(y[mask])) - float(np.mean(p[mask])))
    return float(ece)


def build_features(records: list[dict[str, Any]], builder: Any) -> np.ndarray:
    rows = []
    for row in records:
        have = int(row.get("have", 0) or 0)
        need = int(row.get("need", 0) or 0)
        feat, _ = builder(
            evidence=row,
            indicators=row,
            indicators_with_v4=row,
            legs=row,
            have=have,
            need=need,
            ok_soft=int(row.get("ok_soft", 0) or 0),
            rule_score=float(row.get("score_final_01", row.get("rule_score", 0.0)) or 0.0),
        )
        rows.append(feat)
    return rows


def main() -> int:
    if not _HAS_LGB:
        print("ERROR: lightgbm is not installed. Run: pip install lightgbm", file=sys.stderr)
        return 2

    ap = argparse.ArgumentParser(description="Train LightGBM challenger for MetaModel")
    ap.add_argument("--in-parquet", "--in_parquet", dest="in_parquet", required=True)
    ap.add_argument("--out-json", "--out_json", dest="out_json", required=True)
    ap.add_argument("--out-cv-report", "--out_cv_report", dest="out_cv_report", default="")
    ap.add_argument("--schema", default=META_FEAT_V1_NAME, choices=SCHEMAS.keys())
    ap.add_argument("--label-col", "--label_col", dest="label_col", default="y_edge_cost_aware")
    ap.add_argument("--num-leaves", type=int, default=63)
    ap.add_argument("--n-estimators", type=int, default=300)
    ap.add_argument("--learning-rate", type=float, default=0.05)
    ap.add_argument("--min-child-samples", type=int, default=50)
    ap.add_argument("--purge-cv", "--purge_cv", dest="purge_cv", type=int, default=1)
    ap.add_argument("--time-col", "--time_col", dest="time_col", default="ts_ms")
    ap.add_argument("--t1-col", "--t1_col", dest="t1_col", default="tb_t_hit_ms")
    ap.add_argument("--purge-ms", "--purge_ms", dest="purge_ms", type=int, default=180_000)
    ap.add_argument("--embargo-ms", "--embargo_ms", dest="embargo_ms", type=int, default=60_000)
    ap.add_argument("--splits", type=int, default=5)
    args = ap.parse_args()

    schema_cfg = SCHEMAS[args.schema]
    cols: list[str] = schema_cfg["cols"]
    builder = get_schema_builder(args.schema)
    if builder is None:
        print(f"ERROR: no builder for schema {args.schema}", file=sys.stderr)
        return 1

    try:
        df = pd.read_parquet(args.in_parquet)
    except Exception as e:
        print(f"Error reading parquet: {e}", file=sys.stderr)
        return 1

    if args.label_col not in df.columns:
        if args.label_col == "y_edge_cost_aware" and "y" in df.columns:
            logger.warning("label_col 'y_edge_cost_aware' missing — falling back to 'y'")
            args.label_col = "y"
        else:
            print(f"Error: label_col '{args.label_col}' not found", file=sys.stderr)
            return 1

    if df.empty:
        print("Error: Empty dataframe", file=sys.stderr)
        return 1

    records = df.to_dict(orient="records")
    y = df[args.label_col].astype(int).to_numpy()
    n = len(records)

    print(f"Schema: {args.schema} ({len(cols)} features), n={n}, pos_rate={float(np.mean(y)):.3f}")
    print("Building features via production builder...")

    feat_dicts = build_features(records, builder)
    X = np.array(
        [[float(fd.get(c, 0.0)) for c in cols] for fd in feat_dicts],
        dtype=np.float32,
    )
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    pos = int(np.sum(y == 0))
    neg = int(np.sum(y == 1))
    scale_pos = pos / max(neg, 1)

    lgb_params: dict[str, Any] = {
        "objective": "binary",
        "metric": ["binary_logloss", "auc"],
        "num_leaves": args.num_leaves,
        "learning_rate": args.learning_rate,
        "min_child_samples": args.min_child_samples,
        "scale_pos_weight": scale_pos,
        "n_estimators": args.n_estimators,
        "verbose": -1,
    }

    # ------------------------------------------------------------------
    # Purged walk-forward CV
    # ------------------------------------------------------------------
    cv_folds_report: list[dict] = []
    if int(args.purge_cv) == 1:
        try:
            from ml_core.purged_cv import purged_kfold_time_series

            ts_ms = df[args.time_col].astype(int).to_numpy() if args.time_col in df.columns else np.arange(n, dtype=np.int64) * 1000
            t1_ms = df[args.t1_col].astype(int).to_numpy() if args.t1_col in df.columns else ts_ms + args.purge_ms

            folds = purged_kfold_time_series(
                ts_ms=ts_ms, t1_ms=t1_ms,
                n_splits=args.splits,
                embargo_ms=args.embargo_ms,
            )
            print(f"Purged CV: {len(folds)} folds")
            for i, fold in enumerate(folds):
                if len(fold.train_idx) < 100 or len(fold.test_idx) < 20:
                    continue
                m_cv = lgb.LGBMClassifier(**lgb_params)
                m_cv.fit(X[fold.train_idx], y[fold.train_idx])
                p_cv = m_cv.predict_proba(X[fold.test_idx])[:, 1]
                y_cv = y[fold.test_idx]
                n_unique = len(np.unique(y_cv))
                fold_r: dict = {
                    "fold": i + 1,
                    "n_train": len(fold.train_idx),
                    "n_test": len(fold.test_idx),
                    "pos_rate": float(np.mean(y_cv)),
                    "logloss": float(log_loss(y_cv, p_cv, labels=[0, 1])),
                    "auc": float(roc_auc_score(y_cv, p_cv)) if n_unique > 1 else 0.5,
                    "brier": float(brier_score_loss(y_cv, p_cv)),
                    "ece10": float(_ece(y_cv, p_cv)),
                }
                cv_folds_report.append(fold_r)
                print(f"  fold={i+1} auc={fold_r['auc']:.3f} brier={fold_r['brier']:.4f} ece={fold_r['ece10']:.4f}")
        except Exception as e:
            logger.warning(f"Purged CV skipped: {e}")

    # ------------------------------------------------------------------
    # Final fit on full dataset
    # ------------------------------------------------------------------
    print("Fitting LightGBM on full dataset...")
    model = lgb.LGBMClassifier(**lgb_params)
    model.fit(X, y)

    p_train = model.predict_proba(X)[:, 1]
    train_report = {
        "n": n,
        "pos_rate": float(np.mean(y)),
        "logloss": float(log_loss(y, p_train)),
        "auc": float(roc_auc_score(y, p_train)) if len(np.unique(y)) > 1 else 0.5,
        "brier": float(brier_score_loss(y, p_train)),
        "ece10": float(_ece(y, p_train)),
    }

    artifact = {
        "model_type": "lgbm_challenger",
        "schema_name": args.schema,
        "schema_version": schema_cfg["version"],
        "schema_hash": schema_cfg["hash"],
        "feature_cols": cols,
        "label_col": args.label_col,
        "lgb_params": lgb_params,
        "feature_importances": {
            c: float(v) for c, v in zip(cols, model.feature_importances_)
        },
        "train_report": train_report,
        "cv_report": {
            "n_folds": len(cv_folds_report),
            "mean_auc": float(np.mean([f["auc"] for f in cv_folds_report])) if cv_folds_report else None,
            "mean_brier": float(np.mean([f["brier"] for f in cv_folds_report])) if cv_folds_report else None,
            "mean_ece10": float(np.mean([f["ece10"] for f in cv_folds_report])) if cv_folds_report else None,
            "folds": cv_folds_report,
        },
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.out_json)), exist_ok=True)
    # Save LGB booster separately (binary)
    booster_path = args.out_json.replace(".json", "_booster.txt")
    model.booster_.save_model(booster_path)
    artifact["booster_path"] = booster_path

    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump(artifact, fh, indent=2)
    print(f"Model artifact saved to {args.out_json}")
    print(f"Train: auc={train_report['auc']:.3f} brier={train_report['brier']:.4f} ece={train_report['ece10']:.4f}")

    if cv_folds_report and args.out_cv_report:
        os.makedirs(os.path.dirname(os.path.abspath(args.out_cv_report)), exist_ok=True)
        with open(args.out_cv_report, "w", encoding="utf-8") as fh:
            json.dump(artifact["cv_report"], fh, indent=2)
        print(f"CV report saved to {args.out_cv_report}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
