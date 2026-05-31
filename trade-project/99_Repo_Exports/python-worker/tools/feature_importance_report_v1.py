#!/usr/bin/env python3
"""feature_importance_report_v1.py — Weekly feature importance + drift report.

Computes permutation importance on a trained .joblib model using a held-out
evaluation dataset (parquet). Designed to run weekly as a CI/cron job to
detect feature drift, dead features, and candidates for the denylist.

Usage
-----
  python -m tools.feature_importance_report_v1 \\
      --model  /var/lib/trade/ml_models/edge_stack_v13_of/champions/edge_stack_v1_candidate.joblib \\
      --dataset /tmp/ml_dataset_tb.parquet \\
      --output  /tmp/feature_importance_report.json \\
      [--n-repeats 10] \\
      [--n-jobs -1] \\
      [--top-n 40] \\
      [--dead-threshold 0.0001] \\
      [--label-col label] \\
      [--drop-cols ts_ms,symbol,sid]

Output (JSON)
-------------
{
  "run_ts_utc": "...",
  "model_path": "...",
  "dataset_rows": N,
  "n_features": K,
  "n_repeats": R,
  "mean_accuracy": ...,
  "features": [
    {"rank": 1, "name": "...", "importance_mean": ..., "importance_std": ...},
    ...
  ],
  "dead_features": ["..."],          # importance_mean < dead_threshold
  "denylist_candidates": ["..."],    # dead + potentially noisy (std > 2×mean)
  "schema_drift": {                  # features in model but not in dataset + vice versa
    "missing_from_dataset": [...],
    "extra_in_dataset": [...]
  }
}
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULT_DROP = {
    "label", "y", "target", "outcome", "r_mult", "r_multiple",
    "ts_ms", "event_ts_ms", "ingest_ts_ms",
    "symbol", "sid", "direction", "scenario", "kind",
    "split", "fold", "source", "schema_ver",
}

_NOISE_RATIO = 2.0   # std/mean > this → noisy candidate


def _load_model(path: str) -> Any:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model not found: {path}")
    return joblib.load(path)


def _get_feature_cols(model: Any, df: pd.DataFrame, drop_cols: set[str]) -> list[str]:
    """Prefer model.feature_names_in_ if present; fall back to df columns minus drops."""
    if hasattr(model, "feature_names_in_"):
        return list(model.feature_names_in_)
    if hasattr(model, "feature_cols"):
        return list(model.feature_cols)
    return [c for c in df.columns if c not in drop_cols]


def _get_label_col(df: pd.DataFrame, label_col: str) -> str:
    if label_col in df.columns:
        return label_col
    for candidate in ("label", "y", "target", "outcome", "r_mult"):
        if candidate in df.columns:
            return candidate
    raise ValueError(f"Label column not found. Tried: {label_col}, label, y, target, outcome. Columns: {list(df.columns)[:20]}")


def _score_fn(model: Any) -> str:
    """Detect scoring metric based on model type / method."""
    if hasattr(model, "predict_proba"):
        return "roc_auc"
    return "neg_mean_squared_error"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(
    model_path: str,
    dataset_path: str,
    output_path: str,
    n_repeats: int = 10,
    n_jobs: int = -1,
    top_n: int = 40,
    dead_threshold: float = 0.0001,
    label_col: str = "label",
    extra_drop: set[str] | None = None,
) -> dict[str, Any]:
    drop_cols = _DEFAULT_DROP | (extra_drop or set())

    print(f"[feature_importance] Loading model: {model_path}")
    model = _load_model(model_path)

    print(f"[feature_importance] Loading dataset: {dataset_path}")
    df = pd.read_parquet(dataset_path)
    print(f"[feature_importance] Dataset shape: {df.shape}")

    true_label = _get_label_col(df, label_col)
    y = df[true_label].to_numpy(dtype=np.float32)

    feature_cols = _get_feature_cols(model, df, drop_cols)

    # Schema drift analysis
    model_set = set(feature_cols)
    dataset_set = set(df.columns) - drop_cols - {true_label}
    missing_from_dataset = sorted(model_set - dataset_set)
    extra_in_dataset = sorted(dataset_set - model_set)
    if missing_from_dataset:
        print(f"[feature_importance] WARNING: {len(missing_from_dataset)} model features missing from dataset:")
        for f in missing_from_dataset[:10]:
            print(f"  - {f}")

    # Fill missing columns with 0.0 (fail-open, same as engine behaviour)
    for col in feature_cols:
        if col not in df.columns:
            df[col] = 0.0

    X = df[feature_cols].fillna(0.0).to_numpy(dtype=np.float32)
    scoring = _score_fn(model)
    print(f"[feature_importance] Computing permutation importance: n_repeats={n_repeats} scoring={scoring} n_jobs={n_jobs}")

    t0 = time.time()
    result = permutation_importance(
        model, X, y,
        n_repeats=n_repeats,
        random_state=42,
        n_jobs=n_jobs,
        scoring=scoring,
    )
    elapsed = time.time() - t0
    print(f"[feature_importance] Done in {elapsed:.1f}s")

    # sklearn.utils.Bunch — supports attribute AND dict access; use getattr for type-checker compat
    importances_mean: np.ndarray = getattr(result, "importances_mean")
    importances_std: np.ndarray = getattr(result, "importances_std")

    # Baseline accuracy on test set
    try:
        if hasattr(model, "predict_proba"):
            from sklearn.metrics import roc_auc_score
            proba = model.predict_proba(X)[:, 1]
            y_bin = (y > 0).astype(int) if y.dtype != int else y
            mean_accuracy = float(roc_auc_score(y_bin, proba))
        else:
            from sklearn.metrics import mean_squared_error
            pred = model.predict(X)
            mean_accuracy = -mean_squared_error(y, pred)
    except Exception:
        mean_accuracy = float("nan")

    # Rank features
    order = np.argsort(importances_mean)[::-1]
    ranked = []
    dead_features = []
    denylist_candidates = []

    for rank, idx in enumerate(order, start=1):
        name = feature_cols[idx]
        imp_mean = float(importances_mean[idx])
        imp_std = float(importances_std[idx])
        ranked.append({
            "rank": rank,
            "name": name,
            "importance_mean": round(imp_mean, 8),
            "importance_std": round(imp_std, 8),
        })
        if imp_mean < dead_threshold:
            dead_features.append(name)
        # Noisy: very high variance relative to mean (unreliable signal)
        if imp_mean < dead_threshold or (imp_mean > 0 and imp_std / max(imp_mean, 1e-9) > _NOISE_RATIO):
            if name not in denylist_candidates:
                denylist_candidates.append(name)

    report: dict[str, Any] = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "model_path": model_path,
        "dataset_path": dataset_path,
        "dataset_rows": len(df),
        "n_features": len(feature_cols),
        "n_repeats": n_repeats,
        "scoring": scoring,
        "elapsed_s": round(elapsed, 2),
        "mean_score": round(mean_accuracy, 6) if math.isfinite(mean_accuracy) else None,
        "features": ranked[:top_n],
        "dead_features": dead_features,
        "dead_threshold": dead_threshold,
        "denylist_candidates": sorted(denylist_candidates),
        "schema_drift": {
            "missing_from_dataset": missing_from_dataset,
            "extra_in_dataset": extra_in_dataset[:50],
        },
    }

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"[feature_importance] Report written to {output_path}")

    # Print summary
    print(f"\n{'─'*60}")
    print(f"Top {min(top_n, 20)} features:")
    for row in ranked[:20]:
        bar = "█" * max(1, int(row["importance_mean"] / max(ranked[0]["importance_mean"], 1e-9) * 20))
        print(f"  {row['rank']:3d}. {row['name']:<45s} {row['importance_mean']:.6f} ±{row['importance_std']:.6f}  {bar}")

    print(f"\nDead features ({len(dead_features)} of {len(feature_cols)}):")
    for f in dead_features[:20]:
        print(f"  - {f}")
    if len(dead_features) > 20:
        print(f"  ... and {len(dead_features) - 20} more")

    print(f"\nDenylist candidates: {len(denylist_candidates)}")
    print(f"Schema drift: {len(missing_from_dataset)} missing from dataset, {len(extra_in_dataset)} extra")
    print(f"{'─'*60}\n")

    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Weekly feature importance + drift report")
    ap.add_argument("--model", required=True, help="Path to .joblib model file")
    ap.add_argument("--dataset", required=True, help="Path to parquet dataset")
    ap.add_argument("--output", default="/tmp/feature_importance_report.json", help="Output JSON path")
    ap.add_argument("--n-repeats", type=int, default=10)
    ap.add_argument("--n-jobs", type=int, default=-1)
    ap.add_argument("--top-n", type=int, default=40)
    ap.add_argument("--dead-threshold", type=float, default=0.0001)
    ap.add_argument("--label-col", default="label")
    ap.add_argument("--drop-cols", default="", help="Comma-separated extra columns to drop")
    args = ap.parse_args()

    extra_drop = {c.strip() for c in args.drop_cols.split(",") if c.strip()}

    run(
        model_path=args.model,
        dataset_path=args.dataset,
        output_path=args.output,
        n_repeats=args.n_repeats,
        n_jobs=args.n_jobs,
        top_n=args.top_n,
        dead_threshold=args.dead_threshold,
        label_col=args.label_col,
        extra_drop=extra_drop,
    )


if __name__ == "__main__":
    main()
