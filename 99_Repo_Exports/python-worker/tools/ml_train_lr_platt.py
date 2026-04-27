#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Train ML confirm model (Logistic Regression + Platt scaling).

Input: dataset NDJSON produced by ml_build_dataset_from_ndjson.py
Output: a joblib bundle and a JSON metadata file.

This implements plan Steps C1 (LR baseline) and Step D (Platt calibration).

Validation: time split (train first, test last).
Metrics: PR-AUC, logloss, Brier, ECE (approx).
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import math
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterator, List, Tuple

import joblib
import numpy as np
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import average_precision_score, log_loss, brier_score_loss


def _read_ndjson(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except Exception:
                continue


def _ece(y_true: np.ndarray, p: np.ndarray, n_bins: int = 20) -> float:
    # Expected Calibration Error (simple binning)
    y = y_true.astype(float)
    p = np.clip(p.astype(float), 1e-9, 1 - 1e-9)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        mask = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(mask):
            continue
        conf = float(np.mean(p[mask]))
        acc = float(np.mean(y[mask]))
        w = float(np.mean(mask))
        ece += w * abs(acc - conf)
    return float(ece)


@dataclass
class TrainReport:
    n_train: int
    n_test: int
    pr_auc: float
    logloss: float
    brier: float
    ece: float


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, help="ndjson dataset with X,y_edge")
    ap.add_argument("--out-model", required=True, help="joblib output path")
    ap.add_argument("--out-meta", required=True, help="json metadata output path")
    ap.add_argument("--test-share", type=float, default=0.30, help="last fraction for test (time split)")
    ap.add_argument("--max-rows", type=int, default=0, help="optional limit rows (0=all)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--lr-c", type=float, default=1.0)
    ap.add_argument("--lr-max-iter", type=int, default=200)
    ap.add_argument("--calib", choices=["sigmoid", "isotonic"], default="sigmoid")
    ap.add_argument("--class-weight", choices=["none", "balanced"], default="balanced")
    args = ap.parse_args()

    rows: List[Dict[str, Any]] = []
    for r in _read_ndjson(args.dataset):
        if not isinstance(r, dict):
            continue
        if "X" not in r or "y_edge" not in r:
            continue
        rows.append(r)
        if args.max_rows and len(rows) >= args.max_rows:
            break

    if not rows:
        raise SystemExit("empty dataset")

    # sort by time for time split
    rows.sort(key=lambda z: int(z.get("ts_ms", 0) or 0))

    n = len(rows)
    n_test = max(1, int(round(float(args.test_share) * float(n))))
    n_train = max(1, n - n_test)
    train = rows[:n_train]
    test = rows[n_train:]

    X_train = [r["X"] for r in train]
    y_train = np.array([int(r.get("y_edge", 0) or 0) for r in train], dtype=np.int32)

    X_test = [r["X"] for r in test]
    y_test = np.array([int(r.get("y_edge", 0) or 0) for r in test], dtype=np.int32)

    vec = DictVectorizer(sparse=True)
    A_train = vec.fit_transform(X_train)
    A_test = vec.transform(X_test)

    cw = None if args.class_weight == "none" else "balanced"
    base = LogisticRegression(
        C=float(args.lr_c),
        max_iter=int(args.lr_max_iter),
        solver="liblinear",
        class_weight=cw,
        random_state=int(args.seed),
    )

    # Platt/Isotonic calibration on train only (CV inside train).
    # Using cv=3 keeps it deterministic enough for stable distributions.
    clf = CalibratedClassifierCV(estimator=base, method=args.calib, cv=3)
    clf.fit(A_train, y_train)

    p = clf.predict_proba(A_test)[:, 1]
    pr_auc = float(average_precision_score(y_test, p))
    ll = float(log_loss(y_test, p, labels=[0, 1]))
    brier = float(brier_score_loss(y_test, p))
    ece = float(_ece(y_test, p, n_bins=20))

    rep = TrainReport(
        n_train=int(n_train),
        n_test=int(n_test),
        pr_auc=pr_auc,
        logloss=ll,
        brier=brier,
        ece=ece,
    )

    # bundle
    time_ms = get_ny_time_millis()
    bundle = {
        "vectorizer": vec,
        "model": clf,
        "schema": {
            "feature_names": list(vec.feature_names_),
            "target": "y_edge",
            "dataset": args.dataset,
        },
        "report": asdict(rep),
        "created_ms": time_ms,
    }
    joblib.dump(bundle, args.out_model)

    meta = {
        "model_version": f"lr_platt_{time_ms}",
        "created_ms": time_ms,
        "train_report": asdict(rep),
        "calibration": args.calib,
        "class_weight": args.class_weight,
        "lr": {"C": float(args.lr_c), "max_iter": int(args.lr_max_iter), "solver": "liblinear"},
        "feature_count": len(vec.feature_names_),
        # Decision defaults (can be overridden by ENV/Redis later)
        "p_min_default": 0.55,
    }
    with open(args.out_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

