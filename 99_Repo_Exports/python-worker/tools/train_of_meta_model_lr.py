"""ML Meta-Labeling Trainer: Train LogisticRegression + Platt/Isotonic calibration.

Why:
  ML meta-model поверх текущего rule-gate для улучшения качества решений.
  Сначала SHADOW mode (только пишет evidence.meta_p), затем ENFORCE.

Usage:
  python -m tools.train_of_meta_model_lr --dataset /tmp/dataset.ndjson --out-model /tmp/model.json --out-report /tmp/report.json
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List, Tuple

try:
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score, precision_recall_fscore_support
    from sklearn.calibration import CalibratedClassifierCV
except Exception as e:
    raise SystemExit("Missing deps. Install: pip install numpy scikit-learn") from e


def iter_ndjson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def build_xy(rows: List[Dict[str, Any]], feat_names: List[str]) -> Tuple[np.ndarray, np.ndarray]:
    X = np.zeros((len(rows), len(feat_names)), dtype=np.float32)
    y = np.zeros((len(rows),), dtype=np.int64)
    for i, r in enumerate(rows):
        y[i] = int(r["y"])
        for j, fn in enumerate(feat_names):
            X[i, j] = float(_f(r.get(fn, 0.0)))
    return X, y


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, help="NDJSON from build_of_dataset.py (includes y)")
    ap.add_argument("--out-model", required=True, help="output model JSON (runtime LR)")
    ap.add_argument("--out-report", required=True, help="output training report JSON")
    ap.add_argument("--test-size", type=float, default=0.25)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--threshold", type=float, default=0.5, help="default threshold for runtime")
    args = ap.parse_args()

    rows = list(iter_ndjson(args.dataset))
    min_dataset = int(os.getenv("META_LR_MIN_DATASET", "200"))
    if len(rows) < min_dataset:
        raise SystemExit(f"dataset_too_small n={len(rows)} (need >= {min_dataset} for stable LR)")

    # Interpretable and stable feature set.
    # IMPORTANT: these names must exist in dataset (missing -> 0).
    feat = [
        "base_score",
        "exec_risk_norm",
        "exec_risk_bps",
        "have",
        "need",
        "ok_soft",
        "leg_ofi_leg",
        "leg_fp_edge_absorb",
        "leg_obi_stable",
        "leg_iceberg_strict",
        "leg_abs_lvl_ok",
        "leg_reclaim_recent",
        "leg_weak_progress",
        "leg_sweep_recent",
    ]

    X, y = build_xy(rows, feat)
    
    # Handle single-class case (e.g. all 1s or all 0s)
    classes = np.unique(y)
    if len(classes) < 2:
        print(f"[WARN] Only one class found: {classes}. Creating dummy pass-through model.")
        report = {
            "n": len(rows), "features": feat, "auc": 0.5, "precision": 0.0, "recall": 0.0, "f1": 0.0,
            "threshold": float(args.threshold), "note": "single_class_detected"
        }
        model = {
            "kind": "logreg_v1_dummy", "features": feat, "intercept": 0.0, "coef": [0.0] * len(feat),
            "threshold": float(args.threshold)
        }
        with open(args.out_model, "w", encoding="utf-8") as f:
            json.dump(model, f, ensure_ascii=False, indent=2)
        with open(args.out_report, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        return

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=args.test_size, random_state=args.seed, stratify=y)

    # Calibrated model for offline evaluation
    base = LogisticRegression(
        solver="liblinear",
        C=1.0,
        class_weight="balanced",
        max_iter=300,
    )
    clf = CalibratedClassifierCV(base, method="sigmoid", cv=3)
    clf.fit(X_train, y_train)

    p = clf.predict_proba(X_test)[:, 1]
    auc = float(roc_auc_score(y_test, p))
    pred = (p >= args.threshold).astype(int)
    pr, rc, f1, _ = precision_recall_fscore_support(y_test, pred, average="binary")

    report = {
        "n": len(rows),
        "features": feat,
        "auc": auc,
        "precision": float(pr),
        "recall": float(rc),
        "f1": float(f1),
        "threshold": float(args.threshold),
    }

    # Runtime model: raw LR (un-calibrated) stored as intercept+coef
    raw_lr = LogisticRegression(
        solver="liblinear",
        C=1.0,
        class_weight="balanced",
        max_iter=300,
    )
    raw_lr.fit(X_train, y_train)

    model = {
        "kind": "logreg_v1",
        "features": feat,
        "intercept": float(raw_lr.intercept_[0]),
        "coef": [float(x) for x in raw_lr.coef_[0].tolist()],
        "threshold": float(args.threshold),
    }

    with open(args.out_model, "w", encoding="utf-8") as f:
        json.dump(model, f, ensure_ascii=False, indent=2)
    with open(args.out_report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

