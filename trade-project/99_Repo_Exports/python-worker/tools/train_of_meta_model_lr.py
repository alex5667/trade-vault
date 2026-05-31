from __future__ import annotations

"""ML Meta-Labeling Trainer: Train LogisticRegression + Platt/Isotonic calibration.

Why:
  ML meta-model поверх текущего rule-gate для улучшения качества решений.
  Сначала SHADOW mode (только пишет evidence.meta_p), затем ENFORCE.

Usage:
  python -m tools.train_of_meta_model_lr --dataset /tmp/dataset.ndjson --out-model /tmp/model.json --out-report /tmp/report.json
"""


import argparse
import json
import os
from typing import Any

try:
    import numpy as np
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import precision_recall_fscore_support, roc_auc_score
    from sklearn.model_selection import train_test_split
except Exception as e:
    raise SystemExit("Missing deps. Install: pip install numpy scikit-learn") from e


def iter_ndjson(path: str):
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def build_xy(rows: list[dict[str, Any]], feat_names: list[str]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    X = np.zeros((len(rows), len(feat_names)), dtype=np.float32)
    y = np.zeros((len(rows),), dtype=np.int64)
    w = np.ones((len(rows),), dtype=np.float32)
    for i, r in enumerate(rows):
        y[i] = int(r["y"])
        for j, fn in enumerate(feat_names):
            X[i, j] = float(_f(r.get(fn, 0.0)))
        wi = _f(r.get("ips_weight", 1.0), 1.0)
        w[i] = wi if wi > 0.0 else 1.0
    return X, y, w


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, help="NDJSON from build_of_dataset.py (includes y)")
    ap.add_argument("--out-model", required=True, help="output model JSON (runtime LR)")
    ap.add_argument("--out-report", required=True, help="output training report JSON")
    ap.add_argument("--test-size", type=float, default=0.25)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--threshold", type=float, default=0.5, help="default threshold for runtime")
    ap.add_argument(
        "--use-ips-weights",
        action="store_true",
        default=(os.environ.get("ML_TRAIN_USE_IPS_WEIGHTS", "1").strip().lower() in ("1", "true", "yes", "on")),
        help="Pass sample_weight=ips_weight to clf.fit and raw_lr.fit (default on; env ML_TRAIN_USE_IPS_WEIGHTS).",
    )
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

    X, y, w = build_xy(rows, feat)

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
            "threshold": float(args.threshold),
        }
        with open(args.out_model, "w", encoding="utf-8") as f:
            json.dump(model, f, ensure_ascii=False, indent=2)
        with open(args.out_report, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        return

    X_train, X_test, y_train, y_test, w_train, w_test = train_test_split(
        X, y, w, test_size=args.test_size, random_state=args.seed, stratify=y,
    )

    # Calibrated model for offline evaluation
    base = LogisticRegression(
        solver="liblinear",
        C=1.0,
        class_weight="balanced",
        max_iter=300,
    )
    clf = CalibratedClassifierCV(base, method="sigmoid", cv=3)
    _sw_train = w_train if bool(args.use_ips_weights) else None
    clf.fit(X_train, y_train, sample_weight=_sw_train)

    p = clf.predict_proba(X_test)[:, 1]
    auc = float(roc_auc_score(y_test, p))
    pred = (p >= args.threshold).astype(int)
    pr, rc, f1, _ = precision_recall_fscore_support(y_test, pred, average="binary")

    # Per-slice virtual vs real diagnostics on the test fold.
    virt_share_train = float(np.mean((w_train > 0) & (w_train < 1.0))) if len(w_train) else 0.0
    real_mask_test = np.isclose(w_test, 1.0)
    virt_mask_test = ~real_mask_test
    def _slice_metrics(mask: np.ndarray) -> dict[str, float | int]:
        if not bool(mask.any()):
            return {"n": 0, "auc": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0}
        y_s = y_test[mask]
        p_s = p[mask]
        try:
            auc_s = float(roc_auc_score(y_s, p_s)) if len(set(y_s.tolist())) > 1 else 0.0
        except Exception:
            auc_s = 0.0
        pred_s = (p_s >= args.threshold).astype(int)
        try:
            pr_s, rc_s, f1_s, _ = precision_recall_fscore_support(y_s, pred_s, average="binary", zero_division=0)
        except Exception:
            pr_s, rc_s, f1_s = 0.0, 0.0, 0.0
        return {
            "n": int(mask.sum()),
            "auc": auc_s,
            "precision": float(pr_s),
            "recall": float(rc_s),
            "f1": float(f1_s),
        }

    report = {
        "n": len(rows),
        "features": feat,
        "auc": auc,
        "precision": float(pr),
        "recall": float(rc),
        "f1": float(f1),
        "threshold": float(args.threshold),
        "use_ips_weights": bool(args.use_ips_weights),
        "ips_weight": {
            "p50": float(np.percentile(w_train, 50)) if len(w_train) else 1.0,
            "p99": float(np.percentile(w_train, 99)) if len(w_train) else 1.0,
            "min": float(np.min(w_train)) if len(w_train) else 1.0,
        },
        "train_virtual_share": virt_share_train,
        "slice_real_passed": _slice_metrics(real_mask_test),
        "slice_virtual": _slice_metrics(virt_mask_test),
    }

    # Runtime model: raw LR (un-calibrated) stored as intercept+coef
    raw_lr = LogisticRegression(
        solver="liblinear",
        C=1.0,
        class_weight="balanced",
        max_iter=300,
    )
    raw_lr.fit(X_train, y_train, sample_weight=_sw_train)

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

