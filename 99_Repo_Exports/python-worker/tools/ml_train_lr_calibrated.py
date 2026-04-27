
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Tuple
import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import average_precision_score, log_loss

import joblib  # type: ignore

from core.ml_feature_schema import feature_names, build_feature_vector
from core.ml_metrics_utils import brier_score, ece_score


def load_dataset(path: str) -> List[Dict[str, Any]]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def ece_score(y_true: np.ndarray, p: np.ndarray, n_bins: int = 15) -> float:
    # Expected Calibration Error
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i+1]
        mask = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(mask):
            continue
        acc = float(np.mean(y_true[mask]))
        conf = float(np.mean(p[mask]))
        w = float(np.mean(mask))
        ece += w * abs(acc - conf)
    return float(ece)


def build_xy(rows: List[Dict[str, Any]]) -> Tuple[np.ndarray, np.ndarray, List[int]]:
    X = []
    y = []
    ts = []
    for r in rows:
        xraw = r.get("x")
        # Support both formats: list of features (from nightly pipeline) or dict (legacy)
        if isinstance(xraw, list):
            # Already extracted features from nightly pipeline
            X.append(xraw)
        elif isinstance(xraw, dict):
            # Legacy format: extract features from dict
            indicators = dict(xraw)
            vec, _miss = build_feature_vector(
                symbol=str(xraw.get("symbol","")),
                ts_ms=int(xraw.get("ts_ms", 0)),
                direction=str(xraw.get("direction","")),
                scenario=str(xraw.get("scenario","")),
                indicators=indicators,
                rule_score=float(xraw.get("score", xraw.get("rule_score", 0.0)) or 0.0),
                rule_have=int(xraw.get("have", xraw.get("rule_have", 0)) or 0),
                rule_need=int(xraw.get("need", xraw.get("rule_need", 0)) or 0),
                cancel_spike_veto=int(xraw.get("cancel_spike_veto", 0) or 0),
            )
            X.append(vec)
        else:
            continue
        y.append(int(r.get("y_edge", 0)))
        ts.append(int(r.get("ts_ms", 0)))
    Xn = np.asarray(X, dtype=np.float32)
    yn = np.asarray(y, dtype=np.int32)
    return Xn, yn, ts


def time_split(rows: List[Dict[str, Any]], test_share: float, calib_share: float) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows = sorted(rows, key=lambda r: int(r.get("ts_ms", 0)))
    n = len(rows)
    n_test = int(max(1, n * test_share))
    test = rows[-n_test:]
    train_all = rows[:-n_test]
    n_cal = int(max(1, len(train_all) * calib_share))
    calib = train_all[-n_cal:]
    train_fit = train_all[:-n_cal]
    return train_fit, calib, test


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out-model", required=True)
    ap.add_argument("--out-meta", required=True)
    ap.add_argument("--calib", choices=["sigmoid","isotonic"], default="sigmoid")
    ap.add_argument("--test-share", type=float, default=0.30)
    ap.add_argument("--calib-share", type=float, default=0.20)
    ap.add_argument("--C", type=float, default=1.0)
    ap.add_argument("--max-iter", type=int, default=500)
    args = ap.parse_args()

    rows = load_dataset(args.dataset)
    train_fit, calib, test = time_split(rows, args.test_share, args.calib_share)

    X_train, y_train, _ = build_xy(train_fit)
    X_cal, y_cal, _ = build_xy(calib)
    X_test, y_test, _ = build_xy(test)

    # Base LR
    lr = LogisticRegression(
        C=float(args.C),
        max_iter=int(args.max_iter),
        solver="lbfgs",
        n_jobs=1,
    )
    lr.fit(X_train, y_train)

    # Platt/Isotonic calibration on temporal holdout (calib set)
    cal = CalibratedClassifierCV(lr, method=args.calib, cv="prefit")
    cal.fit(X_cal, y_cal)

    # Evaluate on test
    p_test = cal.predict_proba(X_test)[:, 1]
    pr_auc = float(average_precision_score(y_test, p_test)) if len(set(y_test.tolist())) > 1 else 0.0
    ll = float(log_loss(y_test, p_test, eps=1e-12))
    brier = float(brier_score(y_test.tolist(), p_test.tolist()))
    ece = float(ece_score(y_test.tolist(), p_test.tolist()))

    meta = {
        "model_ver": "ml_confirm_lr_cal_v1",
        "calib": args.calib,
        "feature_names": feature_names(),
        "sizes": {"train": int(len(train_fit)), "calib": int(len(calib)), "test": int(len(test))},
        "metrics": {"pr_auc": pr_auc, "logloss": ll, "brier": brier, "ece": ece},
        "metrics_test": {"pr_auc": pr_auc, "logloss": ll, "brier": brier, "ece": ece},
        "ts_range": {"train_start": int(train_fit[0]["ts_ms"]) if train_fit else 0,
                     "train_end": int(train_fit[-1]["ts_ms"]) if train_fit else 0,
                     "test_start": int(test[0]["ts_ms"]) if test else 0,
                     "test_end": int(test[-1]["ts_ms"]) if test else 0},
    }

    joblib.dump(cal, args.out_model)

    with open(args.out_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(meta["metrics_test"], indent=2))

if __name__ == "__main__":
    main()

