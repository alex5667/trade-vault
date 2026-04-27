from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

from tools._ml_common import read_ndjson, safe_int, ece, brier, pctl

try:
    import joblib
except Exception:
    joblib = None

try:
    import numpy as np
except Exception:
    np = None

def _ts(r: Dict[str, Any]) -> int:
    return safe_int(r.get("ts_ms", r.get("ts", 0)), 0)

def _label(r: Dict[str, Any]) -> int:
    return 1 if safe_int(r.get("y_edge", 0), 0) == 1 else 0

def _to_matrix(rows: List[Dict[str, Any]]):
    X = [r["x"] for r in rows]
    y = [_label(r) for r in rows]
    if np is not None:
        X = np.asarray(X, dtype=float)
    return X, y

def _purge_embargo(rows: List[Dict[str, Any]], train_end_ms: int, test_start_ms: int, embargo_ms: int) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        t = _ts(r)
        if train_end_ms <= t <= train_end_ms + embargo_ms:
            continue
        if test_start_ms - embargo_ms <= t <= test_start_ms:
            continue
        out.append(r)
    return out

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--fold-hours", type=float, default=24.0)
    ap.add_argument("--step-hours", type=float, default=12.0)
    ap.add_argument("--embargo-min", type=float, default=30.0)
    ap.add_argument("--min-train-hours", type=float, default=72.0)
    args = ap.parse_args()

    if joblib is None:
        raise RuntimeError("joblib is required")

    rows = list(read_ndjson(args.dataset))
    rows.sort(key=_ts)
    if not rows:
        raise RuntimeError("empty dataset")

    model = joblib.load(args.model)

    fold_ms = int(args.fold_hours * 3600_000)
    step_ms = int(args.step_hours * 3600_000)
    embargo_ms = int(args.embargo_min * 60_000)
    min_train_ms = int(args.min_train_hours * 3600_000)

    t0 = _ts(rows[0])
    tN = _ts(rows[-1])

    folds = []
    t_test_start = t0 + min_train_ms
    while t_test_start + fold_ms <= tN:
        t_test_end = t_test_start + fold_ms
        t_train_end = t_test_start

        train = [r for r in rows if _ts(r) < t_train_end]
        test = [r for r in rows if t_test_start <= _ts(r) < t_test_end]
        train = _purge_embargo(train, train_end_ms=t_train_end, test_start_ms=t_test_start, embargo_ms=embargo_ms)

        if len(train) < 200 or len(test) < 50:
            t_test_start += step_ms
            continue

        Xte, yte = _to_matrix(test)
        p = model.predict_proba(Xte)[:, 1]
        p_list = p.tolist() if hasattr(p, "tolist") else list(p)

        met = {
            "train_n": len(train),
            "test_n": len(test),
            "t_test_start_ms": t_test_start,
            "t_test_end_ms": t_test_end,
            "brier": brier(p_list, yte),
            "ece": ece(p_list, yte),
            "p_p50": pctl(p_list, 0.50),
            "p_p90": pctl(p_list, 0.90),
            "p_p99": pctl(p_list, 0.99),
            "y_rate": float(sum(yte) / max(1, len(yte))),
        }
        try:
            from sklearn.metrics import log_loss, average_precision_score
            met["logloss"] = float(log_loss(yte, p_list, labels=[0, 1]))
            met["pr_auc"] = float(average_precision_score(yte, p_list))
        except Exception:
            pass

        folds.append(met)
        t_test_start += step_ms

    agg = {}
    if folds:
        for k in ("brier", "ece", "logloss", "pr_auc", "p_p50", "p_p90", "p_p99", "y_rate"):
            vals = [f[k] for f in folds if k in f]
            if vals:
                agg[k + "_mean"] = float(sum(vals) / len(vals))
                agg[k + "_p50"] = float(pctl(vals, 0.50))
                agg[k + "_p90"] = float(pctl(vals, 0.90))
        agg["folds"] = len(folds)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"folds": folds, "agg": agg}, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
