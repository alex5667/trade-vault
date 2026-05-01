#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""Evaluate a trained ML confirm model on a dataset via time split.

Step 5: validation like industry (time split).
"""


import argparse
import json
from typing import Any, Dict, Iterator, List

import joblib
import numpy as np
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
    y = y_true.astype(float)
    p = np.clip(p.astype(float), 1e-9, 1 - 1e-9)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        m = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(m):
            continue
        conf = float(np.mean(p[m]))
        acc = float(np.mean(y[m]))
        w = float(np.mean(m))
        ece += w * abs(acc - conf)
    return float(ece)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--test-share", type=float, default=0.30)
    args = ap.parse_args()

    bundle = joblib.load(args.model)
    vec = bundle["vectorizer"]
    clf = bundle["model"]

    rows: List[Dict[str, Any]] = [r for r in _read_ndjson(args.dataset) if isinstance(r, dict) and "X" in r and "y_edge" in r]
    rows.sort(key=lambda z: int(z.get("ts_ms", 0) or 0))
    n = len(rows)
    n_test = max(1, int(round(args.test_share * n)))
    train = rows[: n - n_test]
    test = rows[n - n_test :]

    X_test = [r["X"] for r in test]
    y_test = np.array([int(r.get("y_edge", 0) or 0) for r in test], dtype=np.int32)

    A_test = vec.transform(X_test)
    p = clf.predict_proba(A_test)[:, 1]

    report = {
        "n": int(n),
        "n_test": int(len(test)),
        "pr_auc": float(average_precision_score(y_test, p)),
        "logloss": float(log_loss(y_test, p, labels=[0, 1])),
        "brier": float(brier_score_loss(y_test, p)),
        "ece": float(_ece(y_test, p)),
        "p_mean": float(np.mean(p)),
        "p_p05": float(np.percentile(p, 5)),
        "p_p50": float(np.percentile(p, 50)),
        "p_p95": float(np.percentile(p, 95)),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

