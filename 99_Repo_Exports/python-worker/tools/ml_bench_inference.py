#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Benchmark ML confirm inference latency (p50/p95/p99).

This is part of Step 6 (SRE/latency budgets).
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, List

import joblib
import numpy as np


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, help="joblib model bundle")
    ap.add_argument("--rows", required=True, help="dataset ndjson or inputs ndjson with X")
    ap.add_argument("--n", type=int, default=5000)
    ap.add_argument("--warmup", type=int, default=200)
    args = ap.parse_args()

    bundle = joblib.load(args.model)
    vec = bundle["vectorizer"]
    clf = bundle["model"]

    # load feature dicts
    Xs: List[Dict[str, Any]] = []
    with open(args.rows, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                j = json.loads(s)
            except Exception:
                continue
            if "X" in j and isinstance(j["X"], dict):
                Xs.append(j["X"])
            elif isinstance(j, dict):
                Xs.append(j)
            if len(Xs) >= max(args.n, args.warmup):
                break
    if not Xs:
        raise SystemExit("no rows")

    # warmup
    for i in range(min(args.warmup, len(Xs))):
        A = vec.transform([Xs[i]])
        _ = clf.predict_proba(A)[:, 1]

    # measure
    lat_us: List[int] = []
    n = min(args.n, len(Xs))
    t0 = time.perf_counter()
    for i in range(n):
        t1 = time.perf_counter()
        A = vec.transform([Xs[i]])
        _ = float(clf.predict_proba(A)[:, 1][0])
        t2 = time.perf_counter()
        lat_us.append(int((t2 - t1) * 1_000_000))
    t_total = time.perf_counter() - t0

    a = np.array(lat_us, dtype=np.int64)
    report = {
        "n": int(n),
        "p50_us": int(np.percentile(a, 50)),
        "p95_us": int(np.percentile(a, 95)),
        "p99_us": int(np.percentile(a, 99)),
        "mean_us": float(np.mean(a)),
        "total_s": float(t_total),
        "qps": float(n / max(1e-9, t_total)),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

