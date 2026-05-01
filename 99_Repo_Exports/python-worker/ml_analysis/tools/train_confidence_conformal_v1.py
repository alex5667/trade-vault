#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
train_confidence_conformal_v1.py

Builds split-conformal thresholds for binary success label on top of calibrated confidence.

Input: JSONL where each line is a dict with (typical) keys:
  - y / label / success  (0/1)
  - confidence_cal / confidence_cal_v1 / confidence (float in [0,1])
  - symbol (str)
  - kind / scenario / signal_kind (str)

Output: JSON (CONF_CONFORMAL_PATH) with:
  schema_version, alpha, global_qhat, buckets{ "SYMBOL|kind": qhat, "SYMBOL|*": qhat }, trained_ts_ms, stats.

Usage:
  python3 -m ml_analysis.tools.train_confidence_conformal_v1 --data_jsonl /path/to/dataset.jsonl --out_json /path/to/conf_conformal_latest.json --alpha 0.10
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import math
import os
import time
from typing import Any, Dict, Iterable, List


def now_ms() -> int:
    return get_ny_time_millis()


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _get(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    ind = d.get("indicators")
    if isinstance(ind, dict):
        for k in keys:
            if k in ind:
                return ind.get(k)
    return None


def _bkey(symbol: str, kind: str) -> str:
    s = (symbol or "unknown").strip().upper()
    k = (kind or "unknown").strip().lower()
    return f"{s}|{k}"


def _quantile_qhat(scores: List[float], alpha: float) -> float:
    """
    Conformal quantile with finite-sample correction:
      q = k-th order statistic where k = ceil((n+1)*(1-alpha))
    """
    n = len(scores)
    if n <= 0:
        return 0.50
    s = sorted(float(x) for x in scores)
    k = int(math.ceil((n + 1) * (1.0 - float(alpha))))
    if k < 1:
        k = 1
    if k > n:
        k = n
    return float(s[k - 1])


def _iter_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_jsonl", required=True)
    ap.add_argument("--out_json", default=os.getenv("CONF_CONFORMAL_OUT_JSON", "conf_conformal_latest.json"))
    ap.add_argument("--alpha", type=float, default=float(os.getenv("CONF_CONFORMAL_ALPHA", "0.10")))
    ap.add_argument("--min_bucket_n", type=int, default=int(os.getenv("CONF_CONFORMAL_MIN_BUCKET_N", "200")))
    args = ap.parse_args()

    alpha = float(args.alpha)
    if alpha <= 0.0 or alpha >= 1.0:
        raise SystemExit("alpha must be in (0,1)")

    global_scores: List[float] = []
    bucket_scores: Dict[str, List[float]] = {}
    symbol_scores: Dict[str, List[float]] = {}

    rows = 0
    used = 0
    for d in _iter_jsonl(args.data_jsonl):
        rows += 1
        y = _get(d, "y", "label", "success")
        p = _get(d, "confidence_cal", "confidence_cal_v1", "confidence", "confidence_raw")
        sym = _get(d, "symbol", "sym")
        kind = _get(d, "kind", "scenario", "signal_kind", "strategy")

        try:
            yv = int(y)
            if yv not in (0, 1):
                continue
            pv = _clamp01(float(p))
        except Exception:
            continue

        symbol = str(sym or "unknown").strip().upper()
        kind_s = str(kind or "unknown").strip().lower()

        s = (1.0 - pv) if yv == 1 else pv
        s = _clamp01(float(s))

        global_scores.append(s)
        bk = _bkey(symbol, kind_s)
        bucket_scores.setdefault(bk, []).append(s)
        symbol_scores.setdefault(f"{symbol}|*", []).append(s)
        used += 1

    if used < 50:
        raise SystemExit(f"Not enough valid rows for conformal training: used={used}, rows={rows}")

    global_qhat = _quantile_qhat(global_scores, alpha)

    buckets_out: Dict[str, float] = {}
    for bk, scs in symbol_scores.items():
        if len(scs) >= args.min_bucket_n:
            buckets_out[bk] = _quantile_qhat(scs, alpha)
    for bk, scs in bucket_scores.items():
        if len(scs) >= args.min_bucket_n:
            buckets_out[bk] = _quantile_qhat(scs, alpha)

    out = {
        "schema_version": "conf_conformal_v1",
        "alpha": alpha,
        "global_qhat": float(global_qhat),
        "buckets": buckets_out,
        "trained_ts_ms": now_ms(),
        "stats": {
            "rows_total": rows,
            "rows_used": used,
            "n_global": len(global_scores),
            "n_buckets": len(buckets_out),
            "min_bucket_n": int(args.min_bucket_n),
        },
    }

    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, sort_keys=True)

    print(f"Wrote {args.out_json} (alpha={alpha:.3f}, global_qhat={global_qhat:.4f}, buckets={len(buckets_out)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
