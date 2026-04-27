from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

from tools._ml_common import read_ndjson, pctl, ece, brier, safe_float

def _key(r: Dict[str, Any]) -> str:
    return str(r.get("sid", ""))

def _p(r: Dict[str, Any]) -> float:
    return safe_float(r.get("p_edge", r.get("p", 0.0)), 0.0)

def _y(r: Dict[str, Any]) -> int:
    try:
        return 1 if int(float(r.get("y_edge", 0) or 0)) == 1 else 0
    except Exception:
        return 0

def _ks_stat(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    a = sorted(a)
    b = sorted(b)
    i = j = 0
    na = len(a)
    nb = len(b)
    d=0.0
    while i<na and j<nb:
        if a[i] <= b[j]:
            i += 1
        else:
            j += 1
        d = max(d, abs(i/na - j/nb))
    return float(d)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--top-k", type=int, default=30)
    args = ap.parse_args()

    base = { _key(r): r for r in read_ndjson(args.baseline) if _key(r) }
    cand = { _key(r): r for r in read_ndjson(args.candidate) if _key(r) }

    keys = sorted(list(set(base.keys()) & set(cand.keys())))
    pb = [_p(base[k]) for k in keys]
    pc = [_p(cand[k]) for k in keys]
    dp = [pc[i]-pb[i] for i in range(len(keys))]

    y = None
    if keys and ("y_edge" in cand[keys[0]]):
        y = [_y(cand[k]) for k in keys]

    report = {
        "n_overlap": len(keys),
        "p_base": {"p50": pctl(pb,0.50), "p90": pctl(pb,0.90), "p99": pctl(pb,0.99)},
        "p_cand": {"p50": pctl(pc,0.50), "p90": pctl(pc,0.90), "p99": pctl(pc,0.99)},
        "delta_p": {"p50": pctl(dp,0.50), "p90": pctl(dp,0.90), "p99": pctl(dp,0.99)},
        "ks": _ks_stat(pb, pc),
    }
    if y is not None:
        report["cand_brier"] = brier(pc, y)
        report["cand_ece"] = ece(pc, y)

    idx = sorted(range(len(keys)), key=lambda i: abs(dp[i]), reverse=True)[:args.top_k]
    top = []
    for i in idx:
        k = keys[i]
        top.append({
            "sid": k,
            "p_base": pb[i],
            "p_cand": pc[i],
            "dp": dp[i],
            "symbol": cand[k].get("symbol", base[k].get("symbol","")),
            "scenario": cand[k].get("scenario", base[k].get("scenario","")),
        })
    report["top_abs_delta"] = top

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
