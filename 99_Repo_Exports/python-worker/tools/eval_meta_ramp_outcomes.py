#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
eval_meta_ramp_outcomes.py

Evaluates outcome of meta ENFORCE canary subset vs control to decide if ramp is safe.

Compares:
  - enforce subset (meta_enforce_applied=1): meanR, tail_rate, winrate
  - control subset (meta_enforce_applied=0): meanR, tail_rate, winrate

Gates:
  - enforce tail_rate must be <= max_tail_enforce (hard cap)
  - mean_delta (enforce - control) must be >= min_pass_delta_mean (can be slightly worse)
  - tail_improve (control - enforce) must be >= min_tail_improve (require improvement)
  - bootstrap CI: tail_delta_p95 < 0 (tail decrease likely)

Usage:
  python tools/eval_meta_ramp_outcomes.py --trades /tmp/trades.ndjson --out /tmp/eval.json --symbols BTCUSDT,ETHUSDT
"""


import argparse
import json
import math
import random
from typing import Any, Dict, Iterator, List, Optional, Tuple


def iter_ndjson(path: str) -> Iterator[Dict[str, Any]]:
    """Iterate over NDJSON file, yielding parsed JSON objects."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return int(d)


def pctl(xs: List[float], q: float) -> float:
    """Compute percentile (q in [0,1])."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def stats(rs: List[float]) -> Dict[str, float]:
    """Compute statistics for return multiples."""
    n = len(rs)
    if n == 0:
        return {"n": 0}
    mean = sum(rs) / n
    win = sum(1 for x in rs if x > 0.0) / n
    tail = sum(1 for x in rs if x <= -1.0) / n
    return {
        "n": float(n),
        "meanR": float(mean),
        "medianR": float(pctl(rs, 0.50)),
        "p05": float(pctl(rs, 0.05)),
        "p95": float(pctl(rs, 0.95)),
        "winrate": float(win),
        "tail_rate": float(tail),
    }


def bootstrap_diff(a: List[float], b: List[float], *, iters: int, seed: int) -> Dict[str, float]:
    """
    Bootstrap confidence intervals for delta meanR and delta tail_rate.
    
    Returns CI for delta = a - b.
    """
    rng = random.Random(seed)
    if len(a) < 20 or len(b) < 20:
        return {"ok": 0.0}

    def sample_mean(xs: List[float]) -> float:
        s = 0.0
        for _ in range(len(xs)):
            s += xs[rng.randrange(0, len(xs))]
        return s / len(xs)

    def sample_tail(xs: List[float]) -> float:
        c = 0
        for _ in range(len(xs)):
            if xs[rng.randrange(0, len(xs))] <= -1.0:
                c += 1
        return c / len(xs)

    dm = []
    dt = []
    for _ in range(iters):
        dm.append(sample_mean(a) - sample_mean(b))
        dt.append(sample_tail(a) - sample_tail(b))

    dm.sort()
    dt.sort()
    return {
        "ok": 1.0,
        "mean_delta_p05": float(dm[int(0.05 * (iters - 1))]),
        "mean_delta_p50": float(dm[int(0.50 * (iters - 1))]),
        "mean_delta_p95": float(dm[int(0.95 * (iters - 1))]),
        "tail_delta_p05": float(dt[int(0.05 * (iters - 1))]),
        "tail_delta_p50": float(dt[int(0.50 * (iters - 1))]),
        "tail_delta_p95": float(dt[int(0.95 * (iters - 1))]),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True, help="NDJSON from export_trade_closed_ndjson.py")
    ap.add_argument("--out", required=True)
    ap.add_argument("--symbols", default="")
    ap.add_argument("--min-n", type=int, default=200)
    ap.add_argument("--min-enforce-n", type=int, default=80)
    ap.add_argument("--min-pass-delta-mean", type=float, default=-0.02, help="enforce meanR can be slightly worse, but bounded")
    ap.add_argument("--min-tail-improve", type=float, default=0.01, help="require tail_rate improvement at least this")
    ap.add_argument("--max-tail-enforce", type=float, default=0.18, help="hard cap tail rate in enforce subset")
    ap.add_argument("--bootstrap-iters", type=int, default=800)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    sym_set = {s.strip().upper() for s in (args.symbols or "").split(",") if s.strip()}

    # Split by meta_enforce_applied
    enforce_rs: List[float] = []
    control_rs: List[float] = []
    missing_tag = 0
    total = 0

    for r in iter_ndjson(args.trades):
        sym = str(r.get("symbol", "") or "").upper()
        if sym_set and sym not in sym_set:
            continue

        # r_mult must exist
        rm = _f(r.get("r_mult", None), None)
        if rm is None:
            continue

        total += 1
        applied = r.get("meta_enforce_applied", None)
        if applied is None:
            missing_tag += 1
            continue

        a = _i(applied, 0)
        if a == 1:
            enforce_rs.append(float(rm))
        else:
            control_rs.append(float(rm))

    out: Dict[str, Any] = {
        "total": total,
        "missing_meta_enforce_applied": missing_tag,
        "enforce": stats(enforce_rs),
        "control": stats(control_rs),
    }

    # If tags missing -> no decision
    if total > 0 and missing_tag > int(0.30 * total):
        out["decision"] = {"ok_to_ramp": False, "reason": "missing_meta_tags"}
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(json.dumps(out["decision"], ensure_ascii=False))
        return

    n_all = len(enforce_rs) + len(control_rs)
    if n_all < args.min_n or len(enforce_rs) < args.min_enforce_n or len(control_rs) < args.min_enforce_n:
        out["decision"] = {"ok_to_ramp": False, "reason": "insufficient_n"}
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(json.dumps(out["decision"], ensure_ascii=False))
        return

    # Compute deltas (enforce - control)
    mean_delta = out["enforce"]["meanR"] - out["control"]["meanR"]
    tail_delta = out["enforce"]["tail_rate"] - out["control"]["tail_rate"]  # want negative
    tail_improve = out["control"]["tail_rate"] - out["enforce"]["tail_rate"]

    out["delta"] = {
        "mean_delta": float(mean_delta),
        "tail_delta": float(tail_delta),
        "tail_improve": float(tail_improve),
    }

    # Bootstrap CI
    ci = bootstrap_diff(enforce_rs, control_rs, iters=args.bootstrap_iters, seed=args.seed)
    out["bootstrap_ci"] = ci

    # Gates:
    reasons = []
    if out["enforce"]["tail_rate"] > args.max_tail_enforce:
        reasons.append(f"tail_enforce_cap_exceeded({out['enforce']['tail_rate']:.2f}>{args.max_tail_enforce:.2f})")
    if mean_delta < args.min_pass_delta_mean:
        reasons.append(f"mean_delta_too_low({mean_delta:.3f}<{args.min_pass_delta_mean:.3f})")
    if tail_improve < args.min_tail_improve:
        reasons.append(f"tail_improve_too_small({tail_improve:.3f}<{args.min_tail_improve:.3f})")

    # Stronger gate using CI (optional but recommended):
    # require that tail_delta_p95 < 0 (tail decrease likely)
    if ci.get("ok", 0.0) == 1.0:
        if float(ci.get("tail_delta_p95", 0.0)) >= 0.0:
            reasons.append("tail_ci_not_strict (tail_delta_p95>=0)")

    ok_to_ramp = (len(reasons) == 0)
    out["decision"] = {"ok_to_ramp": ok_to_ramp, "reasons": reasons}

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(json.dumps(out["decision"], ensure_ascii=False))


if __name__ == "__main__":
    main()

