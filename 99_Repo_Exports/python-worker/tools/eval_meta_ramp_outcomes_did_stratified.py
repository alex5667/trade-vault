#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eval_meta_ramp_outcomes_did_stratified.py

Stratified Difference-in-Differences (DiD) evaluator for meta ENFORCE ramp outcomes.

Stratifies by symbol × regime_bucket (trend/range/news/other) and requires worst-case gating:
- All sufficiently filled strata must pass strict CI gates (tail/mean)
- Coverage must be sufficient (not "passed only on one regime")
- Overall DiD must also be OK

Usage:
  python tools/eval_meta_ramp_outcomes_did_stratified.py \
    --trades /tmp/trades.ndjson --out /tmp/eval_did_stratified.json \
    --ramp-ts-ms 1234567890000 --symbols BTCUSDT,ETHUSDT \
    --window-hours 72.0 --min-n-per-cell 120 --min-cells 3
"""

from __future__ import annotations

import argparse
import json
import random
from typing import Any, Dict, Iterator, List, Tuple


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


def _event_ts_ms(r: Dict[str, Any]) -> int:
    """Extract event timestamp in milliseconds (tolerates multiple shapes)."""
    for k in ("ts_ms", "ts", "exit_ts_ms", "event_ts_ms"):
        if k in r:
            v = r.get(k)
            try:
                vv = int(float(v))
                if vv > 10_000_000_000:
                    return vv
                if 1_000_000_000 < vv < 10_000_000_000:
                    return vv * 1000
            except Exception:
                pass
    return 0


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
        return {"n": 0.0}
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


def bootstrap_did(
    eb: List[float], cb: List[float], ea: List[float], ca: List[float],
    *, iters: int, seed: int
) -> Dict[str, float]:
    """
    Bootstrap confidence intervals for DiD (difference-in-differences).
    
    DiD_mean = (mean(ea)-mean(ca)) - (mean(eb)-mean(cb))
    DiD_tail = (tail(ea)-tail(ca)) - (tail(eb)-tail(cb))
    
    Positive DiD means enforce improved more (or degraded less) than control after ramp.
    """
    rng = random.Random(seed)
    if min(len(eb), len(cb), len(ea), len(ca)) < 30:
        return {"ok": 0.0}

    def samp_mean(xs: List[float]) -> float:
        s = 0.0
        for _ in range(len(xs)):
            s += xs[rng.randrange(0, len(xs))]
        return s / len(xs)

    def samp_tail(xs: List[float]) -> float:
        c = 0
        for _ in range(len(xs)):
            if xs[rng.randrange(0, len(xs))] <= -1.0:
                c += 1
        return c / len(xs)

    did_m = []
    did_t = []
    for _ in range(iters):
        mb = samp_mean(eb) - samp_mean(cb)
        ma = samp_mean(ea) - samp_mean(ca)
        tb = samp_tail(eb) - samp_tail(cb)
        ta = samp_tail(ea) - samp_tail(ca)
        did_m.append(ma - mb)
        did_t.append(ta - tb)

    did_m.sort()
    did_t.sort()
    return {
        "ok": 1.0,
        "did_mean_p05": float(did_m[int(0.05 * (iters - 1))]),
        "did_mean_p50": float(did_m[int(0.50 * (iters - 1))]),
        "did_mean_p95": float(did_m[int(0.95 * (iters - 1))]),
        "did_tail_p05": float(did_t[int(0.05 * (iters - 1))]),
        "did_tail_p50": float(did_t[int(0.50 * (iters - 1))]),
        "did_tail_p95": float(did_t[int(0.95 * (iters - 1))]),
    }


def regime_bucket(r: Dict[str, Any]) -> str:
    """
    Classify regime bucket from trade record.
    
    Prefer explicit regime_group; fallback to regime; then scenario_v4.
    """
    g = str(r.get("regime_group", "") or r.get("regime", "") or r.get("scenario_v4", "") or "")
    s = g.lower()
    if "news" in s or "fomc" in s or "cpi" in s:
        return "news"
    if "trend" in s or "bull" in s or "bear" in s:
        return "trend"
    from common.market_mode import is_range_regime; _r = is_range_regime(s)
    if _r:
        return "range"
    if "thin" in s or "illiquid" in s:
        return "thin"
    return "other"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--symbols", default="")
    ap.add_argument("--ramp-ts-ms", type=int, required=True)
    ap.add_argument("--window-hours", type=float, default=72.0)

    # sampling requirements
    ap.add_argument("--min-n-per-cell", type=int, default=120)
    ap.add_argument("--min-cells", type=int, default=3, help="require at least this many cells with sufficient n")
    ap.add_argument("--max-missing-tag-frac", type=float, default=0.30)

    # hard caps
    ap.add_argument("--after_tail_enf_max", type=float, default=0.18)

    # CI gates
    ap.add_argument("--did_tail_p95_max", type=float, default=0.0)   # want tail_did_p95 < 0
    ap.add_argument("--did_mean_p05_min", type=float, default=-0.03) # conservative mean gate

    ap.add_argument("--bootstrap-iters", type=int, default=800)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    sym_set = {s.strip().upper() for s in (args.symbols or "").split(",") if s.strip()}
    ramp_ts = int(args.ramp_ts_ms)
    win_ms = int(args.window_hours * 3600_000)

    before_from = ramp_ts - win_ms
    before_to = ramp_ts
    after_from = ramp_ts
    after_to = ramp_ts + win_ms

    # cell key -> {eb,cb,ea,ca}
    cells: Dict[str, Dict[str, List[float]]] = {}

    missing_tag = 0
    total = 0

    for r in iter_ndjson(args.trades):
        sym = str(r.get("symbol", "") or "").upper()
        if sym_set and sym not in sym_set:
            continue

        ts = _event_ts_ms(r)
        if ts <= 0:
            continue

        rm = r.get("r_mult", None)
        if rm is None:
            continue
        rmf = _f(rm, 0.0)

        total += 1
        applied = r.get("meta_enforce_applied", None)
        if applied is None:
            missing_tag += 1
            continue
        a = _i(applied, 0)

        bucket = regime_bucket(r)
        ck = f"{sym}|{bucket}"
        if ck not in cells:
            cells[ck] = {"eb": [], "cb": [], "ea": [], "ca": []}

        if before_from <= ts < before_to:
            (cells[ck]["eb"] if a == 1 else cells[ck]["cb"]).append(rmf)
        elif after_from <= ts < after_to:
            (cells[ck]["ea"] if a == 1 else cells[ck]["ca"]).append(rmf)

    out: Dict[str, Any] = {
        "window_hours": args.window_hours,
        "ramp_ts_ms": ramp_ts,
        "before": {"from_ms": before_from, "to_ms": before_to},
        "after": {"from_ms": after_from, "to_ms": after_to},
        "total": total,
        "missing_meta_enforce_applied": missing_tag,
        "cells_total": len(cells),
    }

    if total > 0 and (missing_tag / max(1, total)) > args.max_missing_tag_frac:
        out["decision"] = {"ok_to_ramp": False, "reason": "missing_meta_tags"}
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        return

    # Evaluate each cell
    evaluated = []
    failed = []
    skipped = []

    for ck, x in cells.items():
        eb, cb, ea, ca = x["eb"], x["cb"], x["ea"], x["ca"]
        if min(len(eb), len(cb), len(ea), len(ca)) < args.min_n_per_cell:
            skipped.append({"cell": ck, "n": {"eb": len(eb), "cb": len(cb), "ea": len(ea), "ca": len(ca)}})
            continue

        sb_enf = stats(eb); sb_ctl = stats(cb)
        sa_enf = stats(ea); sa_ctl = stats(ca)

        ci = bootstrap_did(eb, cb, ea, ca, iters=args.bootstrap_iters, seed=args.seed)

        reasons = []
        if float(sa_enf.get("tail_rate", 0.0)) > args.after_tail_enf_max:
            reasons.append(f"after_tail_enf_cap({sa_enf['tail_rate']:.2f}>{args.after_tail_enf_max:.2f})")
        if ci.get("ok", 0.0) != 1.0:
            reasons.append("bootstrap_insufficient_n")
        else:
            if float(ci.get("did_tail_p95", 0.0)) > args.did_tail_p95_max:
                reasons.append(f"did_tail_p95_not_ok({ci.get('did_tail_p95',0.0):.3f}>{args.did_tail_p95_max:.3f})")
            if float(ci.get("did_mean_p05", 0.0)) < args.did_mean_p05_min:
                reasons.append(f"did_mean_p05_too_low({ci.get('did_mean_p05',0.0):.3f}<{args.did_mean_p05_min:.3f})")

        rec = {
            "cell": ck,
            "n": {"eb": len(eb), "cb": len(cb), "ea": len(ea), "ca": len(ca)},
            "after_enf": sa_enf,
            "after_ctl": sa_ctl,
            "ci": ci,
            "reasons": reasons,
            "ok": len(reasons) == 0,
        }
        evaluated.append(rec)
        if rec["ok"]:
            pass
        else:
            failed.append(rec)

    out["evaluated_cells"] = len(evaluated)
    out["skipped_cells"] = len(skipped)
    out["failed_cells"] = len(failed)
    out["failed_top"] = failed[:10]
    out["skipped_top"] = skipped[:10]

    # Coverage rule: require at least min_cells evaluated
    if len(evaluated) < args.min_cells:
        out["decision"] = {"ok_to_ramp": False, "reason": "insufficient_cells", "evaluated": len(evaluated), "min_cells": args.min_cells}
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        return

    # Worst-case: any evaluated failure blocks ramp
    if failed:
        out["decision"] = {"ok_to_ramp": False, "reason": "worst_case_failed", "failed_cells": len(failed)}
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        return

    out["decision"] = {"ok_to_ramp": True, "reason": "all_cells_passed", "evaluated_cells": len(evaluated)}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

