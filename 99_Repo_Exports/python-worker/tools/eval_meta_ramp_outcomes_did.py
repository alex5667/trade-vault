#!/usr/bin/env python3
from __future__ import annotations

from domain.evidence_keys import MetaKeys

"""
eval_meta_ramp_outcomes_did.py

Difference-in-Differences (DiD) evaluator for meta ENFORCE ramp outcomes.

Compares outcome changes:
  - Before ramp: enforce subset vs control subset
  - After ramp: enforce subset vs control subset
  - DiD = (after_enforce - after_control) - (before_enforce - before_control)

Positive DiD means enforce improved more (or degraded less) than control after ramp.

Usage:
  python tools/eval_meta_ramp_outcomes_did.py --trades /tmp/trades.ndjson --out /tmp/eval_did.json --ramp-ts-ms 1234567890000 --symbols BTCUSDT,ETHUSDT
"""


import argparse
import json
import random
from collections.abc import Iterator
from typing import Any


def iter_ndjson(path: str) -> Iterator[dict[str, Any]]:
    """Iterate over NDJSON file, yielding parsed JSON objects."""
    with open(path, encoding="utf-8") as f:
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
        return d


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return d


def _event_ts_ms(r: dict[str, Any]) -> int:
    """Extract event timestamp in milliseconds (tolerates multiple shapes)."""
    # tolerate multiple shapes
    for k in ("ts_ms", "ts", "exit_ts_ms", "event_ts_ms"):
        if k in r:
            v = r.get(k)
            try:
                vv = int(float(v))
                if vv > 10_000_000_000:  # ms
                    return vv
                # seconds → ms
                if 1_000_000_000 < vv < 10_000_000_000:
                    return vv * 1000
            except Exception:
                pass
    return 0


def pctl(xs: list[float], q: float) -> float:
    """Compute percentile (q in [0,1])."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def stats(rs: list[float]) -> dict[str, float]:
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


def _delta(enf: dict[str, float], ctl: dict[str, float]) -> dict[str, float]:
    """Compute delta: enforce - control."""
    return {
        "mean_delta": float(enf["meanR"] - ctl["meanR"]),
        "tail_delta": float(enf["tail_rate"] - ctl["tail_rate"]),  # want negative
        "win_delta": float(enf["winrate"] - ctl["winrate"]),
    }


def bootstrap_did(
    eb: list[float], cb: list[float], ea: list[float], ca: list[float],
    *, iters: int, seed: int
) -> dict[str, float]:
    """
    Bootstrap confidence intervals for DiD (difference-in-differences).
    
    DiD_mean = (mean(ea)-mean(ca)) - (mean(eb)-mean(cb))
    DiD_tail = (tail(ea)-tail(ca)) - (tail(eb)-tail(cb))
    
    Positive DiD means enforce improved more (or degraded less) than control after ramp.
    """
    rng = random.Random(seed)
    if min(len(eb), len(cb), len(ea), len(ca)) < 30:
        return {"ok": 0.0}

    def samp_mean(xs: list[float]) -> float:
        s = 0.0
        for _ in range(len(xs)):
            s += xs[rng.randrange(0, len(xs))]
        return s / len(xs)

    def samp_tail(xs: list[float]) -> float:
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--symbols", default="")
    ap.add_argument("--ramp-ts-ms", type=int, required=True)
    ap.add_argument("--window-hours", type=float, default=72.0)

    ap.add_argument("--min-n-per-group", type=int, default=120)
    ap.add_argument("--max-missing-tag-frac", type=float, default=0.30)

    # Decision gates (DiD):
    # We want tail_did < 0 (tail worsens less / improves more after ramp). Strong gate: tail_did_p95 < 0.
    ap.add_argument("--did_tail_p95_max", type=float, default=0.0)
    # Mean DiD should not be too negative: require did_mean_p05 >= -0.03 (conservative).
    ap.add_argument("--did_mean_p05_min", type=float, default=-0.03)
    # Absolute cap for tail in AFTER enforce subset
    ap.add_argument("--after_tail_enf_max", type=float, default=0.18)

    ap.add_argument("--bootstrap-iters", type=int, default=800)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    sym_set = {s.strip().upper() for s in (args.symbols or "").split(",") if s.strip()}

    ramp_ts = int(args.ramp_ts_ms)
    win_ms = int(args.window_hours * 3600_000)
    before_from = ramp_ts - win_ms
    before_to = ramp_ts
    after_from = ramp_ts
    after_to = ramp_ts + win_ms  # we'll cap by "now" implicitly by available trades

    eb: list[float] = []  # enforce before
    cb: list[float] = []  # control before
    ea: list[float] = []  # enforce after
    ca: list[float] = []  # control after

    missing_tag = 0
    total = 0

    for r in iter_ndjson(args.trades):
        sym = (r.get("symbol", "") or "").upper()
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
        applied = r.get(MetaKeys.ENFORCE_APPLIED, None)
        if applied is None:
            missing_tag += 1
            continue
        a = _i(applied, 0)

        if before_from <= ts < before_to:
            (eb if a == 1 else cb).append(rmf)
        elif after_from <= ts < after_to:
            (ea if a == 1 else ca).append(rmf)

    out: dict[str, Any] = {
        "window_hours": args.window_hours,
        "ramp_ts_ms": ramp_ts,
        "before": {"from_ms": before_from, "to_ms": before_to},
        "after": {"from_ms": after_from, "to_ms": after_to},
        "total": total,
        "missing_meta_enforce_applied": missing_tag,
        "n": {"eb": len(eb), "cb": len(cb), "ea": len(ea), "ca": len(ca)},
    }

    if total > 0 and (missing_tag / max(1, total)) > args.max_missing_tag_frac:
        out["decision"] = {"ok_to_ramp": False, "reason": "missing_meta_tags"}
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        return

    # Minimum sample per group
    if min(len(eb), len(cb), len(ea), len(ca)) < args.min_n_per_group:
        out["decision"] = {"ok_to_ramp": False, "reason": "insufficient_n"}
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        return

    sb_enf = stats(eb)
    sb_ctl = stats(cb)
    sa_enf = stats(ea)
    sa_ctl = stats(ca)

    db = _delta(sb_enf, sb_ctl)
    da = _delta(sa_enf, sa_ctl)

    did = {
        "did_mean": float(da["mean_delta"] - db["mean_delta"]),
        "did_tail": float(da["tail_delta"] - db["tail_delta"]),
        "did_win": float(da["win_delta"] - db["win_delta"]),
    }

    ci = bootstrap_did(eb, cb, ea, ca, iters=args.bootstrap_iters, seed=args.seed)

    out["before_stats"] = {"enforce": sb_enf, "control": sb_ctl, "delta": db}
    out["after_stats"] = {"enforce": sa_enf, "control": sa_ctl, "delta": da}
    out["did"] = did
    out["bootstrap_ci"] = ci

    reasons = []
    # absolute after enforce tail cap
    if float(sa_enf.get("tail_rate", 0.0)) > args.after_tail_enf_max:
        reasons.append(f"after_tail_enf_cap({sa_enf['tail_rate']:.2f}>{args.after_tail_enf_max:.2f})")

    # CI-based decisions
    if ci.get("ok", 0.0) != 1.0:
        reasons.append("bootstrap_insufficient_n")
    else:
        if float(ci.get("did_tail_p95", 0.0)) > args.did_tail_p95_max:
            reasons.append(f"did_tail_p95_not_ok({ci.get('did_tail_p95',0.0):.3f}>{args.did_tail_p95_max:.3f})")
        if float(ci.get("did_mean_p05", 0.0)) < args.did_mean_p05_min:
            reasons.append(f"did_mean_p05_too_low({ci.get('did_mean_p05',0.0):.3f}<{args.did_mean_p05_min:.3f})")

    out["decision"] = {"ok_to_ramp": (len(reasons) == 0), "reasons": reasons}

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

