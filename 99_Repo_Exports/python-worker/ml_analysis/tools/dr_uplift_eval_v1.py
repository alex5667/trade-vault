#!/usr/bin/env python3
from __future__ import annotations

"""
Doubly-Robust uplift estimator for Confidence Calibration A/B.

Inputs:
- decisions NDJSON exported from Redis Stream `decisions:final`
- trades:closed NDJSON exported from Redis Stream `trades:closed`

It joins by sid, computes reward (default: binary 1{realized_R > 0}),
and estimates the average treatment effect (challenger - champion) using AIPW/DR.

This script is designed to be run offline (cron/systemd timer) and write a JSON report that
the promotion manager can consume.
"""

import argparse
import gzip
import json
import math
import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _iter_lines(path: Path) -> Iterator[str]:
    if str(path).endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line
    else:
        with open(path, encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line


def _iter_ndjson(paths: list[Path]) -> Iterator[dict[str, Any]]:
    for p in paths:
        for line in _iter_lines(p):
            try:
                yield json.loads(line)
            except Exception:
                continue


def _expand_paths(p: str) -> list[Path]:
    path = Path(p)
    if path.is_dir():
        out: list[Path] = []
        for ext in ("*.ndjson", "*.ndjson.gz", "*.jsonl", "*.jsonl.gz"):
            out.extend(sorted(path.glob(ext)))
        return out
    return [path]


def _realized_r(row: dict[str, Any]) -> float | None:
    # prefer explicit
    for k in ("realized_R", "r_mult", "r_multiple", "R"):
        if k in row:
            try:
                return float(row[k])
            except Exception:
                pass
    pnl = row.get("pnl")
    risk = row.get("risk_usd") or row.get("risk")
    if pnl is not None and risk:
        try:
            risk_f = float(risk)
            if risk_f > 0:
                return float(pnl) / risk_f
        except Exception:
            return None
    return None


@dataclass
class DRInputs:
    y: float
    a: int
    p1: float
    q0: float
    q1: float


def _dr_pseudo(inp: DRInputs) -> float:
    """
    AIPW/DR pseudo-outcome for treatment effect (challenger - champion):
    (q1 - q0) + I(a=1)*(y-q1)/p1 - I(a=0)*(y-q0)/(1-p1)
    """
    p1 = inp.p1
    if p1 <= 0.0 or p1 >= 1.0:
        return float("nan")
    base = (inp.q1 - inp.q0)
    if inp.a == 1:
        return base + (inp.y - inp.q1) / p1
    return base - (inp.y - inp.q0) / (1.0 - p1)


def _empirical_bernstein_ci(samples: list[float], alpha: float, bound_B: float) -> tuple[float, float]:
    """
    Maurer-Pontil empirical Bernstein bound for mean.
    Assumes samples are bounded in [-B, B] (width = 2B).
    """
    n = len(samples)
    if n <= 1:
        return float("nan"), float("nan")
    mean = sum(samples) / n
    # sample variance
    var = sum((x - mean) ** 2 for x in samples) / (n - 1)
    delta = alpha
    # ln(3/delta)
    l = math.log(max(3.0 / max(delta, 1e-12), 1.0))
    rad = math.sqrt(2.0 * var * l / n)
    slack = 3.0 * (2.0 * bound_B) * l / n
    return mean - rad - slack, mean + rad + slack


def _bootstrap_ci(samples: list[float], alpha: float, B: int, seed: int) -> tuple[float, float]:
    n = len(samples)
    if n == 0:
        return float("nan"), float("nan")
    rng = random.Random(seed)
    means = []
    for _ in range(B):
        s = 0.0
        for _ in range(n):
            s += samples[rng.randrange(n)]
        means.append(s / n)
    means.sort()
    lo_idx = int((alpha / 2.0) * B)
    hi_idx = int((1.0 - alpha / 2.0) * B) - 1
    lo_idx = max(0, min(B - 1, lo_idx))
    hi_idx = max(0, min(B - 1, hi_idx))
    return means[lo_idx], means[hi_idx]


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--decisions", required=True, nargs="+", help="NDJSON file or dir exported from decisions:final")
    ap.add_argument("--closed", required=True, nargs="+", help="NDJSON file or dir exported from trades:closed")
    ap.add_argument("--reward", choices=["binary", "continuous"], default="binary")
    ap.add_argument("--out", required=True, help="Write JSON report here")
    ap.add_argument("--alpha", type=float, default=0.05)
    ap.add_argument("--ci", choices=["bootstrap", "bernstein", "both"], default="both")
    ap.add_argument("--bootstrap-b", type=int, default=400)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--min-propensity", type=float, default=1e-3)
    ap.add_argument("--policy-ok-only", action="store_true", default=True)
    args = ap.parse_args(list(argv) if argv is not None else None)

    decision_paths: list[Path] = []
    for p in args.decisions:
        decision_paths.extend(_expand_paths(p))
    closed_paths: list[Path] = []
    for p in args.closed:
        closed_paths.extend(_expand_paths(p))

    # outcomes by sid
    outcomes: dict[str, dict[str, Any]] = {}
    for row in _iter_ndjson(closed_paths):
        sid = str(row.get("sid") or row.get("signal_id") or "")
        if not sid:
            continue
        # keep latest by ts_ms
        ts = int(row.get("ts_ms") or 0)
        prev = outcomes.get(sid)
        if prev is None or int(prev.get("ts_ms") or 0) <= ts:
            outcomes[sid] = row

    used: list[float] = []
    joined = 0
    skipped = {"no_sid": 0, "no_outcome": 0, "no_conf_cal": 0, "no_q": 0, "bad_p": 0, "policy_fallback": 0, "nan": 0}

    for rec in _iter_ndjson(decision_paths):
        sid = str(rec.get("sid") or (rec.get("ctx") or {}).get("sid") or "")
        if not sid:
            skipped["no_sid"] += 1
            continue
        out = outcomes.get(sid)
        if not out:
            skipped["no_outcome"] += 1
            continue
        joined += 1

        conf_cal = rec.get("conf_cal") if isinstance(rec.get("conf_cal"), dict) else None
        if not conf_cal:
            skipped["no_conf_cal"] += 1
            continue

        # policy sanity: drop fallback samples by default
        if args.policy_ok_only:
            if int(conf_cal.get("fallback_to_champion", 0) or 0) != 0:
                skipped["policy_fallback"] += 1
                continue
            if (conf_cal.get("arm_assigned") and conf_cal.get("arm_taken") and conf_cal["arm_assigned"] != conf_cal["arm_taken"]):
                skipped["policy_fallback"] += 1
                continue

        a = 1 if (conf_cal.get("arm_taken", "champion")) == "challenger" else 0
        p1 = float(conf_cal.get("p_challenger", 0.0) or 0.0)
        if p1 < args.min_propensity or p1 > 1.0 - args.min_propensity:
            skipped["bad_p"] += 1
            continue

        # Q-model surrogates (win probability)
        try:
            q0 = float(conf_cal.get("q_champion"))
            q1 = float(conf_cal.get("q_challenger"))
        except Exception:
            skipped["no_q"] += 1
            continue
        if not (math.isfinite(q0) and math.isfinite(q1)):
            skipped["no_q"] += 1
            continue
        q0 = max(0.0, min(1.0, q0))
        q1 = max(0.0, min(1.0, q1))

        r = _realized_r(out)
        if r is None or not math.isfinite(r):
            skipped["no_outcome"] += 1
            continue

        if args.reward == "binary":
            y = 1.0 if float(r) > 0.0 else 0.0
        else:
            # continuous reward; clipped to tame tails (still risky for bounds)
            y = max(-3.0, min(3.0, float(r)))

        pseudo = _dr_pseudo(DRInputs(y=y, a=a, p1=p1, q0=q0, q1=q1))
        if not math.isfinite(pseudo):
            skipped["nan"] += 1
            continue
        used.append(float(pseudo))

    n = len(used)
    mean = sum(used) / n if n else float("nan")

    report: dict[str, Any] = {
        "version": "dr_uplift_eval_v1",
        "reward": args.reward,
        "n_joined": joined,
        "n_used": n,
        "mean_uplift": mean,
        "skipped": skipped,
        "ci": {},
    }

    if n:
        # bound for Bernstein: use min propensity
        p_min = max(args.min_propensity, 1e-9)
        B = 1.0 + 1.0 / min(p_min, 1.0 - p_min)
        if args.ci in ("bernstein", "both"):
            lo, hi = _empirical_bernstein_ci(used, alpha=args.alpha, bound_B=B)
            report["ci"]["bernstein"] = {"alpha": args.alpha, "lower": lo, "upper": hi, "bound_B": B}
        if args.ci in ("bootstrap", "both"):
            lo, hi = _bootstrap_ci(used, alpha=args.alpha, B=args.bootstrap_b, seed=args.seed)
            report["ci"]["bootstrap"] = {"alpha": args.alpha, "lower": lo, "upper": hi, "B": args.bootstrap_b, "seed": args.seed}

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
