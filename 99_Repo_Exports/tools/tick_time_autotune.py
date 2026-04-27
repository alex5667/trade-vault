"""Autotune helper for tick-time policy.

Reads a Redis Stream (default: stream:tick_time:observability) that contains
per-tick metadata and suggests safe thresholds for:
- TICK_TIME_MAX_PAST_MS
- TICK_TIME_MAX_REORDER_MS
- TICK_TIME_MAX_FUTURE_MS

This tool is *advisory* (offline). It does not modify config.

Usage:
  python -m tools.tick_time_autotune --redis redis://localhost:6379/0 --stream stream:tick_time:observability --count 50000
"""

from __future__ import annotations

import argparse
import json
import math
from typing import Any, Dict, List, Tuple

import redis


def _p(xs: List[int], q: float) -> int:
    if not xs:
        return 0
    xs = sorted(xs)
    q = min(max(q, 0.0), 1.0)
    i = int(math.ceil(q * (len(xs) - 1)))
    return int(xs[i])


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except Exception:
        return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", default="redis://localhost:6379/0")
    ap.add_argument("--stream", default="stream:tick_time:observability")
    ap.add_argument("--count", type=int, default=50000)
    ap.add_argument("--symbol", default="")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis, decode_responses=True)

    # XRANGE from '-' to '+' with COUNT is not supported; use XREVRANGE to get tail.
    entries: List[Tuple[str, Dict[str, str]]] = r.xrevrange(args.stream, max="+", min="-", count=int(args.count))
    entries.reverse()

    ages: List[int] = []
    skews: List[int] = []
    backs: List[int] = []

    for _id, fields in entries:
        if args.symbol and fields.get("symbol") != args.symbol:
            continue
        # payload stored as json in "payload" or direct fields.
        payload = fields.get("payload")
        d: Dict[str, Any] = {}
        if payload:
            try:
                d = json.loads(payload)
            except Exception:
                d = {}
        # Prefer nested meta if present.
        meta = d.get("meta") if isinstance(d, dict) else None
        if isinstance(meta, dict):
            d = meta

        age_ms = _safe_int(d.get("age_ms"))
        skew_ms = _safe_int(d.get("skew_ms"))
        back_ms = _safe_int(d.get("back_ms"))

        if age_ms:
            ages.append(age_ms)
        if skew_ms:
            skews.append(skew_ms)
        if back_ms:
            backs.append(back_ms)

    out = {
        "stream": args.stream,
        "symbol": args.symbol or None,
        "n": len(entries),
        "n_filtered": max(len(ages), len(skews), len(backs)),
        "age_ms": {"p50": _p(ages, 0.50), "p95": _p(ages, 0.95), "p99": _p(ages, 0.99), "max": max(ages) if ages else 0},
        "skew_ms": {"p50": _p(skews, 0.50), "p95": _p(skews, 0.95), "p99": _p(skews, 0.99), "max": max(skews) if skews else 0},
        "back_ms": {"p50": _p(backs, 0.50), "p95": _p(backs, 0.95), "p99": _p(backs, 0.99), "max": max(backs) if backs else 0},
        "suggest": {
            # conservative: p99 * 1.5, rounded up to nearest 100ms
            "TICK_TIME_MAX_PAST_MS": int(math.ceil((_p(ages, 0.99) * 1.5) / 100.0) * 100) if ages else None,
            "TICK_TIME_MAX_FUTURE_MS": int(math.ceil((max(0, _p(skews, 0.99)) * 1.5) / 100.0) * 100) if skews else None,
            "TICK_TIME_MAX_REORDER_MS": int(math.ceil((_p(backs, 0.99) * 1.5) / 10.0) * 10) if backs else None,
        },
    }

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
