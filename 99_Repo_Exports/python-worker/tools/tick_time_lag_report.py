#!/usr/bin/env python3
"""Offline report for tick time distributions from Redis stream.

The stream is written by OrderFlowStrategy when `TICK_TIME_STREAM_ENABLE=1`.

Fields expected in each entry:
  symbol, action(ok/clamp/drop), reason, raw_ts_ms, ts_ms, prev_ts_ms,
  now_wall_ms, age_ms, back_ms

Outputs:
  - per-symbol counts of ok/clamp/drop
  - quantiles for age_ms and back_ms
  - suggested env values for TICK_TIME_MAX_REORDER_MS (from back_ms) and
    TICK_TIME_MAX_PAST_MS (from age_ms) as guidance.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import redis


@dataclass
class Stats:
    n: int
    counts: Dict[str, int]
    age_ms: List[int]
    back_ms: List[int]


def _q(arr: List[int], p: float) -> Optional[float]:
    if not arr:
        return None
    a = np.array(arr, dtype=np.float64)
    return float(np.quantile(a, p))


def _suggest_reorder_ms(back_ms: List[int]) -> Optional[int]:
    if not back_ms:
        return None
    # Prefer p99.5 to avoid over-dropping while keeping tails under control.
    p995 = _q(back_ms, 0.995)
    if p995 is None:
        return None
    # Add small safety buffer (+100ms) and round to nearest 50ms
    v = int(max(0, p995) + 100)
    return int((v + 25) // 50 * 50)


def _suggest_past_ms(age_ms: List[int]) -> Optional[int]:
    if not age_ms:
        return None
    # If you enforce late tick drops elsewhere, use p99.5(age) + buffer.
    p995 = _q(age_ms, 0.995)
    if p995 is None:
        return None
    v = int(max(0, p995) + 200)
    # Round to nearest 250ms
    return int((v + 125) // 250 * 250)


def fetch_stream(
    r: redis.Redis,
    stream: str,
    *,
    count: int,
    start_id: str = "-",
) -> List[Tuple[str, Dict[str, Any]]]:
    # Read last `count` entries using XREVRANGE for efficiency.
    # We reverse to keep chronological order for potential future uses.
    rows = r.xrevrange(stream, max="+", min=start_id, count=count)
    out: List[Tuple[str, Dict[str, Any]]] = []
    for msg_id, fields in rows:
        out.append((msg_id, fields))
    out.reverse()
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Report tick time quantiles from Redis stream metrics:tick_time")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", os.getenv("REDIS_TICKS_URL", "redis://localhost:6379/0")))
    ap.add_argument("--stream", default=os.getenv("TICK_TIME_STREAM_KEY", "metrics:tick_time"))
    ap.add_argument("--n", type=int, default=int(os.getenv("N", "50000") or 50000), help="Number of last entries to scan")
    ap.add_argument("--symbol", default=os.getenv("SYMBOL", ""), help="Optional: filter by symbol")
    ap.add_argument("--json", action="store_true", help="Output JSON only")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    rows = fetch_stream(r, args.stream, count=args.n)

    per: Dict[str, Stats] = {}

    def get_stats(sym: str) -> Stats:
        if sym not in per:
            per[sym] = Stats(n=0, counts=defaultdict(int), age_ms=[], back_ms=[])
        return per[sym]

    for _msg_id, f in rows:
        sym = str(f.get("symbol") or "").upper()
        if not sym:
            continue
        if args.symbol and sym != args.symbol.upper():
            continue
        st = get_stats(sym)
        st.n += 1
        action = str(f.get("action") or "unknown")
        reason = str(f.get("reason") or "unknown")
        st.counts[f"{action}:{reason}"] += 1
        try:
            age = int(float(f.get("age_ms") or 0))
        except Exception:
            age = 0
        try:
            back = int(float(f.get("back_ms") or 0))
        except Exception:
            back = 0
        if age > 0:
            st.age_ms.append(age)
        if back > 0:
            st.back_ms.append(back)

    out: Dict[str, Any] = {"stream": args.stream, "n_scanned": len(rows), "symbols": {}}
    for sym, st in sorted(per.items()):
        out["symbols"][sym] = {
            "n": st.n,
            "counts": dict(sorted(st.counts.items(), key=lambda kv: kv[0])),
            "age_ms": {
                "p50": _q(st.age_ms, 0.50),
                "p95": _q(st.age_ms, 0.95),
                "p99": _q(st.age_ms, 0.99),
                "p995": _q(st.age_ms, 0.995),
                "max": (float(max(st.age_ms)) if st.age_ms else None),
            },
            "back_ms": {
                "p50": _q(st.back_ms, 0.50),
                "p95": _q(st.back_ms, 0.95),
                "p99": _q(st.back_ms, 0.99),
                "p995": _q(st.back_ms, 0.995),
                "max": (float(max(st.back_ms)) if st.back_ms else None),
            },
            "suggest_env": {
                "TICK_TIME_MAX_REORDER_MS": _suggest_reorder_ms(st.back_ms),
                "TICK_TIME_MAX_PAST_MS": _suggest_past_ms(st.age_ms),
            },
        }

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    # Human output
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

