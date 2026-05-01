#!/usr/bin/env python3
from __future__ import annotations
"""
Auto-apply Tick Gate Daily Report

Aggregates decisions published to Redis Stream (default: ops:auto_apply_tick_gate).
This stream is written by the blocker (Step 25/26) when enabled.

ENV:
  REDIS_URL=redis://host:6379/0
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List, Tuple


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or str(v).strip() == "" else str(v).strip()


def _decode_map(m: Dict[Any, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (m or {}).items():
        if isinstance(k, (bytes, bytearray)):
            k = k.decode("utf-8", errors="replace")
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        out[str(k)] = str(v)
    return out


def _ms_now() -> int:
    return get_ny_time_millis()


def _range_start_id(hours: float) -> str:
    start_ms = _ms_now() - int(hours * 3600 * 1000)
    return f"{start_ms}-0"


def fetch_stream(rds, stream: str, hours: float, limit: int) -> List[Tuple[str, Dict[str, str]]]:
    start_id = _range_start_id(hours)
    entries = rds.xrange(stream, min=start_id, max="+", count=limit)
    out: List[Tuple[str, Dict[str, str]]] = []
    for entry_id, fields in entries:
        if isinstance(entry_id, (bytes, bytearray)):
            entry_id = entry_id.decode("utf-8", errors="replace")
        out.append((str(entry_id), _decode_map(fields)))
    return out


def aggregate(entries: List[Tuple[str, Dict[str, str]]]) -> Dict[str, Any]:
    status_c = Counter()
    reason_c = Counter()
    rc_c = Counter()
    by_day = defaultdict(Counter)

    for entry_id, f in entries:
        status = (f.get("status") or f.get("result") or "").strip().lower() or "unknown"
        status_c[status] += 1
        reason = (f.get("pinned_reason") or f.get("reason") or f.get("fail_reason") or "").strip() or "unknown"
        reason_c[reason] += 1
        rc = (f.get("rc") or "").strip()
        if rc:
            rc_c[rc] += 1
        try:
            ms = int(entry_id.split("-")[0])
            day = time.strftime("%Y-%m-%d", time.localtime(ms / 1000))
            by_day[day][status] += 1
        except Exception:
            pass

    return {
        "n": len(entries),
        "status": dict(status_c),
        "top_reasons": reason_c.most_common(12),
        "top_rc": rc_c.most_common(10),
        "by_day": {k: dict(v) for k, v in sorted(by_day.items())},
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--stream", type=str, default=_env("AUTO_APPLY_TICK_GATE_STREAM", "ops:auto_apply_tick_gate"))
    ap.add_argument("--format", type=str, choices=["text", "json"], default="text")
    args = ap.parse_args()

    try:
        import redis  # type: ignore
    except Exception as e:
        raise SystemExit(f"redis not available: {e}")

    redis_url = _env("REDIS_URL", _env("CRYPTO_NOTIFY_REDIS_URL", "redis://localhost:6379/0"))
    rds = redis.Redis.from_url(redis_url, decode_responses=False)

    entries = fetch_stream(rds, args.stream, args.hours, args.limit)
    rep = aggregate(entries)

    if args.format == "json":
        print(json.dumps(rep, ensure_ascii=False, indent=2))
        return 0

    print(f"stream={args.stream} window_hours={args.hours} n={rep['n']}")
    print("status_counts:")
    for k, v in sorted(rep["status"].items(), key=lambda kv: kv[1], reverse=True):
        print(f"  {k}: {v}")
    print("top_reasons:")
    for reason, n in rep["top_reasons"]:
        print(f"  {reason}: {n}")
    if rep["top_rc"]:
        print("top_rc:")
        for rc, n in rep["top_rc"]:
            print(f"  {rc}: {n}")
    if rep["by_day"]:
        print("by_day:")
        for day, cnts in rep["by_day"].items():
            s = ", ".join([f"{k}={v}" for k, v in sorted(cnts.items())])
            print(f"  {day}: {s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
