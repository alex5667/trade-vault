#!/usr/bin/env python3
from __future__ import annotations

"""Report most frequent missing legs directly from metrics:of_gate.

Why:
  - If ok_rate ~ 0 because have < need, you want the dominant missing leg quickly.
  - We store miss_leg as a separate field (and hint in reason), but older entries may only have missing_legs JSON.

Usage:
  REDIS_URL=redis://redis-worker-1:6379/0 python3 tools/of_gate_missing_leg_report.py --limit 8000 --only-veto
"""


import argparse
import json
import os
from collections import Counter
from typing import Any
import contextlib


def _decode(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(x)


def _parse_missing_legs(payload: dict[str, Any]) -> list[str]:
    ml = payload.get("miss_leg")
    if ml:
        s = _decode(ml).strip()
        if s:
            return [s]
    raw = payload.get("missing_legs")
    if not raw:
        return []
    try:
        s = _decode(raw)
        arr = json.loads(s)
        if isinstance(arr, list):
            out = []
            for v in arr:
                if isinstance(v, str) and v.strip():
                    out.append(v.strip())
            return out
    except Exception:
        return []
    return []


async def main_async() -> None:
    ap = argparse.ArgumentParser(description="Report top missing legs from OF gate metrics stream")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", os.getenv("REDIS_MAIN_URL", "redis://localhost:6379/0")), help="Redis URL")
    ap.add_argument("--stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"), help="Metrics stream name")
    ap.add_argument("--limit", type=int, default=int(os.getenv("OF_GATE_MISS_LIMIT", "8000") or 8000), help="How many last entries to scan")
    ap.add_argument("--only-veto", action="store_true", help="Count only ok=0 entries")
    ap.add_argument("--top", type=int, default=15, help="Top-N legs to print")
    args = ap.parse_args()

    try:
        import redis.asyncio as aioredis
    except Exception as exc:
        raise SystemExit(f"redis_asyncio_import_failed: {exc}")

    r = aioredis.from_url(args.redis_url, decode_responses=False)
    ctr = Counter()
    n = 0
    n_eff = 0

    try:
        entries: list[tuple[bytes, dict[bytes, bytes]]] = await r.xrevrange(args.stream, max="+", min="-", count=int(args.limit))
    except Exception as exc:
        raise SystemExit(f"xrevrange_failed: {exc}")

    for _id, fields in entries:
        try:
            payload = {_decode(k): _decode(v) for k, v in (fields or {}).items()}
        except Exception:
            continue
        n += 1
        if args.only_veto:
            try:
                ok = int(float(payload.get("ok", "0") or "0"))
            except Exception:
                ok = 0
            if ok != 0:
                continue
        n_eff += 1
        legs = _parse_missing_legs(payload)
        if not legs:
            continue
        ctr[legs[0]] += 1

    rep = {
        "stream": str(args.stream),
        "scanned": int(n),
        "scanned_effective": int(n_eff),
        "only_veto": bool(args.only_veto),
        "top": [{"leg": k, "count": int(v)} for k, v in ctr.most_common(int(args.top))],
    }
    print(json.dumps(rep, ensure_ascii=False))
    with contextlib.suppress(Exception):
        await r.aclose()


def main() -> None:
    import asyncio
    asyncio.run(main_async())


if __name__ == "__main__":
    main()

