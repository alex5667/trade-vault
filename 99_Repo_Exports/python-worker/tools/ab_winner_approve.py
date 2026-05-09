import argparse
import asyncio
import json
import os
from typing import Any

import redis.asyncio as aioredis

from services.ab_winner_approval import (
    active_arm_key,
    decide_approve,
    lock_key,
    norm_arm,
    norm_grp,
    norm_rg,
    norm_sym,
    sugg_key,
)
from utils.time_utils import get_ny_time_millis
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()

async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--regime", required=True)
    ap.add_argument("--group", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force-arm", default="")  # override winner with explicit arm (A/B/C)
    args = ap.parse_args()

    sym = norm_sym(args.symbol)
    rg = norm_rg(args.regime)
    grp = norm_grp(args.group)

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=20)

    sk = sugg_key(symbol=sym, regime=rg, group=grp)
    raw = await r.get(sk)
    if not raw:
        print(f"NO_SUGGESTION {sk}")
        return 2

    try:
        d: dict[str, Any] = json.loads(raw)
    except Exception:
        print("BAD_JSON")
        return 3

    min_samples = int(os.getenv("AB_WINNER_MIN_SAMPLES", "40"))
    min_edge_r = float(os.getenv("AB_WINNER_MIN_EDGE_R", "0.05"))
    dec = decide_approve(d, min_samples=min_samples, min_edge_r=min_edge_r)

    # Optional manual override
    if args.force_arm:
        fa = norm_arm(args.force_arm)
        dec.ok = True
        dec.winner = fa
        dec.reason = f"forced({fa})"

    if not dec.ok:
        print(f"REJECT symbol={sym} regime={rg} group={grp} winner={dec.winner} reason={dec.reason}")
        return 4

    ak = active_arm_key(symbol=sym, regime=rg, group=grp)
    lk = lock_key(symbol=sym, regime=rg, group=grp)
    lock_sec = int(os.getenv("AB_ACTIVE_ARM_LOCK_SEC", "21600"))  # 6h
    active_ttl = int(os.getenv("AB_ACTIVE_ARM_TTL_SEC", "0"))     # 0 => persistent key

    print(f"APPLY symbol={sym} regime={rg} group={grp} -> {dec.winner} reason={dec.reason} lock_sec={lock_sec} dry={args.dry_run}")
    if args.dry_run:
        return 0

    # Best practice: check lock first (avoid flapping)
    try:
        if await r.get(lk):
            print(f"LOCKED {lk}")
            return 5
    except Exception:
        pass

    # Write active arm
    try:
        if active_ttl > 0:
            await r.set(ak, dec.winner, ex=active_ttl)
        else:
            await r.set(ak, dec.winner)
        await r.set(lk, "1", ex=max(60, lock_sec))
    except Exception as e:
        print(f"WRITE_FAIL {e}")
        return 6

    # Best-effort audit
    audit_stream = os.getenv("CFG_APPLY_AUDIT_STREAM", "stream:trade:entry_audit")
    try:
        payload = {
            "ts_ms": _now_ms(),
            "event": "CFG_APPLY_ACTIVE_ARM",
            "symbol": sym,
            "regime": rg,
            "group": grp,
            "active_arm": dec.winner,
            "reason": dec.reason,
            "min_samples": min_samples,
            "min_edge_r": min_edge_r,
            "suggestion_ts_ms": int(d.get("ts_ms", 0) or 0),
        }
        msg = {
            "type": "cfg_apply_active_arm",
            "ts_ms": str(payload["ts_ms"]),
            "symbol": sym,
            "regime": rg,
            "group": grp,
            "arm": dec.winner,
            "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        }
        await r.xadd(audit_stream, msg, maxlen=50000, approximate=True)
    except Exception:
        pass

    return 0

if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
