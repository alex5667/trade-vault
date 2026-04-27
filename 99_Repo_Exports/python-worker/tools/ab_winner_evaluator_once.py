from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
Run AB Winner Evaluator once (container-friendly).

Used by:
- systemd timer on host (docker exec into container)
- manual ops

Safe-lock: Redis SET key NX EX ttl
"""

import os
import asyncio
import time
import redis.asyncio as aioredis

from services.ab_winner_suggester_service_v2 import ABWinnerSuggesterV2


async def _acquire_lock(r, *, key: str, ttl_sec: int) -> bool:
    try:
        val = str(get_ny_time_millis())
        ok = await r.set(key, val, nx=True, ex=int(ttl_sec))
        return bool(ok)
    except Exception:
        return False


async def main() -> int:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    lock_key = os.getenv("AB_EVAL_LOCK_KEY", "lock:ab_winner_evaluator:v1")
    lock_ttl = int(os.getenv("AB_EVAL_LOCK_TTL_SEC", "3300"))  # < 1h

    r = aioredis.from_url(redis_url, decode_responses=True)
    got = await _acquire_lock(r, key=lock_key, ttl_sec=lock_ttl)
    if not got:
        return 0

    # Run once
    svc = ABWinnerSuggesterV2(redis_client=r)

    # Key enumeration strategy (must match your service logic):
    # Prefer: collect keys from events:trades ingestion state.
    # Fallback: scan latest pointers.
    #
    # Here we call svc.run_once() if it exists, otherwise no-op.
    try:
        if hasattr(svc, "run_once") and callable(getattr(svc, "run_once")):
            await svc.run_once()
        else:
            # Minimal fallback: scan latest pointers and re-publish for them.
            latest_prefix = str(getattr(svc, "latest_prefix", "cfg:suggestions:entry_policy:latest:ab_winner"))
            cur = 0
            pattern = f"{latest_prefix}:*"
            while True:
                cur, keys = await r.scan(cur, match=pattern, count=10000)
                for k in keys or []:
                    # expected: prefix:SYM:REGIME:GROUP:SCENARIO
                    parts = str(k).split(":")
                    if len(parts) < 7:
                        continue
                    symbol = parts[-4]
                    regime = parts[-3]
                    group = parts[-2]
                    scenario = parts[-1]
                    await svc.publish_suggestion(symbol=symbol, regime=regime, group=group, scenario=scenario)
                if int(cur) == 0:
                    break
    except Exception:
        return 1
    finally:
        try:
            await r.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))
