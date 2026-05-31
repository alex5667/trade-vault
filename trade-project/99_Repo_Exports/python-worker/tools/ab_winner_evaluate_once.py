import asyncio
import os
import time

import redis.asyncio as aioredis

from common.log import setup_logger
from core.redis_lock import release_lock, try_acquire_lock
import contextlib

log = setup_logger("ab_winner_eval_once")

LOCK_KEY = os.getenv("AB_WINNER_EVAL_LOCK_KEY", "lock:ab_winner_eval:v1")
LOCK_TTL_SEC = int(os.getenv("AB_WINNER_EVAL_LOCK_TTL_SEC", "3300"))  # 55 min

async def _run_once() -> int:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=50)

    lock = await try_acquire_lock(r, key=LOCK_KEY, ttl_sec=LOCK_TTL_SEC)
    if lock is None:
        log.info("Lock not acquired: %s (skip)", LOCK_KEY)
        with contextlib.suppress(Exception):
            await r.aclose()
        return 0

    t0 = time.time()
    try:
        from services.ab_winner_suggester_service_v2 import ABWinnerSuggesterV2
        svc = ABWinnerSuggesterV2(redis_client=r)
        # run_once should be non-blocking; if it doesn't exist -> fallback to internal scan+score
        if hasattr(svc, "run_once") and callable(svc.run_once):
            await svc.run_once()
        else:
            # minimal fallback: one scoring pass over collected keys (svc implementation-specific)
            if hasattr(svc, "_scan_context_keys"):
                keys = await svc._scan_context_keys()
                for k in keys:
                    with contextlib.suppress(Exception):
                        await svc._score_key(k)
        dt = time.time() - t0
        log.info("Done in %.2fs", dt)
        return 0
    except Exception as e:
        log.exception("ab_winner_evaluate_once failed: %s", e)
        return 2
    finally:
        await release_lock(r, lock, key=LOCK_KEY)
        with contextlib.suppress(Exception):
            await r.aclose()

def main() -> None:
    # asyncio.run is appropriate for a script entry point
    try:
        sys_rc = asyncio.run(_run_once())
        if sys_rc is None: sys_rc = 0
    except KeyboardInterrupt:
        sys_rc = 130
    except Exception:
        sys_rc = 2

    # Do not call sys.exit here if imported, but this is a tool script.
    import sys
    sys.exit(int(sys_rc))

if __name__ == "__main__":
    main()
