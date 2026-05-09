from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import uuid

import redis.asyncio as aioredis

# Ensure service is importable
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.ab_winner_suggester_service_v2 import ABWinnerSuggesterV2
from services.reporting_service import ReportingService
from utils.time_utils import get_ny_time_millis
import contextlib

# Setup basic logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger("ABWinnerJob")


async def _acquire_lock(
    r: aioredis.Redis,
    *,
    key: str,
    ttl_ms: int,
    token: str,
) -> bool:
    """
    Safe-lock via Redis SET NX PX.
    """
    try:
        ok = await r.set(key, token, nx=True, px=int(ttl_ms))
        return bool(ok)
    except Exception:
        return False


async def _release_lock(r: aioredis.Redis, *, key: str, token: str) -> None:
    """
    Release lock only if token matches.
    """
    script = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    end
    return 0
    """
    with contextlib.suppress(Exception):
        await r.eval(script, 1, key, token)


async def _run_iteration(r: aioredis.Redis, lock_key: str, lock_ttl_ms: int, reporter: ReportingService) -> None:
    token = f"{uuid.uuid4()}:{get_ny_time_millis()}"

    ok = await _acquire_lock(r, key=lock_key, ttl_ms=lock_ttl_ms, token=token)
    if not ok:
        log.info("Lock busy, skipping iteration.")
        return

    try:
        log.info("Starting AB Winner evaluation pass...")
        svc = ABWinnerSuggesterV2()
        # Ensure it uses our redis client if service supports it
        with contextlib.suppress(Exception):
            svc.r = getattr(svc, "r", r) or r

        updates: list[str] = await svc.run_once()
        log.info(f"Evaluation complete. Updated {len(updates)} items.")

        if updates and os.getenv("AB_WINNER_TELEGRAM_ENABLED", "0") == "1":
            msg_lines = [
                "🏆 <b>AB Winner Update</b>",
                f"Updated {len(updates)} suggestions:",
                ""
            ]
            # Limit details if too many
            limit_lines = 20
            for i, line in enumerate(updates):
                if i >= limit_lines:
                    msg_lines.append(f"... and {len(updates) - i} more")
                    break
                msg_lines.append(f"• {line}")

            msg = "\n".join(msg_lines)
            reporter.send_telegram_message(msg, tags=["ab_winner", "update"], severity="info")

    except Exception as e:
        log.error(f"Error during iteration: {e}", exc_info=True)
    finally:
        await _release_lock(r, key=lock_key, token=token)


async def _main_loop() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=10,
        socket_timeout=30,
        max_connections=10,
    )

    # Initialize ReportingService (it uses its own sync redis usually, or we pass valid url)
    # ReportingService expects sync redis url often if initializing internal redis, or we can use existing.
    # Note: ReportingService structure in this codebase might use sync redis-py.
    # We will instantiate it simply.
    reporter = ReportingService(redis_url=redis_url)

    lock_key = os.getenv("AB_WINNER_LOCK_KEY", "lock:ab_winner_evaluator:v1")
    lock_ttl_ms = int(os.getenv("AB_WINNER_LOCK_TTL_MS", str(55 * 60 * 1000)))

    interval_sec = int(os.getenv("AB_WINNER_INTERVAL_SEC", "0"))

    if interval_sec <= 0:
        # Run once and exit
        await _run_iteration(r, lock_key, lock_ttl_ms, reporter)
        await r.close()
        return

    log.info(f"Starting loop mode (interval={interval_sec}s)")
    while True:
        start_ts = time.time()
        await _run_iteration(r, lock_key, lock_ttl_ms, reporter)

        elapsed = time.time() - start_ts
        sleep_curr = max(1.0, interval_sec - elapsed)
        log.info(f"Sleeping {sleep_curr:.1f}s...")
        await asyncio.sleep(sleep_curr)


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_main_loop())


if __name__ == "__main__":
    main()
