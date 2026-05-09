"""
Autopilot OF Reports Service.
Runs inside container as a long-lived process.
Triggers hourly Canary Monitor and nightly Regression.
"""

import asyncio
import os
from datetime import UTC, datetime

import redis.asyncio as aioredis

from tools.cron_of_reports import run_report
import contextlib


def _utc_now() -> datetime:
    return datetime.now(UTC)

def _parse_hhmm(x: str, default: str) -> tuple[int, int]:
    s = (x or default).strip()
    try:
        hh, mm = s.split(":")
        return int(hh), int(mm)
    except Exception:
        return 3, 17 # Default to 03:17 UTC

async def _acquire_lock(r: aioredis.Redis, key: str, ttl_sec: int) -> bool:
    try:
        ok = await r.set(key, "1", nx=True, ex=int(ttl_sec))
        return bool(ok)
    except Exception:
        return False

async def main():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r_async = aioredis.from_url(redis_url, decode_responses=True)

    lock_key = os.getenv("OF_REPORTS_LOCK_KEY", "lock:autopilot:of_reports")
    lock_ttl = int(os.getenv("OF_REPORTS_LOCK_TTL_SEC", "3000")) # 50 min

    # Hourly monitor schedule (at minute :07)
    monitor_minute = int(os.getenv("OF_REPORTS_MONITOR_MINUTE", "7"))

    # Nightly regress schedule (HH:MM UTC)
    regress_hhmm = os.getenv("OF_REPORTS_REGRESS_HHMM", "03:17")
    r_h, r_m = _parse_hhmm(regress_hhmm, "03:17")

    last_monitor_hour = -1
    last_regress_day = -1

    print(f"OF Reports Autopilot started. Hourly at :07. Nightly at {r_h:02d}:{r_m:02d} UTC.")

    while True:
        now = _utc_now()

        # Hourly Monitor trigger
        if now.minute == monitor_minute and now.hour != last_monitor_hour:
            if await _acquire_lock(r_async, lock_key, 600): # Light lock for monitor
                print(f"Triggering hourly OF monitor at {now}")
                try:
                    # run_report is synchronous, but we can wrap it if needed.
                    # For safety in asyncio loop, we use run_in_executor.
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, run_report, "monitor")
                except Exception as e:
                    print(f"Monitor error: {e}")
                last_monitor_hour = now.hour

        # Nightly Regression trigger
        if now.hour == r_h and now.minute == r_m and now.day != last_regress_day:
            if await _acquire_lock(r_async, lock_key, lock_ttl):
                print(f"Triggering nightly OF regression at {now}")
                try:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, run_report, "regress")
                except Exception as e:
                    print(f"Regression error: {e}")
                last_regress_day = now.day

        await asyncio.sleep(30)

if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
