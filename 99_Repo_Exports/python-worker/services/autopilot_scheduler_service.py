# -*- coding: utf-8 -*-
"""services.autopilot_scheduler_service

Container-friendly scheduler (no systemd) for autopilot reports.

It runs tools/autopilot_run_once.py periodically and sends results to Telegram.

Scheduling policy (best practice):
  - reports every hour (health + drift visibility)
  - proposals (Redis writes) only once per day by default to avoid flapping
    (controlled via AUTOPILOT_PROPOSE_EVERY_HOURS)

Locking:
  - Redis SETNX lock prevents double-run across replicas.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import redis.asyncio as redis

from core.redis_lock_async import acquire_lock, release_lock, RedisLock


@dataclass
class SchedulerCfg:
    redis_url: str
    lock_key: str
    lock_ttl_sec: int
    every_min: int
    propose_every_hours: int
    out_dir: str
    since_hours: int
    window_days: int


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except Exception:
        return int(default)


def _now_ms() -> int:
    return get_ny_time_millis()


def _sleep_until_next_boundary(*, every_min: int) -> float:
    """Return seconds to sleep until the next aligned minute boundary."""
    m = max(1, int(every_min))
    now = time.time()
    # align to wall-clock minutes
    nxt = (int(now // 60) + 1) * 60
    # align to m-minute grid
    if m > 1:
        while True:
            dt_min = int((nxt // 60) % m)
            if dt_min == 0:
                break
            nxt += 60
    return max(0.0, float(nxt - now))


async def _run_once_task(*, base: Path, propose: bool) -> int:
    args = [
        sys.executable,
        "tools/autopilot_run_once.py",
        "--since-hours",
        str(_env_int("AUTOPILOT_SINCE_HOURS", 168)),
        "--window-days",
        str(_env_int("AUTOPILOT_WINDOW_DAYS", 7)),
        "--out-dir",
        str(os.getenv("AUTOPILOT_OUT_DIR", "/tmp/autopilot")),
    ]
    if propose:
        args.append("--redis-write")
    else:
        # force off, even if env defaults to 1 for manual runs
        os.environ["AUTOPILOT_REDIS_WRITE"] = "0"
        
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(base),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=os.environ.copy(),
    )
    out_b = await proc.stdout.read() if proc.stdout else b""
    rc = int(await proc.wait())
    
    # last-resort: log to stdout for container logs
    try:
        out = out_b.decode("utf-8", "ignore")
        if out:
            print(out[-4000:])
    except Exception:
        pass
    return rc


async def run_forever(cfg: SchedulerCfg) -> None:
    r = redis.from_url(cfg.redis_url, decode_responses=True)
    base = Path(__file__).resolve().parents[1]

    # One-shot jitter so multiple containers don't align perfectly on start.
    try:
        jitter = float(os.getenv("AUTOPILOT_JITTER_SEC", "3.0") or "3.0")
    except Exception:
        jitter = 3.0
    await asyncio.sleep(max(0.0, jitter))

    last_propose_day = ""

    while True:
        sleep_s = _sleep_until_next_boundary(every_min=cfg.every_min)
        await asyncio.sleep(sleep_s)

        # Determine whether we should write proposals on this run.
        # Default: 1x/day (best practice: avoid configuration flapping).
        now = time.gmtime()
        day_key = time.strftime("%Y-%m-%d", now)
        hour = int(time.strftime("%H", now))
        
        propose = False
        if cfg.propose_every_hours <= 0:
            propose = False
        elif cfg.propose_every_hours >= 24:
            # once per day at 08:00 UTC by default
            target_h = _env_int("AUTOPILOT_PROPOSE_AT_HOUR_UTC", 8)
            propose = (day_key != last_propose_day) and (hour == int(target_h))
        else:
            # every N hours
            propose = (hour % int(cfg.propose_every_hours) == 0)

        # Lock: skip if another instance is running.
        lock: Optional[RedisLock] = await acquire_lock(r=r, key=cfg.lock_key, ttl_sec=cfg.lock_ttl_sec)
        if lock is None:
            continue
        try:
            rc = await _run_once_task(base=base, propose=propose)
            if rc == 0 and propose and day_key != last_propose_day:
                last_propose_day = day_key
        finally:
            await release_lock(r=r, lock=lock)


def _load_cfg() -> SchedulerCfg:
    redis_url = os.getenv("AUTOPILOT_REDIS_URL") or os.getenv("REDIS_URL") or "redis://redis-worker-1:6379/0"
    return SchedulerCfg(
        redis_url=str(redis_url),
        lock_key=str(os.getenv("AUTOPILOT_LOCK_KEY", "lock:autopilot:reporter")),
        lock_ttl_sec=_env_int("AUTOPILOT_LOCK_TTL_SEC", 55 * 60),
        every_min=_env_int("AUTOPILOT_EVERY_MIN", 60),
        propose_every_hours=_env_int("AUTOPILOT_PROPOSE_EVERY_HOURS", 24),
        out_dir=str(os.getenv("AUTOPILOT_OUT_DIR", "/tmp/autopilot")),
        since_hours=_env_int("AUTOPILOT_SINCE_HOURS", 168),
        window_days=_env_int("AUTOPILOT_WINDOW_DAYS", 7),
    )


async def _amain() -> None:
    cfg = _load_cfg()
    await run_forever(cfg)


def main() -> int:
    try:
        asyncio.run(_amain())
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"autopilot_scheduler_service fatal: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
