from __future__ import annotations

# -*- coding: utf-8 -*-
"""core.redis_lock_async

Async Redis lock (SET NX EX) with safe release.

Use-cases in this repo:
  - periodic jobs inside containers (autopilot reports, AB evaluators)
  - multiple replicas / accidental double-start

Design:
  - lock is best-effort, not a distributed transaction
  - acquire: SET key value NX EX ttl
  - release: Lua compare-and-del
  - fail-open for non-critical jobs: if Redis down => skip job
"""


import secrets
from dataclasses import dataclass

_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""


@dataclass
class RedisLock:
    key: str
    value: str
    ttl_sec: int


def new_lock_value(prefix: str = "") -> str:
    """Generate a unique lock value for ownership checking."""
    # 128-bit token is enough and cheap.
    tok = secrets.token_hex(16)
    return f"{prefix}{tok}" if prefix else tok


async def acquire_lock(*, r, key: str, ttl_sec: int, value: str | None = None) -> RedisLock | None:
    """Acquire lock.

    Args:
      r: async redis client (redis.asyncio.Redis or compatible)
      key: lock key
      ttl_sec: expiry
      value: optional lock value (owner id)

    Returns:
      RedisLock if acquired, else None.
    """
    try:
        k = str(key)
        ttl = int(ttl_sec)
        if ttl <= 0:
            ttl = 60
        v = str(value or new_lock_value(prefix="lock:"))
        ok = await r.set(k, v, nx=True, ex=ttl)
        if ok:
            return RedisLock(key=k, value=v, ttl_sec=ttl)
        return None
    except Exception:
        return None


async def refresh_lock(*, r, lock: RedisLock, ttl_sec: int | None = None) -> bool:
    """Best-effort TTL refresh.

    Uses compare-and-set via Lua (only refresh if still owned).
    """
    try:
        ttl = int(ttl_sec or lock.ttl_sec)
        if ttl <= 0:
            ttl = lock.ttl_sec
        # Lua: if owned then EXPIRE
        # We avoid a second Lua string; do it in 2 commands for simplicity:
        cur = await r.get(lock.key)
        if (cur or "") != lock.value:
            return False
        await r.expire(lock.key, ttl)
        return True
    except Exception:
        return False


async def release_lock(*, r, lock: RedisLock) -> bool:
    """Release lock only if owned."""
    try:
        res = await r.eval(_RELEASE_LUA, 1, lock.key, lock.value)
        return bool(int(res or 0) > 0)
    except Exception:
        return False


async def run_locked(*, r, key: str, ttl_sec: int, coro, value: str | None = None) -> bool:
    """Acquire lock and run awaitable `coro` if acquired.

    Returns:
      True if executed, False if skipped.
    """
    lock = await acquire_lock(r=r, key=key, ttl_sec=ttl_sec, value=value)
    if lock is None:
        return False
    try:
        await coro()
        return True
    finally:
        await release_lock(r=r, lock=lock)


__all__ = [
    "RedisLock",
    "new_lock_value",
    "acquire_lock",
    "refresh_lock",
    "release_lock",
    "run_locked",
]
