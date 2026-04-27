# -*- coding: utf-8 -*-
"""core.redis_lock

Redis SETNX lock helper for periodic jobs.

We use this for *batch* tasks (autopilot reports/evaluators) to guarantee that
two replicas won't run the same job concurrently.

Notes:
  - Fail-open for lock acquisition errors (job just won't run).
  - Safe release via token check (Lua).
  - Supports sync redis-py clients (redis.Redis) and asyncio ones
    (redis.asyncio.Redis / aioredis-compatible) via separate APIs.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional


_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "  return redis.call('del', KEYS[1]) "
    "else "
    "  return 0 "
    "end"
)


def acquire_lock_sync(*, r: Any, key: str, ttl_sec: int, token: str = "") -> Optional[str]:
    """Acquire lock using sync redis client.

    Returns token if lock acquired, else None.
    """
    k = str(key or "").strip()
    if not k:
        return None
    tok = token or uuid.uuid4().hex
    try:
        ok = r.set(k, tok, nx=True, ex=int(max(1, ttl_sec)))
        return tok if ok else None
    except Exception:
        return None


def release_lock_sync(*, r: Any, key: str, token: str) -> bool:
    """Release lock if token matches."""
    k = str(key or "").strip()
    if not k or not token:
        return False
    try:
        n = r.eval(_RELEASE_LUA, 1, k, str(token))
        return bool(int(n or 0) > 0)
    except Exception:
        return False


async def acquire_lock_async(*, r: Any, key: str, ttl_sec: int, token: str = "") -> Optional[str]:
    """Acquire lock using async redis client."""
    k = str(key or "").strip()
    if not k:
        return None
    tok = token or uuid.uuid4().hex
    try:
        ok = await r.set(k, tok, nx=True, ex=int(max(1, ttl_sec)))
        # redis-py asyncio returns True/False; aioredis may return b'OK'
        if ok is True or ok == "OK" or ok == b"OK":
            return tok
        return None
    except Exception:
        return None


async def release_lock_async(*, r: Any, key: str, token: str) -> bool:
    k = str(key or "").strip()
    if not k or not token:
        return False
    try:
        n = await r.eval(_RELEASE_LUA, 1, k, str(token))
        return bool(int(n or 0) > 0)
    except Exception:
        return False


def lock_key_daily(prefix: str, yyyymmdd: str) -> str:
    return f"{prefix}:{yyyymmdd}"


def utc_yyyymmdd(ts_ms: Optional[int] = None) -> str:
    """YYYYMMDD in UTC for deterministic daily locks."""
    import datetime as _dt

    t = int(ts_ms or get_ny_time_millis())
    dt = _dt.datetime.fromtimestamp(t / 1000.0, tz=_dt.timezone.utc)
    return dt.strftime("%Y%m%d")


# Async convenience wrappers matching expected signatures
async def try_acquire_lock(r: Any, *, key: str, ttl_sec: int) -> Optional[str]:
    """
    Async lock acquisition wrapper.
    Returns token if acquired, None otherwise.
    
    Args:
        r: Async Redis client
        key: Lock key
        ttl_sec: TTL in seconds
    
    Returns:
        Token string if lock acquired, None otherwise
    """
    return await acquire_lock_async(r=r, key=key, ttl_sec=ttl_sec)


async def release_lock(r: Any, lock: str, *, key: str) -> bool:
    """
    Async lock release wrapper.
    
    Args:
        r: Async Redis client
        lock: Token string returned by try_acquire_lock
        key: Lock key (required)
    
    Returns:
        True if lock was released, False otherwise
    """
    return await release_lock_async(r=r, key=key, token=lock)


@dataclass
class RedisLock:
    """
    Async Redis lock class for use with async redis clients.
    
    Usage:
        lock = RedisLock(key="lock:my_job", ttl_sec=60)
        if await lock.acquire(r):
            try:
                # do work
                pass
            finally:
                await lock.release(r)
    """
    key: str
    ttl_sec: int
    _token: str = ""

    async def acquire(self, r: Any) -> bool:
        """
        Acquire lock using async redis client.
        
        Args:
            r: Async Redis client (redis.asyncio.Redis or compatible)
        
        Returns:
            True if lock acquired, False otherwise
        """
        tok = await acquire_lock_async(r=r, key=self.key, ttl_sec=self.ttl_sec)
        if tok:
            self._token = tok
            return True
        self._token = ""
        return False

    async def release(self, r: Any) -> None:
        """
        Release lock if token matches.
        
        Args:
            r: Async Redis client (redis.asyncio.Redis or compatible)
        """
        if not self._token:
            return
        try:
            await release_lock_async(r=r, key=self.key, token=self._token)
        except Exception:
            pass
        finally:
            self._token = ""
