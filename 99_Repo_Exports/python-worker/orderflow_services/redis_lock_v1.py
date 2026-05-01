# -*- coding: utf-8 -*-
from __future__ import annotations
"""redis_lock_v1.py

Small Redis lock helper (SET NX EX + safe release).

Design goals:
- deterministic, low overhead
- safe release via token check (Lua)
- async-friendly (redis.asyncio)

ENV-driven callers should keep TTL conservative.
"""

from utils.time_utils import get_ny_time_millis

import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional


_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
""".strip()


def _now_ms() -> int:
    return get_ny_time_millis()


def _default_token() -> str:
    return f"{os.getpid()}:{_now_ms()}:{uuid.uuid4().hex}"


async def acquire_lock(r: Any, *, key: str, ttl_sec: int, token: Optional[str] = None) -> str:
    """Acquire lock and return token, or empty string if busy."""
    tok = str(token or _default_token())
    try:
        ok = await r.set(str(key), tok, nx=True, ex=int(ttl_sec))
        # redis-py returns True/False or b'OK'
        if ok is True or ok == "OK":
            return tok
    except Exception:
        return ""
    return ""


async def release_lock(r: Any, *, key: str, token: str) -> bool:
    """Release lock if token matches."""
    try:
        res = await r.eval(_RELEASE_LUA, 1, str(key), str(token))
        return int(res or 0) == 1
    except Exception:
        return False


@dataclass
class LockGuard:
    r: Any
    key: str
    ttl_sec: int
    token: str = ""

    async def __aenter__(self) -> "LockGuard":
        self.token = await acquire_lock(self.r, key=self.key, ttl_sec=self.ttl_sec)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.token:
            await release_lock(self.r, key=self.key, token=self.token)
