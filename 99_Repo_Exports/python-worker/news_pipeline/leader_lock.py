from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import redis


_LUA_RENEW = """if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
"""

_LUA_RELEASE = """if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
"""


@dataclass(slots=True)
class LeaderLock:
    """Bit-for-bit compatible leader lock with the Go implementation.

    Go side uses:
      SET key value NX PX ttl
      renew via Lua:
        if GET(key)==value then PEXPIRE(key, ttl_ms) else 0

    This avoids split-brain during renews.
    """

    r: redis.Redis
    key: str
    value: str
    ttl_ms: int

    @classmethod
    def new(cls, *, r: redis.Redis, key: str, ttl_sec: float = 8.0, prefix: str = "py") -> "LeaderLock":
        value = f"{prefix}:{time.time_ns()}"
        return cls(r=r, key=key, value=value, ttl_ms=int(ttl_sec * 1000))

    def try_acquire(self) -> bool:
        # redis-py: set(name, value, nx=True, px=ttl_ms)
        return bool(self.r.set(self.key, self.value, nx=True, px=self.ttl_ms))

    def renew(self) -> bool:
        # returns True if renewed
        res = self.r.eval(_LUA_RENEW, 1, self.key, self.value, str(self.ttl_ms))
        try:
            return int(res) > 0
        except Exception:
            return False

    def release(self) -> bool:
        res = self.r.eval(_LUA_RELEASE, 1, self.key, self.value)
        try:
            return int(res) > 0
        except Exception:
            return False
