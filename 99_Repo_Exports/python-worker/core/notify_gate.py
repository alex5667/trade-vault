from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from core.redis_keys import RedisKeyPrefixes as RK

_LUA_COUNTER_GATE = r"""
-- KEYS[1] = gate_key (per sid)
-- KEYS[2] = counter_key (global)
-- ARGV[1] = ttl_sec
-- ARGV[2] = every_n
local v = redis.call('GET', KEYS[1])
if v then
  return tonumber(v)
end
local c = redis.call('INCR', KEYS[2])
local n = tonumber(ARGV[2])
local ok = 0
if (n <= 1) then
  ok = 1
elseif (c % n == 0) then
  ok = 1
end
redis.call('SETEX', KEYS[1], tonumber(ARGV[1]), tostring(ok))
return ok
"""


@dataclass(frozen=True)
class NotifyGateSettings:
    mode: str = os.getenv("NOTIFY_GATE_MODE", "hash")  # hash | counter
    every_n: int = int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "1"))
    ttl_sec: int = int(os.getenv("NOTIFY_GATE_TTL_SEC", "86400"))
    gate_prefix: str = os.getenv("NOTIFY_GATE_PREFIX", "sig:notify:gate")
    counter_key: str = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", RK.NOTIFY_SIGNAL_COUNTER)


class NotifyGate:
    """
    "Сверхидеал" для notify gating:
    - hash mode: детерминированно по sid (без Redis, без влияния ретраев)
    - counter mode: INCR выполняется ровно 1 раз на sid (Lua + gate_key),
      ретраи не сдвигают счётчик и не меняют решение
    """

    def __init__(self, redis_client: Any, *, settings: Optional[NotifyGateSettings] = None) -> None:
        self.redis = redis_client
        self.settings = settings or NotifyGateSettings()
        self._sha: Optional[str] = None

    def _gate_key(self, sid: str) -> str:
        return f"{self.settings.gate_prefix}:{sid}"

    def _ensure_sha(self) -> str:
        if self._sha:
            return self._sha
        self._sha = str(self.redis.script_load(_LUA_COUNTER_GATE))
        return self._sha

    def should_send(self, sid: str, symbol: Optional[str] = None) -> bool:
        n = int(self.settings.every_n)
        if n == 0:
            return False  # 0 = disabled, no notifications
        if n <= 1:
            return True
        mode = (self.settings.mode or "hash").strip().lower()
        if mode == "hash":
            import zlib
            return (zlib.crc32(sid.encode("utf-8")) % n) == 0
        # counter mode (stable per sid)
        sha = self._ensure_sha()
        
        counter_key = self.settings.counter_key
        if symbol:
            counter_key = f"{counter_key}:{symbol}"
            
        try:
            res = self.redis.evalsha(sha, 2, self._gate_key(sid), counter_key, str(int(self.settings.ttl_sec)), str(n))
        except Exception:
            res = self.redis.eval(_LUA_COUNTER_GATE, 2, self._gate_key(sid), counter_key, str(int(self.settings.ttl_sec)), str(n))
        
        decision = bool(res) and int(res) == 1
        # Debug logging to trace gate decisions
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[NotifyGate] SID={sid} Mode={mode} N={n} Result={res} Decision={decision} Key={counter_key}")
        
        return decision
