from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_LUA_ACQUIRE = r"""
-- KEYS[1] = lease_key
-- ARGV[1] = token
-- ARGV[2] = ttl_ms
local ok = redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX')
if ok then
  return {1}
end
return {0, redis.call('GET', KEYS[1]) or ''}
"""

_LUA_RENEW = r"""
-- KEYS[1] = lease_key
-- ARGV[1] = token
-- ARGV[2] = ttl_ms
local v = redis.call('GET', KEYS[1])
if v and v == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1}
end
return {0}
"""

_LUA_RELEASE = r"""
-- KEYS[1] = lease_key
-- ARGV[1] = token
local v = redis.call('GET', KEYS[1])
if v and v == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return {1}
end
return {0}
"""


@dataclass(frozen=True)
class SidLeaseSettings:
    prefix: str = "sig:lease"


class SidLease:
    """
    Пер-sid single-flight:
      - acquire: SET NX PX token
      - renew/release: compare token внутри Lua
    """

    def __init__(self, redis_client: Any, *, settings: SidLeaseSettings | None = None) -> None:
        self.redis = redis_client
        self.settings = settings or SidLeaseSettings()
        self._sha_acq: str | None = None
        self._sha_ren: str | None = None
        self._sha_rel: str | None = None

    def key(self, sid: str) -> str:
        return f"{self.settings.prefix}:{sid}"

    def _sha(self, which: str) -> str:
        if which == "acq":
            if not self._sha_acq:
                self._sha_acq = str(self.redis.script_load(_LUA_ACQUIRE))
            return self._sha_acq
        if which == "ren":
            if not self._sha_ren:
                self._sha_ren = str(self.redis.script_load(_LUA_RENEW))
            return self._sha_ren
        if which == "rel":
            if not self._sha_rel:
                self._sha_rel = str(self.redis.script_load(_LUA_RELEASE))
            return self._sha_rel
        raise ValueError(which)

    def acquire(self, sid: str, *, token: str, ttl_ms: int) -> bool:
        k = self.key(sid)
        try:
            res = self.redis.evalsha(self._sha("acq"), 1, k, token, str(int(ttl_ms)))
        except Exception:
            res = self.redis.eval(_LUA_ACQUIRE, 1, k, token, str(int(ttl_ms)))
        return bool(res and int(res[0]) == 1)

    def renew(self, sid: str, *, token: str, ttl_ms: int) -> bool:
        k = self.key(sid)
        try:
            res = self.redis.evalsha(self._sha("ren"), 1, k, token, str(int(ttl_ms)))
        except Exception:
            res = self.redis.eval(_LUA_RENEW, 1, k, token, str(int(ttl_ms)))
        return bool(res and int(res[0]) == 1)

    def release(self, sid: str, *, token: str) -> bool:
        k = self.key(sid)
        try:
            res = self.redis.evalsha(self._sha("rel"), 1, k, token)
        except Exception:
            res = self.redis.eval(_LUA_RELEASE, 1, k, token)
        return bool(res and int(res[0]) == 1)







































