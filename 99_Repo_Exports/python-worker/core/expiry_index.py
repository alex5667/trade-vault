from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis

_LUA_POP_DUE = r"""
-- KEYS[1] = zset_key
-- ARGV[1] = now_ms
-- ARGV[2] = limit
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if (#ids > 0) then
  redis.call('ZREM', KEYS[1], unpack(ids))
end
return ids
"""


@dataclass(frozen=True)
class ExpiryIndexSettings:
    zset_key: str


class ExpiryIndex:
    """
    ZSET index of expirable keys:
      member = redis key name
      score  = expire_at_ms

    Used for periodic cleanup of index (and optional deletion of keys),
    without SCAN over keyspace.
    """

    def __init__(self, redis_client: Any, *, settings: ExpiryIndexSettings) -> None:
        self.redis = redis_client
        self.settings = settings
        self._sha: str | None = None

    def _ensure_sha(self) -> str:
        if self._sha:
            return self._sha
        self._sha = str(self.redis.script_load(_LUA_POP_DUE))
        return self._sha

    def add(self, key: str, *, expire_at_ms: int) -> None:
        self.redis.zadd(self.settings.zset_key, {str(key): int(expire_at_ms)})

    def pop_due(self, *, now_ms: int | None = None, limit: int = 500) -> list[str]:
        now_ms = int(now_ms or get_ny_time_millis())
        limit = max(1, int(limit))
        sha = self._ensure_sha()
        try:
            res = self.redis.evalsha(sha, 1, self.settings.zset_key, str(now_ms), str(limit))
        except Exception:
            res = self.redis.eval(_LUA_POP_DUE, 1, self.settings.zset_key, str(now_ms), str(limit))
        if not res:
            return []
        return [str(x) for x in res]
