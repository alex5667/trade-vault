from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


_LUA_POP_DUE_TO_INFLIGHT = r"""
-- KEYS[1] = ready_zset
-- KEYS[2] = inflight_zset
-- ARGV[1] = now_ms
-- ARGV[2] = limit
-- ARGV[3] = lease_ms

local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local lease_ms = tonumber(ARGV[3])
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if (#ids == 0) then
  return {}
end

local inflight_score = now + lease_ms
for i, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('ZADD', KEYS[2], inflight_score, id)
end
return ids
"""

_LUA_REQUEUE_EXPIRED_INFLIGHT = r"""
-- KEYS[1] = inflight_zset
-- KEYS[2] = ready_zset
-- KEYS[3] = due_hash
-- ARGV[1] = now_ms
-- ARGV[2] = limit

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if (#ids == 0) then
  return {}
end

for i, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  local due = redis.call('HGET', KEYS[3], id)
  if (due == false) or (due == nil) then
    due = ARGV[1]
  end
  redis.call('ZADD', KEYS[2], tonumber(due), id)
end
return ids
"""


@dataclass(frozen=True)
class RetryQueueSettings:
    ready_zset: str
    inflight_zset: str
    due_hash: str
    owner_hash: str
    meta_prefix: str = "sig:outbox:retry_meta"
    meta_ttl_sec: int = 86400


class OutboxRetryQueue:
    """
    Redis-backed retry scheduler:
      - schedule(msg_id, due_ms, owner)
      - pop_due_to_inflight(now_ms, limit, lease_ms) via Lua (no duplicate pops across dispatchers)
      - requeue_expired_inflight(now_ms, limit) via Lua (crash-safe: no "lost" ids)
      - cancel(msg_id) clears ready/inflight + meta

    Why 2-phase?
      If you ZREM on pop and crash before XCLAIM, msg_id is "lost" from retry queue.
      With inflight lease, it returns to ready after lease expires.
    """

    def __init__(self, redis_client: Any, *, settings: RetryQueueSettings) -> None:
        self.redis = redis_client
        self.settings = settings
        self._sha_pop_to_inflight: Optional[str] = None
        self._sha_requeue_expired: Optional[str] = None

    def _ensure_pop_to_inflight_sha(self) -> str:
        if self._sha_pop_to_inflight:
            return self._sha_pop_to_inflight
        self._sha_pop_to_inflight = str(self.redis.script_load(_LUA_POP_DUE_TO_INFLIGHT))
        return self._sha_pop_to_inflight

    def _ensure_requeue_sha(self) -> str:
        if self._sha_requeue_expired:
            return self._sha_requeue_expired
        self._sha_requeue_expired = str(self.redis.script_load(_LUA_REQUEUE_EXPIRED_INFLIGHT))
        return self._sha_requeue_expired

    def _meta_key(self, msg_id: str) -> str:
        return f"{self.settings.meta_prefix}:{msg_id}"

    def schedule(
        self,
        msg_id: str,
        *,
        due_ms: int,
        owner: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Schedules retry to READY and clears any INFLIGHT lease for this msg_id.
        This makes "reschedule" safe when called from a message currently in inflight.
        """
        mid = str(msg_id)
        due = int(due_ms)
        # ensure it's not stuck in inflight
        try:
            self.redis.zrem(self.settings.inflight_zset, mid)
        except Exception:
            pass
        self.redis.zadd(self.settings.ready_zset, {mid: due})
        # persist due/owner for requeue and observability
        try:
            self.redis.hset(self.settings.due_hash, mid, str(due))
        except Exception:
            pass
        if owner is not None:
            try:
                self.redis.hset(self.settings.owner_hash, mid, str(owner))
            except Exception:
                pass
        if meta is not None:
            try:
                payload = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
                self.redis.setex(self._meta_key(mid), int(self.settings.meta_ttl_sec), payload)
            except Exception:
                pass

    def cancel(self, msg_id: str) -> None:
        mid = str(msg_id)
        try:
            pipe = self.redis.pipeline(transaction=False)
            pipe.zrem(self.settings.ready_zset, mid)
            pipe.zrem(self.settings.inflight_zset, mid)
            pipe.hdel(self.settings.due_hash, mid)
            pipe.hdel(self.settings.owner_hash, mid)
            pipe.delete(self._meta_key(mid))
            pipe.execute()
        except Exception:
            # best-effort cleanup
            try:
                self.redis.zrem(self.settings.ready_zset, mid)
                self.redis.zrem(self.settings.inflight_zset, mid)
            except Exception:
                pass

    def pop_due_to_inflight(
        self,
        *,
        now_ms: Optional[int] = None,
        limit: int = 200,
        lease_ms: int = 60000,
    ) -> List[str]:
        now_ms = int(now_ms or get_ny_time_millis())
        limit = max(1, int(limit))
        lease_ms = max(1000, int(lease_ms))
        sha = self._ensure_pop_to_inflight_sha()
        try:
            res = self.redis.evalsha(
                sha,
                2,
                self.settings.ready_zset,
                self.settings.inflight_zset,
                str(now_ms),
                str(limit),
                str(lease_ms),
            )
        except Exception:
            res = self.redis.eval(
                _LUA_POP_DUE_TO_INFLIGHT,
                2,
                self.settings.ready_zset,
                self.settings.inflight_zset,
                str(now_ms),
                str(limit),
                str(lease_ms),
            )
        if not res:
            return []
        return [str(x) for x in res]

    def requeue_expired_inflight(
        self,
        *,
        now_ms: Optional[int] = None,
        limit: int = 200,
    ) -> List[str]:
        """
        Moves expired inflight leases back to READY using persisted due_hash score.
        """
        now_ms = int(now_ms or get_ny_time_millis())
        limit = max(1, int(limit))
        sha = self._ensure_requeue_sha()
        try:
            res = self.redis.evalsha(
                sha,
                3,
                self.settings.inflight_zset,
                self.settings.ready_zset,
                self.settings.due_hash,
                str(now_ms),
                str(limit),
            )
        except Exception:
            res = self.redis.eval(
                _LUA_REQUEUE_EXPIRED_INFLIGHT,
                3,
                self.settings.inflight_zset,
                self.settings.ready_zset,
                self.settings.due_hash,
                str(now_ms),
                str(limit),
            )
        if not res:
            return []
        return [str(x) for x in res]

    def sizes(self) -> Tuple[int, int]:
        """(ready, inflight) best-effort"""
        try:
            r = int(self.redis.zcard(self.settings.ready_zset) or 0)
        except Exception:
            r = 0
        try:
            i = int(self.redis.zcard(self.settings.inflight_zset) or 0)
        except Exception:
            i = 0
        return r, i

