from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis

"""
Atomic per-target delivery with idempotent markers, WITHOUT "loss":
 - marker is created only after successful XADD/SETEX, inside Lua
 - marker + index registration happen in same script
This prevents: marker-set -> delivery-fail -> permanent skip.
"""

_LUA_XADD_WITH_MARKER = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = marker_index_zset
-- KEYS[3] = stream
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = marker_expire_at_ms
-- ARGV[3] = maxlen
-- ARGV[4] = payload_json
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
local id = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', 'data', ARGV[4])
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], KEYS[1])
return {1, id}
"""

_LUA_SETEX_WITH_MARKER = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = marker_index_zset
-- KEYS[3] = snap_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = marker_expire_at_ms
-- ARGV[3] = snap_ttl_sec
-- ARGV[4] = payload_json
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
redis.call('SETEX', KEYS[3], ARGV[3], ARGV[4])
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], KEYS[1])
return {1}
"""


_LUA_DELIVER_SETEX_JSON_ONCE = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = snap_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = snap_ttl_sec
-- ARGV[3] = json_payload

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end

redis.call('SETEX', KEYS[2], ARGV[2], ARGV[3])
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
return {1}
"""


_LUA_NOTIFY_ONCE_WITH_GATING = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = notify_stream
-- KEYS[3] = counter_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = maxlen
-- ARGV[3] = every_n
-- ARGV[4] = payload_json (hash fields)

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end

local c = 0
-- counter best-effort, but if it fails -> script fails -> marker NOT set -> retry ok
c = redis.call('INCR', KEYS[3])

local every = tonumber(ARGV[3]) or 1
if every > 1 and (c % every) ~= 0 then
  -- gated: считаем доставленным (иначе может "догнать" позже и сломать семантику every_n)
  redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
  return {2, c}
end

local obj = cjson.decode(ARGV[4])
local args = {'MAXLEN', '~', ARGV[2], '*'}
for k,v in pairs(obj) do
  table.insert(args, tostring(k))
  table.insert(args, tostring(v))
end

local id = redis.call('XADD', KEYS[2], unpack(args))
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
return {1, id}
"""


@dataclass(frozen=True)
class DeliveryAtomicSettings:
    marker_ttl_sec: int = int(os.getenv("SIGNAL_DELIVERY_TTL_SEC", "86400"))
    marker_prefix: str = os.getenv("SIGNAL_DELIVERY_MARKER_PREFIX", "sig:deliver")
    marker_index_zset: str = os.getenv("SIGNAL_DELIVERY_MARKER_INDEX_ZSET", "sig:deliver:idx")


class DeliveryAtomic:
    def __init__(self, redis_client: Any, *, settings: DeliveryAtomicSettings | None = None) -> None:
        self.redis = redis_client
        self.settings = settings or DeliveryAtomicSettings()
        self._sha_xadd: str | None = None
        self._sha_setex: str | None = None

    def marker_key(self, target: str, sid: str) -> str:
        # dedicated namespace (no collisions with old deliver:{target}:{sid})
        return f"{self.settings.marker_prefix}:{target}:{sid}"

    def _ensure_xadd(self) -> str:
        if self._sha_xadd:
            return self._sha_xadd
        self._sha_xadd = str(self.redis.script_load(_LUA_XADD_WITH_MARKER))
        return self._sha_xadd

    def _ensure_setex(self) -> str:
        if self._sha_setex:
            return self._sha_setex
        self._sha_setex = str(self.redis.script_load(_LUA_SETEX_WITH_MARKER))
        return self._sha_setex

    def xadd_once(
        self,
        *,
        marker_key: str,
        stream: str,
        payload: dict[str, Any],
        maxlen: int,
        marker_ttl_sec: int | None = None,
    ) -> tuple[bool, str | None]:
        ttl = int(marker_ttl_sec or self.settings.marker_ttl_sec)
        expire_at_ms = get_ny_time_millis() + ttl * 1000
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        sha = self._ensure_xadd()
        try:
            res = self.redis.evalsha(
                sha,
                3,
                marker_key,
                self.settings.marker_index_zset,
                stream,
                str(ttl),
                str(expire_at_ms),
                str(int(maxlen)),
                body,
            )
        except Exception:
            res = self.redis.eval(
                _LUA_XADD_WITH_MARKER,
                3,
                marker_key,
                self.settings.marker_index_zset,
                stream,
                str(ttl),
                str(expire_at_ms),
                str(int(maxlen)),
                body,
            )
        if not res or int(res[0]) != 1:
            return (False, None)
        return (True, str(res[1]))

    def setex_once(
        self,
        *,
        marker_key: str,
        key: str,
        ttl_sec: int,
        payload: dict[str, Any],
        marker_ttl_sec: int | None = None,
    ) -> bool:
        ttl = int(marker_ttl_sec or self.settings.marker_ttl_sec)
        expire_at_ms = get_ny_time_millis() + ttl * 1000
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        sha = self._ensure_setex()
        try:
            res = self.redis.evalsha(
                sha,
                3,
                marker_key,
                self.settings.marker_index_zset,
                key,
                str(ttl),
                str(expire_at_ms),
                str(int(ttl_sec)),
                body,
            )
        except Exception:
            res = self.redis.eval(
                _LUA_SETEX_WITH_MARKER,
                3,
                marker_key,
                self.settings.marker_index_zset,
                key,
                str(ttl),
                str(expire_at_ms),
                str(int(ttl_sec)),
                body,
            )
        return bool(res) and int(res[0]) == 1
