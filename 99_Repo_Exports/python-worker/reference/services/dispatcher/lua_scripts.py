"""
Lua script management for SignalDispatcher.

Centralizes all Lua scripts used for atomic Redis operations in signal delivery.
Provides SHA caching and fallback execution.
"""

from typing import Any, Dict, List, Optional
import logging


class LuaScriptManager:
    """
    Manages Lua scripts for SignalDispatcher.
    
    Responsibilities:
    - Store all Lua script constants
    - Cache SHA hashes for scripts
    - Execute scripts with evalsha/eval fallback
    - Provide type-safe script execution
    """
    
    # =========================================================================
    # Lua Scripts
    # =========================================================================
    
    XADD_AND_MARK = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = stream
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = maxlen
-- ARGV[3..] = fields (k1,v1,k2,v2,...)

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end

local ok, id = pcall(redis.call, 'XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', unpack(ARGV, 3))
if not ok then
  return {-1, tostring(id)}
end

local ok2, _ = pcall(redis.call, 'SET', KEYS[1], '1', 'EX', ARGV[1])
if not ok2 then
  -- marker failed; rollback to avoid duplicates/loss tradeoff
  pcall(redis.call, 'XDEL', KEYS[2], id)
  return {-2, 'marker_set_failed'}
end
return {1, id}
"""

    SETEX_AND_MARK = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = value_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = value_ttl_sec
-- ARGV[3] = value_json

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end

local ok, _ = pcall(redis.call, 'SETEX', KEYS[2], ARGV[2], ARGV[3])
if not ok then
  return {-1}
end
local ok2, _ = pcall(redis.call, 'SET', KEYS[1], '1', 'EX', ARGV[1])
if not ok2 then
  redis.call('DEL', KEYS[2])
  return {-2}
end
return {1}
"""

    XADD_FIELDS_THEN_MARK = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = stream
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = maxlen
-- ARGV[3...] = field/value pairs
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', unpack(ARGV, 3))
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  redis.call('XDEL', KEYS[2], id)
  return {0}
end
return {1, id}
"""

    SETEX_THEN_MARK = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = value_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = value_ttl_sec
-- ARGV[3] = value_json
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
redis.call('SETEX', KEYS[2], ARGV[2], ARGV[3])
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  redis.call('DEL', KEYS[2])
  return {0}
end
return {1}
"""

    NOTIFY_GATE = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = notify_stream
-- KEYS[3] = counter_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = maxlen
-- ARGV[3] = every_n
-- ARGV[4...] = field/value pairs
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, 0}
end
local n = redis.call('INCR', KEYS[3])
local every = tonumber(ARGV[3]) or 1
local send = 1
if every > 1 and (n % every ~= 0) then
    send = 0
end
local id = nil
if send == 1 then
  id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', unpack(ARGV, 4))
end
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  if id then redis.call('XDEL', KEYS[2], id) end
  return {0, send}
end
return {1, send}
"""

    MARK_AND_XADD = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = target_stream
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = maxlen
-- ARGV[3] = payload_json (field "data")
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  return {0}
end
local r = redis.pcall('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', 'data', ARGV[3])
if type(r) == 'table' and r['err'] then
  redis.call('DEL', KEYS[1])
  return {-1, r['err']}
end
return {1, r}
"""

    MARK_AND_SETEX = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = snap_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = snap_ttl_sec
-- ARGV[3] = snap_payload_json
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  return {0}
end
local r = redis.pcall('SETEX', KEYS[2], ARGV[2], ARGV[3])
if type(r) == 'table' and r['err'] then
  redis.call('DEL', KEYS[1])
  return {-1, r['err']}
end
return {1}
"""

    MARK_AND_NOTIFY = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = notify_stream
-- KEYS[3] = counter_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = maxlen
-- ARGV[3] = every_n
-- ARGV[4] = payload_json
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  return {0}
end
local every_n = tonumber(ARGV[3]) or 1
local send = 1
if every_n > 1 then
  local c = redis.pcall('INCR', KEYS[3])
  if type(c) == 'table' and c['err'] then
    -- INCR failed -> keep send=1 (best-effort)
  else
    if (tonumber(c) % every_n) ~= 0 then
    send = 0
    end
  end
end
if send == 1 then
  local r = redis.pcall('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', 'data', ARGV[4])
  if type(r) == 'table' and r['err'] then
    redis.call('DEL', KEYS[1])
    return {-1, r['err']}
  end
  return {1, r}
end
return {1, 'skipped'}
"""

    XADD_OR_SETEX_THEN_MARK = r"""
-- KEYS[1]=marker_key
-- KEYS[2]=target (stream for xadd OR key for setex)
-- KEYS[3]=gc_zset (optional, may be "")
-- ARGV[1]=marker_ttl_sec
-- ARGV[2]=mode: "xadd"|"setex"
-- ARGV[3]=arg3: maxlen for xadd OR ttl_sec for setex
-- ARGV[4]=sid
-- ARGV[5]=payload_json

local marker_ttl = tonumber(ARGV[1]) or 86400
local mode = ARGV[2] or ""
local arg3 = tonumber(ARGV[3]) or 0
local sid = ARGV[4] or ""
local payload = ARGV[5] or ""

if payload == "" then
  return {1, "skip"}
end

if redis.call('GET', KEYS[1]) then
  return {1, "dedup"}
end

local t = redis.call('TIME')
local now_ms = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)

local out = nil
if mode == "xadd" then
  out = redis.call('XADD', KEYS[2], 'MAXLEN', '~', tostring(arg3), '*', 'sid', sid, 'data', payload)
elseif mode == "setex" then
  redis.call('SETEX', KEYS[2], tostring(arg3), payload)
  out = "ok"
else
  return {0, "bad_mode"}
end

redis.call('SET', KEYS[1], tostring(now_ms), 'EX', tostring(marker_ttl))
if KEYS[3] and KEYS[3] ~= "" then
  redis.call('ZADD', KEYS[3], now_ms, KEYS[1])
end
return {1, out}
"""

    NOTIFY_GATE_XADD_THEN_MARK = r"""
-- KEYS[1]=marker_key
-- KEYS[2]=notify_stream
-- KEYS[3]=counter_key
-- KEYS[4]=gc_zset
-- ARGV[1]=marker_ttl_sec
-- ARGV[2]=maxlen
-- ARGV[3]=sid
-- ARGV[4]=every_n
-- ARGV[5]=field_count
-- ARGV[6..]=field/value pairs (2*field_count args)
--
-- Return: {1, "sent"} or {1, "skipped"} or {1, "dedup"}

local marker = KEYS[1]
if redis.call("EXISTS", marker) == 1 then
  return {1, "dedup"}
end

local every_n = tonumber(ARGV[4]) or 1
if every_n < 1 then every_n = 1 end

local c = redis.call("INCR", KEYS[3])
local should_send = 1
if every_n > 1 then
  if (c % every_n) ~= 0 then
    should_send = 0
  end
end

local ts_ms = redis.call("TIME")
local now_ms = (tonumber(ts_ms[1]) * 1000) + math.floor(tonumber(ts_ms[2]) / 1000)

if should_send == 1 then
  local maxlen = tonumber(ARGV[2]) or 0
  local n = tonumber(ARGV[5]) or 0
  local args = {}
  table.insert(args, KEYS[2])
  if maxlen > 0 then
    table.insert(args, "MAXLEN")
    table.insert(args, "~")
    table.insert(args, maxlen)
  end
  table.insert(args, "*")
  table.insert(args, "sid")
  table.insert(args, ARGV[3])
  local base = 6
  for i=0,(n*2-1) do
    table.insert(args, ARGV[base + i])
  end
  redis.call(unpack({"XADD", unpack(args)}))
end

redis.call("SET", marker, tostring(now_ms), "EX", tonumber(ARGV[1]))
if KEYS[4] ~= "" then
  redis.call("ZADD", KEYS[4], now_ms, marker)
end

if should_send == 1 then
  return {1, "sent"}
end
return {1, "skipped"}
"""

    MARKER_AFTER_DELIVERY = r"""
-- Atomic deliver+marker inside ONE Redis.
-- KEYS[1] = marker_key
-- KEYS[2] = target_stream_or_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = mode ("xadd" or "setex")
-- ARGV[3] = maxlen (for xadd) or ttl_sec (for setex)
-- ARGV[4] = sid
-- ARGV[5] = payload_json

local marker_ttl = tonumber(ARGV[1]) or 86400
local mode = ARGV[2]
local arg3 = tonumber(ARGV[3]) or 0
local sid = ARGV[4] or ""
local payload = ARGV[5] or ""

if payload == "" then
  return {1, "skip"}
end

if redis.call('GET', KEYS[1]) then
  return {1, "dedup"}
end

local t = redis.call('TIME')
local now_ms = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)

if mode == "xadd" then
  local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', tostring(arg3), '*', 'sid', sid, 'data', payload)
  redis.call('SET', KEYS[1], '1', 'EX', tostring(marker_ttl))
  return {1, id}
end

if mode == "setex" then
  redis.call('SETEX', KEYS[2], tostring(arg3), payload)
  redis.call('SET', KEYS[1], '1', 'EX', tostring(marker_ttl))
  return {1, "ok"}
end

return {0, "bad_mode"}
"""

    RELEASE_LEASE = r"""
-- KEYS[1] = lease_key
-- ARGV[1] = token
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
if v == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
"""

    EXTEND_LEASE = r"""
-- KEYS[1] = lease_key
-- ARGV[1] = token
-- ARGV[2] = pttl_ms
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
if v == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

    REENQUEUE_AND_ACK = r"""
-- KEYS[1] = source_stream
-- KEYS[2] = target_stream
-- KEYS[3] = group
-- ARGV[1] = msg_id
-- ARGV[2] = maxlen
-- ARGV[3..] = field/value pairs

local msg_id = ARGV[1]
local maxlen = tonumber(ARGV[2]) or 0

local args = {KEYS[2]}
if maxlen > 0 then
  table.insert(args, "MAXLEN")
  table.insert(args, "~")
  table.insert(args, maxlen)
end
table.insert(args, "*")

for i=3,#ARGV do
  table.insert(args, ARGV[i])
end

local new_id = redis.call("XADD", unpack(args))
redis.call("XACK", KEYS[1], KEYS[3], msg_id)
return new_id
"""

    ZPOP_DUE = r"""
-- KEYS[1] = zset
-- ARGV[1] = max_score
-- ARGV[2] = limit
local limit = tonumber(ARGV[2]) or 1
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, limit)
if #items > 0 then
  redis.call('ZREM', KEYS[1], unpack(items))
end
return items
"""

    DLQ_AND_ACK = r"""
-- KEYS[1] = source_stream
-- KEYS[2] = dlq_stream
-- KEYS[3] = group
-- ARGV[1] = msg_id
-- ARGV[2] = maxlen
-- ARGV[3..] = field/value pairs

local msg_id = ARGV[1]
local maxlen = tonumber(ARGV[2]) or 0

local args = {KEYS[2]}
if maxlen > 0 then
  table.insert(args, "MAXLEN")
  table.insert(args, "~")
  table.insert(args, maxlen)
end
table.insert(args, "*")

for i=3,#ARGV do
  table.insert(args, ARGV[i])
end

redis.call("XADD", unpack(args))
redis.call("XACK", KEYS[1], KEYS[3], msg_id)
return 1
"""

    def __init__(self, redis_client: Any, logger: Optional[logging.Logger] = None):
        """
        Initialize Lua script manager.
        
        Args:
            redis_client: Redis client instance
            logger: Optional logger for debugging
        """
        self.redis = redis_client
        self.logger = logger or logging.getLogger(__name__)
        self._sha_cache: Dict[str, str] = {}
        self._scripts: Dict[str, str] = {
            "xadd_or_setex_then_mark": self.XADD_OR_SETEX_THEN_MARK
            "notify_gate_xadd_then_mark": self.NOTIFY_GATE_XADD_THEN_MARK
            "marker_after_delivery": self.MARKER_AFTER_DELIVERY
            "release_lease": self.RELEASE_LEASE
            "extend_lease": self.EXTEND_LEASE
            "reenqueue_and_ack": self.REENQUEUE_AND_ACK
            "dlq_and_ack": self.DLQ_AND_ACK
            "xadd_and_mark": self.XADD_AND_MARK
            "setex_and_mark": self.SETEX_AND_MARK
            "xadd_fields_then_mark": self.XADD_FIELDS_THEN_MARK
            "setex_then_mark": self.SETEX_THEN_MARK
            "notify_gate": self.NOTIFY_GATE
            "mark_and_xadd": self.MARK_AND_XADD
            "mark_and_setex": self.MARK_AND_SETEX
            "mark_and_notify": self.MARK_AND_NOTIFY
            "zpop_due": self.ZPOP_DUE
        }
    
    def get_sha(self, script_name: str) -> str:
        """
        Get SHA hash for script, loading if needed.
        
        Args:
            script_name: Name of the script
            
        Returns:
            SHA hash of the script
            
        Raises:
            KeyError: If script name is unknown
        """
        if script_name not in self._scripts:
            raise KeyError(f"Unknown script: {script_name}")
        
        if script_name not in self._sha_cache:
            script = self._scripts[script_name]
            sha = self.redis.script_load(script)
            self._sha_cache[script_name] = sha
            self.logger.debug(f"Loaded script {script_name}: {sha[:8]}...")
        
        return self._sha_cache[script_name]
    
    def execute(
        self
        script_name: str
        keys: List[str]
        args: List[Any]
        client: Optional[Any] = None
    ) -> Any:
        """
        Execute Lua script with evalsha/eval fallback.
        
        Args:
            script_name: Name of the script to execute
            keys: Redis keys for the script
            args: Arguments for the script
            client: Optional specific Redis client to use (overrides self.redis)
            
        Returns:
            Script execution result
        """
        target_client = client or self.redis
        try:
            # We need SHA from the target client? 
            # Scripts might not be loaded on target_client.
            # But get_sha() loads on self.redis only?
            # Ideally target_client should load it too.
            # Let's try to use SHA derived from self.redis (it's constant for script content).
            # But we must ensure it's loaded on target_client.
            
            sha = self.get_sha(script_name) # This ensures loaded on self.redis.
            
            # Try running on target_client
            try:
                return target_client.evalsha(sha, len(keys), *keys, *args)
            except Exception as e:
                if "NOSCRIPT" in str(e):
                     # Load on target client then retry
                     script = self._scripts[script_name]
                     new_sha = target_client.script_load(script)
                     return target_client.evalsha(new_sha, len(keys), *keys, *args)
                raise
        except Exception as e:
             # Fallback to eval if evalsha fails (and not just noscript handled above)
             if "NOSCRIPT" in str(e):
                 # This path shouldn't be reached if inner try/except handles it, 
                 # but for safety:
                 self.logger.debug(f"SHA not found for {script_name}, using eval")
                 script = self._scripts[script_name]
                 return target_client.eval(script, len(keys), *keys, *args)
             raise
    
    def preload_all(self) -> None:
        """Preload all scripts to Redis."""
        for script_name in self._scripts:
            self.get_sha(script_name)
        self.logger.info(f"Preloaded {len(self._scripts)} Lua scripts")
