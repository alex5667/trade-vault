from __future__ import annotations

"""
AtomicOutboxXADD
================
Единая атомарная запись в Redis Stream + meta sidecar (trace) без зависимости
от OutboxWriter.

P3 additions:
- payload contract is normalized into an explicit execution intent before XADD
- `orders:exec` can be mirrored atomically as the source-of-truth event log
- event-time and publish-time use a shared epoch-ms contract

Поддерживаем:
  - sync redis.Redis
  - async redis.asyncio.Redis
"""

import os

try:
    from .time_contract import monotonic_ms, utc_epoch_ms
except Exception:
    from time_contract import monotonic_ms, utc_epoch_ms
import json
from typing import Any

from core.redis_keys import RedisStreams as RS

_LUA_ATOMIC_XADD = r"""
-- KEYS[1] = dedup_key (signal_id)
-- KEYS[2] = semantic_key (or "__none__" to disable)
-- KEYS[3] = stream_key
-- KEYS[4] = meta_key (or "__none__" to disable)
-- KEYS[5] = event_stream_key (or "__none__" to disable) — P3 orders:exec mirror
-- ARGV[1] = dedup_ttl_sec
-- ARGV[2] = pending_ttl_sec
-- ARGV[3] = signal_id
-- ARGV[4] = kind
-- ARGV[5] = symbol
-- ARGV[6] = ts
-- ARGV[7] = payload_json
-- ARGV[8] = maxlen (0 disables)
-- ARGV[9] = sem_ttl_sec
-- ARGV[10] = sem_pending_ttl_sec
-- ARGV[11] = meta_json
-- ARGV[12] = meta_ttl_sec
-- ARGV[13] = event_json (INTENT_PUBLISHED fact for orders:exec)
-- returns: {1, entry_id} on success
--          {0, entry_id} on dedup hit (already sent)
--          {2, err} on xadd failure (dedup key is rolled back)
--          {3, err} on concurrent write (PENDING lock held)

local sem_enabled = (KEYS[2] ~= '__none__')
local maxlen = tonumber(ARGV[8]) or 0

local current_dedup = redis.call('GET', KEYS[1])
if current_dedup and current_dedup ~= 'PENDING' then
  return {0, current_dedup}
end
if current_dedup == 'PENDING' then
  return {3, 'CONCURRENT_WRITE'}
end

local ok = redis.call('SET', KEYS[1], 'PENDING', 'NX', 'EX', tonumber(ARGV[2]))
if not ok then return {3, 'CONCURRENT_WRITE'} end

if sem_enabled then
  local ok2 = redis.call('SET', KEYS[2], 'PENDING', 'NX', 'EX', tonumber(ARGV[10]))
  if not ok2 then
    redis.call('DEL', KEYS[1])
    return {3, 'CONCURRENT_WRITE_SEM'}
  end
end

-- XADD to the main signal stream
local xadd_ok, entry_id
if maxlen > 0 then
  xadd_ok, entry_id = pcall(redis.call, 'XADD', KEYS[3], 'MAXLEN', '~', maxlen, '*',
    'signal_id', ARGV[3],
    'kind',      ARGV[4],
    'symbol',    ARGV[5],
    'ts',        ARGV[6],
    'payload',   ARGV[7],
    'data',      ARGV[7]
  )
else
  xadd_ok, entry_id = pcall(redis.call, 'XADD', KEYS[3], '*',
    'signal_id', ARGV[3],
    'kind',      ARGV[4],
    'symbol',    ARGV[5],
    'ts',        ARGV[6],
    'payload',   ARGV[7],
    'data',      ARGV[7]
  )
end

if not xadd_ok then
  redis.call('DEL', KEYS[1])
  if sem_enabled then redis.call('DEL', KEYS[2]) end
  return {2, tostring(entry_id)}
end

redis.call('SET', KEYS[1], entry_id, 'XX', 'EX', tonumber(ARGV[1]))
if sem_enabled then redis.call('SET', KEYS[2], entry_id, 'XX', 'EX', tonumber(ARGV[9])) end

-- Meta Sidecar
local meta_json = ARGV[11]
local meta_ttl  = tonumber(ARGV[12]) or 0
if KEYS[4] ~= '__none__' and meta_ttl > 0 and meta_json ~= nil and meta_json ~= '' then
  redis.call('SET', KEYS[4], meta_json, 'NX', 'EX', meta_ttl)
end

-- P3: Atomically mirror INTENT_PUBLISHED fact into orders:exec (source-of-truth event log)
local event_json = ARGV[13]
if KEYS[5] ~= '__none__' and event_json ~= nil and event_json ~= '' then
  redis.call('XADD', KEYS[5], 'MAXLEN', '~', 50000, '*',
    'sid',           ARGV[3],
    'signal_id',     ARGV[3],
    'action',        'intent_published',
    'status',        'ok',
    'kind',          ARGV[4],
    'symbol',        ARGV[5],
    'ts',            ARGV[6],
    'event_type',    'INTENT_PUBLISHED',
    'main_entry_id', entry_id,
    'payload',       event_json,
    'data',          event_json
  )
end

return {1, entry_id}
"""


_CACHED_OUTBOX_DEDUP_PREFIX = os.getenv("OUTBOX_DEDUP_PREFIX", "outbox:dedup:")
_CACHED_OUTBOX_META_PREFIX = os.getenv("OUTBOX_META_PREFIX", "signal:meta:")
_CACHED_OUTBOX_EVENT_STREAM_ENABLE = os.getenv("OUTBOX_EVENT_STREAM_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
_CACHED_OUTBOX_EVENT_STREAM_KEY = os.getenv("OUTBOX_EVENT_STREAM_KEY", RS.ORDERS_EXEC).strip()

_CACHED_HOST = os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or "unknown-host"
_CACHED_PRODUCER_ID = str(os.getenv("PRODUCER_INSTANCE_ID") or f"{_CACHED_HOST}:{os.getpid()}")

_CACHED_OUTBOX_SCHEMA_VER = os.getenv("OUTBOX_SCHEMA_VER", "execution_intent:v1")
_CACHED_OUTBOX_CONTRACT_VER = os.getenv("OUTBOX_CONTRACT_VER", "execution_contract:v1")
_CACHED_EXECUTION_POLICY_DEFAULT = os.getenv("EXECUTION_POLICY_DEFAULT", "SAFETY_FIRST")
_CACHED_EXIT_POLICY_MODE = os.getenv("EXIT_POLICY_MODE", "SAFETY_FIRST")
_CACHED_TP_LIMIT_WATCHDOG_TIMEOUT_MS = int(os.getenv("TP_LIMIT_WATCHDOG_TIMEOUT_MS", "4000"))
_CACHED_TP_LIMIT_WATCHDOG_MARKET_FALLBACK = os.getenv("TP_LIMIT_WATCHDOG_MARKET_FALLBACK", "1").strip().lower() in {"1", "true", "yes", "on"}

_CACHED_SL_WORKING_TYPE = os.getenv("SL_WORKING_TYPE", "MARK_PRICE")
_CACHED_TP_MARKET_WORKING_TYPE = os.getenv("TP_MARKET_WORKING_TYPE", "MARK_PRICE")
_CACHED_TP_LIMIT_TRIGGER_WORKING_TYPE = os.getenv("TP_LIMIT_TRIGGER_WORKING_TYPE", "MARK_PRICE")
_CACHED_TRAIL_WORKING_TYPE = os.getenv("TRAIL_WORKING_TYPE", "MARK_PRICE")

_CACHED_DEDUP_TTL_SEC = max(1, int(int(os.getenv("EMIT_DEDUP_TTL_MS", "60000")) / 1000))
_CACHED_PENDING_TTL_SEC = max(1, int(int(os.getenv("EMIT_DEDUP_PENDING_TTL_MS", "60000")) / 1000))
_CACHED_OUTBOX_MAXLEN = int(os.getenv("OUTBOX_STREAM_MAXLEN", "100000"))
_CACHED_META_TTL_SEC = int(os.getenv("OUTBOX_META_TTL_SEC", str(_CACHED_DEDUP_TTL_SEC)) or _CACHED_DEDUP_TTL_SEC)

_CACHED_DEFAULTS = {
    "dedup_ttl_sec": _CACHED_DEDUP_TTL_SEC,
    "pending_ttl_sec": _CACHED_PENDING_TTL_SEC,
    "maxlen": _CACHED_OUTBOX_MAXLEN,
    "meta_ttl_sec": _CACHED_META_TTL_SEC,
}

def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _dedup_key(signal_id: str) -> str:
    return f"{_CACHED_OUTBOX_DEDUP_PREFIX}{signal_id}"


def _meta_key(signal_id: str) -> str:
    return f"{_CACHED_OUTBOX_META_PREFIX}{signal_id}"


def _event_stream_key() -> str:
    """Return the orders:exec key for the P3 SoT event log, or '__none__' if disabled."""
    if not _CACHED_OUTBOX_EVENT_STREAM_ENABLE:
        return "__none__"
    return _CACHED_OUTBOX_EVENT_STREAM_KEY or "__none__"


def _producer_instance_id() -> str:
    """Stable producer identity: PRODUCER_INSTANCE_ID env or hostname:pid."""
    return _CACHED_PRODUCER_ID


def _defaults() -> dict[str, int]:
    """Align dedup/stream defaults with UnifiedSignalEmitter."""
    return _CACHED_DEFAULTS


def _prepare_contract_payload(
    signal_id: str,
    kind: str,
    symbol: str,
    payload_obj: dict[str, Any],
    meta_obj: dict[str, Any] | None,
) -> dict[str, Any]:
    """Normalize payload into a concrete execution intent before XADD.

    Stamps missing contract fields (schema_ver, decision_id, execution_policy,
    risk_snapshot, working_type_policy, exit_policy, ts_event_ms, ts_publish_ms)
    using setdefault so caller-provided values are never overwritten.
    """
    payload = dict(payload_obj or {})
    meta = dict(meta_obj or {})
    now_ms = utc_epoch_ms()
    payload.setdefault("sid", str(signal_id))
    payload.setdefault("kind", str(kind or payload.get("kind") or ""))
    payload.setdefault("symbol", str(symbol or payload.get("symbol") or ""))
    payload.setdefault(
        "schema_ver",
        str(payload.get("schema_ver") or meta.get("schema_ver") or _CACHED_OUTBOX_SCHEMA_VER),
    )
    payload.setdefault("decision_id", str(payload.get("decision_id") or meta.get("decision_id") or signal_id))
    payload.setdefault(
        "contract_ver",
        str(payload.get("contract_ver") or meta.get("contract_ver") or _CACHED_OUTBOX_CONTRACT_VER),
    )
    payload.setdefault(
        "execution_plan_id",
        str(payload.get("execution_plan_id") or meta.get("execution_plan_id") or signal_id),
    )
    payload.setdefault(
        "execution_policy",
        str(payload.get("execution_policy") or meta.get("execution_policy") or _CACHED_EXECUTION_POLICY_DEFAULT).upper(),
    )
    payload.setdefault("producer_instance_id", str(payload.get("producer_instance_id") or meta.get("producer_instance_id") or _producer_instance_id()))
    payload.setdefault("ts_event_ms", utc_epoch_ms(payload.get("ts_event_ms") or meta.get("ts_event_ms")))
    # ts_queue_ms: when the intent hit the Redis queue, use ts_event_ms as fallback
    payload.setdefault(
        "ts_queue_ms",
        int(payload.get("ts_queue_ms") or meta.get("ts_queue_ms") or payload.get("ts_event_ms") or now_ms),
    )
    payload["ts_publish_ms"] = now_ms   # always overwrite with real publish time
    payload.setdefault("mono_ms", monotonic_ms())
    payload.setdefault(
        "working_type_policy",
        meta.get("working_type_policy") or payload.get("working_type_policy") or {
            "sl": _CACHED_SL_WORKING_TYPE,
            "tp_market": _CACHED_TP_MARKET_WORKING_TYPE,
            "tp_limit_trigger": _CACHED_TP_LIMIT_TRIGGER_WORKING_TYPE,
            "trail": _CACHED_TRAIL_WORKING_TYPE,
        }
    )
    payload.setdefault(
        "exit_policy",
        meta.get("exit_policy") or payload.get("exit_policy") or {
            "mode": str(_CACHED_EXIT_POLICY_MODE).upper(),
            "watchdog_timeout_ms": _CACHED_TP_LIMIT_WATCHDOG_TIMEOUT_MS,
            "market_fallback": _CACHED_TP_LIMIT_WATCHDOG_MARKET_FALLBACK,
        }
    )
    payload.setdefault("risk_snapshot", meta.get("risk_snapshot") or payload.get("risk_snapshot") or {})
    return payload


def _build_exec_event(signal_id: str, stream_key: str, payload_obj: dict[str, Any]) -> dict[str, Any]:
    """Build the INTENT_PUBLISHED fact for the orders:exec SoT event log."""
    return {
        "event_type": "INTENT_PUBLISHED",
        "sid": str(payload_obj.get("sid") or signal_id),
        "decision_id": str(payload_obj.get("decision_id") or signal_id),
        "schema_ver": (payload_obj.get("schema_ver") or "execution_intent:v1"),
        "contract_ver": (payload_obj.get("contract_ver") or "execution_contract:v1"),
        "execution_plan_id": str(payload_obj.get("execution_plan_id") or signal_id),
        "kind": (payload_obj.get("kind") or ""),
        "symbol": (payload_obj.get("symbol") or ""),
        "execution_policy": (payload_obj.get("execution_policy") or "SAFETY_FIRST"),
        "stream_key": str(stream_key),
        "ts_event_ms": int(payload_obj.get("ts_event_ms") or utc_epoch_ms()),
        "ts_queue_ms": int(payload_obj.get("ts_queue_ms") or payload_obj.get("ts_event_ms") or utc_epoch_ms()),
        "ts_publish_ms": int(payload_obj.get("ts_publish_ms") or utc_epoch_ms()),
        "producer_instance_id": str(payload_obj.get("producer_instance_id") or _producer_instance_id()),
        "risk_snapshot": payload_obj.get("risk_snapshot") or {},
        "working_type_policy": payload_obj.get("working_type_policy") or {},
        "exit_policy": payload_obj.get("exit_policy") or {},
    }


def atomic_xadd_sync(
    redis: Any,
    *,
    stream_key: str,
    signal_id: str,
    payload_obj: dict[str, Any],
    kind: str = "",
    symbol="",
    ts: str = "",
    meta_obj: dict[str, Any] | None = None,
) -> str | None:
    """Sync atomic XADD with P3 execution-intent contract normalization.

    Returns entry_id on success, None on dedup hit.
    Raises RuntimeError on Redis/lock failures (let callers handle retry).
    """
    d = _defaults()
    # P3: normalize payload into a concrete execution intent before writing
    payload_obj = _prepare_contract_payload(signal_id, kind, symbol, payload_obj, meta_obj)
    payload_json = _dumps(payload_obj)
    meta_json = _dumps(meta_obj) if isinstance(meta_obj, dict) and meta_obj else ""
    meta_key = _meta_key(signal_id) if meta_json else "__none__"
    event_stream_key = _event_stream_key()
    event_json = _dumps(_build_exec_event(signal_id, stream_key, payload_obj)) if event_stream_key != "__none__" else ""
    res = redis.eval(
        _LUA_ATOMIC_XADD,
        5,                          # KEYS count (P3: added KEYS[5] = event_stream_key)
        _dedup_key(signal_id),
        "__none__",                 # semantic dedup disabled (can be added later)
        stream_key,
        meta_key,
        event_stream_key,           # KEYS[5]: orders:exec or __none__
        int(d["dedup_ttl_sec"]),
        int(d["pending_ttl_sec"]),
        str(signal_id),
        str(kind or payload_obj.get("kind") or ""),
        str(symbol or payload_obj.get("symbol") or ""),
        str(ts or payload_obj.get("ts_event_ms") or ""),
        payload_json,
        int(d["maxlen"]),
        1,
        1,
        meta_json,
        int(d["meta_ttl_sec"] if meta_json else 0),
        event_json,                 # ARGV[13]: INTENT_PUBLISHED fact
    )
    if isinstance(res, (list, tuple)) and res:
        code = int(res[0])
        if code == 0:
            return str(res[1]) if len(res) > 1 else None
        if code == 1 and len(res) >= 2:
            return str(res[1])
        if code == 2:
            raise RuntimeError(f"atomic_xadd_failed_redis:{res[1] if len(res)>1 else 'unknown'}")
        if code == 3:
            # Concurrent write error. Raising so high-level retry logic can handle it.
            raise RuntimeError(f"atomic_xadd_concurrent_lock:{res[1] if len(res)>1 else 'unknown'}")
    raise RuntimeError("atomic_xadd_bad_response")


async def atomic_xadd_async(
    redis: Any,
    *,
    stream_key: str,
    signal_id: str,
    payload_obj: dict[str, Any],
    kind: str = "",
    symbol="",
    ts: str = "",
    meta_obj: dict[str, Any] | None = None,
) -> str | None:
    """Async version (redis.asyncio.Redis) with P3 execution-intent contract normalization.

    Returns entry_id on success, None on dedup hit.
    Raises RuntimeError on Redis/lock failures (let callers handle retry).
    """
    d = _defaults()
    # P3: normalize payload into a concrete execution intent before writing
    payload_obj = _prepare_contract_payload(signal_id, kind, symbol, payload_obj, meta_obj)
    payload_json = _dumps(payload_obj)
    meta_json = _dumps(meta_obj) if isinstance(meta_obj, dict) and meta_obj else ""
    meta_key = _meta_key(signal_id) if meta_json else "__none__"
    event_stream_key = _event_stream_key()
    event_json = _dumps(_build_exec_event(signal_id, stream_key, payload_obj)) if event_stream_key != "__none__" else ""
    res = await redis.eval(
        _LUA_ATOMIC_XADD,
        5,                          # KEYS count (P3: added KEYS[5] = event_stream_key)
        _dedup_key(signal_id),
        "__none__",                 # semantic dedup disabled (can be added later)
        stream_key,
        meta_key,
        event_stream_key,           # KEYS[5]: orders:exec or __none__
        int(d["dedup_ttl_sec"]),
        int(d["pending_ttl_sec"]),
        str(signal_id),
        str(kind or payload_obj.get("kind") or ""),
        str(symbol or payload_obj.get("symbol") or ""),
        str(ts or payload_obj.get("ts_event_ms") or ""),
        payload_json,
        int(d["maxlen"]),
        1,
        1,
        meta_json,
        int(d["meta_ttl_sec"] if meta_json else 0),
        event_json,                 # ARGV[13]: INTENT_PUBLISHED fact
    )
    if isinstance(res, (list, tuple)) and res:
        code = int(res[0])
        if code == 0:
            return str(res[1]) if len(res) > 1 else None
        if code == 1 and len(res) >= 2:
            return str(res[1])
        if code == 2:
            raise RuntimeError(f"atomic_xadd_failed_redis:{res[1] if len(res)>1 else 'unknown'}")
        if code == 3:
            # Concurrent write error. Raising so high-level retry logic can handle it.
            raise RuntimeError(f"atomic_xadd_concurrent_lock:{res[1] if len(res)>1 else 'unknown'}")
    raise RuntimeError("atomic_xadd_bad_response")
