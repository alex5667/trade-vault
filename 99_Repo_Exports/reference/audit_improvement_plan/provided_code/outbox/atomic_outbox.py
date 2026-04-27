from __future__ import annotations

"""
AtomicOutboxXADD
===============
Единая атомарная запись в Redis Stream + meta sidecar (trace) без зависимости от OutboxWriter.

Нужно для сервисов, которые сейчас делают прямой xadd(... {"data": env_json} ...),
и из-за этого НЕ создают meta sidecar (OUTBOX_META_PREFIX + sid).

Поддерживаем:
  - sync redis.Redis (binance_iceberg_detector.py)
  - async aioredis.Redis (crypto_orderflow_service.py)

Важно:
  - Поле в stream пишем как 'payload' (dispatcher уже умеет fields['payload']).
    Это нормализует формат и позволяет переиспользовать Lua контракт OutboxWriter.
  - Meta sidecar пишем только после успешного XADD.
"""

import os
import json
from typing import Any, Dict, Optional


_LUA_ATOMIC_XADD = r"""
-- KEYS[1] = dedup_key (signal_id)
-- KEYS[2] = semantic_key (or "__none__" to disable)
-- KEYS[3] = stream_key
-- KEYS[4] = meta_key (or "__none__" to disable)
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
-- returns: {1, entry_id} on success
--          {0, entry_id} on dedup hit (already sent)
--          {2, err} on xadd failure (dedup key is rolled back)
--          {3, err} on concurrent write (PENDING lock held)

local sem_enabled = (KEYS[2] ~= '__none__')
local maxlen = tonumber(ARGV[8]) or 0

-- 1. Проверяем существование (Уже успешно отправлено)
local current_dedup = redis.call('GET', KEYS[1])
if current_dedup and current_dedup ~= 'PENDING' then
  return {0, current_dedup} -- Уже отправлено, возвращаем старый ID
end

-- 2. Если висит PENDING, значит другой процесс/ретрай прямо сейчас это пишет
if current_dedup == 'PENDING' then
  return {3, "CONCURRENT_WRITE"} -- Ошибка конкуренции, клиент должен сделать backoff и ретрай
end

-- 3. Захватываем блокировку PENDING
local ok = redis.call('SET', KEYS[1], 'PENDING', 'NX', 'EX', tonumber(ARGV[2]))
if not ok then return {3, "CONCURRENT_WRITE"} end

if sem_enabled then
  local ok2 = redis.call('SET', KEYS[2], 'PENDING', 'NX', 'EX', tonumber(ARGV[10]))
  if not ok2 then
    redis.call('DEL', KEYS[1])
    return {3, "CONCURRENT_WRITE_SEM"}
  end
end

-- 4. Выполняем XADD
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

-- 5. Обработка ошибки XADD
if not xadd_ok then
  -- КРИТИЧНО: Освобождаем блокировку PENDING, чтобы ретрай мог сработать немедленно!
  redis.call('DEL', KEYS[1])
  if sem_enabled then redis.call('DEL', KEYS[2]) end
  return {2, tostring(entry_id)}
end

-- 6. Фиксируем успех
redis.call('SET', KEYS[1], entry_id, 'XX', 'EX', tonumber(ARGV[1]))
if sem_enabled then redis.call('SET', KEYS[2], entry_id, 'XX', 'EX', tonumber(ARGV[9])) end

-- 7. Пишем Meta Sidecar (если есть)
local meta_json = ARGV[11]
local meta_ttl  = tonumber(ARGV[12]) or 0
if KEYS[4] ~= '__none__' and meta_ttl > 0 and meta_json ~= nil and meta_json ~= '' then
  redis.call('SET', KEYS[4], meta_json, 'NX', 'EX', meta_ttl)
end

return {1, entry_id}
"""


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _dedup_key(signal_id: str) -> str:
    p = os.getenv("OUTBOX_DEDUP_PREFIX", "outbox:dedup:")
    return f"{p}{signal_id}"


def _meta_key(signal_id: str) -> str:
    p = os.getenv("OUTBOX_META_PREFIX", "signal:meta:")
    return f"{p}{signal_id}"


def _defaults() -> Dict[str, int]:
    # align with UnifiedSignalEmitter defaults
    dedup_ttl_sec = max(1, int(int(os.getenv("EMIT_DEDUP_TTL_MS", "60000")) / 1000))
    pending_ttl_sec = max(1, int(int(os.getenv("EMIT_DEDUP_PENDING_TTL_MS", "60000")) / 1000))
    maxlen = int(os.getenv("OUTBOX_STREAM_MAXLEN", "100000"))
    meta_ttl_sec = int(os.getenv("OUTBOX_META_TTL_SEC", str(dedup_ttl_sec)) or dedup_ttl_sec)
    return {
        "dedup_ttl_sec": int(dedup_ttl_sec),
        "pending_ttl_sec": int(pending_ttl_sec),
        "maxlen": int(maxlen),
        "meta_ttl_sec": int(meta_ttl_sec),
    }


def atomic_xadd_sync(
    redis: Any,
    *,
    stream_key: str,
    signal_id: str,
    payload_obj: Dict[str, Any],
    kind: str = "",
    symbol: str = "",
    ts: str = "",
    meta_obj: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Возвращает entry_id:
      - str => success
      - None => dedup hit
    """
    d = _defaults()
    payload_json = _dumps(payload_obj)
    meta_json = _dumps(meta_obj) if isinstance(meta_obj, dict) and meta_obj else ""
    meta_key = _meta_key(signal_id) if meta_json else "__none__"
    res = redis.eval(
        _LUA_ATOMIC_XADD,
        4,
        _dedup_key(signal_id),
        "__none__",  # semantic dedup disabled here (can be added later)
        stream_key,
        meta_key,
        int(d["dedup_ttl_sec"]),
        int(d["pending_ttl_sec"]),
        str(signal_id),
        str(kind or ""),
        str(symbol or ""),
        str(ts or ""),
        payload_json,
        int(d["maxlen"]),
        1,
        1,
        meta_json,
        int(d["meta_ttl_sec"] if meta_json else 0),
    )
    if isinstance(res, (list, tuple)) and res:
        code = int(res[0])
        if code == 0:
            return str(res[1]) if len(res) > 1 else None # Return old ID if exists
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
    payload_obj: Dict[str, Any],
    kind: str = "",
    symbol: str = "",
    ts: str = "",
    meta_obj: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Async версия для aioredis.Redis
    """
    d = _defaults()
    payload_json = _dumps(payload_obj)
    meta_json = _dumps(meta_obj) if isinstance(meta_obj, dict) and meta_obj else ""
    meta_key = _meta_key(signal_id) if meta_json else "__none__"
    res = await redis.eval(
        _LUA_ATOMIC_XADD,
        4,
        _dedup_key(signal_id),
        "__none__",
        stream_key,
        meta_key,
        int(d["dedup_ttl_sec"]),
        int(d["pending_ttl_sec"]),
        str(signal_id),
        str(kind or ""),
        str(symbol or ""),
        str(ts or ""),
        payload_json,
        int(d["maxlen"]),
        1,
        1,
        meta_json,
        int(d["meta_ttl_sec"] if meta_json else 0),
    )
    if isinstance(res, (list, tuple)) and res:
        code = int(res[0])
        if code == 0:
            return str(res[1]) if len(res) > 1 else None # Return old ID if exists
        if code == 1 and len(res) >= 2:
            return str(res[1])
        if code == 2:
            raise RuntimeError(f"atomic_xadd_failed_redis:{res[1] if len(res)>1 else 'unknown'}")
        if code == 3:
            # Concurrent write error. Raising so high-level retry logic can handle it.
            raise RuntimeError(f"atomic_xadd_concurrent_lock:{res[1] if len(res)>1 else 'unknown'}")
    raise RuntimeError("atomic_xadd_bad_response")
