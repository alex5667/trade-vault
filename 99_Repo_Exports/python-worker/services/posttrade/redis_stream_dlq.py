# -*- coding: utf-8 -*-
"""Redis Stream DLQ (Dead-Letter Queue) helper — A3 V2.

Используется при fail-open обработке битых событий: вместо того чтобы
бросить исключение или заблокировать upstream, публикуем описание проблемы
в отдельный DLQ-stream и продолжаем работу (ACK / continue).

Назначение:
  - Не теряем данные о «битых» событиях.
  - Downstream (оператор / авто-replay) может забрать DLQ и принять решение.
  - Основной поток events:trades не блокируется.

V2 изменения:
  - Параметры `stream` и `entry_id` переименованы в `src_stream` и `src_entry_id`
    для явного семантического разделения of исходного стрима и DLQ-стрима.
  - Добавлен параметр `approximate` (default True) для оптимизации XADD.
  - Добавлен параметр `payload_max_bytes` для ограничения размера payload.
  - Payload теперь кладётся как JSON-строка в поле `payload` (не с префиксом payload_).

Формат записи в DLQ-stream (V2):
  ts_ms         str   epoch ms момента публикации в DLQ
  reason        str   краткий код причины («position_closed_contract_violation», …)
  error         str   подробное описание ошибки (конкатенация errors[])
  src_stream    str   исходный stream («events:trades»)
  src_entry_id  str   entry_id в source_stream («*» если произошло до xadd)
  payload       str   JSON-сериализованный оригинальный payload (обрезан до payload_max_bytes)

Пример использования:
    from services.posttrade.redis_stream_dlq import publish_dlq

    publish_dlq(
        redis_client=self.r
        dlq_stream="events:trades:dlq"
        reason="position_closed_contract_violation"
        error="; ".join(errs)
        src_stream="events:trades"
        src_entry_id="*"
        payload=stream_payload
        maxlen=200_000
    )
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional

log = logging.getLogger("redis_stream_dlq")


async def publish_dlq(
    redis_client: Any
    *
    dlq_stream: str
    reason: str
    error: str
    src_stream: Optional[str] = None
    src_entry_id: str = "*"
    payload: Optional[Dict[str, Any]] = None
    maxlen: int = 200_000
    approximate: bool = True
    payload_max_bytes: int = 8192
    stream: Optional[str] = None,    # Alias for src_stream (V1 compat)
    entry_id: Optional[str] = "*",   # Alias for src_entry_id (V1 compat)
    **kwargs: Any
) -> Optional[str]:
    """Опубликовать ошибочное событие в DLQ-stream (V2 Async API).

    Fail-safe: если публикация в DLQ тоже упадёт — логируем и продолжаем.
    Никогда не поднимает исключение выше.

    Args:
        redis_client:     Инстанс ``redis.asyncio.Redis``.
        dlq_stream:       Имя DLQ-stream (например ``events:trades:dlq``).
        reason:           Краткий код причины.
        error:            Подробное описание.
        src_stream:       Исходный stream.
        src_entry_id:     Entry ID в source_stream ("*" если до xadd).
        payload:          Оригинальный payload события.
        maxlen:           Максимальная длина DLQ-stream.
        approximate:      Использовать приближённый trim.
        payload_max_bytes: Максимальный размер payload в байтах.
        stream:           Legacy-алиас для src_stream.
        entry_id:         Legacy-алиас для src_entry_id.
        **kwargs:         Дополнительные аргументы (для совместимости).
    """
    # Backward compatibility logic
    s_stream = src_stream or stream or kwargs.get("stream") or ""
    s_entry_id = src_entry_id if src_entry_id != "*" else (entry_id or kwargs.get("entry_id") or "*")

    try:
        dlq_entry: Dict[str, str] = {
            "ts_ms":        str(get_ny_time_millis())
            "reason":       str(reason or "unknown")
            "error":        str(error or "")[:4000]
            "src_stream":   str(s_stream or "")
            "src_entry_id": str(s_entry_id or "*")
        }

        if payload is not None:
            try:
                raw = json.dumps(payload, ensure_ascii=False, default=str)
                raw_bytes = raw.encode("utf-8")
                if len(raw_bytes) > payload_max_bytes:
                    raw = raw_bytes[:payload_max_bytes].decode("utf-8", errors="ignore")
                dlq_entry["payload"] = raw
            except Exception:
                dlq_entry["payload"] = "{}"

        result = await redis_client.xadd(
            dlq_stream
            dlq_entry
            maxlen=int(maxlen)
            approximate=bool(approximate)
        )

        log.warning(
            "⚠️  DLQ publish (async) | stream=%s reason=%s entry=%s | %s"
            dlq_stream
            reason
            result
            str(error)[:200]
        )
        return result

    except Exception as exc:
        log.error(
            "❌ Failed to publish DLQ entry (async) | stream=%s reason=%s: %s"
            dlq_stream
            reason
            exc
        )
        return None


def publish_dlq_sync(
    redis_client: Any
    *
    dlq_stream: str
    reason: str
    error: str
    src_stream: Optional[str] = None
    src_entry_id: str = "*"
    payload: Optional[Dict[str, Any]] = None
    maxlen: int = 200_000
    approximate: bool = True
    payload_max_bytes: int = 8192
    stream: Optional[str] = None,    # Alias for src_stream (V1 compat)
    entry_id: Optional[str] = "*",   # Alias for src_entry_id (V1 compat)
    **kwargs: Any
) -> Optional[str]:
    """Синхронная версия publish_dlq для рабочих процессов без asyncio."""
    # Backward compatibility logic
    s_stream = src_stream or stream or kwargs.get("stream") or ""
    s_entry_id = src_entry_id if src_entry_id != "*" else (entry_id or kwargs.get("entry_id") or "*")

    try:
        dlq_entry: Dict[str, str] = {
            "ts_ms":        str(get_ny_time_millis())
            "reason":       str(reason or "unknown")
            "error":        str(error or "")[:4000]
            "src_stream":   str(s_stream or "")
            "src_entry_id": str(s_entry_id or "*")
        }

        if payload is not None:
            try:
                raw = json.dumps(payload, ensure_ascii=False, default=str)
                raw_bytes = raw.encode("utf-8")
                if len(raw_bytes) > payload_max_bytes:
                    raw = raw_bytes[:payload_max_bytes].decode("utf-8", errors="ignore")
                dlq_entry["payload"] = raw
            except Exception:
                dlq_entry["payload"] = "{}"

        result = redis_client.xadd(
            dlq_stream
            dlq_entry
            maxlen=int(maxlen)
            approximate=bool(approximate)
        )

        log.warning(
            "⚠️  DLQ publish (sync) | stream=%s reason=%s entry=%s | %s"
            dlq_stream
            reason
            result
            str(error)[:200]
        )
        return result

    except Exception as exc:
        log.error(
            "❌ Failed to publish DLQ entry (sync) | stream=%s reason=%s: %s"
            dlq_stream
            reason
            exc
        )
        return None

