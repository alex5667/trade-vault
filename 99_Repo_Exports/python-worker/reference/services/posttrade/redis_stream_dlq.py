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
        redis_client=self.r,
        dlq_stream="events:trades:dlq",
        reason="position_closed_contract_violation",
        error="; ".join(errs),
        src_stream="events:trades",
        src_entry_id="*",
        payload=stream_payload,
        maxlen=200_000,
    )
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import logging
import time
from typing import Any, Dict, Optional

log = logging.getLogger("redis_stream_dlq")


def publish_dlq(
    redis_client: Any,
    *,
    dlq_stream: str,
    reason: str,
    error: str,
    src_stream: str,
    src_entry_id: str = "*",
    payload: Optional[Dict[str, Any]] = None,
    maxlen: int = 200_000,
    approximate: bool = True,
    payload_max_bytes: int = 8192,
) -> Optional[str]:
    """Опубликовать ошибочное событие в DLQ-stream (V2 API).

    Fail-safe: если публикация в DLQ тоже упадёт — логируем и продолжаем.
    Никогда не поднимает исключение выше.

    Args:
        redis_client:     Инстанс ``redis.Redis`` (уже сконфигурированный, не None).
        dlq_stream:       Имя DLQ-stream (например ``events:trades:dlq``).
        reason:           Краткий код причины (без пробелов, например
                          ``position_closed_contract_violation``).
        error:            Подробное описание — результат ``"; ".join(errors)``.
        src_stream:       Исходный stream, из которого пришёл payload.
        src_entry_id:     Entry ID в source_stream (``"*"`` если до xadd).
        payload:          Оригинальный payload события (будет сохранён как JSON
                          в поле ``payload``). None → поле не добавляется.
        maxlen:           Максимальная длина DLQ-stream (approximate trim).
        approximate:      Использовать приближённый trim для производительности.
        payload_max_bytes: Максимальный размер payload в UTF-8 байтах (обрезается).

    Returns:
        Redis stream entry ID новой записи или None при ошибке публикации.
    """
    try:
        dlq_entry: Dict[str, str] = {
            "ts_ms":        str(get_ny_time_millis()),
            "reason":       str(reason or "unknown"),
            "error":        str(error or "")[:4000],  # обрезаем чтобы не раздуть entry
            "src_stream":   str(src_stream or ""),
            "src_entry_id": str(src_entry_id or "*"),
        }

        # Payload как JSON-строка (V2: одно поле вместо многих payload_*)
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
            dlq_stream,
            dlq_entry,
            maxlen=int(maxlen),
            approximate=bool(approximate),
        )

        log.warning(
            "⚠️  DLQ publish | stream=%s reason=%s entry=%s | %s",
            dlq_stream,
            reason,
            result,
            str(error)[:200],
        )
        return result

    except Exception as exc:
        # DLQ публикация не должна останавливать основной поток
        log.error(
            "❌ Failed to publish DLQ entry | stream=%s reason=%s: %s",
            dlq_stream,
            reason,
            exc,
        )
        return None
