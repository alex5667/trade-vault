"""
Публикация подготовленных данных сигналов в Redis Streams и кеш‑ключи.
Содержит функцию publish_list, которая сохраняет пакет и отправляет событие в соответствующий стрим.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from core.signals_redis_client import get_signals_redis
from publisher.stream_publisher import publish_signal_to_stream

logger = logging.getLogger(__name__)


def publish_list(key: str, entries: List[Dict[str, Any]]) -> None:
    """Сохраняет список в Redis и публикует событие напрямую в Redis Streams (без Trigger).

    Публикация по правилам:
    - gainers → channel='top:gainers' → stream:top-gainers
    - losers  → channel='top:losers'  → stream:top-losers
    - volume  → channel='signal:volume' → stream:volume-signals
    - funding → channel='signal:funding' → stream:funding-signals
    """
    try:
        # Используем Redis клиент для сигналов (порт 6380)
        redis_client = get_signals_redis()
        serialized = json.dumps(entries, ensure_ascii=False)
        redis_client.setex(key, 300, serialized)
        logger.debug("Published %d entries to key: %s", len(entries), key)

        # Определяем канал для Streams
        if "gainers" in key:
            channel = "top:gainers"
        elif "losers" in key:
            channel = "top:losers"
        elif "volume" in key:
            channel = "signal:volume"
        elif "funding" in key:
            channel = "signal:funding"
        else:
            # по умолчанию отправляем как сигнал с исходным ключом
            channel = f"signal:{key}"

        event = {
            "type": channel,
            "payload": entries,
            "size": len(entries),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        success = publish_signal_to_stream(channel, event)
        if success:
            logger.info("Batch published to stream channel: %s", channel)
        else:
            logger.error("Failed to publish to stream channel: %s", channel)
    except Exception as e:
        logger.error("Error publishing list %s: %s", key, e) 