from utils.time_utils import get_ny_time_millis

"""
Публикатор сообщений в Redis Streams (с дублированием на два Redis).

Назначение:
- Публикует сигналы одновременно в redis-worker-1 (порт 6380) и redis-worker-2 (порт 6381)
- Поддерживает дедупликацию сигналов
- Содержит маппинг логических каналов на реальные стримы Redis
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any

import redis

from core.config import SIGNAL_DEDUP_TTL_SEC, STREAM_MAPPING, STREAM_MAX_LENGTH
from core.dual_redis_client import get_dual_signals_redis
from core.redis_client import get_redis

logger = logging.getLogger(__name__)


class StreamPublisher:
    """Класс для публикации сообщений в два Redis Streams одновременно."""

    def __init__(self):
        # Инициализируется лениво при первом вызовом publish_to_stream
        self._redis_client = None
        self.stream_mapping = STREAM_MAPPING

    @property
    def redis_client(self):
        if self._redis_client is None:
            self._redis_client = get_dual_signals_redis()
        return self._redis_client

    def publish_to_stream(self, stream_name: str, data: dict[str, Any], max_length: int = STREAM_MAX_LENGTH) -> str | None:
        """
        Публикует сообщение в оба Redis Stream.
        
        Args:
            stream_name: Имя стрима Redis
            data: Объект данных (словарь)
            max_length: Максимальная длина стрима
        
        Returns:
            str | None: ID первого успешного сообщения или None при ошибке
        """
        try:
            # Проверяем соединение с Redis
            if not self._check_connection():
                return None

            # Добавляем метаданные
            message_data = {
                'data': json.dumps(data),
                'ts_ms': str(get_ny_time_millis()),
                'timestamp': str(get_ny_time_millis()),  # Kept for backward compatibility
                'type': data.get('type', 'unknown'),
                'symbol': data.get('symbol', 'unknown')
            }

            # Публикуем в оба стрима
            message_id_1, message_id_2 = self.redis_client.xadd(
                stream_name,
                message_data,
                maxlen=max_length,
                approximate=True
            )

            if message_id_1 or message_id_2:
                logger.debug("✅ Message published to stream %s (r1=%s r2=%s)", stream_name, message_id_1, message_id_2)

            return message_id_1 or message_id_2

        except redis.exceptions.ConnectionError as e:
            logger.error("Redis connection error publishing to %s: %s", stream_name, e)
            return None
        except redis.exceptions.TimeoutError as e:
            logger.error("Redis timeout publishing to %s: %s", stream_name, e)
            return None
        except Exception as e:
            logger.error("Unexpected error publishing to stream %s: %s", stream_name, e)
            return None

    def _check_connection(self) -> bool:
        """Проверяет доступность хотя бы одного Redis."""
        try:
            ping_result = self.redis_client.ping()
            if not ping_result:
                logger.warning("Neither Redis instance responding to ping")
                return False
            return True
        except Exception as e:
            logger.warning("Redis connection check failed: %s", e)
            return False

    def get_stream_info(self, stream_name: str) -> dict | None:
        """Возвращает информацию о стриме Redis (XINFO STREAM)."""
        # Используем первый доступный клиент
        if hasattr(self.redis_client, 'client_1') and self.redis_client.client_1:
            try:
                info = self.redis_client.client_1.xinfo_stream(stream_name)
                return info
            except Exception as e:
                logger.error("Error getting stream info for %s: %s", stream_name, e)
        return None

    def create_consumer_group(self, stream_name: str, group_name: str, start_id: str = '$') -> bool:
        """Создаёт consumer group для указанного стрима в обоих Redis."""
        success = False

        # Создаем в первом Redis
        if hasattr(self.redis_client, 'client_1') and self.redis_client.client_1:
            try:
                self.redis_client.client_1.xgroup_create(stream_name, group_name, start_id, mkstream=True)
                logger.info("Consumer group %s created for stream %s in Redis-1", group_name, stream_name)
                success = True
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    logger.debug("Consumer group %s already exists for stream %s in Redis-1", group_name, stream_name)
                    success = True
                else:
                    logger.error("Error creating consumer group %s in Redis-1: %s", group_name, e)
            except Exception as e:
                logger.error("Unexpected error creating consumer group %s in Redis-1: %s", group_name, e)

        # Создаем во втором Redis
        if hasattr(self.redis_client, 'client_2') and self.redis_client.client_2:
            try:
                self.redis_client.client_2.xgroup_create(stream_name, group_name, start_id, mkstream=True)
                logger.info("Consumer group %s created for stream %s in Redis-2", group_name, stream_name)
                success = True
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    logger.debug("Consumer group %s already exists for stream %s in Redis-2", group_name, stream_name)
                    success = True
                else:
                    logger.error("Error creating consumer group %s in Redis-2: %s", group_name, e)
            except Exception as e:
                logger.error("Unexpected error creating consumer group %s in Redis-2: %s", group_name, e)

        return success


def _build_dedup_key(data: dict) -> str:
    """Строит ключ дедупликации сигналов."""
    symbol = data.get('symbol', 'unknown')
    signal_type = data.get('type', 'unknown')
    candle_open_ms = data.get('t') or data.get('openTime')
    if candle_open_ms is None:
        minute_bucket = datetime.now(UTC).strftime('%Y%m%d%H%M')
        return f"dedup:signal:{signal_type}:{symbol}:{minute_bucket}"
    return f"dedup:signal:{signal_type}:{symbol}:{candle_open_ms}"


def publish_signal_to_stream(channel: str, data: dict) -> bool:
    """
    Публикует сигнал в оба Redis Stream.
    
    Args:
        channel: Логический канал
        data: Данные сигнала
        
    Returns:
        bool: True если хотя бы в один Redis успешно опубликовано
    """
    try:
        publisher = StreamPublisher()

        # Преобразуем имя канала в имя стрима
        stream_name = publisher.stream_mapping.get(
            channel,
            f"stream:{channel.replace('signal:', '').replace('trigger:', '').replace('top:', '')}"
        )

        # Дедупликация (используем основной Redis)
        try:
            main_redis = get_redis()
            dedup_key = _build_dedup_key(data)
            was_set = main_redis.set(dedup_key, 1, ex=SIGNAL_DEDUP_TTL_SEC, nx=True)
            if not was_set:
                return False
        except Exception as dedup_err:
            logger.warning("Signal dedup error: %s", dedup_err)

        # Валидация данных
        if data.get('type') == 'volatilityRange':
            logger.debug("Signal data: range=%s avgRange=%s", data.get('range'), data.get('avgRange'))

            if 'volatility' in data and (data['volatility'] == 0 or data['volatility'] is None):
                old_vol = data['volatility']
                if 'range' in data and 'avgRange' in data and data['avgRange'] > 0:
                    data['volatility'] = round((abs(float(data['range'])) / float(data['avgRange'])) * 100, 2)
                else:
                    data['volatility'] = 100.0
                logger.debug("Corrected volatility from %s to %s", old_vol, data['volatility'])

        elif data.get('type') == 'volatilitySpike':
            if 'volatility' in data and (data['volatility'] == 0 or data['volatility'] is None):
                if 'high' in data and 'low' in data and 'open' in data:
                    high = float(data['high'])
                    low = float(data['low'])
                    open_price = float(data['open'])
                    data['volatility'] = round(((high - low) / open_price) * 100, 2)

        # Публикуем данные в оба Redis Stream
        logger.info("Publishing signal %s to stream %s", data.get('type', 'unknown'), stream_name)

        message_id = publisher.publish_to_stream(stream_name, data)

        if message_id:
            logger.info("Signal %s sent to stream %s", data.get('type', 'unknown'), stream_name)
            return True
        else:
            return False

    except Exception as e:
        logger.error("Error publishing signal to stream: %s", e)
        return False


# Создаем экземпляр для использования в других модулях
stream_publisher = StreamPublisher()
