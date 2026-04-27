from utils.time_utils import get_ny_time_millis
"""
Публикатор сообщений в Redis Streams (с дублированием на два Redis).

Назначение:
- Публикует сигналы одновременно в redis-worker-1 (порт 6380) и redis-worker-2 (порт 6381)
- Поддерживает дедупликацию сигналов
- Содержит маппинг логических каналов на реальные стримы Redis
"""

import json
import sys
import os
from typing import Dict, Any, Optional
import time

from core.dual_redis_client import get_dual_signals_redis
from core.redis_client import get_redis
import redis
from core.config import STREAM_MAPPING, STREAM_MAX_LENGTH, SIGNAL_DEDUP_TTL_SEC
from datetime import datetime, timezone


class DualStreamPublisher:
    """Класс для публикации сообщений в два Redis Streams одновременно."""
    
    def __init__(self):
        # Инициализируется лениво при первом вызове, чтобы не блокировать импорт
        self._redis_client = None
        self.stream_mapping = STREAM_MAPPING
    
    @property
    def redis_client(self):
        if self._redis_client is None:
            self._redis_client = get_dual_signals_redis()
        return self._redis_client
    
    def publish_to_stream(self, stream_name: str, data: Dict[str, Any], max_length: int = STREAM_MAX_LENGTH) -> Optional[tuple]:
        """
        Публикует сообщение в оба Redis Stream.
        
        Args:
            stream_name: Имя стрима Redis
            data: Объект данных (словарь)
            max_length: Максимальная длина стрима
        
        Returns:
            tuple | None: (ID сообщения в redis-1, ID сообщения в redis-2) или None при ошибке
        """
        try:
            # Проверяем соединение с Redis
            if not self._check_connection():
                return None
            
            # Добавляем метаданные
            message_data = {
                'data': json.dumps(data),
                'timestamp': str(get_ny_time_millis()),
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
                print(f"✅ Сообщение опубликовано в стрим {stream_name}")
                if message_id_1:
                    print(f"   📤 Redis-1 (6380): ID {message_id_1}")
                if message_id_2:
                    print(f"   📤 Redis-2 (6381): ID {message_id_2}")
                sys.stdout.flush()
            
            return (message_id_1, message_id_2)
            
        except Exception as e:
            print(f"❌ Неожиданная ошибка при публикации в стрим: {e}")
            sys.stdout.flush()
            return None
    
    def _check_connection(self) -> bool:
        """Проверяет доступность хотя бы одного Redis."""
        try:
            ping_result = self.redis_client.ping()
            if not ping_result:
                print("⚠️ Ни один из Redis не отвечает на ping")
                sys.stdout.flush()
                return False
            return True
        except Exception as e:
            print(f"⚠️ Ошибка проверки соединения Redis: {e}")
            sys.stdout.flush()
            return False


def _build_dedup_key(data: dict) -> str:
    """Строит ключ дедупликации сигналов."""
    symbol = data.get('symbol', 'unknown')
    signal_type = data.get('type', 'unknown')
    candle_open_ms = data.get('t') or data.get('openTime')
    if candle_open_ms is None:
        minute_bucket = datetime.now(timezone.utc).strftime('%Y%m%d%H%M')
        return f"dedup:signal:{signal_type}:{symbol}:{minute_bucket}"
    return f"dedup:signal:{signal_type}:{symbol}:{candle_open_ms}"


def publish_signal_to_stream_dual(channel: str, data: dict) -> bool:
    """
    Публикует сигнал в оба Redis Stream.
    
    Args:
        channel: Логический канал
        data: Данные сигнала
        
    Returns:
        bool: True если хотя бы в один Redis успешно опубликовано
    """
    try:
        publisher = DualStreamPublisher()
        
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
            print(f"⚠️ Ошибка дедупликации сигналов: {dedup_err}")
            sys.stdout.flush()
        
        # Валидация данных
        if data.get('type') == 'volatilityRange':
            if 'volatility' in data and (data['volatility'] == 0 or data['volatility'] is None):
                if 'range' in data and 'avgRange' in data and data['avgRange'] > 0:
                    data['volatility'] = round((abs(float(data['range'])) / float(data['avgRange'])) * 100, 2)
                else:
                    data['volatility'] = 100.0
        
        elif data.get('type') == 'volatilitySpike':
            if 'volatility' in data and (data['volatility'] == 0 or data['volatility'] is None):
                if 'high' in data and 'low' in data and 'open' in data:
                    high = float(data['high'])
                    low = float(data['low'])
                    open_price = float(data['open'])
                    data['volatility'] = round(((high - low) / open_price) * 100, 2)
        
        # Публикуем данные в оба Redis Stream
        print(f"📢 Публикация сигнала {data.get('type', 'unknown')} в стрим: {stream_name}")
        sys.stdout.flush()
        
        message_ids = publisher.publish_to_stream(stream_name, data)
        
        if message_ids and (message_ids[0] or message_ids[1]):
            print(f"✅ Сигнал {data.get('type', 'unknown')} отправлен в оба стрима {stream_name}")
            sys.stdout.flush()
            return True
        else:
            return False
            
    except Exception as e:
        print(f"❌ Ошибка при публикации сигнала в стрим: {e}")
        sys.stdout.flush()
        return False


# Создаем экземпляр для использования в других модулях
dual_stream_publisher = DualStreamPublisher()
