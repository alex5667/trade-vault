#!/usr/bin/env python3
"""
Stream Utils
Утилиты для работы с Redis Streams.

Содержит вспомогательные методы для:
- создания consumer group-ов сразу для нескольких стримов,
- проверки и логирования pending-сообщений,
- форматирования сообщений для логов,
- получения информации о стриме и обрезки стрима,
- проверки соединения с Redis.
"""

import redis
import sys
import time
from typing import List, Dict, Any, Optional


class StreamUtils:
    """Утилиты для работы с Redis Streams."""
    
    @staticmethod
    def create_consumer_groups(redis_client, streams: List[str], consumer_group: str) -> bool:
        """
        Создание consumer groups для всех стримов.
        
        Args:
            redis_client: Клиент Redis
            streams: Список стримов
            consumer_group: Имя группы потребителей
            
        Returns:
            bool: True если все группы созданы успешно
        """
        success = True
        for stream_name in streams:
            max_retries = 30  # Retry for up to 30 attempts (about 2.5 minutes)
            retry_count = 0

            while retry_count < max_retries:
                try:
                    # Создаем consumer group, начиная с конца стрима (только новые сообщения)
                    redis_client.xgroup_create(
                        stream_name,
                        consumer_group,
                        id='$',  # Начинаем с новых сообщений
                        mkstream=True  # Создаем стрим если не существует
                    )
                    print(f"✅ Consumer group {consumer_group} создана для стрима {stream_name}")
                    break  # Success, exit retry loop
                except redis.exceptions.ResponseError as e:
                    error_msg = str(e)
                    if "BUSYGROUP" in error_msg:
                        print(f"ℹ️ Consumer group {consumer_group} уже существует для стрима {stream_name}")
                        break  # Success, exit retry loop
                    elif "Redis is loading the dataset in memory" in error_msg:
                        retry_count += 1
                        wait_time = min(5 * retry_count, 30)  # Exponential backoff, max 30 seconds
                        print(f"⚠️ Redis загружает данные в память (попытка {retry_count}/{max_retries}), ждём {wait_time} сек...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Ошибка создания consumer group для {stream_name}: {e}")
                        success = False
                        break  # Fatal error, exit retry loop
                except Exception as e:
                    print(f"❌ Неожиданная ошибка при создании consumer group для {stream_name}: {e}")
                    success = False
                    break  # Fatal error, exit retry loop

            # If we exhausted all retries for Redis loading
            if retry_count >= max_retries:
                print(f"❌ Не удалось создать consumer group для {stream_name} после {max_retries} попыток: Redis всё ещё загружает данные")
                success = False
        
        return success
    
    @staticmethod
    def process_pending_messages(redis_client, stream_name: str, consumer_group: str):
        """
        Обработка pending сообщений (которые были доставлены, но ещё не подтверждены XACK).
        
        Args:
            redis_client: Клиент Redis
            stream_name: Имя стрима
            consumer_group: Имя группы потребителей
        """
        try:
            print("🔄 Проверка pending сообщений...")
            
            pending = redis_client.xpending_range(
                stream_name, 
                consumer_group, 
                '-', '+', 100
            )
            
            if pending:
                print(f"📦 Найдено {len(pending)} pending сообщений")
                for msg in pending:
                    print(f"   • ID: {msg['message_id']}, Время: {msg['time_since_delivered']}мс")
            else:
                print("✅ Pending сообщений не найдено")
                
        except Exception as e:
            print(f"❌ Ошибка при обработке pending сообщений: {e}")
    
    @staticmethod
    def validate_stream_data(data: Dict) -> bool:
        """
        Базовая валидация полей сообщения стрима.
        
        Args:
            data: Данные для валидации
            
        Returns:
            bool: True если данные валидны
        """
        if not isinstance(data, dict):
            return False
        
        # Проверяем обязательные поля
        required_fields = ['data']
        for field in required_fields:
            if field not in data:
                return False
        
        return True
    
    @staticmethod
    def format_message_for_logging(message_id: str, fields: Dict) -> str:
        """
        Форматирование сообщения для удобного логирования.
        
        Args:
            message_id: ID сообщения
            fields: Поля сообщения
            
        Returns:
            str: Отформатированная строка для логирования
        """
        try:
            # Извлекаем тип сообщения и символ
            message_type = "unknown"
            symbol = "N/A"
            if 'data' in fields:
                import json
                data = json.loads(fields['data'])
                message_type = data.get('type', 'unknown')
                symbol = data.get('symbol', 'N/A')
            
            return f"ID: {message_id}, Type: {message_type}, Symbol: {symbol}"
            
        except Exception as e:
            return f"ID: {message_id}, Error: {e}"
    
    @staticmethod
    def get_stream_info(redis_client, stream_name: str) -> Optional[Dict]:
        """
        Получение информации о стриме (XINFO STREAM).
        
        Args:
            redis_client: Клиент Redis
            stream_name: Имя стрима
            
        Returns:
            Dict: Информация о стриме или None
        """
        try:
            info = redis_client.xinfo_stream(stream_name)
            return info
        except Exception as e:
            print(f"❌ Ошибка получения информации о стриме {stream_name}: {e}")
            return None
    
    @staticmethod
    def trim_stream(redis_client, stream_name: str, max_len: int) -> bool:
        """
        Обрезка стрима до указанного размера (XTRIM ~ MAXLEN).
        
        Args:
            redis_client: Клиент Redis
            stream_name: Имя стрима
            max_len: Максимальная длина стрима
            
        Returns:
            bool: True если обрезка успешна
        """
        try:
            redis_client.xtrim(stream_name, maxlen=max_len, approximate=True)
            print(f"🧹 Стрим {stream_name} обрезан до {max_len} сообщений")
            return True
        except Exception as e:
            print(f"❌ Ошибка обрезки стрима {stream_name}: {e}")
            return False
    
    @staticmethod
    def check_redis_connection(redis_client) -> bool:
        """
        Проверка подключения к Redis (PING).
        
        Args:
            redis_client: Клиент Redis
            
        Returns:
            bool: True если подключение активно
        """
        try:
            redis_client.ping()
            return True
        except Exception as e:
            print(f"❌ Ошибка подключения к Redis: {e}")
            return False
    
    @staticmethod
    def format_stream_list(streams: List[str]) -> str:
        """
        Форматирование списка стримов для вывода в лог.
        
        Args:
            streams: Список стримов
            
        Returns:
            str: Отформатированная строка
        """
        if not streams:
            return "нет"
        
        if len(streams) <= 3:
            return ", ".join(streams)
        else:
            return f"{', '.join(streams[:3])} и еще {len(streams) - 3}" 