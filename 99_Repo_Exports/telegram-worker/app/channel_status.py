"""
Модуль для проверки статуса каналов Telegram.

Назначение:
- Проверять статус каналов в Redis
- Фильтровать каналы по статусу (пропускать INACTIVE и ARCHIVED)
- Предоставлять единый интерфейс для работы со статусами каналов
- Поддерживать совместимость с разными форматами ключей
"""

import logging
from enum import Enum
from typing import List, Optional, Set, Union

import redis


class ChannelStatus(Enum):
    """Статусы каналов Telegram."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ARCHIVED = "ARCHIVED"


class ChannelStatusChecker:
    """
    Класс для проверки и фильтрации статусов каналов.
    
    Проверяет статус каналов в Redis по ключу telegram:channel:{username}:status
    и фильтрует каналы, исключая INACTIVE и ARCHIVED.
    
    Поддерживает совместимость с разными форматами ключей:
    - telegram:channel:username:status (без @)
    - telegram:channel:@username:status (с @)
    """
    
    def __init__(self, redis_client: redis.Redis, logger=None):
        """
        Инициализирует проверщик статусов каналов.
        
        Аргументы:
            redis_client: Redis клиент для проверки статусов
            logger: Логгер для вывода сообщений
        """
        self.redis_client = redis_client
        self.logger = logger or logging.getLogger(__name__)
        self.status_key_prefix = "telegram:channel:"
        self.status_key_suffix = ":status"
    
    def get_channel_status(self, username: str) -> Optional[str]:
        """
        Получает статус канала из Redis.
        
        Поддерживает совместимость с разными форматами ключей.
        
        Аргументы:
            username: имя пользователя канала (может быть с @ или без)
            
        Возвращает:
            Статус канала или None, если статус не найден
        """
        try:
            # Убираем @ если есть для чистого username
            clean_username = username.lstrip('@')
            
            # Пробуем разные форматы ключей для совместимости
            possible_keys = [
                f"{self.status_key_prefix}{clean_username}{self.status_key_suffix}",  # без @
                f"{self.status_key_prefix}{username}{self.status_key_suffix}",       # с @ если есть
            ]
            
            # Проверяем каждый возможный ключ
            for key in possible_keys:
                try:
                    # Сначала пробуем получить как hash
                    status = self.redis_client.hget(key, "status")
                    if status is not None:
                        return status
                    
                    # Если не получилось, пробуем как обычное значение (для обратной совместимости)
                    status = self.redis_client.get(key)
                    if status is not None:
                        return status
                except Exception:
                    # Игнорируем ошибки и продолжаем
                    continue
            
            # Если ни один ключ не найден
            return None
            
        except Exception as e:
            self.logger.warning("⚠️ Ошибка при получении статуса канала %s: %s", username, e)
            return None
    
    def is_channel_active(self, username: str) -> bool:
        """
        Проверяет, активен ли канал (не INACTIVE и не ARCHIVED).
        
        Аргументы:
            username: имя пользователя канала (может быть с @ или без)
            
        Возвращает:
            True если канал активен, False если INACTIVE/ARCHIVED или ошибка
        """
        status = self.get_channel_status(username)
        if status is None:
            # Если статус не найден, считаем канал активным (для обратной совместимости)
            return True
        
        return status not in [ChannelStatus.INACTIVE.value, ChannelStatus.ARCHIVED.value]
    
    def filter_active_channels(self, channels: List[str]) -> List[str]:
        """
        Фильтрует список каналов, оставляя только активные.
        
        Аргументы:
            channels: список каналов (могут быть @username или chat_id)
            
        Возвращает:
            Список только активных каналов
        """
        active_channels = []
        
        for channel in channels:
            if isinstance(channel, int):
                # Числовые ID всегда добавляем
                active_channels.append(channel)
            else:
                # Для строк проверяем статус
                clean_channel = channel.strip().lstrip('@')
                if not clean_channel:
                    continue
                
                if self.is_channel_active(channel):  # Передаем полный channel
                    active_channels.append(channel)
                else:
                    status = self.get_channel_status(channel)
                    self.logger.debug("⏸️ Канал %s пропущен (статус: %s)", channel, status)
        
        return active_channels
    
    def get_active_channels(self) -> List[str]:
        """
        Получает список активных каналов из Redis.
        
        Возвращает:
            Список активных каналов
        """
        try:
            # Получаем все каналы из основного списка
            all_channels = self.redis_client.smembers("telegram:channels:usernames")
            self.logger.debug(f"Найдено {len(all_channels)} каналов в telegram:channels:usernames")
            
            active_channels = []
            
            for channel in all_channels:
                # Проверяем статус канала
                status_key = f"telegram:channel:{channel}:status"
                status = self.redis_client.get(status_key)
                
                if status is None:
                    # Если статус не найден, считаем канал активным
                    self.logger.debug(f"{channel} - статус не найден, считаем активным")
                    active_channels.append(channel)
                elif status == "ACTIVE":
                    self.logger.debug(f"{channel} - статус ACTIVE")
                    active_channels.append(channel)
                else:
                    self.logger.debug(f"{channel} - статус {status}, пропускаем")
            
            self.logger.debug(f"Итого активных каналов: {len(active_channels)}")
            return active_channels
            
        except Exception as e:
            self.logger.warning("⚠️ Ошибка при получении активных каналов: %s", e)
            return []
    
    def filter_active_channels_set(self, channels: Set[Union[str, int]]) -> Set[Union[str, int]]:
        """
        Фильтрует множество каналов, оставляя только активные.
        
        Аргументы:
            channels: множество каналов (могут быть @username или chat_id)
            
        Возвращает:
            Множество только активных каналов
        """
        active_channels = set()
        
        for channel in channels:
            if isinstance(channel, int):
                # Числовые ID всегда добавляем
                active_channels.add(channel)
            else:
                # Для строк проверяем статус
                clean_channel = channel.strip().lstrip('@')
                if not clean_channel:
                    continue
                
                if self.is_channel_active(channel):  # Передаем полный channel
                    active_channels.add(channel)
                else:
                    status = self.get_channel_status(channel)
                    print(f"⏸️ Канал {channel} пропущен (статус: {status})")
        
        return active_channels
    
    def set_channel_status(self, username: str, status: str) -> bool:
        """
        Устанавливает статус канала в Redis.
        
        Аргументы:
            username: имя пользователя канала (может быть с @ или без)
            status: новый статус канала
            
        Возвращает:
            True если статус установлен успешно, False в случае ошибки
        """
        try:
            # Убираем @ если есть для чистого username
            clean_username = username.lstrip('@')
            key = f"{self.status_key_prefix}{clean_username}{self.status_key_suffix}"
            
            # Устанавливаем статус как обычное значение
            self.redis_client.set(key, status)
            
            self.logger.info("✅ Статус канала %s установлен: %s", username, status)
            return True
            
        except Exception as e:
            self.logger.error("❌ Ошибка при установке статуса канала %s: %s", username, e)
            return False
    
    def get_all_channel_statuses(self) -> dict:
        """
        Получает статусы всех каналов.
        
        Возвращает:
            Словарь {channel: status}
        """
        try:
            all_channels = self.redis_client.smembers("telegram:channels:usernames")
            statuses = {}
            
            for channel in all_channels:
                status = self.get_channel_status(channel)
                statuses[channel] = status
            
            return statuses
            
        except Exception as e:
            self.logger.warning("⚠️ Ошибка при получении статусов каналов: %s", e)
            return {}
