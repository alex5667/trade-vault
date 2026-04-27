"""
Мониторинг активности каналов Telegram.

Назначение:
- Отслеживать активность каналов
- Выявлять пропущенные сообщения
- Отправлять алерты о проблемах
- Ведение статистики по каналам
"""

import time
import redis
import json
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

@dataclass
class ChannelActivity:
    """Данные об активности канала."""
    channel_name: str
    last_message_time: Optional[float] = None
    message_count: int = 0
    last_activity_check: float = 0
    is_active: bool = True
    missed_messages: int = 0
    last_alert_time: float = 0

class ChannelMonitor:
    """Мониторинг активности каналов."""
    
    def __init__(self, redis_client: redis.Redis, logger: logging.Logger):
        self.redis = redis_client
        self.logger = logger
        self.channels: Dict[str, ChannelActivity] = {}
        self.alert_cooldown = 300  # 5 минут между алертами
        self.inactivity_threshold = 3600  # 1 час без сообщений = неактивность
        
    def add_channel(self, channel_name: str):
        """Добавляет канал для мониторинга."""
        if channel_name not in self.channels:
            self.channels[channel_name] = ChannelActivity(
                channel_name=channel_name,
                last_activity_check=time.time()
            )
            self.logger.info(f"📊 Добавлен канал для мониторинга: {channel_name}")
    
    def update_channel_activity(self, channel_name: str, message_time: float = None):
        """Обновляет активность канала при получении сообщения."""
        if channel_name not in self.channels:
            self.add_channel(channel_name)
        
        channel = self.channels[channel_name]
        current_time = message_time or time.time()
        
        # Обновляем статистику
        channel.message_count += 1
        channel.last_message_time = current_time
        channel.last_activity_check = current_time
        channel.is_active = True
        channel.missed_messages = 0
        
        # Сохраняем в Redis
        self._save_channel_stats(channel)
        
        self.logger.debug(f"📈 Обновлена активность канала {channel_name}: {channel.message_count} сообщений")
    
    def check_channel_health(self, channel_name: str) -> Dict[str, any]:
        """Проверяет здоровье канала."""
        if channel_name not in self.channels:
            return {"status": "not_monitored", "message": "Канал не отслеживается"}
        
        channel = self.channels[channel_name]
        current_time = time.time()
        
        # Проверяем последнюю активность
        if channel.last_message_time:
            time_since_last_message = current_time - channel.last_message_time
            if time_since_last_message > self.inactivity_threshold:
                channel.is_active = False
                return {
                    "status": "inactive",
                    "message": f"Канал неактивен {time_since_last_message/3600:.1f} часов",
                    "last_message": datetime.fromtimestamp(channel.last_message_time).isoformat(),
                    "time_since_last": time_since_last_message
                }
        
        return {
            "status": "active",
            "message": "Канал активен",
            "message_count": channel.message_count,
            "last_message": datetime.fromtimestamp(channel.last_message_time).isoformat() if channel.last_message_time else None
        }
    
    def get_inactive_channels(self) -> List[str]:
        """Возвращает список неактивных каналов."""
        inactive = []
        current_time = time.time()
        
        for channel_name, channel in self.channels.items():
            if channel.last_message_time:
                time_since_last = current_time - channel.last_message_time
                if time_since_last > self.inactivity_threshold:
                    inactive.append(channel_name)
        
        return inactive
    
    def get_channel_stats(self) -> Dict[str, any]:
        """Возвращает статистику по всем каналам."""
        stats = {
            "total_channels": len(self.channels),
            "active_channels": 0,
            "inactive_channels": 0,
            "total_messages": 0,
            "channels": {}
        }
        
        current_time = time.time()
        
        for channel_name, channel in self.channels.items():
            is_active = True
            if channel.last_message_time:
                time_since_last = current_time - channel.last_message_time
                is_active = time_since_last <= self.inactivity_threshold
            
            if is_active:
                stats["active_channels"] += 1
            else:
                stats["inactive_channels"] += 1
            
            stats["total_messages"] += channel.message_count
            stats["channels"][channel_name] = {
                "message_count": channel.message_count,
                "last_message": datetime.fromtimestamp(channel.last_message_time).isoformat() if channel.last_message_time else None,
                "is_active": is_active,
                "missed_messages": channel.missed_messages
            }
        
        return stats
    
    def _save_channel_stats(self, channel: ChannelActivity):
        """Сохраняет статистику канала в Redis."""
        stats_key = f"telegram:channel:{channel.channel_name}:stats"
        # Senior Dev Fix: Конвертируем все значения в Redis-compatible types
        stats_data = {
            "message_count": str(channel.message_count),
            "last_message_time": str(channel.last_message_time) if channel.last_message_time else "0",
            "last_activity_check": str(channel.last_activity_check),
            "is_active": "1" if channel.is_active else "0",  # ✅ Boolean → String
            "missed_messages": str(channel.missed_messages)
        }
        
        self.redis.hset(stats_key, mapping=stats_data)
        self.redis.expire(stats_key, 86400 * 7)  # 7 дней
    
    def load_channel_stats(self):
        """Загружает статистику каналов из Redis."""
        pattern = "telegram:channel:*:stats"
        
        # Используем SCAN вместо KEYS для лучшей производительности
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
            
            for key in keys:
                # Извлекаем имя канала из ключа (key может быть bytes или str)
                if isinstance(key, bytes):
                    channel_name = key.decode('utf-8').replace("telegram:channel:", "").replace(":stats", "")
                else:
                    channel_name = str(key).replace("telegram:channel:", "").replace(":stats", "")
                
                if channel_name not in self.channels:
                    self.channels[channel_name] = ChannelActivity(channel_name=channel_name)
                
                channel = self.channels[channel_name]
                stats = self.redis.hgetall(key)
                
                if stats:
                    # Senior Dev Fix: Обрабатываем и bytes и str
                    def get_stat(key_name, default=0):
                        """Универсальный getter для stats."""
                        val = stats.get(key_name.encode('utf-8')) or stats.get(key_name)
                        if val is None:
                            return default
                        if isinstance(val, bytes):
                            return val.decode('utf-8')
                        return str(val)
                    
                    channel.message_count = int(get_stat('message_count', '0'))
                    lmt = get_stat('last_message_time', '0')
                    channel.last_message_time = float(lmt) if lmt and lmt != '0' else None
                    channel.last_activity_check = float(get_stat('last_activity_check', '0'))
                    channel.is_active = get_stat('is_active', '1') == '1'
                    channel.missed_messages = int(get_stat('missed_messages', '0'))
            
            if cursor == 0:
                break
        
        self.logger.info(f"📊 Загружена статистика для {len(self.channels)} каналов")
    
    def send_alert(self, message: str, channel_name: str = None):
        """Отправляет алерт о проблеме с каналом."""
        current_time = time.time()
        
        # Проверяем cooldown для алертов
        if channel_name and channel_name in self.channels:
            channel = self.channels[channel_name]
            if current_time - channel.last_alert_time < self.alert_cooldown:
                return False
            channel.last_alert_time = current_time
        
        # Отправляем алерт в Redis stream
        alert_data = {
            "message": message,
            "channel": channel_name or "system",
            "timestamp": str(int(current_time * 1000)),
            "type": "channel_alert"
        }
        
        self.redis.xadd("telegram:alerts", alert_data)
        self.logger.warning(f"🚨 АЛЕРТ: {message}")
        
        return True
    
    def monitor_channels(self):
        """Основной цикл мониторинга каналов."""
        self.logger.info("🔍 Запуск мониторинга каналов...")
        
        while True:
            try:
                current_time = time.time()
                inactive_channels = self.get_inactive_channels()
                
                # Отправляем алерты для неактивных каналов
                for channel_name in inactive_channels:
                    channel = self.channels[channel_name]
                    if channel.is_active:  # Только если канал был активен
                        channel.is_active = False
                        self.send_alert(
                            f"Канал {channel_name} неактивен более {self.inactivity_threshold/3600:.1f} часов",
                            channel_name
                        )
                
                # Сохраняем статистику
                for channel in self.channels.values():
                    self._save_channel_stats(channel)
                
                # Логируем статистику каждые 10 минут
                if int(current_time) % 600 == 0:
                    stats = self.get_channel_stats()
                    self.logger.info(f"📊 Статистика каналов: {stats['active_channels']} активных, {stats['inactive_channels']} неактивных, {stats['total_messages']} сообщений")
                
                time.sleep(60)  # Проверяем каждую минуту
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка в мониторинге каналов: {e}")
                time.sleep(60)
