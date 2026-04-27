#!/usr/bin/env python3
"""
Stream Statistics
Модуль для сбора и отображения статистики Redis Streams Consumer
"""

import time
import sys
from typing import Dict, Any


class StreamStatistics:
    """
    Класс для сбора и отображения статистики обработки стримов
    """
    
    def __init__(self):
        """Инициализация статистики"""
        self.stats = {
            'total_messages': 0,
            'messages_by_stream': {},
            'last_message_time': None,
            'errors': 0,
            'start_time': time.time()
        }
    
    def update_stats(self, stream_name: str, message_id: str):
        """
        Обновление статистики при получении сообщения
        
        Args:
            stream_name: Имя стрима
            message_id: ID сообщения
        """
        # Увеличиваем общий счетчик сообщений
        self.stats['total_messages'] += 1
        
        # Обновляем счетчик по стримам
        if stream_name not in self.stats['messages_by_stream']:
            self.stats['messages_by_stream'][stream_name] = 0
        self.stats['messages_by_stream'][stream_name] += 1
        
        # Обновляем время последнего сообщения
        self.stats['last_message_time'] = time.time()
    
    def increment_errors(self):
        """Увеличение счетчика ошибок"""
        self.stats['errors'] += 1
    
    def get_total_messages(self) -> int:
        """Возвращает общее количество сообщений"""
        return self.stats['total_messages']
    
    def get_errors_count(self) -> int:
        """Возвращает количество ошибок"""
        return self.stats['errors']
    
    def get_messages_by_stream(self) -> Dict[str, int]:
        """Возвращает статистику по стримам"""
        return self.stats['messages_by_stream'].copy()
    
    def get_last_message_time(self) -> float:
        """Возвращает время последнего сообщения"""
        return self.stats['last_message_time']
    
    def get_uptime(self) -> float:
        """Возвращает время работы в секундах"""
        return time.time() - self.stats['start_time']
    
    def get_messages_per_second(self) -> float:
        """Возвращает количество сообщений в секунду"""
        uptime = self.get_uptime()
        if uptime > 0:
            return self.stats['total_messages'] / uptime
        return 0.0
    
    def print_stats(self):
        """Вывод статистики"""
        print(f"\n📊 СТАТИСТИКА СТРИМОВ:")
        print(f"   📈 Всего сообщений: {self.stats['total_messages']}")
        print(f"   ❌ Ошибок: {self.stats['errors']}")
        
        # Статистика по стримам
        if self.stats['messages_by_stream']:
            print(f"   📋 По стримам:")
            for stream_name, count in self.stats['messages_by_stream'].items():
                print(f"      • {stream_name}: {count}")
        
        # Время последнего сообщения (UTC)
        if self.stats['last_message_time']:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(self.stats['last_message_time'], tz=timezone.utc)
            last_time = dt.strftime('%H:%M:%S UTC')
            print(f"   🕐 Последнее сообщение: {last_time}")
        
        # Время работы и производительность
        uptime = self.get_uptime()
        uptime_str = self._format_uptime(uptime)
        print(f"   ⏱️ Время работы: {uptime_str}")
        
        messages_per_second = self.get_messages_per_second()
        if messages_per_second > 0:
            print(f"   🚀 Сообщений/сек: {messages_per_second:.2f}")
        
        # Процент ошибок
        if self.stats['total_messages'] > 0:
            error_percentage = (self.stats['errors'] / self.stats['total_messages']) * 100
            print(f"   ⚠️ Процент ошибок: {error_percentage:.2f}%")
    
    def _format_uptime(self, seconds: float) -> str:
        """Форматирование времени работы"""
        if seconds < 60:
            return f"{seconds:.0f}с"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.0f}м"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}ч"
    
    def reset_stats(self):
        """Сброс статистики"""
        self.stats = {
            'total_messages': 0,
            'messages_by_stream': {},
            'last_message_time': None,
            'errors': 0,
            'start_time': time.time()
        }
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """Возвращает краткую сводку статистики"""
        return {
            'total_messages': self.stats['total_messages'],
            'errors': self.stats['errors'],
            'uptime': self.get_uptime(),
            'messages_per_second': self.get_messages_per_second(),
            'streams_count': len(self.stats['messages_by_stream'])
        } 