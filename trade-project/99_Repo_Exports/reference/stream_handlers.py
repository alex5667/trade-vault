#!/usr/bin/env python3
"""
Обработчики сообщений стримов (универсальные хендлеры для StreamConsumer).

Назначение:
- Инкапсулируют логику обработки сообщений из различных стримов (volatility, топы, новые пары и т.д.).
- Вызываются универсальным потребителем `stream_consumer.StreamConsumer`.
"""

import json
import time
import sys
from typing import Dict, Any


class StreamMessageHandler:
    """
    Обработчик сообщений из Redis Streams
    """
    
    def __init__(self):
        """Инициализация обработчика сообщений"""
        self.message_count = 0
    
    def process_stream_message(self, stream_name: str, message_id: str, fields: Dict[str, str]):
        """
        Обработка полученного сообщения из стрима
        
        Args:
            stream_name: Имя стрима
            message_id: ID сообщения
            fields: Поля сообщения
        """
        try:
            # Извлекаем данные из поля 'data'
            if 'data' not in fields:
                print(f"⚠️ Сообщение {message_id} не содержит поле 'data'")
                return
            
            # Парсим JSON данные
            message_data = json.loads(fields['data'])

            # Поддержка случая, когда data — это массив, а не объект
            if isinstance(message_data, list):
                print(f"ℹ️ Сообщение {message_id} содержит массив из {len(message_data)} элементов")
                message_data = {
                    'type': 'bulk',
                    'items': message_data,
                    'count': len(message_data)
                }
            elif not isinstance(message_data, dict):
                # Неподдерживаемый тип: приводим к универсальному виду
                print(f"⚠️ Неожиданный тип данных в 'data': {type(message_data).__name__}")
                message_data = {
                    'type': 'unknown',
                    'raw': message_data
                }

            message_type = message_data.get('type', 'unknown')
            
            # Увеличиваем счетчик сообщений
            self.message_count += 1
            
            # Выводим информацию о сообщении
            self._print_message_info(stream_name, message_id, message_data, message_type)
            
            # Обрабатываем специфичные типы сообщений
            self._handle_specific_message_type(message_type, message_data)
            
            # Выводим краткую статистику каждые 10 сообщений
            if self.message_count % 10 == 0:
                print(f"📊 Обработано сообщений: {self.message_count}")
                
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга JSON в сообщении {message_id}: {e}")
        except Exception as e:
            print(f"❌ Ошибка обработки сообщения {message_id}: {e}")
    
    def _print_message_info(self, stream_name: str, message_id: str, message_data: Dict, message_type: str):
        """Вывод основной информации о сообщении"""
        from datetime import datetime, timezone
        timestamp = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        symbol = message_data.get('symbol', 'N/A')
        
        print(f"\n🚨 [{timestamp}] СООБЩЕНИЕ ИЗ СТРИМА:")
        print(f"   📡 Стрим: {stream_name}")
        print(f"   🆔 ID: {message_id}")
        print(f"   🔍 Тип: {message_type}")
        print(f"   💱 Символ: {symbol}")
    
    def _handle_specific_message_type(self, message_type: str, message_data: Dict):
        """Обработка специфичных типов сообщений"""
        if message_type == 'volatilityRange':
            self._handle_volatility_range(message_data)
        elif message_type in ['volatility', 'volatilitySpike']:
            self._handle_volatility_spike(message_data)
        elif message_type == 'top-gainers':
            self._handle_gainers(message_data)
        elif message_type == 'top-losers':
            self._handle_losers(message_data)
        elif message_type == 'ws-new-pairs':
            self._handle_new_pairs(message_data)
        elif message_type == 'bulk':
            self._handle_bulk(message_data)
    
    def _handle_volatility_range(self, message_data: Dict):
        """Обработка сигнала волатильности по диапазону"""
        range_val = message_data.get('range', 'N/A')
        avg_range = message_data.get('avgRange', 'N/A')
        volatility = message_data.get('volatility', 'N/A')
        
        print(f"   📊 Диапазон: {range_val}")
        print(f"   📈 Средний диапазон: {avg_range}")
        print(f"   ⚡ Волатильность: {volatility}%")
    
    def _handle_volatility_spike(self, message_data: Dict):
        """Обработка сигнала всплеска волатильности"""
        volatility = message_data.get('volatility', 'N/A')
        threshold = message_data.get('threshold', 'N/A')
        
        print(f"   ⚡ Волатильность: {volatility}%")
        print(f"   🎯 Порог: {threshold}%")
    
    def _handle_gainers(self, message_data: Dict):
        """Обработка сигнала растущих активов"""
        change_percent = message_data.get('priceChangePercent', 'N/A')
        volume = message_data.get('volume', 'N/A')
        
        print(f"   📈 Изменение: {change_percent}%")
        print(f"   📊 Объем: {volume}")
    
    def _handle_losers(self, message_data: Dict):
        """Обработка сигнала падающих активов"""
        change_percent = message_data.get('priceChangePercent', 'N/A')
        volume = message_data.get('volume', 'N/A')
        
        print(f"   📉 Изменение: {change_percent}%")
        print(f"   📊 Объем: {volume}")
    
    def _handle_new_pairs(self, message_data: Dict):
        """Обработка сигнала новых торговых пар"""
        pairs = message_data.get('pairs', [])
        count = len(pairs) if isinstance(pairs, list) else 0
        
        print(f"   🆕 Новых пар: {count}")
        if count > 0 and count <= 5:  # Показываем только первые 5 пар
            for pair in pairs[:5]:
                print(f"      • {pair}")
        elif count > 5:
            print(f"      • ... и еще {count - 5} пар")

    def _handle_bulk(self, message_data: Dict):
        """Обработка bulk-сообщений (когда 'data' пришел массивом)"""
        items = message_data.get('items', [])
        count = message_data.get('count', len(items))
        print(f"   📦 Bulk-сообщение: {count} элементов")
        # Показываем первые 3 элемента
        for idx, item in enumerate(items[:3], start=1):
            preview = item if isinstance(item, dict) else str(item)
            print(f"      {idx}. {preview[:200]}")
    
    def get_message_count(self) -> int:
        """Возвращает количество обработанных сообщений"""
        return self.message_count
    
    def reset_message_count(self):
        """Сбрасывает счетчик сообщений"""
        self.message_count = 0 