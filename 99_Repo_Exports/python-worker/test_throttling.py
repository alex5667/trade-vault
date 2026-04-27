#!/usr/bin/env python3
"""
Тест для проверки работы LogThrottler
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.log_throttler import LogThrottler

def test_log_throttling():
    """Тестирует механизм throttling логов"""
    print("🔧 Тестирование LogThrottler...")
    
    throttler = LogThrottler()
    
    # Симулируем 25000 устаревших тикеров
    print("\n📊 Симуляция 25000 устаревших тикеров:")
    logged_count = 0
    
    for i in range(1, 25001):
        symbol = f"TEST{i % 100}USDT"  # Создаем разные символы
        message = f"⏰ Тикер {symbol} устарел: 1715763618575 < 1761558292408"
        
        # Используем один ключ для всех сообщений об устаревших тикерах
        if throttler.log_with_count("expired_ticker_test", message, 10000):
            logged_count += 1
    
    print(f"\n📈 Результаты:")
    print(f"   Общее количество проверок: 25000")
    print(f"   Количество выведенных сообщений: {logged_count}")
    print(f"   Процент сокращения логов: {(25000 - logged_count) / 25000 * 100:.2f}%")
    
    # Проверяем счетчик
    count = throttler.get_count("expired_ticker_test")
    print(f"   Финальный счетчик: {count}")
    
    print("\n✅ Тест завершен!")

if __name__ == "__main__":
    test_log_throttling()
