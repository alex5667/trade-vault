#!/usr/bin/env python3
"""
Скрипт для отладки проблем с данными тикеров
"""

import sys
import os

# Добавляем текущую директорию в путь для импортов
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.ticker_data import debug_ticker_data, get_ticker_data, get_ticker_24h_metrics
from core.redis_client import get_redis


def main():
    """Основная функция отладки"""
    print("🔍 ОТЛАДКА ДАННЫХ ТИКЕРОВ")
    print("=" * 50)
    
    # Тестируем с символом, который упоминался в ошибке
    symbol = "IOSTUSDT"
    
    print(f"\n📊 Проверяем данные для {symbol}")
    print("-" * 30)
    
    # Отладочная информация
    debug_ticker_data(symbol)
    
    print(f"\n📊 Получаем данные тикера для {symbol}")
    print("-" * 30)
    
    # Пытаемся получить данные тикера
    ticker_data = get_ticker_data(symbol)
    if ticker_data:
        print(f"✅ Данные тикера получены: {type(ticker_data)}")
        if isinstance(ticker_data, dict):
            print(f"   Символ: {ticker_data.get('symbol', 'N/A')}")
            print(f"   Последняя цена: {ticker_data.get('lastPrice', 'N/A')}")
            print(f"   Объем: {ticker_data.get('volume', 'N/A')}")
        elif isinstance(ticker_data, list):
            print(f"   Получен список из {len(ticker_data)} элементов")
    else:
        print(f"❌ Данные тикера не найдены для {symbol}")
    
    print(f"\n📊 Получаем метрики 24h для {symbol}")
    print("-" * 30)
    
    # Пытаемся получить метрики
    metrics = get_ticker_24h_metrics(symbol)
    if metrics:
        print(f"✅ Метрики получены: {metrics}")
    else:
        print(f"❌ Метрики не найдены для {symbol}")
    
    print(f"\n🔍 Проверяем все ключи ticker24h в Redis")
    print("-" * 30)
    
    # Проверяем все ключи ticker24h
    try:
        redis_client = get_redis()
        keys = redis_client.keys("binance:ticker24h:*")
        print(f"Найдено {len(keys)} ключей ticker24h")
        
        if keys:
            # Показываем первые 5 ключей
            for i, key in enumerate(keys[:5]):
                print(f"  {i+1}. {key}")
            
            if len(keys) > 5:
                print(f"  ... и еще {len(keys) - 5} ключей")
                
            # Проверяем один из ключей
            if keys:
                sample_key = keys[0]
                sample_data = redis_client.get(sample_key)
                if sample_data:
                    try:
                        parsed = redis_client.json().loads(sample_data)
                        print(f"\nПример данных из {sample_key}:")
                        print(f"  Тип: {type(parsed)}")
                        if isinstance(parsed, dict):
                            print(f"  Ключи: {list(parsed.keys())}")
                        elif isinstance(parsed, list):
                            print(f"  Количество элементов: {len(parsed)}")
                    except Exception as e:
                        print(f"  Ошибка парсинга: {e}")
                        
    except Exception as e:
        print(f"❌ Ошибка при проверке Redis: {e}")


if __name__ == "__main__":
    main() 