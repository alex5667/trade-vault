"""
Модуль для работы с данными тикеров из Redis Stream.

Назначение:
- Получение данных тикера по символу из Redis Stream
- Парсинг и обработка данных для использования в сигналах
- Кэширование данных для оптимизации производительности
"""

import json
import time
from typing import Any

from core.redis_client import get_redis


class TickerDataManager:
    """Менеджер для работы с данными тикеров"""

    def __init__(self):
        self.redis_client = get_redis()
        self.cache = {}
        self.cache_ttl = 60  # TTL кэша в секундах

    def get_ticker_data(self, symbol: str) -> dict[str, Any] | None:
        """
        Получает данные тикера по символу из Redis ключей
        
        Args:
            symbol: Символ торговой пары
            
        Returns:
            Словарь с данными тикера или None, если данные не найдены
        """
        try:
            # Проверяем кэш
            cache_key = f"ticker_cache:{symbol}"
            if cache_key in self.cache:
                cache_time, data = self.cache[cache_key]
                if time.time() - cache_time < self.cache_ttl:
                    print(f"📋 TickerDataManager: Используем кэшированные данные для {symbol}")
                    return data
                else:
                    print(f"📋 TickerDataManager: Кэш для {symbol} устарел, обновляем")

            # Получаем данные из Redis ключа (где тикеры сохраняются индивидуально)
            key = f"binance:ticker24h:{symbol}"
            print(f"🔍 TickerDataManager: Ищем данные в Redis ключе {key}")

            data = self.redis_client.get(key)

            if data:
                print(f"✅ TickerDataManager: Найдены данные для {symbol} в Redis")
                try:
                    ticker_data = json.loads(data)
                    if isinstance(ticker_data, dict):
                        # Кэшируем данные
                        self.cache[cache_key] = (time.time(), ticker_data)
                        print(f"📋 TickerDataManager: Данные для {symbol} закэшированы")
                        return ticker_data
                    elif isinstance(ticker_data, list):
                        # Если по какой-то причине сохранен список, берем первый элемент
                        if len(ticker_data) > 0 and isinstance(ticker_data[0], dict):
                            ticker = ticker_data[0]
                            self.cache[cache_key] = (time.time(), ticker)
                            print(f"⚠️ TickerDataManager: Для {symbol} найден список, используем первый элемент")
                            return ticker
                        else:
                            print(f"⚠️ Данные тикера для {symbol} содержат пустой список")
                    else:
                        print(f"⚠️ Данные тикера для {symbol} не являются словарем или списком: {type(ticker_data)}")
                except json.JSONDecodeError as e:
                    print(f"⚠️ Ошибка при парсинге данных тикера для {symbol}: {e}")
                    print(f"🔍 Сырые данные: {data[:200]}...")
            else:
                print(f"❌ TickerDataManager: Данные для {symbol} не найдены в Redis")

            return None

        except Exception as e:
            print(f"❌ Ошибка при получении данных тикера для {symbol}: {e}")
            return None

    def get_ticker_24h_metrics(self, symbol: str) -> dict[str, float] | None:
        """
        Получает 24h метрики тикера по символу
        
        Args:
            symbol: Символ торговой пары
            
        Returns:
            Словарь с метриками или None, если данные не найдены
        """
        ticker_data = self.get_ticker_data(symbol)
        if not ticker_data:
            return None

        if not isinstance(ticker_data, dict):
            print(f"⚠️ Данные тикера для {symbol} не являются словарем: {type(ticker_data)}")
            return None

        try:
            # Извлекаем числовые значения
            metrics = {
                'high_24h': float(ticker_data.get('highPrice', 0)),
                'low_24h': float(ticker_data.get('lowPrice', 0)),
                'price_change_percent': float(ticker_data.get('priceChangePercent', 0)),
                'volume_change_percent': 0.0,  # Binance API не предоставляет это поле
                'open_price': float(ticker_data.get('openPrice', 0)),
                'last_price': float(ticker_data.get('lastPrice', 0)),
                'volume': float(ticker_data.get('volume', 0)),
                'quote_volume': float(ticker_data.get('quoteVolume', 0))
            }

            return metrics

        except (ValueError, TypeError) as e:
            print(f"❌ Ошибка при парсинге метрик для {symbol}: {e}")
            return None

    def debug_ticker_data(self, symbol: str) -> None:
        """
        Отладочный метод для проверки данных тикера в Redis
        
        Args:
            symbol: Символ торговой пары
        """
        try:
            # Проверяем Redis ключ
            key = f"binance:ticker24h:{symbol}"
            data = self.redis_client.get(key)

            if data:
                print(f"🔍 Debug: Найден ключ {key}")
                print(f"🔍 Debug: Тип данных: {type(data)}")
                print(f"🔍 Debug: Размер данных: {len(data)} байт")

                try:
                    parsed = json.loads(data)
                    print(f"🔍 Debug: Тип после парсинга: {type(parsed)}")
                    if isinstance(parsed, list):
                        print(f"🔍 Debug: Список содержит {len(parsed)} элементов")
                        if len(parsed) > 0:
                            print(f"🔍 Debug: Первый элемент: {type(parsed[0])}")
                    elif isinstance(parsed, dict):
                        print(f"🔍 Debug: Словарь содержит ключи: {list(parsed.keys())}")
                except json.JSONDecodeError as e:
                    print(f"🔍 Debug: Ошибка парсинга JSON: {e}")
            else:
                print(f"🔍 Debug: Ключ {key} не найден в Redis")

        except Exception as e:
            print(f"🔍 Debug: Ошибка при отладке данных тикера для {symbol}: {e}")


# Глобальный экземпляр менеджера (lazy initialization)
_ticker_manager = None

def _get_ticker_manager():
    """Lazy initialization of ticker manager"""
    global _ticker_manager
    if _ticker_manager is None:
        _ticker_manager = TickerDataManager()
    return _ticker_manager


def get_ticker_24h_metrics(symbol: str) -> dict[str, float] | None:
    """
    Удобная функция для получения 24h метрик тикера
    
    Args:
        symbol: Символ торговой пары
        
    Returns:
        Словарь с метриками или None, если данные не найдены
    """
    return _get_ticker_manager().get_ticker_24h_metrics(symbol)


def get_ticker_data(symbol: str) -> dict[str, Any] | None:
    """
    Удобная функция для получения полных данных тикера
    
    Args:
        symbol: Символ торговой пары
        
    Returns:
        Словарь с данными тикера или None, если данные не найдены
    """
    return _get_ticker_manager().get_ticker_data(symbol)


def debug_ticker_data(symbol: str) -> None:
    """
    Удобная функция для отладки данных тикера
    
    Args:
        symbol: Символ торговой пары
    """
    return _get_ticker_manager().debug_ticker_data(symbol)
