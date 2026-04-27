#!/usr/bin/env python3
"""
Ticker Data Handler — обработчик данных тикеров (24ч) из Redis Streams.

Назначение:
- Принимать пакет тикеров, сохранять их в Redis с TTL.
- Извлекать перечень символов для актуализации WebSocket-подписок.
- Предоставлять методы получения информации о конкретном тикере и всех тикерах из Redis.
"""

import json
import sys
from typing import Callable, List, Dict, Any


class TickerDataHandler:
    """Обработчик данных тикеров (24ч)."""
    
    def __init__(self, redis_client, ws_callback: Callable[[list], None]):
        """
        Args:
            redis_client: Клиент Redis для сохранения и чтения тикеров
            ws_callback: Колбэк для пересоздания/обновления WS-подписок по списку символов
        """
        self.redis_client = redis_client
        self.ws_callback = ws_callback
    
    def handle_ticker_stream_data(self, data) -> None:
        """
        Обрабатывает входные данные тикеров из стрима: сохраняет и обновляет WS-символы.
        
        Args:
            data: Данные тикера (list | str | dict)
        """
        try:
            # Извлекаем данные тикеров
            tickers_data = self._extract_ticker_data(data)
            
            if not tickers_data:
                print("⚠️ Нет данных тикеров для обработки")
                return
                
            print(f"📈 TickerHandler: Получены данные тикеров: {len(tickers_data)} записей")
            sys.stdout.flush()
            
            # Сохраняем данные в Redis для других компонентов
            self._save_ticker_data(tickers_data)
            
            # Извлекаем символы для WebSocket подключений
            symbols = self._extract_symbols(tickers_data)
            
            # Отправляем символы для подключения WebSocket
            if symbols and self.ws_callback:
                # Закомментировано для уменьшения шума в логах
                # print(f"📡 TickerHandler: Отправка {len(symbols)} символов для WebSocket")
                # sys.stdout.flush()
                self.ws_callback(symbols)
                
        except Exception as e:
            print(f"❌ TickerHandler: Ошибка обработки тикеров: {e}")
            sys.stdout.flush()
    
    def _extract_ticker_data(self, data) -> List[Dict]:
        """
        Унифицирует входной формат данных тикеров в список словарей.
        
        Args:
            data: Данные в различных форматах
            
        Returns:
            List[Dict]: Список данных тикеров
        """
        tickers_data = []
        
        print(f"🔍 TickerHandler: Обрабатываем данные типа {type(data)}")
        
        if isinstance(data, list):
            # Если data уже список тикеров, проверяем что все элементы - словари
            print(f"🔍 TickerHandler: Получен список из {len(data)} элементов")
            for item in data:
                if isinstance(item, dict):
                    tickers_data.append(item)
                elif isinstance(item, str):
                    try:
                        parsed_item = json.loads(item)
                        if isinstance(parsed_item, dict):
                            tickers_data.append(parsed_item)
                        else:
                            print(f"⚠️ Элемент списка не является словарем после парсинга JSON: {type(parsed_item)}")
                    except json.JSONDecodeError:
                        print(f"⚠️ Не удалось распарсить JSON элемент списка: {item}")
                else:
                    print(f"⚠️ Пропускаем элемент списка неверного типа: {type(item)}")
        elif isinstance(data, str):
            # Если data строка JSON
            print(f"🔍 TickerHandler: Получена строка JSON длиной {len(data)} символов")
            try:
                parsed_data = json.loads(data)
                if isinstance(parsed_data, list):
                    # Рекурсивно обрабатываем список
                    tickers_data = self._extract_ticker_data(parsed_data)
                elif isinstance(parsed_data, dict):
                    tickers_data = [parsed_data]
                else:
                    print(f"⚠️ Распарсенный JSON не является списком или словарем: {type(parsed_data)}")
            except json.JSONDecodeError as e:
                print(f"⚠️ Ошибка парсинга JSON строки: {e}")
        elif isinstance(data, dict):
            # Если data словарь, ищем ключ 'tickers' или используем весь словарь
            print(f"🔍 TickerHandler: Получен словарь с ключами: {list(data.keys())}")
            tickers_data = data.get('tickers', [data] if data else [])
        else:
            print(f"⚠️ Неподдерживаемый тип данных: {type(data)}")
        
        print(f"🔍 TickerHandler: Извлечено {len(tickers_data)} тикеров")
        return tickers_data
    
    def _save_ticker_data(self, tickers_data: List[Dict]) -> None:
        """
        Сохраняет данные тикеров в Redis с TTL.
        
        Args:
            tickers_data: Список данных тикеров
        """
        try:
            if not isinstance(tickers_data, list):
                print(f"⚠️ _save_ticker_data: tickers_data не является списком: {type(tickers_data)}")
                return
                
            print(f"💾 TickerHandler: Сохраняем {len(tickers_data)} тикеров в Redis")
            
            for ticker in tickers_data:
                if not isinstance(ticker, dict):
                    print(f"⚠️ Пропускаем элемент тикера неверного типа: {type(ticker)}")
                    continue
                
                # Валидируем обязательные поля тикера
                if not self._validate_ticker_for_saving(ticker):
                    print(f"⚠️ Пропускаем невалидный тикер: {ticker.get('symbol', 'unknown')}")
                    continue
                    
                symbol = self._extract_symbol_from_ticker(ticker)
                
                if symbol:
                    key = f"binance:ticker24h:{symbol}"
                    value = json.dumps(ticker)
                    self.redis_client.setex(key, 3600, value)  # TTL 1 час
                    # Закомментировано для уменьшения шума в логах
                    # print(f"💾 TickerHandler: Сохранен тикер для {symbol}")
                else:
                    print(f"⚠️ TickerHandler: Не удалось извлечь символ из тикера: {ticker}")
                    
        except Exception as e:
            print(f"❌ TickerHandler: Ошибка сохранения тикеров: {e}")
            sys.stdout.flush()
    
    def _validate_ticker_for_saving(self, ticker: Dict) -> bool:
        """
        Валидирует тикер перед сохранением в Redis
        
        Args:
            ticker: Данные тикера для валидации
            
        Returns:
            bool: True если тикер валиден для сохранения
        """
        try:
            # Проверяем обязательные поля
            required_fields = ['symbol', 'lastPrice', 'volume']
            for field in required_fields:
                if field not in ticker:
                    print(f"⚠️ Тicker не содержит обязательное поле '{field}'")
                    return False
            
            # Проверяем, что symbol не пустой
            if not ticker['symbol']:
                print(f"⚠️ Тicker имеет пустой символ")
                return False
            
            # Проверяем, что числовые поля можно преобразовать в float
            try:
                float(ticker['lastPrice'])
                float(ticker['volume'])
            except (ValueError, TypeError):
                print(f"⚠️ Тicker содержит невалидные числовые значения")
                return False
            
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка валидации тикера: {e}")
            return False
    
    def _extract_symbol_from_ticker(self, ticker) -> str:
        """
        Извлекает символ из данных тикера (поддержка dict/JSON-строки).
        
        Args:
            ticker: Данные тикера
            
        Returns:
            str: Символ или пустая строка
        """
        symbol = ''
        
        try:
            if isinstance(ticker, dict):
                symbol = ticker.get('symbol', '')
            elif isinstance(ticker, str):
                try:
                    ticker_dict = json.loads(ticker)
                    if isinstance(ticker_dict, dict):
                        symbol = ticker_dict.get('symbol', '')
                    else:
                        print(f"⚠️ Распарсенный JSON элемент не является словарем: {type(ticker_dict)}")
                except json.JSONDecodeError as e:
                    print(f"⚠️ Не удалось распарсить JSON элемент: {e}")
            else:
                print(f"⚠️ Неподдерживаемый тип элемента тикера: {type(ticker)}")
        except Exception as e:
            print(f"⚠️ Ошибка при извлечении символа из тикера: {e}")
        
        return symbol
    
    def _extract_symbols(self, data: List[Dict]) -> List[str]:
        """
        Извлекает символы из данных тикеров для WebSocket-подписок.
        Фильтрует только USDT пары, исключая UP/DOWN-токены.
        
        Args:
            data: Список данных тикеров
            
        Returns:
            List[str]: Список символов для подключения WebSocket
        """
        symbols = []
        
        if not isinstance(data, list):
            print(f"⚠️ _extract_symbols: data не является списком: {type(data)}")
            return symbols
        
        for ticker in data:
            if not isinstance(ticker, dict):
                print(f"⚠️ Пропускаем элемент неверного типа в _extract_symbols: {type(ticker)}")
                continue
                
            symbol = self._extract_symbol_from_ticker(ticker)
            
            # Фильтруем только USDT пары, исключаем UP/DOWN токены
            if (symbol.endswith('USDT') and 
                'UP' not in symbol and 
                'DOWN' not in symbol):
                symbols.append(symbol)
        
        return symbols
    
    def validate_ticker_data(self, data: Dict) -> bool:
        """
        Базовая валидация структуры тикера.
        
        Args:
            data: Данные тикера
            
        Returns:
            bool: True если данные валидны
        """
        if not isinstance(data, dict):
            return False
        
        # Проверяем обязательные поля
        required_fields = ['symbol']
        for field in required_fields:
            if field not in data:
                return False
        
        return True
    
    def get_ticker_info(self, symbol: str) -> Dict:
        """
        Получает информацию о тикере из Redis по ключу.
        
        Args:
            symbol: Символ торговой пары
            
        Returns:
            Dict: Данные тикера или пустой словарь
        """
        try:
            key = f"binance:ticker24h:{symbol}"
            data = self.redis_client.get(key)
            
            if data:
                return json.loads(data)
            else:
                return {}
                
        except Exception as e:
            print(f"❌ TickerHandler: Ошибка получения данных тикера для {symbol}: {e}")
            return {}
    
    def get_all_tickers(self) -> List[Dict]:
        """
        Возвращает все тикеры из Redis по паттерну `binance:ticker24h:*`.
        
        Returns:
            List[Dict]: Список всех тикеров
        """
        try:
            pattern = "binance:ticker24h:*"
            tickers = []
            cursor = 0
            
            # Используем SCAN вместо keys для совместимости с Redis
            while True:
                result = self.redis_client.scan(cursor, match=pattern, count=1000)
                cursor, keys = result
                
                for key in keys:
                    data = self.redis_client.get(key)
                    if data:
                        tickers.append(json.loads(data))
                
                if cursor == 0:
                    break
            
            return tickers
            
        except Exception as e:
            print(f"❌ TickerHandler: Ошибка получения всех тикеров: {e}")
            return [] 