"""
Утилиты для работы с временными метками в едином формате.

Стандарт проекта: Unix timestamp в миллисекундах (UTC)
"""

from typing import Optional, Union, Any, Dict
import time
from datetime import datetime, timezone


def get_current_timestamp_ms() -> int:
    """
    Возвращает текущее время в Unix timestamp в миллисекундах (UTC).
    
    Returns:
        int: Текущее время в миллисекундах с 1970-01-01 00:00:00 UTC
        
    Example:
        >>> ts = get_current_timestamp_ms()
        >>> print(ts)
        1697366459999
    """
    return int(time.time() * 1000)


def format_timestamp_for_redis(ts: int) -> str:
    """
    Форматирует timestamp для записи в Redis.
    
    Args:
        ts: Unix timestamp в миллисекундах
        
    Returns:
        str: Строковое представление timestamp
        
    Example:
        >>> formatted = format_timestamp_for_redis(1697366459999)
        >>> print(formatted)
        '1697366459999'
    """
    return str(ts)


def extract_event_timestamp(
    data: Dict[str, Any], 
    field: str,
    fallback_to_now: bool = False
) -> int:
    """
    Извлекает timestamp события из данных.
    
    Args:
        data: Словарь с данными
        field: Имя поля с timestamp (например, 'closeTime', 'timestamp')
        fallback_to_now: Если True и timestamp не найден, вернет текущее время.
                        По умолчанию False - вернет 0.
        
    Returns:
        int: Unix timestamp в миллисекундах или 0 если не найден
        
    Note:
        По умолчанию НЕ использует текущее время как fallback!
        Используйте fallback_to_now=True только для служебных событий.
        
    Example:
        >>> candle_data = {'closeTime': 1697366459999, 'symbol': 'BTCUSDT'}
        >>> ts = extract_event_timestamp(candle_data, 'closeTime')
        >>> print(ts)
        1697366459999
        
        >>> # С fallback
        >>> empty_data = {}
        >>> ts = extract_event_timestamp(empty_data, 'closeTime', fallback_to_now=True)
        >>> print(ts > 0)
        True
    """
    value = data.get(field)
    
    if value is None:
        return get_current_timestamp_ms() if fallback_to_now else 0
    
    # Обработка различных типов
    if isinstance(value, (int, float)):
        return int(value)
    
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
    
    # Если не удалось извлечь
    return get_current_timestamp_ms() if fallback_to_now else 0


def extract_binance_close_time(candle_data: Dict[str, Any]) -> int:
    """
    Извлекает closeTime из данных свечи Binance (поддерживает разные форматы).
    
    Args:
        candle_data: Данные свечи от Binance
        
    Returns:
        int: Unix timestamp в миллисекундах
        
    Example:
        >>> candle = {'T': 1697366459999, 'symbol': 'BTCUSDT'}
        >>> ts = extract_binance_close_time(candle)
        >>> print(ts)
        1697366459999
    """
    # Binance использует разные поля в разных API
    for field in ['closeTime', 'T', 'close_time']:
        if field in candle_data:
            return extract_event_timestamp(candle_data, field)
    
    # Если нет closeTime, пробуем openTime + interval
    if 'openTime' in candle_data or 't' in candle_data:
        open_time = extract_event_timestamp(candle_data, 'openTime') or extract_event_timestamp(candle_data, 't')
        if open_time:
            # Определяем interval и добавляем к openTime
            interval = candle_data.get('i', '1m')
            interval_ms = parse_interval_to_ms(interval)
            return open_time + interval_ms
    
    return 0


def parse_interval_to_ms(interval: str) -> int:
    """
    Конвертирует строковый interval ('1m', '5m', '1h', etc.) в миллисекунды.
    
    Args:
        interval: Интервал в формате Binance ('1m', '5m', '15m', '1h', '4h', '1d', etc.)
        
    Returns:
        int: Количество миллисекунд
        
    Example:
        >>> ms = parse_interval_to_ms('1m')
        >>> print(ms)
        60000
    """
    intervals = {
        '1m': 60 * 1000,
        '3m': 3 * 60 * 1000,
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '30m': 30 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '2h': 2 * 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '6h': 6 * 60 * 60 * 1000,
        '8h': 8 * 60 * 60 * 1000,
        '12h': 12 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000,
        '3d': 3 * 24 * 60 * 60 * 1000,
        '1w': 7 * 24 * 60 * 60 * 1000,
        '1M': 30 * 24 * 60 * 60 * 1000,  # Приблизительно
    }
    return intervals.get(interval, 60 * 1000)  # По умолчанию 1 минута


def timestamp_to_iso(ts_ms: int) -> str:
    """
    Конвертирует Unix timestamp в миллисекундах в ISO 8601 строку.
    Используется для логов и отображения (НЕ для хранения в Redis).
    
    Args:
        ts_ms: Unix timestamp в миллисекундах
        
    Returns:
        str: ISO 8601 строка с UTC timezone
        
    Example:
        >>> iso = timestamp_to_iso(1697366459999)
        >>> print(iso)
        '2023-10-15T12:34:19.999000+00:00'
    """
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.isoformat()


def timestamp_to_human(ts_ms: int, format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
    """
    Конвертирует Unix timestamp в человекочитаемую строку.
    Используется для отображения пользователю (НЕ для хранения).
    
    Args:
        ts_ms: Unix timestamp в миллисекундах
        format_str: Формат строки (strftime)
        
    Returns:
        str: Форматированная строка
        
    Example:
        >>> human = timestamp_to_human(1697366459999)
        >>> print(human)
        '2023-10-15 12:34:19'
    """
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime(format_str)


def validate_timestamp(ts: Union[int, str]) -> bool:
    """
    Валидирует timestamp (проверяет, что это разумное значение).
    
    Args:
        ts: Timestamp для проверки
        
    Returns:
        bool: True если timestamp валидный
        
    Example:
        >>> validate_timestamp(1697366459999)
        True
        >>> validate_timestamp(123)  # Слишком маленький
        False
    """
    try:
        if isinstance(ts, str):
            ts = int(ts)
        
        # Проверяем диапазон (после 2020 и до 2033)
        return 1600000000000 < ts < 2000000000000
    except (ValueError, TypeError):
        return False


def create_redis_stream_fields(
    data: Dict[str, Any],
    timestamp_field: str = 'timestamp',
    use_event_time: bool = True,
    event_time_field: str = 'closeTime'
) -> Dict[str, str]:
    """
    Создает словарь полей для Redis Stream с правильным timestamp.
    
    Args:
        data: Исходные данные
        timestamp_field: Имя поля для timestamp в результате
        use_event_time: Использовать время события (True) или текущее (False)
        event_time_field: Имя поля с временем события в data
        
    Returns:
        Dict[str, str]: Словарь с полями для XAdd, все значения - строки
        
    Example:
        >>> candle = {'closeTime': 1697366459999, 'symbol': 'BTCUSDT', 'close': '28500'}
        >>> fields = create_redis_stream_fields(candle)
        >>> print(fields['timestamp'])
        '1697366459999'
    """
    # Определяем timestamp
    if use_event_time:
        ts = extract_event_timestamp(data, event_time_field, fallback_to_now=True)
    else:
        ts = get_current_timestamp_ms()
    
    # Создаем поля
    fields = {timestamp_field: format_timestamp_for_redis(ts)}
    
    # Добавляем остальные данные, конвертируя в строки
    for key, value in data.items():
        if value is not None and key != timestamp_field:
            if isinstance(value, bool):
                fields[key] = 'true' if value else 'false'
            elif isinstance(value, (list, dict)):
                import json
                fields[key] = json.dumps(value)
            else:
                fields[key] = str(value)
    
    return fields


# Алиасы для удобства
get_utc_timestamp_ms = get_current_timestamp_ms
format_ts = format_timestamp_for_redis
extract_ts = extract_event_timestamp

