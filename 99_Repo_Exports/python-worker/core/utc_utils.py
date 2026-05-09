from utils.time_utils import get_ny_time_millis

"""
UTC Utilities - Утилиты для работы с UTC временем

ВАЖНО: ВСЕ временные метки в проекте должны быть в UTC!

Используйте функции из этого модуля вместо:
- datetime.now() → utc_now()
- datetime.fromtimestamp() → utc_from_timestamp()
- time.strftime() → utc_strftime()
"""

import time
from datetime import UTC, datetime, timezone


def utc_now() -> datetime:
    """
    Возвращает текущее время в UTC.
    
    Returns:
        datetime с timezone=UTC
        
    Examples:
        >>> now = utc_now()
        >>> print(now.isoformat())
        '2025-11-01T12:34:56.789123+00:00'
    """
    return datetime.now(timezone.utc)


def utc_timestamp_ms() -> int:
    """
    Возвращает текущий UTC timestamp в миллисекундах.
    
    Returns:
        int timestamp в мс
        
    Examples:
        >>> ts = utc_timestamp_ms()
        >>> print(ts)
        1730476896789
    """
    return get_ny_time_millis()


def utc_timestamp_sec() -> int:
    """
    Возвращает текущий UTC timestamp в секундах.
    
    Returns:
        int timestamp в секундах
        
    Examples:
        >>> ts = utc_timestamp_sec()
        >>> print(ts)
        1730476896
    """
    return int(time.time())


def utc_from_timestamp(timestamp_sec: float) -> datetime:
    """
    Преобразует Unix timestamp (секунды) в UTC datetime.
    
    Args:
        timestamp_sec: Timestamp в секундах
        
    Returns:
        datetime с timezone=UTC
        
    Examples:
        >>> dt = utc_from_timestamp(1730476896)
        >>> print(dt.isoformat())
        '2025-11-01T12:34:56+00:00'
    """
    return datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)


def utc_from_timestamp_ms(timestamp_ms: int) -> datetime:
    """
    Преобразует Unix timestamp (миллисекунды) в UTC datetime.
    
    Args:
        timestamp_ms: Timestamp в миллисекундах
        
    Returns:
        datetime с timezone=UTC
        
    Examples:
        >>> dt = utc_from_timestamp_ms(1730476896789)
        >>> print(dt.isoformat())
        '2025-11-01T12:34:56.789000+00:00'
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def utc_strftime(fmt: str = '%Y-%m-%d %H:%M:%S UTC') -> str:
    """
    Форматирует текущее UTC время в строку.
    
    Args:
        fmt: Формат строки (по умолчанию с UTC суффиксом)
        
    Returns:
        Отформатированная строка времени
        
    Examples:
        >>> s = utc_strftime()
        >>> print(s)
        '2025-11-01 12:34:56 UTC'
        
        >>> s = utc_strftime('%H:%M:%S %d.%m.%Y UTC')
        >>> print(s)
        '12:34:56 01.11.2025 UTC'
    """
    return utc_now().strftime(fmt)


def utc_isoformat(timestamp_ms: int | None = None) -> str:
    """
    Возвращает ISO формат времени в UTC.
    
    Args:
        timestamp_ms: Timestamp в мс (если None, используется текущее время)
        
    Returns:
        ISO формат строки с UTC timezone
        
    Examples:
        >>> s = utc_isoformat()
        >>> print(s)
        '2025-11-01T12:34:56.789123+00:00'
        
        >>> s = utc_isoformat(1730476896789)
        >>> print(s)
        '2025-11-01T12:34:56.789000+00:00'
    """
    if timestamp_ms is None:
        return utc_now().isoformat()
    return utc_from_timestamp_ms(timestamp_ms).isoformat()


def format_timestamp_for_log(timestamp_ms: int) -> str:
    """
    Форматирует timestamp для логов в читаемом виде (UTC).
    
    Args:
        timestamp_ms: Timestamp в миллисекундах
        
    Returns:
        Строка формата: "2025-11-01 12:34:56 UTC"
        
    Examples:
        >>> s = format_timestamp_for_log(1730476896789)
        >>> print(s)
        '2025-11-01 12:34:56 UTC'
    """
    dt = utc_from_timestamp_ms(timestamp_ms)
    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')


# Константы timezone
UTC = UTC


# Aliases для обратной совместимости
now_utc = utc_now
timestamp_ms_utc = utc_timestamp_ms
timestamp_sec_utc = utc_timestamp_sec


if __name__ == "__main__":
    """Демонстрация использования"""
    print("UTC Utilities Demo")
    print("=" * 60)

    print(f"Current UTC time: {utc_now()}")
    print(f"Current UTC timestamp (ms): {utc_timestamp_ms()}")
    print(f"Current UTC timestamp (sec): {utc_timestamp_sec()}")
    print(f"Formatted: {utc_strftime()}")
    print(f"ISO format: {utc_isoformat()}")

    print()
    print("Conversion examples:")
    ts_ms = 1730476896789
    print(f"Timestamp {ts_ms} ms:")
    print(f"  → datetime: {utc_from_timestamp_ms(ts_ms)}")
    print(f"  → ISO: {utc_isoformat(ts_ms)}")
    print(f"  → Log format: {format_timestamp_for_log(ts_ms)}")

