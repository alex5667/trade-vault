"""
Unit тесты для time_utils.py
"""

import pytest
import time
from datetime import datetime
from time_utils import (
    get_current_timestamp_ms,
    format_timestamp_for_redis,
    extract_event_timestamp,
    extract_binance_close_time,
    parse_interval_to_ms,
    timestamp_to_iso,
    timestamp_to_human,
    validate_timestamp,
    create_redis_stream_fields
)


class TestTimestampBasics:
    """Тесты базовых функций для работы с timestamp"""
    
    def test_get_current_timestamp_ms(self):
        """Тест получения текущего времени в миллисекундах"""
        ts = get_current_timestamp_ms()
        
        # Проверяем, что это действительно миллисекунды
        assert ts > 1600000000000  # После 2020
        assert ts < 2000000000000  # До 2033
        
        # Проверяем, что это близко к time.time() * 1000
        current = int(time.time() * 1000)
        assert abs(ts - current) < 100  # Разница меньше 100ms
    
    def test_format_timestamp_for_redis(self):
        """Тест форматирования timestamp для Redis"""
        ts = 1697366459999
        formatted = format_timestamp_for_redis(ts)
        
        assert isinstance(formatted, str)
        assert formatted == "1697366459999"
        assert formatted.isdigit()
        assert len(formatted) == 13
    
    def test_validate_timestamp(self):
        """Тест валидации timestamp"""
        # Валидные timestamps
        assert validate_timestamp(1697366459999) is True
        assert validate_timestamp("1697366459999") is True
        
        # Невалидные timestamps
        assert validate_timestamp(123) is False  # Слишком маленький
        assert validate_timestamp("abc") is False  # Не число
        assert validate_timestamp(None) is False


class TestExtractEventTimestamp:
    """Тесты извлечения timestamp из данных"""
    
    def test_extract_from_dict_int(self):
        """Тест извлечения int timestamp"""
        data = {'closeTime': 1697366459999, 'symbol': 'BTCUSDT'}
        ts = extract_event_timestamp(data, 'closeTime')
        assert ts == 1697366459999
    
    def test_extract_from_dict_string(self):
        """Тест извлечения string timestamp"""
        data = {'closeTime': '1697366459999', 'symbol': 'BTCUSDT'}
        ts = extract_event_timestamp(data, 'closeTime')
        assert ts == 1697366459999
    
    def test_extract_from_dict_float(self):
        """Тест извлечения float timestamp"""
        data = {'closeTime': 1697366459999.0, 'symbol': 'BTCUSDT'}
        ts = extract_event_timestamp(data, 'closeTime')
        assert ts == 1697366459999
    
    def test_extract_missing_field_no_fallback(self):
        """Тест извлечения отсутствующего поля без fallback"""
        data = {'symbol': 'BTCUSDT'}
        ts = extract_event_timestamp(data, 'closeTime', fallback_to_now=False)
        assert ts == 0
    
    def test_extract_missing_field_with_fallback(self):
        """Тест извлечения отсутствующего поля с fallback"""
        data = {'symbol': 'BTCUSDT'}
        ts = extract_event_timestamp(data, 'closeTime', fallback_to_now=True)
        assert ts > 1600000000000
        assert ts < 2000000000000


class TestExtractBinanceCloseTime:
    """Тесты извлечения closeTime из данных Binance"""
    
    def test_extract_from_websocket_format(self):
        """Тест извлечения из формата WebSocket (поле T)"""
        candle = {'T': 1697366459999, 'symbol': 'BTCUSDT'}
        ts = extract_binance_close_time(candle)
        assert ts == 1697366459999
    
    def test_extract_from_rest_format(self):
        """Тест извлечения из формата REST API (поле closeTime)"""
        candle = {'closeTime': 1697366459999, 'symbol': 'BTCUSDT'}
        ts = extract_binance_close_time(candle)
        assert ts == 1697366459999
    
    def test_calculate_from_open_time(self):
        """Тест расчета closeTime из openTime + interval"""
        candle = {
            't': 1697366400000,  # openTime
            'i': '1m',
            'symbol': 'BTCUSDT'
        }
        ts = extract_binance_close_time(candle)
        assert ts == 1697366400000 + 60000  # openTime + 1 minute


class TestParseInterval:
    """Тесты парсинга интервалов"""
    
    def test_parse_1m(self):
        """Тест парсинга 1 минуты"""
        ms = parse_interval_to_ms('1m')
        assert ms == 60 * 1000
    
    def test_parse_5m(self):
        """Тест парсинга 5 минут"""
        ms = parse_interval_to_ms('5m')
        assert ms == 5 * 60 * 1000
    
    def test_parse_1h(self):
        """Тест парсинга 1 часа"""
        ms = parse_interval_to_ms('1h')
        assert ms == 60 * 60 * 1000
    
    def test_parse_1d(self):
        """Тест парсинга 1 дня"""
        ms = parse_interval_to_ms('1d')
        assert ms == 24 * 60 * 60 * 1000
    
    def test_parse_unknown_defaults_to_1m(self):
        """Тест парсинга неизвестного интервала (по умолчанию 1m)"""
        ms = parse_interval_to_ms('unknown')
        assert ms == 60 * 1000


class TestTimestampConversion:
    """Тесты конвертации timestamp"""
    
    def test_timestamp_to_iso(self):
        """Тест конвертации в ISO 8601"""
        ts = 1697366459999
        iso = timestamp_to_iso(ts)
        
        assert isinstance(iso, str)
        assert '2023-10-15' in iso
        assert 'T' in iso
        assert '+00:00' in iso or 'Z' in iso
    
    def test_timestamp_to_human(self):
        """Тест конвертации в человекочитаемый формат"""
        ts = 1697366459999
        human = timestamp_to_human(ts)
        
        assert isinstance(human, str)
        assert '2023-10-15' in human
        assert '12:34:19' in human


class TestCreateRedisStreamFields:
    """Тесты создания полей для Redis Stream"""
    
    def test_create_with_event_time(self):
        """Тест создания с временем события"""
        candle = {
            'closeTime': 1697366459999,
            'symbol': 'BTCUSDT',
            'close': '28500',
            'volume': '123.45'
        }
        
        fields = create_redis_stream_fields(candle, use_event_time=True)
        
        assert 'timestamp' in fields
        assert fields['timestamp'] == '1697366459999'
        assert fields['symbol'] == 'BTCUSDT'
        assert fields['close'] == '28500'
    
    def test_create_with_current_time(self):
        """Тест создания с текущим временем"""
        data = {'symbol': 'BTCUSDT', 'action': 'added'}
        
        fields = create_redis_stream_fields(
            data, 
            use_event_time=False,
            event_time_field='closeTime'
        )
        
        assert 'timestamp' in fields
        ts = int(fields['timestamp'])
        assert ts > 1600000000000
        assert ts < 2000000000000
    
    def test_all_values_are_strings(self):
        """Тест что все значения конвертированы в строки"""
        data = {
            'symbol': 'BTCUSDT',
            'count': 100,
            'active': True,
            'tags': ['crypto', 'futures'],
            'meta': {'exchange': 'binance'}
        }
        
        fields = create_redis_stream_fields(data, use_event_time=False)
        
        for key, value in fields.items():
            assert isinstance(value, str), f"Field {key} is not string: {type(value)}"


class TestEdgeCases:
    """Тесты граничных случаев"""
    
    def test_extract_with_none_value(self):
        """Тест извлечения когда значение None"""
        data = {'closeTime': None, 'symbol': 'BTCUSDT'}
        ts = extract_event_timestamp(data, 'closeTime')
        assert ts == 0
    
    def test_extract_with_invalid_string(self):
        """Тест извлечения с невалидной строкой"""
        data = {'closeTime': 'invalid', 'symbol': 'BTCUSDT'}
        ts = extract_event_timestamp(data, 'closeTime')
        assert ts == 0
    
    def test_create_fields_with_none_values(self):
        """Тест создания полей с None значениями"""
        data = {'symbol': 'BTCUSDT', 'leverage': None}
        
        fields = create_redis_stream_fields(data, use_event_time=False)
        
        # None значения не должны попадать в fields
        assert 'leverage' not in fields or fields['leverage'] == ''


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

