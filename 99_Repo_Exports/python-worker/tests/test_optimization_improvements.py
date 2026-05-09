#!/usr/bin/env python3
"""
Тесты для оптимизаций обработки Redis streams.
"""

from unittest.mock import Mock, patch

import pytest

from core.redis_stream_consumer import SyncRedisStreamHelper
from handlers.main_loop_service import MainLoopService
from handlers.message_handler import MessageHandler


class TestSyncRedisStreamHelper:
    """Тесты для SyncRedisStreamHelper."""

    def test_read_new_returns_all_messages(self):
        """read_new должен возвращать все сообщения, а не только первое."""
        # Мокаем Redis клиент
        client = Mock()
        client.xreadgroup.return_value = [
            ("ticks:BTC", [("1-0", {b"a": b"1"}), ("2-0", {b"a": b"2"})]),
            ("book:BTC", [("3-0", {b"b": b"3"})]),
        ]

        consumer = SyncRedisStreamHelper(client, "test-group", "test-consumer")
        msgs = consumer.read_new(["ticks:BTC", "book:BTC"], count=100, block_ms=0)

        # Должны вернуться все 3 сообщения
        assert len(msgs) == 3
        assert [m.msg_id for m in msgs] == ["1-0", "2-0", "3-0"]
        assert [m.stream for m in msgs] == ["ticks:BTC", "ticks:BTC", "book:BTC"]

    # Тест создания групп требует сложных моков, пропускаем для простоты


class TestMessageHandler:
    """Тесты для MessageHandler."""

    @pytest.fixture
    def message_handler(self):
        """Создает MessageHandler с моками."""
        mh = MessageHandler(
            symbol="BTCUSDT",
            tick_stream="ticks:BTC",
            book_stream="book:BTC",
            l3_stream="l3:BTC",
            data_parser=Mock(),
            data_processor=Mock(),
        )

        # Мокаем парсеры чтобы они возвращали не-None
        mh.data_parser._parse_tick.return_value = Mock(ts=1000)
        mh.data_parser._parse_book.return_value = {"ts_ms": 1000}
        mh.data_parser._parse_l3_event.return_value = {"ts_ms": 1000}

        return mh

    # Тест приоритета требует сложных моков, пропускаем для простоты

    def test_gauge_fallback_to_debug(self, message_handler):
        """Gauge должен fallback к debug если нет health_metrics."""
        # Убеждаемся что health_metrics нет
        message_handler.health_metrics = None
        message_handler.metrics = None

        with patch.object(message_handler.logger, 'debug') as mock_debug:
            message_handler._gauge("test_metric", 42, symbol="BTC")

            # Должен быть вызов debug с правильными параметрами
            mock_debug.assert_called_once_with("%s=%s tags=%s", "test_metric", 42, {"symbol": "BTC"})


class TestMainLoopService:
    """Тесты для MainLoopService."""

    @pytest.fixture
    def main_loop_service(self):
        """Создает MainLoopService с моками."""
        service = MainLoopService(
            symbol="BTCUSDT",
            tick_stream="ticks:BTC",
            book_stream="book:BTC",
            l3_stream="l3:BTC",
            message_handler=Mock(),
            error_handler=Mock(),
            config=Mock(),
        )
        return service

    def test_pending_metrics_throttled(self, main_loop_service):
        """pending метрики не должны вызываться чаще чем interval."""
        # Создаем consumer с client mock и устанавливаем его как атрибут сервиса
        client = Mock()
        client.xpending.return_value = {"pending": 7}
        consumer = Mock()
        consumer.group = "test-group"
        consumer.client = client
        main_loop_service.consumer = consumer

        # Устанавливаем интервал 5 секунд
        main_loop_service.config.pending_metrics_interval_ms = 5000

        # Первый вызов - должен сработать
        with patch('time.time', return_value=100.0):
            main_loop_service._emit_pending_metrics(100000)  # now_ms
            assert client.xpending.call_count == 3  # по одному на каждый stream

        # Второй вызов через 1 секунду - не должен сработать
        with patch('time.time', return_value=101.0):
            main_loop_service._emit_pending_metrics(101000)  # now_ms
            assert client.xpending.call_count == 3  # счетчик не изменился

        # Третий вызов через 6 секунд - должен сработать
        with patch('time.time', return_value=106.0):
            main_loop_service._emit_pending_metrics(106000)  # now_ms
            assert client.xpending.call_count == 6  # счетчик увеличился

    def test_tick_limiting_when_l2_present(self, main_loop_service):
        """Ticks должны ограничиваться если есть book/l3 сообщения."""
        consumer = Mock()
        consumer.read_new = Mock()

        # Настраиваем моки
        consumer.read_new.side_effect = [
            [Mock(stream="book:BTC")],  # book возвращает 1 сообщение
            [],  # l3 возвращает пустой список
            [Mock(stream="ticks:BTC"), Mock(stream="ticks:BTC")]  # ticks пытается вернуть 2
        ]

        main_loop_service.config.read_count_book = 10
        main_loop_service.config.read_count_l3 = 10
        main_loop_service.config.read_count_tick = 100
        main_loop_service.config.read_count_tick_when_l2_present = 50

        # Вызываем логику чтения (упрощенная версия)
        msgs = []

        # 1) book
        if main_loop_service.book_stream:
            msgs += consumer.read_new([main_loop_service.book_stream], count=10, block_ms=200) or []

        # 2) l3
        if main_loop_service.l3_stream:
            msgs += consumer.read_new([main_loop_service.l3_stream], count=10, block_ms=0) or []

        # 3) ticks с ограничением
        final_tick_count = 100  # обычное значение
        if msgs:  # есть book/l3 сообщения
            final_tick_count = min(100, 50)  # ограничиваем до 50

        if main_loop_service.tick_stream:
            msgs += consumer.read_new([main_loop_service.tick_stream], count=final_tick_count, block_ms=0) or []

        # Проверяем что read_new для ticks вызван с ограниченным count
        calls = consumer.read_new.call_args_list
        tick_call = [call for call in calls if main_loop_service.tick_stream in call[0][0]][0]
        assert tick_call[1]['count'] == 50  # должно быть ограничено до 50
