from utils.time_utils import get_ny_time_millis
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# поправьте импорт под ваш проект
from handlers.message_handler import MessageHandler


class DummyBackoff:
    def next_sleep(self) -> float:
        return 0.0


def test_process_message_batch_prioritizes_book_then_l3_then_ticks():
    import time

    h = MessageHandler.__new__(MessageHandler)

    h.symbol = "BTCUSDT"
    h.tick_stream = "ticks:BTCUSDT"
    h.book_stream = "book:BTCUSDT"
    h.l3_stream = "l3:BTCUSDT"

    # приоритет (как в вашем коде): book=0, l3=1, ticks=2
    h._priority = lambda s: 0 if s == h.book_stream else (1 if s == h.l3_stream else 2)

    # зависимости
    calls = []

    class DP:
        def _process_book(self, book):
            calls.append("book")

        def _process_tick(self, tick):
            calls.append("tick")

    h.data_processor = DP()

    # l3 обрабатывается через handler._process_l3_event
    h._process_l3_event = lambda ev: calls.append("l3")

    # парсер: возвращаем валидные объекты
    h.data_parser = SimpleNamespace(
        _parse_tick=lambda fields: SimpleNamespace(
            ts=get_ny_time_millis() - 10, last=100.0, is_buyer_maker=False, volume=1.0
        ),
        _parse_book=lambda fields: {"ts_ms": get_ny_time_millis() - 20, "snapshot": "dummy"},
        _parse_l3_event=lambda fields: {"ts_ms": get_ny_time_millis() - 30},
    )

    # служебное
    h.logger = MagicMock()
    h.max_fail_retries = 3
    h._is_transient_error = lambda e: False
    h._try_add_dlq_or_backoff = lambda *args, **kwargs: True

    consumer = SimpleNamespace(ack=MagicMock())
    backoff = DummyBackoff()
    fail_counts = {}

    # входной порядок специально "неправильный": ticks, book, l3
    msgs = [
        SimpleNamespace(stream=h.tick_stream, msg_id="1-0", fields={"x": "1"}),
        SimpleNamespace(stream=h.book_stream, msg_id="2-0", fields={"x": "2"}),
        SimpleNamespace(stream=h.l3_stream, msg_id="3-0", fields={"x": "3"}),
    ]

    tick_cnt, book_cnt, all_success = h.process_message_batch(
        msgs=msgs,
        backoff=backoff,
        fail_counts=fail_counts,
        consumer=consumer,
    )

    assert all_success is True
    assert book_cnt == 1
    assert tick_cnt == 1
    assert calls == ["book", "l3", "tick"], "Должно быть book → l3 → ticks"
    assert consumer.ack.call_count == 3