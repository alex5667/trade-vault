from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest

from handlers.message_handler import MessageHandler


class DummyBackoff:
    def next_sleep(self) -> float:
        return 0.0


class HM:
    def __init__(self):
        self.lags = []

    def on_stream_lag(self, symbol: str, stream_kind: str, lag_ms: int) -> None:
        self.lags.append((symbol, stream_kind, int(lag_ms)))


def test_message_handler_records_stream_lags(monkeypatch):
    # фиксируем now_ms = 2000
    import handlers.message_handler as mh_mod
    monkeypatch.setattr(mh_mod.time, "time", lambda: 2.0)

    h = MessageHandler.__new__(MessageHandler)
    h.symbol = "BTCUSDT"
    h.tick_stream = "ticks:BTCUSDT"
    h.book_stream = "book:BTCUSDT"
    h.l3_stream = "l3:BTCUSDT"

    h._priority = lambda s: 0 if s == h.book_stream else (1 if s == h.l3_stream else 2)

    calls = []

    class DP:
        def _process_tick(self, t): calls.append("tick")
        def _process_book(self, b): calls.append("book")

    h.data_processor = DP()
    h._process_l3_event = lambda ev: calls.append("l3")

    # tick.ts=1000 => lag=1000
    # book.ts_ms=1500 => lag=500
    # l3.ts_ms=1800 => lag=200
    h.data_parser = SimpleNamespace(
        _parse_tick=lambda f: SimpleNamespace(ts=1000, last=1.0, is_buyer_maker=False, volume=1.0),
        _parse_book=lambda f: {"ts_ms": 1500, "snapshot": "x"},
        _parse_l3_event=lambda f: {"ts_ms": 1800},
    )

    h.health_metrics = HM()

    h.logger = MagicMock()
    h.max_fail_retries = 3
    h._is_transient_error = lambda e: False
    h._try_add_dlq_or_backoff = lambda *args, **kwargs: True

    consumer = SimpleNamespace(ack=MagicMock())
    backoff = DummyBackoff()
    fail_counts = {}

    msgs = [
        SimpleNamespace(stream=h.tick_stream, msg_id="1-0", fields={}),
        SimpleNamespace(stream=h.book_stream, msg_id="2-0", fields={}),
        SimpleNamespace(stream=h.l3_stream, msg_id="3-0", fields={}),
    ]

    tick_cnt, book_cnt, all_success = h.process_message_batch(
        msgs=msgs, backoff=backoff, fail_counts=fail_counts, consumer=consumer
    )

    assert all_success is True
    assert calls == ["book", "l3", "tick"]  # приоритет

    assert ("BTCUSDT", "book", 500) in h.health_metrics.lags
    assert ("BTCUSDT", "l3", 200) in h.health_metrics.lags
    assert ("BTCUSDT", "ticks", 1000) in h.health_metrics.lags
