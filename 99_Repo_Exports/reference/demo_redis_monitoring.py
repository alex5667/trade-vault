#!/usr/bin/env python3
"""
Demo script for Redis Stream Monitoring functionality.

Shows how the Redis stream monitoring system works:
- pending_len() method
- HealthMetrics lag/pending aggregation
- Priority processing in MessageHandler/MainLoopService
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from core.redis_stream_consumer import SyncRedisStreamHelper, _parse_xpending_summary
from health_metrics import HealthMetrics
from handlers.message_handler import MessageHandler
from handlers.main_loop_service import MainLoopService


def demo_parse_xpending():
    """Demo XPENDING parsing functionality."""
    print("🔍 Testing XPENDING parsing...")

    # Test dict format (common)
    result = _parse_xpending_summary({"pending": 42, "min": "0-0", "max": "1-0"})
    assert result == 42
    print("  ✓ Dict format: 42 pending messages")

    # Test tuple format (older redis-py)
    result = _parse_xpending_summary((15, "0-0", "1-0", []))
    assert result == 15
    print("  ✓ Tuple format: 15 pending messages")

    # Test error cases
    result = _parse_xpending_summary(None)
    assert result == 0
    print("  ✓ None/error handling: 0 pending messages")


def demo_pending_len():
    """Demo pending_len() method with mock Redis."""
    print("\n📊 Testing pending_len() method...")

    class MockRedis:
        def __init__(self, response):
            self.response = response

        def xpending(self, stream, group):
            return self.response

    # Success case
    redis = MockRedis({"pending": 25})
    consumer = SyncRedisStreamHelper(redis, "test-group", "test-consumer")
    result = consumer.pending_len("book:BTCUSDT")
    assert result == 25
    print("  ✓ Success case: 25 pending messages")

    # NOGROUP case (consumer group not created yet)
    from redis.exceptions import ResponseError
    redis = MockRedis.__new__(MockRedis)
    redis.xpending = MagicMock(side_effect=ResponseError("NOGROUP No such key"))
    consumer = SyncRedisStreamHelper(redis, "test-group", "test-consumer")
    result = consumer.pending_len("book:BTCUSDT")
    assert result == 0
    print("  ✓ NOGROUP case: 0 pending messages (safe default)")


def demo_health_metrics():
    """Demo HealthMetrics lag and pending tracking."""
    print("\n📈 Testing HealthMetrics aggregation...")

    class MockPipeline:
        def __init__(self):
            self.calls = []

        def set(self, key, value, **kwargs):
            self.calls.append(("set", key, value))
            return self

        def hset(self, key, mapping):
            self.calls.append(("hset", key, mapping))
            return self

        def expire(self, key, ttl):
            self.calls.append(("expire", key, ttl))
            return self

        def execute(self):
            return []

    class MockRedis:
        def __init__(self):
            self.pipe = MockPipeline()

        def pipeline(self):
            return self.pipe

    hm = HealthMetrics(redis_url="redis://mock", window_sec=5)
    hm._redis = MockRedis()

    # Simulate stream lag measurements
    hm.on_stream_lag("BTCUSDT", "book", 100)
    hm.on_stream_lag("BTCUSDT", "book", 200)
    hm.on_stream_lag("BTCUSDT", "ticks", 50)

    # Simulate pending measurements (multiple calls for aggregation)
    hm.on_pending_len("BTCUSDT", "book", 10)
    hm.on_pending_len("BTCUSDT", "book", 10)  # avg will be 10
    hm.on_pending_len("BTCUSDT", "ticks", 5)
    hm.on_pending_len("BTCUSDT", "ticks", 5)  # avg will be 5

    # Flush and check what gets published
    hm._flush_snapshot()

    pipeline = hm._redis.pipe
    set_calls = [(k, v) for op, k, v in pipeline.calls if op == "set"]

    # Check lag metrics
    book_lag_key = next((v for k, v in set_calls if "book_lag_ms" in k), None)
    assert book_lag_key == 150.0  # (100+200)/2
    print("  ✓ Book lag avg: 150ms")

    ticks_lag_key = next((v for k, v in set_calls if "ticks_lag_ms" in k), None)
    assert ticks_lag_key == 50.0
    print("  ✓ Ticks lag avg: 50ms")

    # Check pending metrics (averages)
    book_pending_key = next((v for k, v in set_calls if "book_pending_avg" in k), None)
    assert abs(float(book_pending_key) - 10.0) < 1e-6
    print("  ✓ Book pending avg: 10.0 messages")

    ticks_pending_key = next((v for k, v in set_calls if "ticks_pending_avg" in k), None)
    assert abs(float(ticks_pending_key) - 5.0) < 1e-6
    print("  ✓ Ticks pending avg: 5.0 messages")


def demo_priority_processing():
    """Demo priority processing in MessageHandler."""
    print("\n⚡ Testing priority processing...")

    # Create MessageHandler instance
    h = MessageHandler.__new__(MessageHandler)
    h.symbol = "BTCUSDT"
    h.tick_stream = "ticks:BTCUSDT"
    h.book_stream = "book:BTCUSDT"
    h.l3_stream = "l3:BTCUSDT"

    # Priority function: book=0, l3=1, ticks=2
    h._priority = lambda s: 0 if s == h.book_stream else (1 if s == h.l3_stream else 2)

    # Track processing order
    calls = []

    class MockDataProcessor:
        def _process_book(self, book):
            calls.append("book")

        def _process_tick(self, tick):
            calls.append("tick")

    h.data_processor = MockDataProcessor()
    h._process_l3_event = lambda ev: calls.append("l3")

    # Mock parsers
    h.data_parser = SimpleNamespace(
        _parse_tick=lambda fields: SimpleNamespace(ts=int(time.time() * 1000) - 10, last=100.0, is_buyer_maker=False, volume=1.0),
        _parse_book=lambda fields: {"ts_ms": int(time.time() * 1000) - 20, "snapshot": "dummy"},
        _parse_l3_event=lambda fields: {"ts_ms": int(time.time() * 1000) - 30},
    )

    # Mock dependencies
    h.logger = MagicMock()
    h.max_fail_retries = 3
    h._is_transient_error = lambda e: False
    h._try_add_dlq_or_backoff = lambda *args, **kwargs: True

    consumer = SimpleNamespace(ack=MagicMock())
    backoff = SimpleNamespace(next_sleep=lambda: 0.0)
    fail_counts = {}

    # Input: wrong order (ticks, book, l3)
    # Expected output: correct order (book, l3, ticks)
    msgs = [
        SimpleNamespace(stream=h.tick_stream, msg_id="1-0", fields={"x": "1"}),
        SimpleNamespace(stream=h.book_stream, msg_id="2-0", fields={"x": "2"}),
        SimpleNamespace(stream=h.l3_stream, msg_id="3-0", fields={"x": "3"}),
    ]

    tick_cnt, book_cnt, all_success = h.process_message_batch(msgs, backoff, fail_counts, consumer)

    assert all_success is True
    assert book_cnt == 1
    assert tick_cnt == 1
    assert calls == ["book", "l3", "tick"], f"Expected ['book', 'l3', 'tick'], got {calls}"
    assert consumer.ack.call_count == 3

    print("  ✓ Priority processing: book → l3 → ticks")
    print("  ✓ ACK called for all messages")
    print("  ✓ Counters correct: book=1, ticks=1")


def main():
    """Run all demos."""
    print("🚀 Redis Stream Monitoring Demo")
    print("=" * 40)

    demo_parse_xpending()
    demo_pending_len()
    demo_health_metrics()
    demo_priority_processing()

    print("\n" + "=" * 40)
    print("✅ All Redis Stream Monitoring demos passed!")
    print("\n📖 See REDIS_STREAM_MONITORING.md for full documentation")


if __name__ == "__main__":
    main()
