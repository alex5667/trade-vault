import time
from dataclasses import dataclass

from common.metrics2 import InMemoryMetrics, normalize_ts_ms, should_drop_by_watermark


def test_normalize_ts_ms_seconds_to_ms():
    assert normalize_ts_ms(1_700_000_000) == 1_700_000_000 * 1000


def test_normalize_ts_ms_ms_passthrough():
    assert normalize_ts_ms(1_700_000_000_123) == 1_700_000_000_123


def test_watermark_future_drop():
    now = 2_000_000
    ts = now + 10_000
    drop, reason = should_drop_by_watermark(now_ms=now, ts_ms=ts, max_future_ms=1500, max_past_ms=120000)
    assert drop is True
    assert reason == "future_tick"


def test_watermark_past_drop():
    now = 2_000_000
    ts = now - 200_000
    drop, reason = should_drop_by_watermark(now_ms=now, ts_ms=ts, max_future_ms=1500, max_past_ms=120000)
    assert drop is True
    assert reason == "past_tick"


def test_watermark_ok():
    now = 2_000_000
    ts = now - 10_000
    drop, reason = should_drop_by_watermark(now_ms=now, ts_ms=ts, max_future_ms=1500, max_past_ms=120000)
    assert drop is False
    assert reason == ""
