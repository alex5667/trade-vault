from __future__ import annotations

from core.time_utils import extract_tick_ts_ms, normalize_epoch_ms


def test_normalize_epoch_ms():
    # Already ms
    assert normalize_epoch_ms(1700000000000) == 1700000000000
    # Seconds -> ms
    assert normalize_epoch_ms(1700000000) == 1700000000000
    # String variants
    assert normalize_epoch_ms("1700000000000") == 1700000000000
    assert normalize_epoch_ms("1700000000") == 1700000000000
    # floats
    assert normalize_epoch_ms(1700000000.5) == 1700000000000
    # invalid
    assert normalize_epoch_ms(None) == 0
    assert normalize_epoch_ms("abc") == 0
    assert normalize_epoch_ms(0) == 0
    assert normalize_epoch_ms(-1) == 0

def test_extract_tick_ts_ms():
    # Common keys
    assert extract_tick_ts_ms({"ts": 1700000000000}) == 1700000000000
    assert extract_tick_ts_ms({"tick_ts": 1700000000000}) == 1700000000000
    assert extract_tick_ts_ms({"event_time": 1700000000000}) == 1700000000000
    assert extract_tick_ts_ms({"ts_ms": 1700000000000}) == 1700000000000

    # Seconds in keys (auto-normalized by normalize_epoch_ms)
    assert extract_tick_ts_ms({"ts": 1700000000}) == 1700000000000

    # Missing / empty
    assert extract_tick_ts_ms({}) == 0
    assert extract_tick_ts_ms({"other": 123}) == 0
    assert extract_tick_ts_ms(None) == 0
