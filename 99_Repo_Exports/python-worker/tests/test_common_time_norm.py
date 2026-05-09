from datetime import UTC, datetime

from common.time_norm import normalize_epoch_ms


def test_normalize_epoch_ms_modern_epoch_seconds():
    assert normalize_epoch_ms(1_700_000_000) == 1_700_000_000_000
    assert normalize_epoch_ms("1700000000") == 1_700_000_000_000


def test_normalize_epoch_ms_milliseconds_passthrough():
    assert normalize_epoch_ms(1_700_000_000_123) == 1_700_000_000_123
    assert normalize_epoch_ms("1700000000123") == 1_700_000_000_123


def test_normalize_epoch_ms_microseconds_and_nanoseconds():
    assert normalize_epoch_ms(1_700_000_000_123_456) == 1_700_000_000_123
    assert normalize_epoch_ms(1_700_000_000_123_456_789) == 1_700_000_000_123


def test_normalize_epoch_ms_iso_datetime():
    dt = datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)
    assert normalize_epoch_ms(dt) == 1_700_000_000_000
    assert normalize_epoch_ms("2023-11-14T22:13:20Z") == 1_700_000_000_000
