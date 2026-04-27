from __future__ import annotations

from domain.time_utils import normalize_ts_ms, ctx_epoch_ms


class Ctx:
    def __init__(self, ts_ms=None, ts=None):
        self.ts_ms = ts_ms
        self.ts = ts


def test_normalize_ts_ms_invalid_returns_zero():
    assert normalize_ts_ms(None) == 0
    assert normalize_ts_ms("") == 0
    assert normalize_ts_ms("   ") == 0
    assert normalize_ts_ms(0) == 0
    assert normalize_ts_ms(-5) == 0


def test_normalize_ts_ms_seconds_to_ms():
    assert normalize_ts_ms(1_700_000_000) == 1_700_000_000_000
    assert normalize_ts_ms("1700000000") == 1_700_000_000_000


def test_normalize_ts_ms_keeps_ms():
    assert normalize_ts_ms(1_700_000_000_000) == 1_700_000_000_000


def test_ctx_epoch_ms_priority_ts_ms_then_ts():
    assert ctx_epoch_ms(Ctx(ts_ms=1_700_000_000_000, ts=1_600_000_000_000)) == 1_700_000_000_000
    assert ctx_epoch_ms(Ctx(ts_ms=None, ts=1_700_000_000)) == 1_700_000_000_000