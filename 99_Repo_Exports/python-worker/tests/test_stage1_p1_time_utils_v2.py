from utils.time_utils import get_ny_time_millis
"""
tests/test_stage1_p1_time_utils_v2.py

Stage1-P1 regression tests for normalize_epoch_ms_v2 and NormalizedEpochMs.

Coverage:
- Milliseconds input (>= 1e11)   -> kind="ms"
- Seconds input (< 1e11)        -> kind="sec", value * 1000
- Garbage / None / empty string -> kind="now", clamped=True
- Negative / zero               -> kind="now", clamped=True
- String numeric inputs
- .ts_ms attribute access (used by tick_processor instead of int(tick_ts))
"""
import time
import pytest
from common.time_utils import normalize_epoch_ms_v2, NormalizedEpochMs

# Fixed reference timestamps
TS_SEC = 1_700_000_000          # 2023-11-14 in seconds
TS_MS  = 1_700_000_000_000      # same in milliseconds


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_ms_passthrough():
    """Millisecond epoch (>= 1e11) should pass through unchanged."""
    res = normalize_epoch_ms_v2(TS_MS, now_ms=TS_MS)
    assert isinstance(res, NormalizedEpochMs)
    assert res.kind == "ms"
    assert res.ts_ms == TS_MS
    assert res.clamped is False
    assert res.reason == "as_ms"


def test_seconds_converted_to_ms():
    """Unix seconds (< 1e11) should be multiplied by 1000."""
    res = normalize_epoch_ms_v2(TS_SEC, now_ms=TS_MS)
    assert res.kind == "sec"
    assert res.ts_ms == TS_SEC * 1000
    assert res.clamped is False
    assert res.reason == "sec_to_ms"


def test_numeric_string_ms():
    """Numeric string representing ms epoch."""
    res = normalize_epoch_ms_v2(str(TS_MS), now_ms=TS_MS)
    assert res.kind == "ms"
    assert res.ts_ms == TS_MS


def test_numeric_string_seconds():
    """Numeric string representing seconds epoch."""
    res = normalize_epoch_ms_v2(str(TS_SEC), now_ms=TS_MS)
    assert res.kind == "sec"
    assert res.ts_ms == TS_SEC * 1000


def test_float_seconds():
    """Float seconds value."""
    res = normalize_epoch_ms_v2(float(TS_SEC), now_ms=TS_MS)
    assert res.kind == "sec"
    assert res.ts_ms == TS_SEC * 1000


# ---------------------------------------------------------------------------
# Garbage / fallback paths
# ---------------------------------------------------------------------------

def test_none_returns_now():
    res = normalize_epoch_ms_v2(None, now_ms=TS_MS)
    assert res.kind == "now"
    assert res.clamped is True
    assert res.ts_ms == TS_MS
    assert res.reason == "none"


def test_empty_string_returns_now():
    res = normalize_epoch_ms_v2("", now_ms=TS_MS)
    assert res.kind == "now"
    assert res.clamped is True
    assert res.ts_ms == TS_MS
    assert res.reason == "empty_str"


def test_whitespace_string_returns_now():
    res = normalize_epoch_ms_v2("   ", now_ms=TS_MS)
    assert res.kind == "now"
    assert res.clamped is True


def test_non_numeric_string_returns_now():
    res = normalize_epoch_ms_v2("garbage_value", now_ms=TS_MS)
    assert res.kind == "now"
    assert res.clamped is True
    assert res.reason == "non_numeric"


def test_negative_value_returns_now():
    res = normalize_epoch_ms_v2(-1234, now_ms=TS_MS)
    assert res.kind == "now"
    assert res.clamped is True
    assert res.reason == "non_positive"


def test_zero_returns_now():
    res = normalize_epoch_ms_v2(0, now_ms=TS_MS)
    assert res.kind == "now"
    assert res.clamped is True


# ---------------------------------------------------------------------------
# Attribute access (.ts_ms) — the primary usage in tick_processor
# ---------------------------------------------------------------------------

def test_ts_ms_attribute_access():
    """tick_processor uses .ts_ms — ensure it is accessible."""
    res = normalize_epoch_ms_v2(TS_MS)
    assert hasattr(res, "ts_ms")
    assert isinstance(res.ts_ms, int)


def test_ts_ms_attribute_seconds_input():
    """Even for seconds input .ts_ms should equal seconds * 1000."""
    res = normalize_epoch_ms_v2(TS_SEC)
    assert res.ts_ms == TS_SEC * 1000


# ---------------------------------------------------------------------------
# Frozen / immutable
# ---------------------------------------------------------------------------

def test_result_is_frozen():
    """NormalizedEpochMs is a frozen dataclass — mutation must raise."""
    res = normalize_epoch_ms_v2(TS_MS)
    with pytest.raises((AttributeError, TypeError)):
        res.ts_ms = 0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# now_ms defaults to wall clock
# ---------------------------------------------------------------------------

def test_none_without_now_ms_uses_wall_clock():
    """None input with no now_ms hint returns current wall clock (approx)."""
    before = get_ny_time_millis()
    res = normalize_epoch_ms_v2(None)
    after = get_ny_time_millis()
    assert res.kind == "now"
    assert before <= res.ts_ms <= after + 100  # 100ms tolerance
