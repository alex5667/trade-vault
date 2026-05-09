"""Unit tests for _normalize_ts_ms helper in SignalOrchestrator.

Contract (orchestrator._normalize_ts_ms, fallback=0):
  - epoch_ms in [now-7d; now+1m] → passthrough
  - epoch_s (<10_000_000_000) in range → *1000 → passthrough
  - zero / None / stale / future-skew / invalid → returns 0
    (NOT now_ms — "fail visible" rather than "quiet substitute")
"""
from handlers.crypto_orderflow.pipeline.orchestrator import _normalize_ts_ms

NOW_MS = 1_744_444_800_000  # 2026-04-12 (fixed anchor for tests)
_7D_MS = 7 * 24 * 3_600 * 1_000
_1MIN_MS = 60 * 1_000


# ── Happy-path ────────────────────────────────────────────────────────────────

def test_epoch_ms_passthrough():
    ts = NOW_MS - 3_600_000  # 1h ago, already ms
    assert _normalize_ts_ms(ts, NOW_MS) == ts


def test_epoch_s_auto_multiplied_to_ms():
    ts_s = NOW_MS // 1000  # epoch_seconds
    assert _normalize_ts_ms(ts_s, NOW_MS) == ts_s * 1000


def test_string_int_ms_accepted():
    ts = NOW_MS - 1_000
    assert _normalize_ts_ms(str(ts), NOW_MS) == ts


def test_boundary_exactly_at_minus_7d():
    edge = NOW_MS - _7D_MS  # exactly at lower bound (inclusive)
    assert _normalize_ts_ms(edge, NOW_MS) == edge


def test_boundary_exactly_at_plus_1min():
    edge = NOW_MS + _1MIN_MS  # exactly at upper bound (inclusive)
    assert _normalize_ts_ms(edge, NOW_MS) == edge


# ── Anomaly → fallback = 0 (NOT now_ms) ─────────────────────────────────────
# The function is deliberately "fail visible": bad timestamps become 0 so
# downstream consumers can detect them, rather than silently injecting now_ms.

def test_zero_returns_0():
    assert _normalize_ts_ms(0, NOW_MS) == 0


def test_none_returns_0():
    assert _normalize_ts_ms(None, NOW_MS) == 0


def test_negative_returns_0():
    assert _normalize_ts_ms(-1, NOW_MS) == 0


def test_stale_beyond_7d_returns_0():
    stale = NOW_MS - _7D_MS - 1_000  # 1 second past the lower bound
    assert _normalize_ts_ms(stale, NOW_MS) == 0


def test_future_beyond_1min_returns_0():
    future = NOW_MS + _1MIN_MS + 1_000  # 1 second past the upper bound
    assert _normalize_ts_ms(future, NOW_MS) == 0


def test_microseconds_too_large_returns_0():
    # epoch_us would be ~1_744_444_800_000_000 which is >> 10^12 ms → treated
    # as ms but wildly in the future → anomaly → 0
    ts_us = NOW_MS * 1_000
    assert _normalize_ts_ms(ts_us, NOW_MS) == 0


def test_invalid_string_returns_0():
    assert _normalize_ts_ms("not-a-number", NOW_MS) == 0


def test_float_nan_returns_0():
    assert _normalize_ts_ms(float("nan"), NOW_MS) == 0
