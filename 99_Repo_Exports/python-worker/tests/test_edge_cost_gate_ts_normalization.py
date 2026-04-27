from __future__ import annotations

from types import SimpleNamespace

import pytest


class _RedisNoCalls:
    """If EdgeCostGate tries to query EMA when it shouldn't, this will fail the test."""
    def __getattr__(self, name):
        raise AssertionError(f"Redis method called unexpectedly: {name}")


class _RedisHash:
    def __init__(self, h):
        self._h = dict(h)
        self.requests = []
    def hgetall(self, key):
        self.requests.append(key)
        # Return data for any slipema key
        if "slipema:" in key:
            return dict(self._h)
        return {}


def test_estimate_slippage_minutes_of_day_ts_is_rejected_and_skips_ema():
    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

    ctx = SimpleNamespace(
        bid=100.0,
        ask=100.2,  # spread_bps ~ 20 => half ~ 10
        tf="1m",
        kind="absorption",
    )
    # minutes-of-day (non-epoch) must be rejected by strict normalizer => EMA disabled, no redis calls.
    v = estimate_slippage_bps(
        ctx,
        redis_client=_RedisNoCalls(),
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=600,  # non-epoch small number
        default_bps=5.0,
        use_spread_half=True,
    )
    assert v >= 9.0


def test_estimate_slippage_ts_invalid_flag_forces_skip_ema_even_if_ts_ms_looks_ok():
    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

    ctx = SimpleNamespace(bid=100.0, ask=100.2, tf="1m", kind="absorption", ts_invalid=1)
    v = estimate_slippage_bps(
        ctx,
        redis_client=_RedisNoCalls(),  # must not be touched
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=1_700_000_000_000,  # looks like epoch ms, but ts_invalid must disable EMA
        default_bps=5.0,
        use_spread_half=True,
    )
    assert v >= 9.0


def test_estimate_slippage_invalid_ts_skips_ema_and_does_not_touch_redis():
    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

    ctx = SimpleNamespace(
        bid=100.0,
        ask=100.2,   # spread_bps ~ 20 bps => half ~ 10 bps
        tf="1m",
        kind="absorption",
    )
    # invalid ts -> 0
    v = estimate_slippage_bps(
        ctx,
        redis_client=_RedisNoCalls(),
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=0,
        default_bps=5.0,
        use_spread_half=True,
    )
    # spread_bps = (100.2 - 100.0) / 100.1 * 10000 ≈ 19.98 bps
    # half_spread ≈ 9.99 bps, should dominate default 5.0
    assert v == pytest.approx(9.99, rel=1e-2)


def test_estimate_slippage_seconds_ts_normalizes_and_uses_ema_when_available():
    """Test that seconds timestamps are normalized and EMA can be used."""
    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps, _normalize_ts_ms_fail_open

    # Test timestamp normalization
    ts_norm = _normalize_ts_ms_fail_open(1_700_000_000)  # seconds
    assert ts_norm == 1_700_000_000_000  # converted to ms

    ctx = SimpleNamespace(
        bid=100.0,
        ask=100.02,  # spread_bps ~ 2 bps => half ~ 1 bps
        tf="1m",
        kind="absorption",
    )

    # Test that function runs with valid timestamp (seconds get normalized)
    r = _RedisHash({})
    v = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=1_700_000_000,  # seconds - should be normalized
        default_bps=5.0,
        use_spread_half=True,
    )
    # Should return default/spread since no EMA data available
    assert isinstance(v, float)
    assert v >= 5.0  # At least default