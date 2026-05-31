"""Unit tests: LiqMap feature injection via OrderFlowStrategy._maybe_add_liqmap_features.

The liqmap injection was refactored from TickProcessor._inject_liqmap_features into
Strategy._maybe_add_liqmap_features (services/orderflow/strategy.py).

These tests guard against:
  - Feature extraction silently returning empty results from cached snapshots
  - Cache TTL logic causing unnecessary Redis fetches
  - None/missing payload triggering a crash (fail-open contract)
"""

import asyncio
from typing import Any


def _make_pre_parsed_payload(*, ts_ms: int) -> dict:
    """Build a payload already parsed by try_parse_liqmap_snapshot_json."""
    levels = [
        {"price": 99.0, "long_usd": 300_000.0, "short_usd": 0.0},
        {"price": 101.0, "long_usd": 0.0, "short_usd": 200_000.0},
    ]
    return {
        "ts_ms": int(ts_ms),
        "levels": levels,
        # Pre-computed by parser
        "_num_levels": [(99.0, 300_000.0, 0.0), (101.0, 0.0, 200_000.0)],
        "_tot_long_usd": 300_000.0,
        "_tot_short_usd": 200_000.0,
    }


class _FakeAsyncRedis:
    def __init__(self):
        self.mget_calls: list = []
        self.payloads: dict[str, Any] = {}

    async def mget(self, *keys):
        self.mget_calls.extend(keys)
        return [self.payloads.get(k) for k in keys]


def _make_strategy(fake_redis) -> Any:
    from services.orderflow.strategy import OrderFlowStrategy

    st = OrderFlowStrategy.__new__(OrderFlowStrategy)
    st.redis = fake_redis
    st.liqmap_features_enable = True
    st.liqmap_feature_windows = ["1h"]
    st.liqmap_feature_cache_ms = 1500
    st.liqmap_feature_redis_timeout_s = 0.5
    st.liqmap_feature_max_stale_ms = 120_000
    st.liqmap_feature_peak_range_bps = 600.0
    st.liqmap_feature_front_run_bps = 20.0
    st.liqmap_feature_sl_buffer_bps = 15.0
    st.liqmap_snapshot_key_prefix = "liqmap:snapshot"
    return st


class _Runtime:
    symbol = "BTCUSDT"


def test_liqmap_features_extracted_from_pre_seeded_cache():
    """Features must be populated from a warm cache (no Redis fetch needed)."""
    fake_redis = _FakeAsyncRedis()
    st = _make_strategy(fake_redis)
    runtime = _Runtime()
    now_ms = 1_000_000

    # Pre-seed cache so no fetch is triggered
    runtime.liqmap_snapshot_cache = {
        "1h": {"fetch_ms": now_ms, "payload": _make_pre_parsed_payload(ts_ms=now_ms - 500)},
    }

    indicators: dict[str, Any] = {}
    asyncio.run(
        st._maybe_add_liqmap_features(
            runtime=runtime,
            indicators=indicators,
            mid_px=100.0,
            now_ms=now_ms,
        )
    )

    assert "liqmap_1h_levels_n" in indicators, f"levels_n missing; got {list(indicators)}"
    assert float(indicators["liqmap_1h_levels_n"]) == 2.0
    assert "liqmap_1h_stale_ms" in indicators
    assert "liqmap_1h_is_stale" in indicators
    # No Redis fetch — cache was fresh
    assert fake_redis.mget_calls == [], "Unexpected Redis mget in warm-cache test"


def test_liqmap_features_extracted_on_first_cold_cache_call():
    """Cold cache must populate liqmap_* on the same live tick, not one tick later."""
    import json

    fake_redis = _FakeAsyncRedis()
    st = _make_strategy(fake_redis)
    runtime = _Runtime()
    now_ms = 1_500_000

    snap = {
        "ts_ms": now_ms - 500,
        "symbol": "BTCUSDT",
        "window": "1h",
        "levels": [
            {"price": 99.0, "long_usd": 300_000.0, "short_usd": 0.0},
            {"price": 101.0, "long_usd": 0.0, "short_usd": 200_000.0},
        ],
    }
    fake_redis.payloads["liqmap:snapshot:BTCUSDT:1h"] = json.dumps(snap)

    indicators: dict[str, Any] = {}
    asyncio.run(
        st._maybe_add_liqmap_features(
            runtime=runtime,
            indicators=indicators,
            mid_px=100.0,
            now_ms=now_ms,
        )
    )

    assert fake_redis.mget_calls == ["liqmap:snapshot:BTCUSDT:1h"]
    assert indicators["liqmap_1h_levels_n"] == 2.0
    assert indicators["liqmap_levels_n"] == 2.0
    assert indicators["liqmap_ok"] == 1


def test_liqmap_cache_prevents_redis_refetch_within_ttl():
    """Within cache_ms window, no extra Redis fetch must occur."""
    fake_redis = _FakeAsyncRedis()
    st = _make_strategy(fake_redis)
    runtime = _Runtime()
    now_ms = 2_000_000

    payload = _make_pre_parsed_payload(ts_ms=now_ms - 100)
    # Fetch timestamp is recent (within 1500ms TTL)
    runtime.liqmap_snapshot_cache = {
        "1h": {"fetch_ms": now_ms - 200, "payload": payload},
    }

    indicators1: dict[str, Any] = {}
    asyncio.run(
        st._maybe_add_liqmap_features(
            runtime=runtime,
            indicators=indicators1,
            mid_px=100.0,
            now_ms=now_ms,
        )
    )

    indicators2: dict[str, Any] = {}
    asyncio.run(
        st._maybe_add_liqmap_features(
            runtime=runtime,
            indicators=indicators2,
            mid_px=100.0,
            now_ms=now_ms + 100,  # still within TTL
        )
    )

    assert fake_redis.mget_calls == [], "mget called despite cache being fresh"
    assert indicators2.get("liqmap_1h_levels_n") == indicators1.get("liqmap_1h_levels_n")


def test_liqmap_none_payload_does_not_crash():
    """None payload in cache must not raise; should produce only staleness defaults."""
    fake_redis = _FakeAsyncRedis()
    st = _make_strategy(fake_redis)
    runtime = _Runtime()
    now_ms = 3_000_000

    # Cache hit but payload is None (parse failure or empty Redis response)
    runtime.liqmap_snapshot_cache = {
        "1h": {"fetch_ms": now_ms, "payload": None},
    }

    indicators: dict[str, Any] = {}
    asyncio.run(
        st._maybe_add_liqmap_features(
            runtime=runtime,
            indicators=indicators,
            mid_px=100.0,
            now_ms=now_ms,
        )
    )
    # Must not crash; liqmap_ok should be 0 (no valid levels)
    assert indicators.get("liqmap_ok", 0) == 0


def test_liqmap_disabled_flag_skips_all_processing():
    """liqmap_features_enable=False must exit immediately with empty indicators."""
    fake_redis = _FakeAsyncRedis()
    st = _make_strategy(fake_redis)
    st.liqmap_features_enable = False
    runtime = _Runtime()
    runtime.liqmap_snapshot_cache = {}

    indicators: dict[str, Any] = {}
    asyncio.run(
        st._maybe_add_liqmap_features(
            runtime=runtime,
            indicators=indicators,
            mid_px=100.0,
            now_ms=1_000_000,
        )
    )

    assert indicators == {}, f"Expected empty indicators when disabled; got {indicators}"
    assert fake_redis.mget_calls == []
