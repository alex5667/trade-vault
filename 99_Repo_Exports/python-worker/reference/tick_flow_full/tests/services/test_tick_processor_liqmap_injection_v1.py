# tick_flow_full/tests/services/test_tick_processor_liqmap_injection_v1.py
# -*- coding: utf-8 -*-
"""Unit tests: LiqMap injection in TickProcessor.

Goal
----
Guard against silent regressions where:
  - Redis snapshot parsing fails and we stop emitting liqmap_* indicators
  - cache/refresh logic breaks and starts hammering Redis

These tests are intentionally *minimal* and only touch the LiqMap injection helper.
They do NOT require a running Redis, WS, or the full TickProcessor runtime.
"""

import asyncio
import json
from typing import Any, Dict, Optional


class _FakeAsyncRedis:
    def __init__(self, kv: Dict[str, Optional[bytes]]):
        self._kv = dict(kv)
        self.get_calls = []

    async def get(self, key: str):
        self.get_calls.append(key)
        return self._kv.get(key)


def _make_snapshot_json(*, ts_ms: int, symbol: str, window: str) -> bytes:
    snap = {
        "v": 1
        "ts_ms": int(ts_ms)
        "symbol": str(symbol)
        "window": str(window)
        "levels": [
            # Above
            {"side": "ask", "price": 101.0, "usd": 200_000.0, "cnt": 10}
            {"side": "ask", "price": 102.0, "usd": 50_000.0, "cnt": 5}
            # Below
            {"side": "bid", "price": 99.0, "usd": 300_000.0, "cnt": 12}
            {"side": "bid", "price": 98.0, "usd": 40_000.0, "cnt": 4}
        ]
    }
    return json.dumps(snap).encode("utf-8")


def test_tick_processor_injects_liqmap_features_from_redis_cache_and_failopen():
    # Import from SoT (tick_flow_full/) so this test enforces the contract there.
    from services.orderflow.components.tick_processor import TickProcessor

    # Construct a minimal TickProcessor instance without calling __init__ (too heavy).
    tp = TickProcessor.__new__(TickProcessor)
    tp._liqmap_cache = {}
    # Newer SoT implementation uses a separate next-refresh cache.
    tp._liqmap_next_refresh_ts_ms = {}
    tp.redis = _FakeAsyncRedis(
        {
            "liqmap:snapshot:BTCUSDT:1h": _make_snapshot_json(
                ts_ms=1_000_000 - 500, symbol="BTCUSDT", window="1h"
            )
        }
    )

    # Minimal config knobs used by _inject_liqmap_features.
    tp.liqmap_features_enable = True
    tp.liqmap_features_windows = ["1h"]
    # Both knobs are set for backward compatibility across refactors.
    tp.liqmap_features_refresh_ms = 1500
    tp.liqmap_features_fetch_interval_ms = 1500
    tp.liqmap_features_failopen_stale_ms = 120_000
    tp.liqmap_snapshot_key_prefix = "liqmap:snapshot"

    # Make near-band wide enough to include the 99/101 levels in near_*.
    tp.liqmap_near_band_bps = 200.0
    tp.liqmap_peak_min_share = 0.05

    class _Runtime:
        symbol = "BTCUSDT"

    indicators: Dict[str, Any] = {}

    # 1) First call => hits Redis, parses snapshot, writes liqmap_* keys.
    asyncio.run(
        tp._inject_liqmap_features(
            runtime=_Runtime()
            now_ms=1_000_000
            price=100.0
            indicators=indicators
        )
    )

    assert tp.redis.get_calls == ["liqmap:snapshot:BTCUSDT:1h"]
    assert "liqmap_1h_total_usd" in indicators, "LiqMap totals must be present in indicators"
    assert "liqmap_1h_age_ms" in indicators, "Snapshot age must be present in indicators"
    assert indicators["liqmap_1h_total_usd"] > 0.0

    # 2) Second call (within fetch interval) => MUST NOT hit Redis again.
    indicators2: Dict[str, Any] = {}
    asyncio.run(
        tp._inject_liqmap_features(
            runtime=_Runtime()
            now_ms=1_000_000 + 200,  # < fetch_interval_ms
            price=100.0
            indicators=indicators2
        )
    )
    assert tp.redis.get_calls == ["liqmap:snapshot:BTCUSDT:1h"], "Cache/TTL broken: extra Redis get"
    assert indicators2.get("liqmap_1h_total_usd") == indicators.get("liqmap_1h_total_usd")

    # 3) Parse error => fail-open with zero defaults, no exception.
    tp.redis._kv["liqmap:snapshot:BTCUSDT:1h"] = b"{not-json"
    indicators3: Dict[str, Any] = {}
    asyncio.run(
        tp._inject_liqmap_features(
            runtime=_Runtime()
            now_ms=1_000_000 + 2000,  # force refresh
            price=100.0
            indicators=indicators3
        )
    )
    assert "liqmap_1h_total_usd" in indicators3
    assert float(indicators3["liqmap_1h_total_usd"]) == 0.0
    assert "liqmap_1h_age_ms" not in indicators3 or float(indicators3.get("liqmap_1h_age_ms", 0.0)) >= 0.0
