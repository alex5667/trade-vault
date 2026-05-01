"""Tests for cross-venue context snapshot reader.

Covers: JSON roundtrip, bytes/str/dict parsing, missing fields → defaults,
bad JSON → None, NaN/Inf → 0.0, ctx_key format.
"""
import json
import pytest
from services.orderflow.crossvenue_context import (
    CrossVenueContextSnapshot,
    from_json,
    from_dict,
    ctx_key,
    SCHEMA_VERSION,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _sample_payload(**overrides):
    base = {
        "schema_version": 1,
        "symbol": "BTCUSDT",
        "ts_ms": 1760000000000,
        "primary_venue": "binance_usdm",
        "venues": {
            "binance": {"mid": 64000.5, "bid": 64000.0, "ask": 64001.0, "ts_ms": 1760000000000, "stale": 0},
            "coinbase": {"mid": 64004.2, "bid": 64003.9, "ask": 64004.5, "ts_ms": 1760000000000, "stale": 0},
            "kraken": {"mid": 64002.7, "bid": 64002.2, "ask": 64003.2, "ts_ms": 1760000000000, "stale": 0},
        },
        "cross_venue_mid_spread_bps": 0.65,
        "binance_vs_coinbase_mid_bps": -0.58,
        "binance_vs_kraken_mid_bps": -0.34,
        "binance_vs_okx_mid_bps": 0.0,
        "cross_venue_direction_agree": 1.0,
        "cross_venue_trade_imbalance": 0.22,
        "venue_dislocation_z": 1.4,
        "venue_stale_count": 0,
        "quality_status": "OK",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Basic parsing
# ---------------------------------------------------------------------------

def test_from_json_str_roundtrip():
    payload = _sample_payload()
    snap = from_json(json.dumps(payload))
    assert snap is not None
    assert snap.symbol == "BTCUSDT"
    assert snap.ts_ms == 1760000000000
    assert snap.cross_venue_mid_spread_bps == pytest.approx(0.65)
    assert snap.quality_status == "OK"
    assert snap.schema_version == 1
    assert snap.venue_stale_count == 0


def test_from_json_bytes():
    payload = _sample_payload()
    raw = json.dumps(payload).encode("utf-8")
    snap = from_json(raw)
    assert snap is not None
    assert snap.symbol == "BTCUSDT"


def test_from_dict_direct():
    snap = from_dict(_sample_payload())
    assert snap is not None
    assert snap.primary_venue == "binance_usdm"
    assert snap.cross_venue_direction_agree == pytest.approx(1.0)


def test_symbol_uppercased():
    payload = _sample_payload(symbol="ethusdt")
    snap = from_dict(payload)
    assert snap is not None
    assert snap.symbol == "ETHUSDT"


# ---------------------------------------------------------------------------
# Missing fields → defaults
# ---------------------------------------------------------------------------

def test_missing_optional_fields_defaults():
    snap = from_dict({"symbol": "SOLUSDT", "ts_ms": 12345})
    assert snap is not None
    assert snap.cross_venue_mid_spread_bps == 0.0
    assert snap.binance_vs_coinbase_mid_bps == 0.0
    assert snap.binance_vs_kraken_mid_bps == 0.0
    assert snap.binance_vs_okx_mid_bps == 0.0
    assert snap.cross_venue_direction_agree == 0.0
    assert snap.cross_venue_trade_imbalance == 0.0
    assert snap.venue_dislocation_z == 0.0
    assert snap.venue_stale_count == 0
    assert snap.quality_status == "UNKNOWN"
    assert snap.primary_venue == "binance_usdm"


# ---------------------------------------------------------------------------
# NaN / Inf / bad values → 0.0
# ---------------------------------------------------------------------------

def test_nan_inf_fields_become_zero():
    payload = _sample_payload(
        cross_venue_mid_spread_bps=float("nan"),
        venue_dislocation_z=float("inf"),
        cross_venue_direction_agree=float("-inf"),
    )
    snap = from_dict(payload)
    assert snap is not None
    assert snap.cross_venue_mid_spread_bps == 0.0
    assert snap.venue_dislocation_z == 0.0
    assert snap.cross_venue_direction_agree == 0.0


def test_string_float_parsed():
    payload = _sample_payload(cross_venue_mid_spread_bps="1.23", venue_stale_count="2")
    snap = from_dict(payload)
    assert snap is not None
    assert snap.cross_venue_mid_spread_bps == pytest.approx(1.23)
    assert snap.venue_stale_count == 2


# ---------------------------------------------------------------------------
# Bad inputs → None
# ---------------------------------------------------------------------------

def test_bad_json_returns_none():
    assert from_json(None) is None
    assert from_json("not-json") is None
    assert from_json(b"{{broken") is None
    assert from_json(42) is None
    assert from_json([]) is None
    assert from_json("[]") is None


def test_empty_symbol_returns_none():
    assert from_dict({"symbol": "", "ts_ms": 0}) is None
    assert from_dict({"ts_ms": 0}) is None
    assert from_dict({}) is None


# ---------------------------------------------------------------------------
# ctx_key
# ---------------------------------------------------------------------------

def test_ctx_key_default_prefix():
    assert ctx_key("BTCUSDT") == "ctx:crossvenue:BTCUSDT"
    assert ctx_key("btcusdt") == "ctx:crossvenue:BTCUSDT"
    assert ctx_key("ETHUSDT") == "ctx:crossvenue:ETHUSDT"


def test_ctx_key_custom_prefix():
    assert ctx_key("SOLUSDT", prefix="test:cv:") == "test:cv:SOLUSDT"


# ---------------------------------------------------------------------------
# Async reader (mock Redis)
# ---------------------------------------------------------------------------

import asyncio


@pytest.mark.asyncio
async def test_aread_returns_none_on_none_redis():
    from services.orderflow.crossvenue_context import aread_crossvenue_context
    result = await aread_crossvenue_context(None, symbol="BTCUSDT")
    assert result is None


@pytest.mark.asyncio
async def test_aread_returns_none_on_redis_error():
    from services.orderflow.crossvenue_context import aread_crossvenue_context

    class BadRedis:
        async def get(self, key):
            raise RuntimeError("connection refused")

    result = await aread_crossvenue_context(BadRedis(), symbol="BTCUSDT")
    assert result is None


@pytest.mark.asyncio
async def test_aread_parses_valid_payload():
    from services.orderflow.crossvenue_context import aread_crossvenue_context, _LOCAL_CACHE

    # Invalidate cache for this test
    _LOCAL_CACHE.clear()

    payload = json.dumps(_sample_payload())

    class MockRedis:
        async def get(self, key):
            return payload.encode()

    result = await aread_crossvenue_context(MockRedis(), symbol="BTCUSDT")
    assert result is not None
    assert result.symbol == "BTCUSDT"
    assert result.quality_status == "OK"
