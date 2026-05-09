"""
Tests for deribit_context.py — DeribitContextSnapshot reader from Redis.
"""
import json
import math
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.orderflow.deribit_context import (
    _f,
    _i,
    aread_deribit_context,
)

# ─── _f / _i helpers ─────────────────────────────────────────────────────────

def test_f_normal():
    assert _f("54.2") == pytest.approx(54.2)

def test_f_inf_returns_default():
    assert _f(float("inf")) == 0.0

def test_f_nan_returns_default():
    assert _f(float("nan")) == 0.0

def test_f_none_returns_default():
    assert _f(None) == 0.0

def test_f_non_numeric_returns_default():
    assert _f("abc") == 0.0

def test_i_normal():
    assert _i("42") == 42

def test_i_float_string():
    assert _i("3.9") == 3

def test_i_none_returns_default():
    assert _i(None) == 0


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def _make_redis(raw: str):
    r = MagicMock()
    r.get = AsyncMock(return_value=raw.encode("utf-8"))
    return r


def _btcusdt_payload():
    return {
        "schema_version": 1,
        "symbol": "BTCUSDT",
        "currency": "BTC",
        "ts_ms": 1760000000000,
        "btc_options_oi_proxy": 12_500_000_000,
        "eth_options_oi_proxy": 7_200_000_000,
        "deribit_iv_proxy": 54.2,
        "deribit_iv_z": 1.8,
        "deribit_funding_8h": 0.00012,
        "deribit_perp_basis_bps": 3.4,
        "btc_eth_vol_regime": "vol_expansion",
        "quality_status": "OK",
    }


def _global_payload():
    return {
        "schema_version": 1,
        "ts_ms": 1760000000000,
        "btc_options_oi_proxy": 12_500_000_000.0,
        "eth_options_oi_proxy": 7_200_000_000.0,
        "btc_deribit_iv_proxy": 54.2,
        "eth_deribit_iv_proxy": 61.4,
        "btc_deribit_iv_z": 1.8,
        "eth_deribit_iv_z": 2.2,
        "btc_deribit_funding_8h": 0.00012,
        "eth_deribit_funding_8h": 0.00018,
        "btc_eth_vol_regime": "vol_expansion",
        "quality_status": "OK",
    }


# ─── Key routing ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_btcusdt_reads_symbol_key():
    redis = _make_redis(json.dumps(_btcusdt_payload()))
    snap = await aread_deribit_context(redis, symbol="BTCUSDT")
    redis.get.assert_called_once_with("ctx:deribit:BTCUSDT")
    assert snap is not None
    assert snap.symbol == "BTCUSDT"
    assert snap.currency == "BTC"


@pytest.mark.asyncio
async def test_ethusdt_reads_symbol_key():
    payload = {"schema_version": 1, "symbol": "ETHUSDT", "currency": "ETH",
               "ts_ms": 1760000000000, "btc_options_oi_proxy": 0, "eth_options_oi_proxy": 0,
               "deribit_iv_proxy": 61.4, "deribit_iv_z": 2.2, "deribit_funding_8h": 0.00018,
               "deribit_perp_basis_bps": 2.1, "btc_eth_vol_regime": "normal", "quality_status": "OK"}
    redis = _make_redis(json.dumps(payload))
    snap = await aread_deribit_context(redis, symbol="ETHUSDT")
    redis.get.assert_called_once_with("ctx:deribit:ETHUSDT")
    assert snap is not None


@pytest.mark.asyncio
async def test_altcoin_reads_global_key():
    redis = _make_redis(json.dumps(_global_payload()))
    snap = await aread_deribit_context(redis, symbol="SOLUSDT")
    redis.get.assert_called_once_with("ctx:deribit:global")
    assert snap is not None


# ─── Snapshot field parsing ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_snapshot_fields_btcusdt():
    redis = _make_redis(json.dumps(_btcusdt_payload()))
    snap = await aread_deribit_context(redis, symbol="BTCUSDT")

    assert snap.schema_version == 1
    assert snap.ts_ms == 1760000000000
    assert snap.btc_options_oi_proxy == pytest.approx(12_500_000_000.0)
    assert snap.deribit_iv_proxy == pytest.approx(54.2)
    assert snap.deribit_iv_z == pytest.approx(1.8)
    assert snap.deribit_funding_8h == pytest.approx(0.00012)
    assert snap.deribit_perp_basis_bps == pytest.approx(3.4)
    assert snap.btc_eth_vol_regime == "vol_expansion"
    assert snap.quality_status == "OK"


@pytest.mark.asyncio
async def test_global_iv_proxy_fallback_fields():
    """Global payload uses btc_deribit_iv_proxy key; reader must map it correctly."""
    redis = _make_redis(json.dumps(_global_payload()))
    snap = await aread_deribit_context(redis, symbol="SOLUSDT")
    # btc_deribit_iv_proxy → deribit_iv_proxy
    assert snap.deribit_iv_proxy == pytest.approx(54.2)
    assert snap.deribit_iv_z == pytest.approx(1.8)
    assert snap.deribit_funding_8h == pytest.approx(0.00012)


# ─── Fail-open paths ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_redis_none_returns_none():
    snap = await aread_deribit_context(None, symbol="BTCUSDT")
    assert snap is None


@pytest.mark.asyncio
async def test_redis_key_missing_returns_none():
    redis = MagicMock()
    redis.get = AsyncMock(return_value=None)
    snap = await aread_deribit_context(redis, symbol="BTCUSDT")
    assert snap is None


@pytest.mark.asyncio
async def test_redis_invalid_json_returns_none():
    redis = _make_redis("not json at all {{{")
    snap = await aread_deribit_context(redis, symbol="BTCUSDT")
    assert snap is None


@pytest.mark.asyncio
async def test_redis_exception_returns_none():
    redis = MagicMock()
    redis.get = AsyncMock(side_effect=ConnectionError("down"))
    snap = await aread_deribit_context(redis, symbol="BTCUSDT")
    assert snap is None


@pytest.mark.asyncio
async def test_empty_dict_returns_snapshot_with_defaults():
    """An empty dict should produce a snapshot with safe defaults (not None)."""
    redis = _make_redis(json.dumps({}))
    snap = await aread_deribit_context(redis, symbol="BTCUSDT")
    assert snap is not None
    assert snap.schema_version == 1
    assert snap.quality_status == "UNKNOWN"
    assert math.isfinite(snap.deribit_iv_proxy)


# ─── Snapshot is immutable (frozen dataclass) ─────────────────────────────────

@pytest.mark.asyncio
async def test_snapshot_is_frozen():
    redis = _make_redis(json.dumps(_btcusdt_payload()))
    snap = await aread_deribit_context(redis, symbol="BTCUSDT")
    with pytest.raises((AttributeError, TypeError)):
        snap.quality_status = "MUTATED"  # type: ignore[misc]
