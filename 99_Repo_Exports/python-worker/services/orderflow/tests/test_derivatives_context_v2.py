"""test_derivatives_context_v2.py — Tests for v2 schema fields and build_snapshot_v2."""

import json
import pytest

from services.orderflow.derivatives_context import (
    SCHEMA_VERSION
    SCHEMA_VERSION_V2
    DerivativesContextSnapshot
    build_snapshot
    build_snapshot_v2
    from_dict
    from_json
)


# ─── Schema version constants ──────────────────────────────────────────────────

def test_schema_version_v2_defined():
    assert SCHEMA_VERSION_V2 == 2
    assert SCHEMA_VERSION == 1


# ─── V2 fields present in dataclass ───────────────────────────────────────────

def test_v2_fields_exist_with_defaults():
    snap = DerivativesContextSnapshot(
        schema_version=2
        symbol="BTCUSDT"
        ts_ms=1760000000000
        venue="binance"
        funding_rate=0.0001
        funding_rate_abs=0.0001
        funding_rate_z=0.5
        premium_index=67000.0
        basis_bps=5.0
        open_interest=12000.0
        delta_oi_5m=100.0
        oi_notional_usd=804_000_000.0
        funding_extreme=0
        basis_extreme=0
        oi_accel=0
        # v2 fields default to 0.0
    )
    assert snap.long_short_ratio == 0.0
    assert snap.long_short_ratio_z == 0.0
    assert snap.taker_buy_sell_imbalance == 0.0
    assert snap.liq_buy_notional_1m == 0.0
    assert snap.liq_sell_notional_1m == 0.0
    assert snap.liq_imbalance_z == 0.0
    assert snap.market_breadth_ret_24h == 0.0
    assert snap.market_breadth_volume_z == 0.0
    assert snap.leader_btc_eth_confirm == 0.0


# ─── build_snapshot_v2 ────────────────────────────────────────────────────────

def _make_v2_snap(**overrides) -> DerivativesContextSnapshot:
    defaults = dict(
        symbol="BTCUSDT"
        ts_ms=1760000000000
        venue="binance"
        funding_rate=0.0001
        funding_history=[0.0001] * 10
        premium_index=67000.0
        mark_price=67050.0
        index_price=67000.0
        open_interest=12000.0
        previous_open_interest=11900.0
        funding_extreme_abs=0.00075
        basis_extreme_abs_bps=10.0
        oi_accel_abs_usd=100_000.0
    )
    defaults.update(overrides)
    return build_snapshot_v2(**defaults)


def test_build_snapshot_v2_schema_version():
    snap = _make_v2_snap()
    assert snap.schema_version == SCHEMA_VERSION_V2


def test_build_snapshot_v2_core_fields():
    snap = _make_v2_snap()
    assert snap.symbol == "BTCUSDT"
    assert snap.venue == "binance"
    assert snap.funding_rate == pytest.approx(0.0001)
    assert snap.basis_bps == pytest.approx(7.46, abs=0.1)  # (67050-67000)/67000*10000


def test_build_snapshot_v2_v2_fields_passed():
    snap = _make_v2_snap(
        long_short_ratio=1.5
        long_short_ratio_z=2.1
        taker_buy_sell_imbalance=0.3
        liq_buy_notional_1m=500_000.0
        liq_sell_notional_1m=1_200_000.0
        liq_imbalance_z=2.8
        market_breadth_ret_24h=0.02
        market_breadth_volume_z=1.4
        leader_btc_eth_confirm=0.6
    )
    assert snap.long_short_ratio == pytest.approx(1.5)
    assert snap.long_short_ratio_z == pytest.approx(2.1)
    assert snap.taker_buy_sell_imbalance == pytest.approx(0.3)
    assert snap.liq_buy_notional_1m == pytest.approx(500_000.0)
    assert snap.liq_sell_notional_1m == pytest.approx(1_200_000.0)
    assert snap.liq_imbalance_z == pytest.approx(2.8)
    assert snap.market_breadth_ret_24h == pytest.approx(0.02)
    assert snap.market_breadth_volume_z == pytest.approx(1.4)
    assert snap.leader_btc_eth_confirm == pytest.approx(0.6)


def test_build_snapshot_v2_defaults_to_zero():
    snap = _make_v2_snap()  # no v2 fields
    assert snap.long_short_ratio == 0.0
    assert snap.liq_imbalance_z == 0.0
    assert snap.market_breadth_ret_24h == 0.0


# ─── from_dict backward compatibility ────────────────────────────────────────

def test_from_dict_v1_payload_loads_without_v2_fields():
    """V1 payload (no v2 keys) must load correctly with v2 defaults."""
    payload = {
        "schema_version": 1
        "symbol": "ETHUSDT"
        "ts_ms": 1760000000000
        "venue": "binance"
        "funding_rate": 0.0002
        "funding_rate_abs": 0.0002
        "funding_rate_z": 1.1
        "premium_index": 3500.0
        "basis_bps": 3.5
        "open_interest": 500000.0
        "delta_oi_5m": 1000.0
        "oi_notional_usd": 1_750_000_000.0
        "funding_extreme": 0
        "basis_extreme": 0
        "oi_accel": 0
    }
    snap = from_dict(payload)
    assert snap is not None
    assert snap.symbol == "ETHUSDT"
    assert snap.long_short_ratio == 0.0       # v2 default
    assert snap.liq_imbalance_z == 0.0        # v2 default
    assert snap.market_breadth_ret_24h == 0.0  # v2 default


def test_from_dict_v2_payload_loads_all_fields():
    payload = {
        "schema_version": 2
        "symbol": "SOLUSDT"
        "ts_ms": 1760000000000
        "venue": "binance"
        "funding_rate": 0.0003
        "funding_rate_abs": 0.0003
        "funding_rate_z": 2.5
        "premium_index": 150.0
        "basis_bps": 8.0
        "open_interest": 2_000_000.0
        "delta_oi_5m": 50_000.0
        "oi_notional_usd": 300_000_000.0
        "funding_extreme": 0
        "basis_extreme": 0
        "oi_accel": 0
        "long_short_ratio": 1.3
        "long_short_ratio_z": 1.8
        "taker_buy_sell_imbalance": -0.15
        "liq_buy_notional_1m": 300_000.0
        "liq_sell_notional_1m": 700_000.0
        "liq_imbalance_z": 3.2
        "market_breadth_ret_24h": -0.008
        "market_breadth_volume_z": 0.9
        "leader_btc_eth_confirm": 0.7
    }
    snap = from_dict(payload)
    assert snap is not None
    assert snap.schema_version == 2
    assert snap.long_short_ratio == pytest.approx(1.3)
    assert snap.liq_imbalance_z == pytest.approx(3.2)
    assert snap.market_breadth_ret_24h == pytest.approx(-0.008)


def test_from_json_roundtrip_v2():
    snap = _make_v2_snap(
        liq_imbalance_z=2.4
        market_breadth_ret_24h=0.015
        leader_btc_eth_confirm=0.9
    )
    raw = snap.to_json()
    restored = from_json(raw)
    assert restored is not None
    assert restored.schema_version == SCHEMA_VERSION_V2
    assert restored.liq_imbalance_z == pytest.approx(2.4)
    assert restored.market_breadth_ret_24h == pytest.approx(0.015)
    assert restored.leader_btc_eth_confirm == pytest.approx(0.9)


def test_from_json_none_returns_none():
    assert from_json(None) is None


def test_from_json_invalid_json_returns_none():
    assert from_json("not-json{{{") is None


def test_from_json_missing_symbol_returns_none():
    raw = json.dumps({"schema_version": 2, "ts_ms": 1760000000000})
    assert from_json(raw) is None


# ─── to_json includes v2 keys ─────────────────────────────────────────────────

def test_to_json_includes_all_v2_keys():
    snap = _make_v2_snap(
        long_short_ratio=1.4
        liq_imbalance_z=2.5
        market_breadth_ret_24h=0.01
    )
    d = json.loads(snap.to_json())
    required_v2_keys = {
        "long_short_ratio"
        "long_short_ratio_z"
        "taker_buy_sell_imbalance"
        "liq_buy_notional_1m"
        "liq_sell_notional_1m"
        "liq_imbalance_z"
        "market_breadth_ret_24h"
        "market_breadth_volume_z"
        "leader_btc_eth_confirm"
    }
    missing = required_v2_keys - set(d.keys())
    assert not missing, f"Missing v2 keys in JSON: {missing}"
