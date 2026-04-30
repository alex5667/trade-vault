"""Tests for DefiLlama context snapshot reader."""
import json
import pytest
from services.orderflow.defillama_context import (
    DefiLlamaContextSnapshot
    from_json
    from_dict
    ctx_key
    SCHEMA_VERSION
)


def _sample_payload(**overrides):
    base = {
        "schema_version": 1
        "symbol": "SOLUSDT"
        "chain": "Solana"
        "ts_ms": 1700000000000
        "stablecoin_mcap_total": 130_000_000_000.0
        "stablecoin_mcap_delta_1d": 100_000_000.0
        "stablecoin_mcap_delta_7d": 500_000_000.0
        "stablecoin_risk_regime": "neutral"
        "chain_tvl_usd": 5_000_000_000.0
        "chain_tvl_delta_1d_pct": 0.3
        "dex_volume_24h_usd": 2_000_000_000.0
        "dex_volume_delta_1d_pct": 5.0
        "dex_volume_spike_z": 1.5
        "fees_24h_usd": 50_000_000.0
        "revenue_24h_usd": 20_000_000.0
        "fees_revenue_momentum": 0.1
        "defillama_perps_oi_usd": 1_000_000_000.0
        "defillama_perps_oi_delta_1d_pct": 2.5
        "quality_status": "OK"
    }
    base.update(overrides)
    return base


def test_snapshot_from_json_roundtrip():
    payload = _sample_payload()
    raw = json.dumps(payload)
    snap = from_json(raw)
    assert snap is not None
    assert snap.symbol == "SOLUSDT"
    assert snap.chain == "Solana"
    assert snap.stablecoin_mcap_total == 130_000_000_000.0
    assert snap.quality_status == "OK"
    # Roundtrip
    j2 = snap.to_json()
    snap2 = from_json(j2)
    assert snap2 == snap


def test_snapshot_from_bytes():
    payload = _sample_payload()
    raw = json.dumps(payload).encode("utf-8")
    snap = from_json(raw)
    assert snap is not None
    assert snap.symbol == "SOLUSDT"


def test_snapshot_missing_fields_defaults():
    payload = {"symbol": "ETHUSDT", "chain": "Ethereum", "ts_ms": 123}
    snap = from_dict(payload)
    assert snap is not None
    assert snap.stablecoin_mcap_total == 0.0
    assert snap.dex_volume_spike_z == 0.0
    assert snap.quality_status == "UNKNOWN"
    assert snap.stablecoin_risk_regime == "unknown"


def test_bad_json_returns_none():
    assert from_json(None) is None
    assert from_json("not json") is None
    assert from_json(42) is None
    assert from_json(b"{{broken") is None
    assert from_json("[]") is None


def test_empty_symbol_returns_none():
    payload = {"symbol": "", "chain": "Solana"}
    assert from_dict(payload) is None


def test_ctx_key():
    assert ctx_key("SOLUSDT") == "ctx:defillama:SOLUSDT"
    assert ctx_key("solusdt") == "ctx:defillama:SOLUSDT"
    assert ctx_key("ETHUSDT", prefix="test:") == "test:ETHUSDT"


def test_nan_inf_fields_default():
    payload = _sample_payload(
        stablecoin_mcap_total=float("nan")
        dex_volume_spike_z=float("inf")
    )
    snap = from_dict(payload)
    assert snap is not None
    assert snap.stablecoin_mcap_total == 0.0
    assert snap.dex_volume_spike_z == 0.0
