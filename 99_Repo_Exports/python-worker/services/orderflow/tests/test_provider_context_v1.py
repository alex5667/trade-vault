"""Tests for provider_context.py — snapshot parsing and reader contract."""
from __future__ import annotations

import json
import pytest
from services.orderflow.provider_context import (
    ProviderContextSnapshot
    ctx_key
    from_dict
    from_json
)

_VALID = {
    "schema_version": 1
    "symbol": "BTCUSDT"
    "ts_ms": 1714000000000
    "provider_global_mcap": 2500e9
    "provider_total_volume": 120e9
    "provider_btc_dominance": 54.2
    "provider_eth_dominance": 17.1
    "mcap_disagreement_bps": 80.0
    "volume_disagreement_bps": 50.0
    "btc_dom_disagreement_bps": 20.0
    "provider_quality": "ok"
    "provider_top_gainer": 0
    "provider_top_loser": 0
    "provider_rel_strength_24h": 2.5
    "provider_volume_mcap_ratio": 0.048
    "quality_status": "OK"
}


def test_from_dict_valid():
    snap = from_dict(_VALID)
    assert snap is not None
    assert isinstance(snap, ProviderContextSnapshot)
    assert snap.symbol == "BTCUSDT"
    assert snap.provider_global_mcap == 2500e9
    assert snap.provider_btc_dominance == pytest.approx(54.2)
    assert snap.quality_status == "OK"


def test_from_dict_missing_symbol_returns_none():
    d = dict(_VALID)
    d.pop("symbol")
    assert from_dict(d) is None


def test_from_dict_empty_symbol_returns_none():
    d = dict(_VALID)
    d["symbol"] = ""
    assert from_dict(d) is None


def test_from_json_bytes():
    raw = json.dumps(_VALID).encode()
    snap = from_json(raw)
    assert snap is not None
    assert snap.symbol == "BTCUSDT"


def test_from_json_none_returns_none():
    assert from_json(None) is None


def test_from_json_invalid_json_returns_none():
    assert from_json("{bad json}") is None


def test_from_json_non_dict_returns_none():
    assert from_json(json.dumps([1, 2, 3])) is None


def test_float_fields_coerce_strings():
    d = dict(_VALID)
    d["provider_global_mcap"] = "2500000000000.00"
    d["mcap_disagreement_bps"] = "80.0"
    snap = from_dict(d)
    assert snap is not None
    assert snap.provider_global_mcap == pytest.approx(2500e9)


def test_nan_inf_coerced_to_zero():
    d = dict(_VALID)
    d["provider_global_mcap"] = float("nan")
    d["provider_total_volume"] = float("inf")
    snap = from_dict(d)
    assert snap is not None
    assert snap.provider_global_mcap == 0.0
    assert snap.provider_total_volume == 0.0


def test_ctx_key():
    assert ctx_key("btcusdt") == "ctx:provider:BTCUSDT"
    assert ctx_key("ETHUSDT") == "ctx:provider:ETHUSDT"


def test_schema_version_defaults():
    d = dict(_VALID)
    d.pop("schema_version")
    snap = from_dict(d)
    assert snap is not None
    assert snap.schema_version == 1


def test_quality_status_defaults_unknown():
    d = dict(_VALID)
    d.pop("quality_status")
    snap = from_dict(d)
    assert snap is not None
    assert snap.quality_status == "UNKNOWN"


def test_top_gainer_loser_int():
    d = dict(_VALID)
    d["provider_top_gainer"] = 1
    d["provider_top_loser"] = 0
    snap = from_dict(d)
    assert snap.provider_top_gainer == 1
    assert snap.provider_top_loser == 0


def test_provider_quality_fallback():
    d = dict(_VALID)
    d["provider_quality"] = "fallback"
    snap = from_dict(d)
    assert snap.provider_quality == "fallback"
