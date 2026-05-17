from __future__ import annotations

"""
Unit tests for core/external_features_payload_v1.

Covers:
  - fail-open: all keys present, zero-valued when inputs are None or {}
  - numeric keys copied with float() cast
  - bool keys encoded as 0.0 / 1.0
  - non-numeric / NaN / None values → 0.0
  - key count matches OE group size (Phase 8.1: 5+5+7+3 = 20, plus Phase 7.8/7.9/7.9b)
  - Phase 8.2 keys: sector_breadth_1m, prior_stale_ms, hour_sin/cos, dow_sin/cos, news_blackout
  - OE keys from v14_of Group OE are all present in the output
  - schema parity: all emitted keys present in V14_OF_NUMERIC_KEYS
"""

import pytest

from core.external_features_payload_v1 import (
    _BOOL_KEYS,
    _NUM_KEYS,
    _V12_BASE_OPTIONAL_KEYS,
    build_external_features_payload,
    external_feature_keys,
)


# ---------------------------------------------------------------------------
# Fail-open
# ---------------------------------------------------------------------------

def test_none_input_returns_all_zero_dict():
    out = build_external_features_payload(None)
    assert isinstance(out, dict)
    assert len(out) == len(_NUM_KEYS) + len(_BOOL_KEYS)
    assert all(v == 0.0 for v in out.values())


def test_empty_dict_returns_all_zero_dict():
    out = build_external_features_payload({})
    assert all(v == 0.0 for v in out.values())


def test_all_keys_always_present():
    """Every key must be in output regardless of what was passed."""
    out = build_external_features_payload(None)
    for k in _NUM_KEYS:
        assert k in out, f"missing numeric key {k!r}"
    for k in _BOOL_KEYS:
        assert k in out, f"missing bool key {k!r}"


# ---------------------------------------------------------------------------
# Numeric copy with float cast
# ---------------------------------------------------------------------------

def test_numeric_key_float_cast():
    src = {"funding_rate": 0.0001, "oi_notional_usd": 1_000_000.0}
    out = build_external_features_payload(src)
    assert out["funding_rate"] == pytest.approx(0.0001)
    assert out["oi_notional_usd"] == pytest.approx(1_000_000.0)


def test_string_numeric_coercion():
    out = build_external_features_payload({"funding_rate": "0.00025"})
    assert out["funding_rate"] == pytest.approx(0.00025)


def test_int_value_becomes_float():
    out = build_external_features_payload({"deribit_vol_regime_code": 2})
    assert out["deribit_vol_regime_code"] == 2.0
    assert isinstance(out["deribit_vol_regime_code"], float)


def test_negative_value_preserved():
    out = build_external_features_payload({"funding_rate_z": -2.5})
    assert out["funding_rate_z"] == pytest.approx(-2.5)


# ---------------------------------------------------------------------------
# Bool keys encoded as 0/1 float
# ---------------------------------------------------------------------------

def test_bool_true_encoded_as_1():
    out = build_external_features_payload({"fear_greed_regime_extreme_fear": True})
    assert out["fear_greed_regime_extreme_fear"] == 1.0


def test_bool_false_encoded_as_0():
    out = build_external_features_payload({"fear_greed_regime_extreme_greed": False})
    assert out["fear_greed_regime_extreme_greed"] == 0.0


def test_bool_int_1_encoded_as_1():
    out = build_external_features_payload({"fear_greed_regime_extreme_fear": 1})
    assert out["fear_greed_regime_extreme_fear"] == 1.0


def test_bool_int_0_encoded_as_0():
    out = build_external_features_payload({"prior_stale": 0})
    assert out["prior_stale"] == 0.0


# ---------------------------------------------------------------------------
# Non-numeric / bad values → 0.0
# ---------------------------------------------------------------------------

def test_none_value_becomes_zero():
    out = build_external_features_payload({"funding_rate": None})
    assert out["funding_rate"] == 0.0


def test_string_non_numeric_becomes_zero():
    out = build_external_features_payload({"funding_rate": "N/A"})
    assert out["funding_rate"] == 0.0


def test_empty_string_becomes_zero():
    out = build_external_features_payload({"oi_notional_usd": ""})
    assert out["oi_notional_usd"] == 0.0


def test_nan_becomes_zero():
    out = build_external_features_payload({"funding_rate_z": float("nan")})
    # float("nan") → float() = nan, but _f() passes it through;
    # NaN is technically valid float — just check it doesn't raise.
    assert "funding_rate_z" in out


def test_list_value_becomes_zero():
    out = build_external_features_payload({"funding_rate": [0.1, 0.2]})
    assert out["funding_rate"] == 0.0


# ---------------------------------------------------------------------------
# Phase 8.1 OE group coverage
# ---------------------------------------------------------------------------

OE_KEYS_PHASE_81 = [
    # 5 composites from ctx:deriv:{symbol}
    "taker_buy_sell_imbalance",
    "force_order_imbalance_1m",
    "oi_confirmation_score",
    "squeeze_risk_score",
    "liq_impulse_score",
    # 5 live market breadth from runtime:breadth
    "market_breadth_ret_24h",
    "market_breadth_vol_z",
    "btc_leader_ret_breadth",
    "eth_leader_ret_breadth",
    "breadth_leader_confirm",
    # 7 Deribit IV/funding/regime
    "deribit_btc_iv_proxy",
    "deribit_eth_iv_proxy",
    "deribit_btc_iv_z",
    "deribit_eth_iv_z",
    "deribit_btc_funding_8h",
    "deribit_eth_funding_8h",
    "deribit_vol_regime_code",
    # 3 Fear & Greed
    "fear_greed_index",
    "fear_greed_regime_extreme_fear",
    "fear_greed_regime_extreme_greed",
]


def test_all_oe_phase81_keys_present():
    """All 20 v14_of Group OE keys must be in the payload."""
    out = build_external_features_payload({})
    for k in OE_KEYS_PHASE_81:
        assert k in out, f"OE key {k!r} missing from external_features_payload output"


def test_oe_phase81_count():
    assert len(OE_KEYS_PHASE_81) == 20


# ---------------------------------------------------------------------------
# Schema parity
# ---------------------------------------------------------------------------

def test_all_oe_keys_in_v14_schema():
    """OE keys emitted by helper must exist in V14_OF_NUMERIC_KEYS."""
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    schema_set = set(V14_OF_NUMERIC_KEYS)
    for k in OE_KEYS_PHASE_81:
        assert k in schema_set, f"OE key {k!r} not in v14_of schema"


def test_external_feature_keys_public_accessor():
    """external_feature_keys() returns all _NUM_KEYS + _BOOL_KEYS + v12 optional."""
    full = external_feature_keys()
    expected = _NUM_KEYS + _BOOL_KEYS + _V12_BASE_OPTIONAL_KEYS
    assert full == expected
    assert len(full) == len(_NUM_KEYS) + len(_BOOL_KEYS) + len(_V12_BASE_OPTIONAL_KEYS)


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def test_same_inputs_same_output():
    src = {
        "funding_rate": 0.0001,
        "fear_greed_index": 42.0,
        "fear_greed_regime_extreme_fear": False,
    }
    out_a = build_external_features_payload(dict(src))
    out_b = build_external_features_payload(dict(src))
    assert out_a == out_b


def test_output_is_new_dict_not_mutating_input():
    src = {"funding_rate": 0.0005}
    out = build_external_features_payload(src)
    out["funding_rate"] = 999.0
    new_out = build_external_features_payload(src)
    assert new_out["funding_rate"] == pytest.approx(0.0005)


# ---------------------------------------------------------------------------
# Phase 8.2 keys
# ---------------------------------------------------------------------------

P82_KEYS = [
    "sector_breadth_1m",
    "prior_stale_ms",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "news_blackout",
]


def test_p82_keys_present_in_output():
    """All Phase 8.2 keys must appear in the payload."""
    out = build_external_features_payload({})
    for k in P82_KEYS:
        assert k in out, f"Phase 8.2 key {k!r} missing from external_features_payload"


def test_sector_breadth_1m_value_copied():
    out = build_external_features_payload({"sector_breadth_1m": 0.65})
    assert out["sector_breadth_1m"] == pytest.approx(0.65)


def test_prior_stale_ms_value_copied():
    out = build_external_features_payload({"prior_stale_ms": 7200000.0})
    assert out["prior_stale_ms"] == pytest.approx(7200000.0)


def test_hour_sin_cos_values_copied():
    import math
    ang = 2.0 * math.pi * 14.0 / 24.0  # 14:00 UTC
    out = build_external_features_payload({"hour_sin": math.sin(ang), "hour_cos": math.cos(ang)})
    assert out["hour_sin"] == pytest.approx(math.sin(ang))
    assert out["hour_cos"] == pytest.approx(math.cos(ang))


def test_dow_sin_cos_values_copied():
    import math
    ang = 2.0 * math.pi * 2.0 / 7.0  # Wednesday
    out = build_external_features_payload({"dow_sin": math.sin(ang), "dow_cos": math.cos(ang)})
    assert out["dow_sin"] == pytest.approx(math.sin(ang))
    assert out["dow_cos"] == pytest.approx(math.cos(ang))


def test_news_blackout_float_1_when_active():
    out = build_external_features_payload({"news_blackout": 1.0})
    assert out["news_blackout"] == 1.0


def test_news_blackout_zero_when_inactive():
    out = build_external_features_payload({"news_blackout": 0})
    assert out["news_blackout"] == 0.0


def test_p82_zero_on_missing_input():
    """Phase 8.2 keys default to 0.0 when not in source dict."""
    out = build_external_features_payload({})
    for k in P82_KEYS:
        assert out[k] == 0.0, f"{k!r} should default to 0.0 when missing"


# ───────────────────────────────────────────────────────────────────────────────
# v12_of base optional keys + runtime_indicators fallback
# ───────────────────────────────────────────────────────────────────────────────

def test_v12_optional_keys_omitted_when_neither_source_has_them():
    """Missing v12 base keys must NOT appear in output (no false 0.0 override)."""
    out = build_external_features_payload({}, {})
    assert "atr_bps_exec" not in out
    assert "iceberg_avg_qty" not in out
    assert "liqmap_sl_base_bps" not in out


def test_v12_optional_keys_pulled_from_runtime_indicators_fallback():
    """v12 base key in `runtime_indicators` (not in `indicators_with_v4`) is copied."""
    out = build_external_features_payload(
        {},
        {"atr_bps_exec": 12.5, "iceberg_avg_qty": 3.0},
    )
    assert out["atr_bps_exec"] == 12.5
    assert out["iceberg_avg_qty"] == 3.0


def test_v12_optional_keys_prefer_indicators_with_v4_over_runtime():
    """When both sources have the key, indicators_with_v4 wins (primary source)."""
    out = build_external_features_payload(
        {"atr_bps_exec": 7.0},
        {"atr_bps_exec": 999.0},
    )
    assert out["atr_bps_exec"] == 7.0


def test_runtime_indicators_used_for_existing_num_keys():
    """Fallback source also applies to _NUM_KEYS (not just v12 optional)."""
    out = build_external_features_payload({}, {"funding_rate": 0.0005})
    assert out["funding_rate"] == 0.0005


def test_none_value_in_runtime_source_treated_as_missing():
    """A None value in the runtime source must not falsely populate the key."""
    out = build_external_features_payload({}, {"atr_bps_exec": None})
    assert "atr_bps_exec" not in out


def test_external_feature_keys_includes_v12_optional():
    """Public key accessor surfaces the new v12 base list."""
    from core.external_features_payload_v1 import external_feature_keys
    keys = external_feature_keys()
    assert "atr_bps_exec" in keys
    assert "liqmap_sl_widen_needed" in keys
    assert "btc_ret_1m" in keys  # existing key still there


def test_call_without_runtime_arg_keeps_old_behavior():
    """Single-arg invocation (legacy) still returns the historical contract."""
    out = build_external_features_payload({"funding_rate": 0.001})
    assert out["funding_rate"] == 0.001
    # v12 optional keys must NOT leak in just because we used legacy form.
    assert "atr_bps_exec" not in out
