"""
tests/test_trade_profile_router.py
====================================
Unit-тесты для TradeProfileRouter и TradeProfile DTO.

Covers:
  - Выбор профиля по regime_bucket
  - Symbol-specific override выигрывает над default
  - Запрещённые kinds → DENY с reason_code
  - Canary / shadow логика через ENV
  - vol-scaling risk_multiplier
  - build_signal_profile_meta()
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from services.trade_profile_router import (
    TradeProfileRouter,
    TradeProfile,
    ProfileDecision,
    build_signal_profile_meta,
    _BUILTIN_PROFILES,
    _REGIME_PROFILE_MAP,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def router_shadow():
    """Router в shadow-режиме (ENFORCE=0, MODE=SHADOW)."""
    with patch.dict(os.environ, {
        "TRADE_PROFILE_ROUTER_ENABLED": "1",
        "TRADE_PROFILE_MODE": "SHADOW",
        "TRADE_PROFILE_CANARY_SHARE_TREND": "0.0",
        "TRADE_PROFILE_CANARY_SHARE_RANGE": "0.0",
        "TRADE_PROFILE_CANARY_SHARE_THIN": "0.0",
    }):
        yield TradeProfileRouter()


@pytest.fixture()
def router_enforce():
    """Router в enforce-режиме с 100% canary для теста."""
    with patch.dict(os.environ, {
        "TRADE_PROFILE_ROUTER_ENABLED": "1",
        "TRADE_PROFILE_MODE": "ENFORCE",
        "TRADE_PROFILE_CANARY_SHARE_TREND": "1.0",
        "TRADE_PROFILE_CANARY_SHARE_RANGE": "1.0",
        "TRADE_PROFILE_CANARY_SHARE_THIN": "0.0",
    }):
        yield TradeProfileRouter()


# ---------------------------------------------------------------------------
# Базовый выбор профиля
# ---------------------------------------------------------------------------

def test_trend_breakout_selected(router_enforce):
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="trend", kind="breakout")
    assert dec.profile.name == "trend_breakout_v1"
    assert dec.allowed is True


def test_range_absorption_selected(router_enforce):
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="range", kind="absorption")
    assert dec.profile.name == "range_absorption_v1"
    assert dec.allowed is True


def test_thin_defensive_selected(router_enforce):
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="thin", kind="extreme")
    assert dec.profile.name == "thin_defensive_v1"
    # thin_defensive_v1.mode == SHADOW_BY_DEFAULT → mode должен быть SHADOW
    assert dec.mode == "SHADOW"


def test_mixed_fallback_default(router_enforce):
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="mixed", kind="breakout")
    assert dec.profile.name == "default_v1"


# ---------------------------------------------------------------------------
# Kind gates
# ---------------------------------------------------------------------------

def test_breakout_denied_in_range_profile(router_enforce):
    """range_absorption_v1 запрещает breakout."""
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="range", kind="breakout")
    assert dec.allowed is False
    assert dec.reason_code == "kind_denied_for_profile"


def test_absorption_denied_in_trend_if_not_allowed(router_enforce):
    """trend_breakout_v1 не включает absorption в allowed_kinds."""
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="trend", kind="absorption")
    assert dec.allowed is False
    assert dec.reason_code == "kind_not_allowed_for_regime"


def test_allowed_kind_passes(router_enforce):
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="trend", kind="obi_spike")
    assert dec.allowed is True


# ---------------------------------------------------------------------------
# Symbol-specific override
# ---------------------------------------------------------------------------

def test_symbol_override_wins(router_enforce):
    """Symbol-specific profile override должен выиграть над default."""
    custom_profile = {
        "name": "custom_btc_v1",
        "regime_bucket": "trend",
        "allowed_kinds": ["breakout"],
        "deny_kinds": [],
        "min_p_edge": 0.65,
        "min_confidence": 0.67,
        "max_expected_slippage_bps": 5.0,
        # flat stop_atr_mult → used for all classes
        "stop_atr_mult": 0.8,
        # flat max_zone_bp → used for all classes
        "max_zone_bp": 12.0,
        "tp_rr": "1.5,2.5,4.0",
        "tp1_atr_mult": 0.7,
        "trailing_profile": "rocket_v1",
        "execution_policy": "MAKER_FIRST",
        # flat risk_multiplier → used for all tiers
        "risk_multiplier": 0.9,
        "min_net_edge_bps": 4.0,
        "mode": "LIVE",
        "reason_code": "btc_symbol_override",
    }
    overrides = {
        "cfg:trade_profile:BTCUSDT:trend:breakout": custom_profile,
    }
    dec = router_enforce.route(
        symbol="BTCUSDT",
        regime_bucket="trend",
        kind="breakout",
        overrides=overrides,
    )
    assert dec.profile.name == "custom_btc_v1"
    assert dec.profile.min_p_edge == 0.65
    assert dec.profile.trailing_profile == "rocket_v1"
    # flat stop_atr_mult → всем классам одинаковое значение
    assert dec.profile.stop_atr_mult_majors == 0.8
    # flat risk_multiplier → все tier одинаковые
    assert dec.profile.risk_multiplier_tier_b == 0.9


def test_default_override_fallback(router_enforce):
    """Если symbol override нет, но есть default override — использовать его."""
    default_profile = {
        "name": "default_trend_override_v1",
        "regime_bucket": "trend",
        "allowed_kinds": ["breakout", "obi_spike"],
        "deny_kinds": [],
        "min_p_edge": 0.58,
        "min_confidence": 0.60,
        "max_expected_slippage_bps": 12.0,
        # tiered stop_atr_mult
        "stop_atr_mult": {"majors": 0.90, "alts": 0.95, "memes": 1.10},
        # tiered max_zone_bp
        "max_zone_bp": {"majors": 10.0, "alts": 14.0, "memes": 20.0},
        "tp_rr": "1.3,2.1,3.1",
        "tp1_atr_mult": 0.85,
        "trailing_profile": "trend_runner_v1",
        "execution_policy": "MAKER_FIRST",
        # tiered risk_multiplier
        "risk_multiplier": {"tier_A": 0.95, "tier_B": 0.85, "tier_C": 0.50},
        "min_net_edge_bps": 3.0,
        "mode": "LIVE",
        "reason_code": "default_trend_redis",
    }
    overrides = {
        "cfg:trade_profile:default:trend:breakout": default_profile,
    }
    dec = router_enforce.route(
        symbol="SOLUSDT",
        regime_bucket="trend",
        kind="breakout",
        overrides=overrides,
    )
    assert dec.profile.name == "default_trend_override_v1"
    assert dec.profile.stop_atr_mult_majors == 0.90
    assert dec.profile.max_zone_bp_alts == 14.0
    assert dec.profile.risk_multiplier_tier_a == 0.95
    assert dec.profile.risk_multiplier_tier_c == 0.50


# ---------------------------------------------------------------------------
# Router disabled
# ---------------------------------------------------------------------------

def test_router_disabled_allows_all():
    with patch.dict(os.environ, {"TRADE_PROFILE_ROUTER_ENABLED": "0"}):
        r = TradeProfileRouter()
        dec = r.route(symbol="ANY", regime_bucket="thin", kind="breakout")
        assert dec.allowed is True
        assert dec.reason_code == "router_disabled"


# ---------------------------------------------------------------------------
# Shadow mode
# ---------------------------------------------------------------------------

def test_shadow_mode_allowed_but_shadow(router_shadow):
    """Shadow-mode: allowed=True но mode=SHADOW."""
    dec = router_shadow.route(symbol="BTCUSDT", regime_bucket="trend", kind="breakout")
    assert dec.allowed is True
    assert dec.mode == "SHADOW"


# ---------------------------------------------------------------------------
# build_signal_profile_meta
# ---------------------------------------------------------------------------

def test_build_meta_basic(router_enforce):
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="trend", kind="breakout")
    meta = build_signal_profile_meta(dec, symbol_tier="A", symbol_class="majors")
    assert meta["trade_profile"] == "trend_breakout_v1"
    assert meta["execution_policy"] == "MAKER_FIRST"
    # tier_A для trend = 1.10 — допустимо выше 1.0
    assert 0 < meta["risk_multiplier"] <= 2.0
    assert meta["risk_multiplier"] == pytest.approx(1.10, abs=0.01)
    assert meta["regime_bucket"] == "trend"
    # class-specific fields
    assert "stop_atr_mult" in meta
    assert "max_zone_bp" in meta
    assert meta["stop_atr_mult"] == pytest.approx(0.85, abs=0.01)   # majors
    assert meta["max_zone_bp"] == pytest.approx(12.0, abs=0.01)     # majors
    assert meta["min_confidence"] == pytest.approx(0.58, abs=0.01)


def test_build_meta_vol_scaling(router_enforce):
    """Волатильный скейлинг уменьшает risk_multiplier."""
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="trend", kind="breakout")
    meta_normal = build_signal_profile_meta(dec, symbol_tier="A")
    meta_vol = build_signal_profile_meta(
        dec, symbol_tier="A",
        realized_vol_bps=200.0,  # реализованная vol в 2x выше target
        target_vol_bps=100.0,
    )
    # При realized > target → risk_multiplier должен быть < 1.0
    assert meta_vol["risk_multiplier"] <= meta_normal["risk_multiplier"]


def test_build_meta_tier_c_reduces_risk(router_enforce):
    """Tier C уменьшает risk_multiplier относительно Tier A."""
    dec = router_enforce.route(symbol="BTCUSDT", regime_bucket="trend", kind="breakout")
    meta_a = build_signal_profile_meta(dec, symbol_tier="A")
    meta_c = build_signal_profile_meta(dec, symbol_tier="C")
    assert meta_c["risk_multiplier"] < meta_a["risk_multiplier"]


# ---------------------------------------------------------------------------
# Thin profile — всегда shadow пока нет статистики
# ---------------------------------------------------------------------------

def test_thin_always_shadow_regardless_of_global_mode(router_enforce):
    """thin_defensive_v1.mode == SHADOW_BY_DEFAULT → даже в ENFORCE mode = SHADOW."""
    dec = router_enforce.route(symbol="1000PEPEUSDT", regime_bucket="thin", kind="extreme")
    assert dec.mode == "SHADOW"
    assert dec.profile.name == "thin_defensive_v1"


# ---------------------------------------------------------------------------
# Все профили присутствуют в каталоге
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", [
    "trend_breakout_v1",
    "range_absorption_v1",
    "thin_defensive_v1",
    "high_vol_breakout_v1",
    "default_v1",
])
def test_builtin_profiles_present(name: str):
    assert name in _BUILTIN_PROFILES


@pytest.mark.parametrize("bucket", ["trend", "range", "thin", "mixed"])
def test_all_buckets_mapped(bucket: str):
    assert bucket in _REGIME_PROFILE_MAP
    profile_name = _REGIME_PROFILE_MAP[bucket]
    assert profile_name in _BUILTIN_PROFILES
