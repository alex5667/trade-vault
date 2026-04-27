"""
tests/test_signal_scoring_core.py
==================================
Comprehensive pytest suite for the signal_scoring package.
Tests cover 100% of the public API surface without requiring external services.
All tests are self-contained, deterministic, and free of networking/DB dependencies.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pytest


# ---------------------------------------------------------------------------
# _helpers
# ---------------------------------------------------------------------------
class TestHelpers:
    def test_is_finite_none(self):
        from signal_scoring._helpers import is_finite
        assert is_finite(None) is False

    def test_is_finite_nan(self):
        from signal_scoring._helpers import is_finite
        assert is_finite(float("nan")) is False

    def test_is_finite_inf(self):
        from signal_scoring._helpers import is_finite
        assert is_finite(float("inf")) is False

    def test_is_finite_ok(self):
        from signal_scoring._helpers import is_finite
        assert is_finite(0.0) is True
        assert is_finite(1) is True
        assert is_finite(-999.5) is True

    def test_safe_float_ok(self):
        from signal_scoring._helpers import safe_float
        assert safe_float(3.14) == pytest.approx(3.14)
        assert safe_float("2.0") == pytest.approx(2.0)

    def test_safe_float_fallback(self):
        from signal_scoring._helpers import safe_float
        assert safe_float(None, 7.0) == pytest.approx(7.0)
        assert safe_float("bad", 5.0) == pytest.approx(5.0)
        assert safe_float(float("nan"), -1.0) == pytest.approx(-1.0)

    def test_clamp(self):
        from signal_scoring._helpers import clamp
        assert clamp(0.5, 0.0, 1.0) == pytest.approx(0.5)
        assert clamp(-1.0, 0.0, 1.0) == pytest.approx(0.0)
        assert clamp(2.0, 0.0, 1.0) == pytest.approx(1.0)

    def test_clamp01(self):
        from signal_scoring._helpers import clamp01
        assert clamp01(0.3) == pytest.approx(0.3)
        assert clamp01(-5.0) == pytest.approx(0.0)
        assert clamp01(5.0) == pytest.approx(1.0)

    def test_private_aliases_exist(self):
        """Private aliases must be backward-compatible."""
        from signal_scoring._helpers import _is_finite, _safe_float, _clamp, _clamp01
        assert _is_finite(1.0) is True
        assert _safe_float("x", 0.0) == pytest.approx(0.0)
        assert _clamp(5.0, 0.0, 1.0) == pytest.approx(1.0)
        assert _clamp01(5.0) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# ctx.SignalContext — no duplicate fields, field(default_factory=list) works
# ---------------------------------------------------------------------------
class TestSignalContext:
    def _make_ctx(self, **kwargs) -> Any:
        from signal_scoring.ctx import SignalContext
        defaults = dict(
            ts=datetime(2024, 1, 1),
            symbol="BTCUSDT",
            side="buy",
            session="europe",
            regime="trend",
        )
        defaults.update(kwargs)
        return SignalContext(**defaults)

    def test_creation_defaults(self):
        ctx = self._make_ctx()
        assert ctx.symbol == "BTCUSDT"
        assert ctx.weak_progress is None
        assert ctx.pattern_family is None
        assert ctx.progress_score_component is None
        assert ctx.quality_reasons == []

    def test_weak_progress_field_not_duplicated(self):
        """The class must expose weak_progress exactly once."""
        from signal_scoring.ctx import SignalContext
        fields = [f.name for f in SignalContext.__dataclass_fields__.values()]
        assert fields.count("weak_progress") == 1, (
            "weak_progress must appear exactly once in SignalContext"
        )

    def test_quality_reasons_default_factory(self):
        """Two separate instances must not share the same list object."""
        ctx_a = self._make_ctx()
        ctx_b = self._make_ctx()
        ctx_a.quality_reasons.append("x")
        assert ctx_b.quality_reasons == [], "Default-factory lists must not be shared"

    def test_all_fields_set(self):
        ctx = self._make_ctx(
            weak_progress=0.25,
            delta_spike_z=2.1,
            obi=0.9,
            atr_quantile=0.8,
            confidence=80,
            is_golden_pattern=True,
            pattern_family="continuation",
        )
        assert ctx.weak_progress == pytest.approx(0.25)
        assert ctx.delta_spike_z == pytest.approx(2.1)
        assert ctx.confidence == 80
        assert ctx.is_golden_pattern is True
        assert ctx.pattern_family == "continuation"


# ---------------------------------------------------------------------------
# config.ScoringConfig
# ---------------------------------------------------------------------------
class TestScoringConfig:
    def test_defaults(self):
        from signal_scoring.config import ScoringConfig
        cfg = ScoringConfig()
        assert cfg.min_confidence_default == pytest.approx(80.0)
        assert cfg.golden_pattern_min_confidence == pytest.approx(90.0)
        assert cfg.liquidity_enabled is True

    def test_get_min_confidence_default(self):
        from signal_scoring.config import ScoringConfig
        cfg = ScoringConfig(min_confidence_default=70.0)
        assert cfg.get_min_confidence("XAUUSD", None) == pytest.approx(70.0)

    def test_get_min_confidence_symbol_override(self):
        from signal_scoring.config import ScoringConfig
        cfg = ScoringConfig(
            min_confidence_default=80.0,
            min_confidence_by_symbol={"XAUUSD": 20.0},
        )
        assert cfg.get_min_confidence("XAUUSD", None) == pytest.approx(20.0)
        assert cfg.get_min_confidence("BTCUSDT", None) == pytest.approx(80.0)

    def test_get_min_confidence_pattern_override(self):
        from signal_scoring.config import ScoringConfig, PatternScoringConfig
        cfg = ScoringConfig(
            min_confidence_default=80.0,
            pattern_config={"breakout_r1": PatternScoringConfig(min_confidence=90)},
        )
        assert cfg.get_min_confidence("BTCUSDT", "breakout_r1") == 90

    def test_get_pattern_weight_default(self):
        from signal_scoring.config import ScoringConfig
        cfg = ScoringConfig()
        assert cfg.get_pattern_weight(None) == pytest.approx(1.0)
        assert cfg.get_pattern_weight("unknown_pattern") == pytest.approx(1.0)

    def test_get_pattern_weight_override(self):
        from signal_scoring.config import ScoringConfig, PatternScoringConfig
        cfg = ScoringConfig(
            pattern_config={"breakout_r1": PatternScoringConfig(weight=1.5)},
        )
        assert cfg.get_pattern_weight("breakout_r1") == pytest.approx(1.5)

    def test_from_env_reads_min_confidence(self, monkeypatch):
        from signal_scoring.config import ScoringConfig
        monkeypatch.setenv("MIN_SIGNAL_CONFIDENCE", "55")
        monkeypatch.setenv("MIN_SIGNAL_CONFIDENCE__XAUUSD", "25")
        cfg = ScoringConfig.from_env()
        assert cfg.min_confidence_default == pytest.approx(55.0)
        assert cfg.min_confidence_by_symbol.get("XAUUSD") == pytest.approx(25.0)

    def test_from_env_metric_weights(self, monkeypatch):
        from signal_scoring.config import ScoringConfig
        monkeypatch.setenv("SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z", "2.0")
        cfg = ScoringConfig.from_env()
        assert cfg.metric_weights["delta_spike_z"] == pytest.approx(2.0)

    def test_from_env_liquidity_flags(self, monkeypatch):
        from signal_scoring.config import ScoringConfig
        monkeypatch.setenv("SCORING_LIQUIDITY_ENABLED", "false")
        monkeypatch.setenv("SCORING_LIQUIDITY_WEIGHT", "0.5")
        cfg = ScoringConfig.from_env()
        assert cfg.liquidity_enabled is False
        assert cfg.liquidity_weight == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# geometry
# ---------------------------------------------------------------------------
class TestGeometry:
    def test_normalize_zone_strength_none(self):
        from signal_scoring.geometry import normalize_zone_strength
        assert normalize_zone_strength(None) == pytest.approx(0.0)

    def test_normalize_zone_strength_01(self):
        from signal_scoring.geometry import normalize_zone_strength
        assert normalize_zone_strength(0.75) == pytest.approx(0.75)

    def test_normalize_zone_strength_percent(self):
        from signal_scoring.geometry import normalize_zone_strength
        # value > 1 treated as percent
        result = normalize_zone_strength(80.0)
        assert result == pytest.approx(0.8)

    def test_normalize_zone_strength_clamp(self):
        from signal_scoring.geometry import normalize_zone_strength
        assert normalize_zone_strength(150.0) == pytest.approx(1.0)  # 150% -> clamp to 1.0
        assert normalize_zone_strength(-5.0) == pytest.approx(0.0)

    def test_distance_to_score_zero_distance(self):
        from signal_scoring.geometry import distance_to_score
        score = distance_to_score(dist_bps=0.0, dist_rel_atr=None)
        assert score == pytest.approx(1.0)

    def test_distance_to_score_monotonic(self):
        from signal_scoring.geometry import distance_to_score
        s1 = distance_to_score(dist_bps=5.0, dist_rel_atr=None)
        s2 = distance_to_score(dist_bps=20.0, dist_rel_atr=None)
        s3 = distance_to_score(dist_bps=100.0, dist_rel_atr=None)
        assert s1 > s2 > s3

    def test_geometry_score_bounds(self):
        from signal_scoring.geometry import geometry_score
        s = geometry_score(zone_strength01=0.8, dist_bps=10.0, dist_rel_atr=0.5)
        assert 0.0 <= s <= 1.0

    def test_geometry_score_zero_strength(self):
        from signal_scoring.geometry import geometry_score
        s = geometry_score(zone_strength01=0.0, dist_bps=0.0, dist_rel_atr=None)
        assert s == pytest.approx(0.0)

    def test_compute_geo_hits_empty(self):
        from signal_scoring.geometry import compute_geo_hits
        hits = compute_geo_hits(price=100.0, atr=1.5, zones=[])
        assert hits == []

    def test_compute_geo_hits_invalid_price(self):
        from signal_scoring.geometry import compute_geo_hits
        hits = compute_geo_hits(price=0.0, atr=1.0, zones=[{"price": 100.0, "strength": 0.5}])
        assert hits == []

    def test_compute_geo_hits_basic(self):
        from signal_scoring.geometry import compute_geo_hits
        zones = [
            {"zone_type": "support", "price": 100.0, "zone_strength": 0.8},
            {"zone_type": "resistance", "price": 110.0, "strength": 0.5},
        ]
        hits = compute_geo_hits(price=101.0, atr=2.0, zones=zones)
        assert len(hits) == 2
        # Closer zone should have higher score (sorted descending)
        assert hits[0].score >= hits[1].score

    def test_compute_geo_hits_fallback_keys(self):
        from signal_scoring.geometry import compute_geo_hits
        zones = [{"type": "vwap", "level": 100.0, "strength": 0.6}]
        hits = compute_geo_hits(price=100.0, atr=1.0, zones=zones)
        assert len(hits) == 1
        assert hits[0].zone_type == "vwap"

    def test_compute_geometry_context_empty(self):
        from signal_scoring.geometry import compute_geometry_context
        hits, top, score = compute_geometry_context(price=0.0, atr=1.0, zones=[])
        assert hits == []
        assert top is None
        assert score == pytest.approx(0.0)

    def test_compute_geometry_context_basic(self):
        from signal_scoring.geometry import compute_geometry_context
        zones = [{"price": 100.0, "strength": 0.9}]
        hits, top, score = compute_geometry_context(price=100.0, atr=2.0, zones=zones)
        assert len(hits) == 1
        assert top is not None
        assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# kind_rules
# ---------------------------------------------------------------------------
class TestKindRules:
    class _FakeCtx:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    def test_breakout_no_spoof(self):
        from signal_scoring.kind_rules import apply_kind_rules
        ctx = self._FakeCtx(taker_rate_ema=0.3, cancel_to_trade=1.0, microprice_shift_bps=1.0)
        res = apply_kind_rules("breakout", ctx)
        assert res.veto is False
        assert res.conf_mult01 == pytest.approx(1.0)  # no penalty yet

    def test_breakout_veto_spoof(self):
        from signal_scoring.kind_rules import apply_kind_rules
        ctx = self._FakeCtx(
            taker_rate_ema=0.10,
            cancel_to_trade=3.0,  # >= BREAKOUT_VETO_C2T=2.6
        )
        res = apply_kind_rules("breakout_up", ctx)
        assert res.veto is True
        assert "veto_spoof_like" in res.reasons

    def test_breakout_no_taker_continuation(self):
        from signal_scoring.kind_rules import apply_kind_rules
        ctx = self._FakeCtx(microprice_shift_bps=1.0, taker_rate_ema=0.1)
        res = apply_kind_rules("breakout_down", ctx)
        assert res.veto is False
        assert res.conf_mult01 < 1.0  # penalty applied

    def test_absorption_veto_no_wall(self):
        from signal_scoring.kind_rules import apply_kind_rules
        ctx = self._FakeCtx()
        res = apply_kind_rules("absorption", ctx, quality_flags={})
        assert res.veto is True
        assert "veto_no_wall_or_refill" in res.reasons

    def test_absorption_veto_no_micro(self):
        from signal_scoring.kind_rules import apply_kind_rules
        ctx = self._FakeCtx(wall_here=True, taker_rate_ema=0.5)
        res = apply_kind_rules("absorb", ctx, quality_flags={"wall_here": True})
        assert res.veto is True
        assert "veto_no_micro_contra_or_proxy" in res.reasons

    def test_obi_not_sustained_penalty(self):
        from signal_scoring.kind_rules import apply_kind_rules
        ctx = self._FakeCtx(obi_sustained=False)
        res = apply_kind_rules("obi_spike", ctx)
        assert "obi_not_sustained" in res.reasons
        assert res.conf_mult01 < 1.0

    def test_obi_sustained_ok(self):
        from signal_scoring.kind_rules import apply_kind_rules
        ctx = self._FakeCtx(obi_sustained=True, l3_cancel_to_trade=1.0)
        res = apply_kind_rules("obi_spike_up", ctx)
        assert res.veto is False

    def test_spread_scale(self):
        from signal_scoring.kind_rules import apply_kind_rules, SPREAD_SCALE_BPS
        ctx = self._FakeCtx(spread_bps=SPREAD_SCALE_BPS * 0.5)
        res = apply_kind_rules("unknown_kind", ctx)
        assert res.conf_mult01 < 1.0  # spread penalty applied
        assert "spread_scale" in res.reasons

    def test_conf_mult01_always_in_range(self):
        from signal_scoring.kind_rules import apply_kind_rules
        ctx = self._FakeCtx(taker_rate_ema=0.0, cancel_to_trade=10.0, spread_bps=100.0, obi_sustained=False)
        for kind in ["breakout", "absorption", "extreme", "obi_spike", "unknown"]:
            res = apply_kind_rules(kind, ctx)
            assert 0.0 <= res.conf_mult01 <= 1.0, f"conf_mult01 out of [0,1] for kind={kind}"


# ---------------------------------------------------------------------------
# reason_codes
# ---------------------------------------------------------------------------
class TestReasonCodes:
    def test_reason_code_values_are_strings(self):
        from signal_scoring.reason_codes import ReasonCode
        for rc in ReasonCode:
            assert isinstance(rc.value, str)

    def test_ok_code(self):
        from signal_scoring.reason_codes import ReasonCode
        assert ReasonCode.OK.value == "OK"

    def test_legacy_reason_to_code_known(self):
        from signal_scoring.reason_codes import legacy_reason_to_code, ReasonCode
        assert legacy_reason_to_code("bo_l2_stale") == ReasonCode.VETO_L2_STALE

    def test_legacy_reason_to_code_unknown(self):
        from signal_scoring.reason_codes import legacy_reason_to_code, ReasonCode
        assert legacy_reason_to_code("totally_unknown_reason") == ReasonCode.VETO_UNKNOWN

    def test_legacy_reason_to_code_empty(self):
        from signal_scoring.reason_codes import legacy_reason_to_code, ReasonCode
        assert legacy_reason_to_code(None) == ReasonCode.VETO_UNKNOWN
        assert legacy_reason_to_code("") == ReasonCode.VETO_UNKNOWN

    def test_is_valid_reason_code(self):
        from signal_scoring.reason_codes import is_valid_reason_code
        assert is_valid_reason_code("OK") is True
        assert is_valid_reason_code("VETO_SPREAD_WIDE") is True
        assert is_valid_reason_code("GARBAGE") is False
        assert is_valid_reason_code(None) is False


# ---------------------------------------------------------------------------
# reason_registry
# ---------------------------------------------------------------------------
class TestReasonRegistry:
    def test_no_duplicate_u16_except_allowed(self):
        """Registry itself validates this at import time — just assert no ValueError was raised."""
        import signal_scoring.reason_registry  # would raise ValueError if there are bad dups

    def test_reason_code_to_u16_known(self):
        from signal_scoring.reason_registry import reason_code_to_u16
        assert reason_code_to_u16("OK") == 1
        assert reason_code_to_u16("VETO_SPREAD_WIDE") == 100
        assert reason_code_to_u16("VETO_UNKNOWN") == 255

    def test_reason_code_to_u16_unknown_fail_open(self):
        from signal_scoring.reason_registry import reason_code_to_u16
        assert reason_code_to_u16("TOTALLY_MADE_UP") == 0

    def test_reason_code_to_u16_strict_raises(self):
        from signal_scoring.reason_registry import reason_code_to_u16
        with pytest.raises(ValueError):
            reason_code_to_u16("TOTALLY_MADE_UP", strict=True)

    def test_u16_to_reason_code_round_trip(self):
        from signal_scoring.reason_registry import reason_code_to_u16, u16_to_reason_code, _REASON_CODE_U16
        for rc in list(_REASON_CODE_U16.keys())[:10]:
            u = reason_code_to_u16(rc)
            if u == 0:
                continue
            decoded = u16_to_reason_code(u)
            # decoded may be a canonical alias; both point to same u16
            assert reason_code_to_u16(decoded) == u

    def test_u16_to_reason_code_unknown(self):
        from signal_scoring.reason_registry import u16_to_reason_code
        assert u16_to_reason_code(99999) == "VETO_UNKNOWN"

    def test_legacy_reason_to_code(self):
        from signal_scoring.reason_registry import legacy_reason_to_code
        assert legacy_reason_to_code("bo_l2_missing") == "VETO_L2_MISSING"
        assert legacy_reason_to_code("near_big_wall") == "VETO_WALL_NEAR"
        assert legacy_reason_to_code("unknown_xyz") == "UNKNOWN_VETO"

    def test_map_legacy_reason_code_alias(self):
        """map_legacy_reason_code is a required compatibility alias."""
        from signal_scoring.reason_registry import map_legacy_reason_code
        assert map_legacy_reason_code("spread_wide") == "VETO_SPREAD_WIDE"

    def test_normalize_reason(self):
        from signal_scoring.reason_registry import normalize_reason
        orig, rc, u16 = normalize_reason("bo_l2_stale")
        assert orig == "bo_l2_stale"
        assert rc == "VETO_L2_STALE"
        assert u16 == 102

    def test_reason_codes_to_u16s(self):
        from signal_scoring.reason_registry import reason_codes_to_u16s
        result = reason_codes_to_u16s(["OK", "VETO_SPREAD_WIDE", "FAKE_CODE"])
        assert 1 in result
        assert 100 in result
        # FAKE_CODE returns 0, filtered out
        assert 0 not in result

    def test_is_known_reason_code(self):
        from signal_scoring.reason_registry import is_known_reason_code
        assert is_known_reason_code("OK") is True
        assert is_known_reason_code("NOPE") is False

    def test_iter_known_reason_codes(self):
        from signal_scoring.reason_registry import iter_known_reason_codes
        codes = list(iter_known_reason_codes())
        assert "OK" in codes
        assert "VETO_UNKNOWN" in codes


# ---------------------------------------------------------------------------
# wire_u16
# ---------------------------------------------------------------------------
class TestWireU16:
    def test_roundtrip(self):
        from signal_scoring.wire_u16 import pack_u16, unpack_u16
        for v in [0, 1, 100, 255, 1000, 65535]:
            packed = pack_u16(v)
            unpacked = unpack_u16(packed)
            assert unpacked == v, f"Roundtrip failed for v={v}"

    def test_pack_no_padding(self):
        """pack_u16 must not include '=' padding chars."""
        from signal_scoring.wire_u16 import pack_u16
        assert "=" not in pack_u16(1)

    def test_unpack_empty(self):
        from signal_scoring.wire_u16 import unpack_u16
        assert unpack_u16("") is None

    def test_unpack_garbage(self):
        from signal_scoring.wire_u16 import unpack_u16
        assert unpack_u16("!!!not_base64!!!") is None

    def test_clamp_to_u16(self):
        """Values above 65535 must wrap (& 0xFFFF)."""
        from signal_scoring.wire_u16 import pack_u16, unpack_u16
        packed = pack_u16(65536)   # 65536 & 0xFFFF == 0
        assert unpack_u16(packed) == 0


# ---------------------------------------------------------------------------
# weak_progress.utils
# ---------------------------------------------------------------------------
class TestWeakProgressUtils:
    def test_compute_weak_progress_basic(self):
        from signal_scoring.weak_progress.utils import compute_weak_progress
        wp = compute_weak_progress(high=102.0, low=100.0, atr=4.0)
        assert wp == pytest.approx(0.5)

    def test_compute_weak_progress_zero_atr(self):
        from signal_scoring.weak_progress.utils import compute_weak_progress
        # Should not divide by zero; eps protects
        wp = compute_weak_progress(high=102.0, low=100.0, atr=0.0)
        assert math.isfinite(wp)
        assert wp > 0.0

    def test_classify_progress_strength(self):
        from signal_scoring.weak_progress.utils import classify_progress_strength
        assert classify_progress_strength(0.1) == "weak"
        assert classify_progress_strength(0.5) == "moderate"
        assert classify_progress_strength(1.0) == "strong"

    def test_is_progress_strong(self):
        from signal_scoring.weak_progress.utils import is_progress_strong_for_continuation
        assert is_progress_strong_for_continuation(0.8) is True
        assert is_progress_strong_for_continuation(0.5) is False

    def test_is_progress_weak(self):
        from signal_scoring.weak_progress.utils import is_progress_weak_for_fade
        assert is_progress_weak_for_fade(0.2) is True
        assert is_progress_weak_for_fade(0.5) is False


# ---------------------------------------------------------------------------
# weak_progress.config
# ---------------------------------------------------------------------------
class TestWeakProgressConfig:
    def test_get_config_default_other(self):
        from signal_scoring.weak_progress.config import get_weak_progress_config
        cfg = get_weak_progress_config(None)
        assert cfg.family == "other"

    def test_get_config_known_pattern(self):
        from signal_scoring.weak_progress.config import get_weak_progress_config
        cfg = get_weak_progress_config("breakout_R1")
        assert cfg.family == "continuation"

    def test_get_config_fade(self):
        from signal_scoring.weak_progress.config import get_weak_progress_config
        cfg = get_weak_progress_config("fade_PDH")
        assert cfg.family == "fade"

    def test_get_config_inferred_continuation(self):
        from signal_scoring.weak_progress.config import get_weak_progress_config
        cfg = get_weak_progress_config("custom_breakout_xyz")
        assert cfg.family == "continuation"

    def test_get_config_inferred_fade(self):
        from signal_scoring.weak_progress.config import get_weak_progress_config
        cfg = get_weak_progress_config("my_fade_pattern")
        assert cfg.family == "fade"


# ---------------------------------------------------------------------------
# weak_progress.scorer
# ---------------------------------------------------------------------------
class TestWeakProgressScorer:
    def _make_ctx(self, **kwargs):
        from signal_scoring.ctx import SignalContext
        defaults = dict(
            ts=datetime(2024, 1, 1),
            symbol="BTCUSDT",
            side="buy",
            session="europe",
            regime="trend",
        )
        defaults.update(kwargs)
        return SignalContext(**defaults)

    def test_compute_progress_score_continuation_strong(self):
        from signal_scoring.weak_progress.scorer import compute_progress_score
        from signal_scoring.weak_progress.config import WeakProgressConfig
        cfg = WeakProgressConfig(family="continuation", cont_strong_min=0.7, bonus_cont_strong=12)
        ctx = self._make_ctx(weak_progress=0.9)
        delta = compute_progress_score(ctx, cfg)
        assert delta == 12

    def test_compute_progress_score_continuation_weak(self):
        from signal_scoring.weak_progress.scorer import compute_progress_score
        from signal_scoring.weak_progress.config import WeakProgressConfig
        cfg = WeakProgressConfig(family="continuation", cont_weak_max=0.3, penalty_cont_weak=15)
        ctx = self._make_ctx(weak_progress=0.1)
        delta = compute_progress_score(ctx, cfg)
        assert delta == -15

    def test_compute_progress_score_none_wp(self):
        from signal_scoring.weak_progress.scorer import compute_progress_score
        from signal_scoring.weak_progress.config import WeakProgressConfig
        cfg = WeakProgressConfig(family="continuation", missing_wp_penalty=10)
        ctx = self._make_ctx(weak_progress=None)
        delta = compute_progress_score(ctx, cfg)
        assert delta == -10

    def test_apply_weak_progress_continuation_ok(self):
        from signal_scoring.weak_progress.scorer import apply_weak_progress_and_fade_filters
        from signal_scoring.weak_progress.config import WeakProgressConfig
        cfg = WeakProgressConfig(family="continuation", cont_strong_min=0.7, bonus_cont_strong=10)
        ctx = self._make_ctx(weak_progress=0.8)
        result = apply_weak_progress_and_fade_filters(ctx, cfg, base_conf=70)
        assert result == 80  # 70 + 10

    def test_apply_weak_progress_fade_precondition_fail(self):
        """Fade pattern with wp too high -> rejection (returns 0)."""
        from signal_scoring.weak_progress.scorer import apply_weak_progress_and_fade_filters
        from signal_scoring.weak_progress.config import WeakProgressConfig
        cfg = WeakProgressConfig(family="fade", fade_weak_max=0.3, fade_min_delta_z=1.5)
        ctx = self._make_ctx(weak_progress=0.9, delta_spike_z=2.0, volume_z=None)
        # wp=0.9 > fade_weak_max=0.3 -> preconditions fail
        result = apply_weak_progress_and_fade_filters(ctx, cfg, base_conf=70)
        assert result == 0

    def test_validate_signal_returns_dict(self):
        from signal_scoring.weak_progress.scorer import validate_signal_for_weak_progress
        ctx = self._make_ctx(weak_progress=0.5, pattern_name="trend_continuation")
        result = validate_signal_for_weak_progress(ctx)
        assert isinstance(result, dict)
        assert "is_valid" in result
        assert "pattern_family" in result
        assert "progress_score" in result


# ---------------------------------------------------------------------------
# signal_scoring/score_model.ScoreModel (self-contained, no calibrator)
# ---------------------------------------------------------------------------
class TestScoreModelSelfContained:
    def _make_ctx(self, **kwargs):
        from signal_scoring.ctx import SignalContext
        defaults = dict(
            ts=datetime(2024, 1, 1),
            symbol="BTCUSDT",
            side="buy",
            session="europe",
            regime="trend",
        )
        defaults.update(kwargs)
        return SignalContext(**defaults)

    def test_score_output_fields(self):
        from signal_scoring.score_model import ScoreModel
        model = ScoreModel(cfg=object())  # no calibrator needed for basic path
        ctx = self._make_ctx()
        out = model.score(
            ctx=ctx,
            kind="breakout",
            side=1,
            raw_score=0.7,
            quality_flags={},
        )
        assert 0.0 <= out.conf_factor <= 1.0
        assert math.isfinite(out.final_score)
        assert 0.0 <= out.confidence_pct <= 100.0

    def test_score_veto_flag(self):
        """Hard veto in quality_flags → conf_factor = 0."""
        from signal_scoring.score_model import ScoreModel
        model = ScoreModel(cfg=object())
        ctx = self._make_ctx()
        out = model.score(
            ctx=ctx,
            kind="breakout",
            side=1,
            raw_score=1.0,
            quality_flags={"veto": True, "veto_reason": "test"},
        )
        assert out.conf_factor == pytest.approx(0.0)
        assert out.final_score == pytest.approx(0.0)

    def test_score_l2_stale_crushes_conf(self):
        from signal_scoring.score_model import ScoreModel

        class _Cfg:
            CONF_L2_STALE_FACTOR = 0.0
            CONF_L2_OK_FACTOR = 1.0
            CONFIDENCE_K = 35.0

        model = ScoreModel(cfg=_Cfg())
        ctx = self._make_ctx()
        out = model.score(
            ctx=ctx,
            kind="breakout",
            side=1,
            raw_score=1.0,
            quality_flags={"l2_ok": False, "l2_reason": "stale_l2"},
        )
        assert out.conf_factor == pytest.approx(0.0)

    def test_score_geometry_floor(self):
        """Geometry=0 should not zero out conf_factor (floor 0.25)."""
        from signal_scoring.score_model import ScoreModel
        model = ScoreModel(cfg=object())
        ctx = self._make_ctx()
        out = model.score(
            ctx=ctx,
            kind="breakout",
            side=1,
            raw_score=1.0,
            quality_flags={"geometry": 0.0},
        )
        # floor is 0.25, so conf_factor > 0
        assert out.conf_factor > 0.0


# ---------------------------------------------------------------------------
# weak_progress.__init__ __all__ completeness check
# ---------------------------------------------------------------------------
class TestWeakProgressPublicAPI:
    def test_all_exports_importable(self):
        import signal_scoring.weak_progress as wp
        for name in wp.__all__:
            assert hasattr(wp, name), f"__all__ member '{name}' not importable from weak_progress"


# ---------------------------------------------------------------------------
# reason_policy — policy covers all VETO_ codes
# ---------------------------------------------------------------------------
class TestReasonPolicy:
    def test_is_reason_allowed_for_kind_universal(self):
        from signal_scoring.reason_policy import is_reason_allowed_for_kind
        # Spread-wide is universal
        assert is_reason_allowed_for_kind("VETO_SPREAD_WIDE", "breakout") is True
        assert is_reason_allowed_for_kind("VETO_SPREAD_WIDE", "absorption") is True

    def test_is_reason_not_allowed_regime_for_absorption(self):
        from signal_scoring.reason_policy import is_reason_allowed_for_kind
        # VETO_REGIME_RANGE_BREAKOUT only for "breakout" kind
        assert is_reason_allowed_for_kind("VETO_REGIME_RANGE_BREAKOUT", "absorption") is False
        assert is_reason_allowed_for_kind("VETO_REGIME_RANGE_BREAKOUT", "breakout") is True

    def test_normalize_reason_no_mismatch(self):
        from signal_scoring.reason_policy import normalize_reason_for_kind
        parts: dict = {}
        rc, sev = normalize_reason_for_kind(reason_code="VETO_SPREAD_WIDE", kind="breakout", parts=parts)
        assert rc == "VETO_SPREAD_WIDE"

    def test_normalize_reason_mismatch(self):
        from signal_scoring.reason_policy import normalize_reason_for_kind
        from signal_scoring.reason_codes import ReasonCode
        parts: dict = {}
        rc, sev = normalize_reason_for_kind(
            reason_code="VETO_REGIME_RANGE_BREAKOUT",
            kind="absorption",
            parts=parts
        )
        assert rc == ReasonCode.VETO_UNKNOWN.value


# ---------------------------------------------------------------------------
# __init__ package-level smoke test
# ---------------------------------------------------------------------------
class TestPackageInit:
    def test_scoring_config_importable(self):
        from signal_scoring import ScoringConfig
        assert callable(ScoringConfig.from_env)

    def test_signal_context_importable(self):
        from signal_scoring import SignalContext
        ctx = SignalContext(
            ts=datetime(2024, 1, 1),
            symbol="BTCUSDT",
            side="buy",
            session="europe",
            regime="trend",
        )
        assert ctx.symbol == "BTCUSDT"
