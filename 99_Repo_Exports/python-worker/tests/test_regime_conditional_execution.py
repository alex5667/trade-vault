"""Tests for core.regime_conditional_execution (Task 3.1).

Covers:
  - Default bucket lookup for (vol×trend) combinations.
  - Fallback hierarchy: symbol-specific → GLOBAL → wildcard → global.
  - Normalizers for compound regime labels (e.g. "trending_bear_short_trend_follow").
  - Shadow vs. enforce-mode behavior + skip-veto gating.
  - Per-bucket enforce flag override of global enforce.
  - Runtime overrides reader (HMAC verification + TTL).
  - Shadow-diff record_shadow_diff helper.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from unittest.mock import MagicMock

import pytest

from core.regime_conditional_execution import (
    ExecutionPolicy,
    RegimeConditionalExecutionEngine,
    _RegimeExecOverridesReader,
    _default_buckets,
    _norm_trend,
    _norm_vol,
    emit_veto_metric,
    get_engine,
    record_shadow_diff,
    reset_engine_for_tests,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    """Each test runs with default env (engine enabled, shadow mode)."""
    for k in (
        "REGIME_EXEC_ENGINE_ENABLED",
        "REGIME_EXEC_ENGINE_ENFORCE",
        "REGIME_EXEC_SKIP_CHOPPY",
        "REGIME_EXEC_BUCKETS_PATH",
        "AUTOCAL_REGIME_EXEC_READ_ENABLED",
        "AUTOCAL_REGIME_EXEC_KEY",
        "REGIME_EXEC_AUTOCAL_HMAC_SECRET",
    ):
        monkeypatch.delenv(k, raising=False)
    reset_engine_for_tests()
    yield
    reset_engine_for_tests()


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------


class TestNormalizers:
    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("shock", "shock"),
            ("SHOCK", "shock"),
            ("normal", "normal"),
            ("calm", "calm"),
            ("na", "na"),
            ("", "na"),
            (None, "na"),
            ("unknown", "na"),
            ("bogus", "na"),
            ("none", "na"),
        ],
    )
    def test_norm_vol(self, input_val, expected):
        assert _norm_vol(input_val) == expected

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("trending", "trending"),
            ("trending_bear", "trending_bear"),
            ("trending_bear_short_trend_follow", "trending_bear"),
            ("trend_up", "trending"),
            ("range", "range"),
            ("range_protective", "range"),
            ("expansion", "expansion"),
            ("squeeze", "squeeze"),
            ("mixed", "mixed"),
            ("na", "na"),
            ("", "na"),
            (None, "na"),
            ("unknown_state", "na"),
        ],
    )
    def test_norm_trend(self, input_val, expected):
        assert _norm_trend(input_val) == expected


# ---------------------------------------------------------------------------
# Default bucket policy
# ---------------------------------------------------------------------------


class TestDefaultBuckets:
    def test_high_vol_trending_wide_tp_tight_trail(self):
        engine = RegimeConditionalExecutionEngine()
        p = engine.select_policy(
            vol_regime="shock", trend_regime="trending", symbol="BTCUSDT"
        )
        assert p.bucket == "GLOBAL|shock|trending"
        assert p.tp1_target_r == 1.5
        assert p.tp_ratios == [0.40, 0.30, 0.30]
        assert p.trail_profile == "rocket_v1"
        assert p.trail_atr_mult == 1.0
        assert not p.skip

    def test_low_vol_range_fast_scale_out_no_trail(self):
        engine = RegimeConditionalExecutionEngine()
        p = engine.select_policy(vol_regime="calm", trend_regime="range")
        assert p.tp1_target_r == 0.3
        assert p.tp_ratios == [0.70, 0.30]
        assert p.trail_profile == "range_protective"
        assert not p.skip

    def test_normal_vol_range_is_skip(self):
        engine = RegimeConditionalExecutionEngine()
        p = engine.select_policy(vol_regime="normal", trend_regime="range")
        assert p.skip is True
        assert "choppy" in p.reason.lower()

    def test_squeeze_is_skip_via_wildcard(self):
        engine = RegimeConditionalExecutionEngine()
        # wildcard match "GLOBAL|any|squeeze" should fire for any vol.
        for vol in ("shock", "normal", "calm", "na"):
            p = engine.select_policy(vol_regime=vol, trend_regime="squeeze")
            assert p.skip is True, f"vol={vol} should map to skip"

    def test_mixed_is_skip_via_wildcard(self):
        engine = RegimeConditionalExecutionEngine()
        p = engine.select_policy(vol_regime="normal", trend_regime="mixed")
        assert p.skip is True

    def test_expansion_wide_trail(self):
        engine = RegimeConditionalExecutionEngine()
        p = engine.select_policy(vol_regime="normal", trend_regime="expansion")
        assert p.trail_profile == "expansion_v1"
        assert p.trail_atr_mult == 1.5

    def test_trending_bear_routes_to_bear_profile(self):
        engine = RegimeConditionalExecutionEngine()
        p = engine.select_policy(
            vol_regime="normal", trend_regime="trending_bear"
        )
        assert p.trail_profile == "rocket_v1_bear"

    def test_global_passthrough_when_no_match(self):
        # vol=na, trend=na — falls through to "global" passthrough.
        engine = RegimeConditionalExecutionEngine(buckets={"global": {"reason": "x"}})
        p = engine.select_policy(vol_regime="na", trend_regime="na")
        assert p.bucket == "global"
        assert p.trail_profile is None
        assert p.tp_ratios is None
        assert not p.skip


# ---------------------------------------------------------------------------
# Fallback hierarchy
# ---------------------------------------------------------------------------


class TestFallbackHierarchy:
    def test_symbol_specific_overrides_global(self):
        buckets = _default_buckets()
        buckets["BTCUSDT|shock|trending"] = {
            "trail_profile": "btc_special",
            "tp1_target_r": 2.0,
            "reason": "btc override",
        }
        engine = RegimeConditionalExecutionEngine(buckets=buckets)
        p = engine.select_policy(
            vol_regime="shock", trend_regime="trending", symbol="BTCUSDT"
        )
        assert p.bucket == "BTCUSDT|shock|trending"
        assert p.trail_profile == "btc_special"
        assert p.fallback_depth == 0

    def test_falls_back_to_global_when_no_symbol_match(self):
        engine = RegimeConditionalExecutionEngine()
        p = engine.select_policy(
            vol_regime="shock", trend_regime="trending", symbol="UNKNOWNUSDT"
        )
        assert p.bucket == "GLOBAL|shock|trending"
        assert p.fallback_depth > 0

    def test_compound_label_normalized_for_lookup(self):
        engine = RegimeConditionalExecutionEngine()
        p = engine.select_policy(
            vol_regime="normal",
            trend_regime="trending_bear_short_trend_follow",
        )
        assert p.bucket == "GLOBAL|normal|trending_bear"


# ---------------------------------------------------------------------------
# Enforce / shadow / skip semantics
# ---------------------------------------------------------------------------


class TestEnforceShadow:
    def test_shadow_mode_by_default(self):
        engine = RegimeConditionalExecutionEngine()
        assert engine.is_enforce() is False
        p = engine.select_policy(vol_regime="normal", trend_regime="range")
        assert engine.effective_enforce(p) is False

    def test_global_enforce_flag(self):
        engine = RegimeConditionalExecutionEngine(enforce_global=True)
        assert engine.is_enforce() is True
        p = engine.select_policy(vol_regime="shock", trend_regime="trending")
        assert engine.effective_enforce(p) is True

    def test_per_bucket_enforce_overrides_global_off(self):
        buckets = _default_buckets()
        buckets["GLOBAL|shock|trending"]["enforce"] = 1
        engine = RegimeConditionalExecutionEngine(
            buckets=buckets, enforce_global=False
        )
        p = engine.select_policy(vol_regime="shock", trend_regime="trending")
        assert p.enforce is True
        assert engine.effective_enforce(p) is True

    def test_per_bucket_enforce_overrides_global_on(self):
        buckets = _default_buckets()
        buckets["GLOBAL|shock|trending"]["enforce"] = 0
        engine = RegimeConditionalExecutionEngine(
            buckets=buckets, enforce_global=True
        )
        p = engine.select_policy(vol_regime="shock", trend_regime="trending")
        assert p.enforce is False
        assert engine.effective_enforce(p) is False

    def test_should_skip_requires_enforce_and_skip_choppy(self):
        engine = RegimeConditionalExecutionEngine(
            enforce_global=True, skip_choppy=True
        )
        p = engine.select_policy(vol_regime="normal", trend_regime="range")
        assert p.skip is True
        assert engine.should_skip(p) is True

    def test_skip_not_applied_without_skip_choppy(self):
        engine = RegimeConditionalExecutionEngine(
            enforce_global=True, skip_choppy=False
        )
        p = engine.select_policy(vol_regime="normal", trend_regime="range")
        assert p.skip is True
        assert engine.should_skip(p) is False

    def test_skip_not_applied_in_shadow(self):
        engine = RegimeConditionalExecutionEngine(
            enforce_global=False, skip_choppy=True
        )
        p = engine.select_policy(vol_regime="normal", trend_regime="range")
        assert engine.should_skip(p) is False


# ---------------------------------------------------------------------------
# Engine.from_env
# ---------------------------------------------------------------------------


class TestFromEnv:
    def test_from_env_defaults(self):
        engine = RegimeConditionalExecutionEngine.from_env()
        assert engine.enabled is True
        assert engine.enforce_global is False
        assert engine.skip_choppy is False

    def test_from_env_enforce(self, monkeypatch):
        monkeypatch.setenv("REGIME_EXEC_ENGINE_ENFORCE", "1")
        monkeypatch.setenv("REGIME_EXEC_SKIP_CHOPPY", "1")
        engine = RegimeConditionalExecutionEngine.from_env()
        assert engine.is_enforce() is True
        assert engine.skip_choppy is True

    def test_from_env_disabled(self, monkeypatch):
        monkeypatch.setenv("REGIME_EXEC_ENGINE_ENABLED", "0")
        engine = RegimeConditionalExecutionEngine.from_env()
        assert engine.enabled is False
        p = engine.select_policy(vol_regime="shock", trend_regime="trending")
        assert p.bucket == "disabled"

    def test_from_env_loads_buckets_path(self, monkeypatch, tmp_path):
        cfg = tmp_path / "extra.json"
        cfg.write_text(
            json.dumps(
                {
                    "GLOBAL|shock|trending": {
                        "trail_profile": "custom",
                        "tp1_target_r": 99.0,
                        "reason": "from json",
                    }
                }
            )
        )
        monkeypatch.setenv("REGIME_EXEC_BUCKETS_PATH", str(cfg))
        engine = RegimeConditionalExecutionEngine.from_env()
        p = engine.select_policy(vol_regime="shock", trend_regime="trending")
        assert p.trail_profile == "custom"
        assert p.tp1_target_r == 99.0


# ---------------------------------------------------------------------------
# Runtime overrides reader (HMAC + TTL)
# ---------------------------------------------------------------------------


def _hmac_sign(payload: dict, secret: str) -> str:
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()


class TestOverridesReader:
    def test_no_snapshot_returns_none(self):
        rc = MagicMock()
        rc.get.return_value = None
        reader = _RegimeExecOverridesReader(rc)
        assert reader.get_bucket("GLOBAL|shock|trending") is None

    def test_valid_snapshot_returns_bucket(self):
        rc = MagicMock()
        body = {
            "ts_ms": int(time.time() * 1000),
            "buckets": {
                "GLOBAL|shock|trending": {
                    "trail_profile": "live_override",
                    "enforce": 1,
                }
            },
        }
        rc.get.return_value = json.dumps(body)
        reader = _RegimeExecOverridesReader(rc, refresh_ms=1)
        b = reader.get_bucket("GLOBAL|shock|trending")
        assert b is not None
        assert b["trail_profile"] == "live_override"

    def test_hmac_mismatch_ignored(self):
        rc = MagicMock()
        body = {
            "ts_ms": int(time.time() * 1000),
            "buckets": {"GLOBAL|shock|trending": {"trail_profile": "x"}},
        }
        body_signed = {**body, "sig": "deadbeef"}  # wrong sig
        rc.get.return_value = json.dumps(body_signed)
        reader = _RegimeExecOverridesReader(
            rc, refresh_ms=1, hmac_secret="secret"
        )
        assert reader.get_bucket("GLOBAL|shock|trending") is None

    def test_hmac_valid_accepted(self):
        rc = MagicMock()
        body = {
            "ts_ms": int(time.time() * 1000),
            "buckets": {"GLOBAL|shock|trending": {"trail_profile": "x"}},
        }
        sig = _hmac_sign(body, "secret")
        rc.get.return_value = json.dumps({**body, "sig": sig})
        reader = _RegimeExecOverridesReader(
            rc, refresh_ms=1, hmac_secret="secret"
        )
        b = reader.get_bucket("GLOBAL|shock|trending")
        assert b is not None
        assert b["trail_profile"] == "x"

    def test_stale_snapshot_returns_none(self):
        rc = MagicMock()
        body = {
            "ts_ms": int(time.time() * 1000) - 60 * 60 * 1000,  # 1h old
            "buckets": {"GLOBAL|shock|trending": {"trail_profile": "x"}},
        }
        rc.get.return_value = json.dumps(body)
        reader = _RegimeExecOverridesReader(rc, refresh_ms=1, stale_ms=10_000)
        assert reader.get_bucket("GLOBAL|shock|trending") is None

    def test_runtime_override_wins_over_static(self):
        rc = MagicMock()
        body = {
            "ts_ms": int(time.time() * 1000),
            "buckets": {
                "GLOBAL|shock|trending": {
                    "trail_profile": "runtime_winner",
                    "tp1_target_r": 9.9,
                }
            },
        }
        rc.get.return_value = json.dumps(body)
        reader = _RegimeExecOverridesReader(rc, refresh_ms=1)
        engine = RegimeConditionalExecutionEngine(overrides_reader=reader)
        p = engine.select_policy(vol_regime="shock", trend_regime="trending")
        assert p.trail_profile == "runtime_winner"
        assert p.tp1_target_r == 9.9


# ---------------------------------------------------------------------------
# Shadow diff helper
# ---------------------------------------------------------------------------


class TestShadowDiff:
    def test_no_diff_when_policy_matches_actual(self):
        p = ExecutionPolicy(
            bucket="GLOBAL|shock|trending",
            trail_profile="rocket_v1",
            tp_ratios=[0.5, 0.5],
            tp1_target_r=1.0,
        )
        diff = record_shadow_diff(
            p,
            actual_trail_profile="rocket_v1",
            actual_tp_ratios=[0.5, 0.5],
            actual_tp1_target_r=1.0,
        )
        assert diff == {}

    def test_records_trail_profile_diff(self):
        p = ExecutionPolicy(
            bucket="GLOBAL|shock|trending", trail_profile="rocket_v1"
        )
        diff = record_shadow_diff(
            p,
            actual_trail_profile="protective_only",
            actual_tp_ratios=None,
            actual_tp1_target_r=None,
        )
        assert "trail_profile" in diff
        assert diff["trail_profile"]["proposed"] == "rocket_v1"
        assert diff["trail_profile"]["actual"] == "protective_only"

    def test_records_tp_ratios_diff(self):
        p = ExecutionPolicy(
            bucket="GLOBAL|calm|range", tp_ratios=[0.7, 0.3]
        )
        diff = record_shadow_diff(
            p,
            actual_trail_profile=None,
            actual_tp_ratios=[0.8, 0.2],
            actual_tp1_target_r=None,
        )
        assert "tp_ratios" in diff
        assert diff["tp_ratios"]["proposed"] == [0.7, 0.3]

    def test_no_diff_when_policy_field_none(self):
        p = ExecutionPolicy(bucket="global")
        diff = record_shadow_diff(
            p,
            actual_trail_profile="rocket_v1",
            actual_tp_ratios=[0.5, 0.5],
            actual_tp1_target_r=1.0,
        )
        assert diff == {}


# ---------------------------------------------------------------------------
# Veto metric
# ---------------------------------------------------------------------------


class TestVetoMetric:
    def test_emit_veto_metric_does_not_raise(self):
        p = ExecutionPolicy(bucket="GLOBAL|normal|range", skip=True)
        # Should be a no-op or counter increment; never raise.
        emit_veto_metric(p, symbol="BTCUSDT", kind="breakout")


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------


class TestVolRegimeResolver:
    """Verify SignalPipeline._resolve_vol_regime_label wires VolRegimeTracker output."""

    def _resolver(self):
        from services.orderflow.signal_pipeline import SignalPipeline
        return SignalPipeline._resolve_vol_regime_label

    def test_uses_indicators_when_present(self):
        out = self._resolver()(
            indicators={"vol_regime_label": "SHOCK"}, runtime=None
        )
        assert out == "shock"

    def test_falls_back_to_runtime_dynamic_cfg(self):
        from core.dyn_cfg_keys import DynCfgKeys as DK

        class Rt:
            dynamic_cfg = {
                DK.VOL_REGIME_LABEL: "calm",
                DK.VOL_RATIO_Z: 0.42,
                DK.VOL_RATIO: 1.1,
                DK.VOL_FAST_BPS: 25.0,
                DK.VOL_SLOW_BPS: 22.0,
            }

        ind: dict = {}
        out = self._resolver()(indicators=ind, runtime=Rt())
        assert out == "calm"
        assert ind["vol_regime_label"] == "calm"
        # Side-effect mirrors raw stats into indicators for ML/audit.
        assert ind["vol_ratio_z"] == 0.42
        assert ind["vol_ratio"] == 1.1
        assert ind["vol_fast_bps"] == 25.0
        assert ind["vol_slow_bps"] == 22.0

    def test_returns_na_when_no_source(self):
        out = self._resolver()(indicators={}, runtime=None)
        assert out == "na"

    def test_indicators_override_runtime(self):
        from core.dyn_cfg_keys import DynCfgKeys as DK

        class Rt:
            dynamic_cfg = {DK.VOL_REGIME_LABEL: "calm"}

        out = self._resolver()(
            indicators={"vol_regime_label": "shock"}, runtime=Rt()
        )
        assert out == "shock"

    def test_does_not_overwrite_existing_vol_ratio_z(self):
        from core.dyn_cfg_keys import DynCfgKeys as DK

        class Rt:
            dynamic_cfg = {
                DK.VOL_REGIME_LABEL: "normal",
                DK.VOL_RATIO_Z: 99.0,
            }

        ind = {"vol_ratio_z": 1.23}
        self._resolver()(indicators=ind, runtime=Rt())
        # Existing value preserved (setdefault semantics).
        assert ind["vol_ratio_z"] == 1.23


class TestSingleton:
    def test_get_engine_returns_singleton(self):
        e1 = get_engine()
        e2 = get_engine()
        assert e1 is e2

    def test_get_engine_returns_none_when_disabled(self, monkeypatch):
        monkeypatch.setenv("REGIME_EXEC_ENGINE_ENABLED", "0")
        reset_engine_for_tests()
        assert get_engine() is None

    def test_reset_clears_singleton(self):
        e1 = get_engine()
        reset_engine_for_tests()
        e2 = get_engine()
        assert e1 is not e2
