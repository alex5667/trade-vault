"""
tests/test_regime_hardening.py

P0/P1 hardening tests for MarketRegimeService and regime contracts.
All tests must pass before REGIME_POLICY_MODE=ENFORCE.

Covers:
  - symbol != "unknown" in snapshot (P0)
  - ts_event_ms used, not wall clock (P0)
  - hysteresis confirm_bars gate (P1)
  - fast_override switches immediately (P1)
  - exit_band_score holds previous trend regime (P1)
  - expansion_bull/bear have valid REGIME_ID (P0 contract)
  - regime_to_id("unknown") != regime_to_id("range") (invariant)
  - regime transition payload contract (P1)
"""

from __future__ import annotations

import pytest

from common.market_mode import regime_to_id, normalize_regime
from common.regime_contract import (
    RegimeLabel,
    RegimeSwitchPolicy,
    should_switch,
)
from handlers.regime_service import MarketRegimeService, RegimeConfig, RegimeFeatures


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _trend_features() -> RegimeFeatures:
    """Strong trend features → score > score_hi."""
    return RegimeFeatures(atr_q=0.9, adx_q=0.9, delta_ema=3.0, hold_side_score=0.8)


def _range_features() -> RegimeFeatures:
    """Range features → score < score_lo."""
    return RegimeFeatures(atr_q=0.2, adx_q=0.2, delta_ema=0.0, hold_side_score=0.0, vwap_cross_rate=0.5)


# ─────────────────────────────────────────────────────────────────────────────
# P0: symbol != "unknown"
# ─────────────────────────────────────────────────────────────────────────────

class TestSnapshotSymbolNotUnknown:
    """RegimeSnapshot.symbol must never be the string 'unknown'."""

    def test_symbol_propagated_on_first_call(self):
        svc = MarketRegimeService(RegimeConfig())
        f = _trend_features()
        svc.update_regime(f, symbol="BTCUSDT", ts_event_ms=1_000)
        snap = svc.get_current_regime().snapshot
        assert snap is not None
        assert snap.symbol == "BTCUSDT"

    def test_symbol_upper_normalised(self):
        svc = MarketRegimeService(RegimeConfig())
        svc.update_regime(_range_features(), symbol="ethusdt", ts_event_ms=1_000)
        snap = svc.get_current_regime().snapshot
        assert snap is not None
        assert snap.symbol == "ETHUSDT"

    def test_symbol_default_is_not_lowercase_unknown(self):
        """Even without symbol argument the default is 'UNKNOWN' (upper), not 'unknown'."""
        svc = MarketRegimeService(RegimeConfig())
        svc.update_regime(_range_features(), ts_event_ms=1_000)
        snap = svc.get_current_regime().snapshot
        assert snap is not None
        # Must not be the lowercase sentinel that breaks joins/Timescale queries
        assert snap.symbol != "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# P0: ts_event_ms used, not wall clock
# ─────────────────────────────────────────────────────────────────────────────

class TestDeterministicTime:
    """update_regime() must use ts_event_ms for all timing, not time.time()."""

    def test_last_update_ms_equals_ts_event_ms(self):
        svc = MarketRegimeService(RegimeConfig())
        ts = 1_700_000_000_000
        svc.update_regime(_trend_features(), symbol="BTCUSDT", ts_event_ms=ts)
        st = svc.get_current_regime()
        assert st.last_update_ms == ts

    def test_snapshot_ts_calc_ms_equals_ts_event_ms_when_no_override(self):
        svc = MarketRegimeService(RegimeConfig())
        ts = 1_700_000_001_000
        svc.update_regime(_trend_features(), symbol="BTCUSDT", ts_event_ms=ts)
        snap = svc.get_current_regime().snapshot
        assert snap is not None
        assert snap.ts_calc_ms == ts
        assert snap.ts_event_ms == ts

    def test_ts_calc_ms_override_respected(self):
        svc = MarketRegimeService(RegimeConfig())
        ts_event = 1_700_000_002_000
        ts_calc = 1_700_000_002_050  # 50ms processing latency
        svc.update_regime(_trend_features(), symbol="BTCUSDT", ts_event_ms=ts_event, ts_calc_ms=ts_calc)
        snap = svc.get_current_regime().snapshot
        assert snap is not None
        assert snap.ts_event_ms == ts_event
        assert snap.ts_calc_ms == ts_calc

    def test_replay_same_ts_gives_same_state(self):
        """Two identical replays must produce identical states."""
        def _run(ts_base: int) -> str:
            svc = MarketRegimeService(RegimeConfig(score_hi=0.2, score_lo=-0.2))
            f = _trend_features()
            # 3 bars to satisfy confirm_bars=3 default, with enough hold gap
            svc.update_regime(f, symbol="BTCUSDT", ts_event_ms=ts_base)
            svc.update_regime(f, symbol="BTCUSDT", ts_event_ms=ts_base + 1_000)
            svc.update_regime(f, symbol="BTCUSDT", ts_event_ms=ts_base + 200_000)
            return svc.get_current_regime().regime

        r1 = _run(1_700_000_000_000)
        r2 = _run(1_700_000_000_000)
        assert r1 == r2, "Replay determinism broken"


# ─────────────────────────────────────────────────────────────────────────────
# P1: hysteresis — confirm_bars required before switch
# ─────────────────────────────────────────────────────────────────────────────

class TestHysteresisConfirmBars:
    """Regime must not switch before confirm_bars consecutive identical candidates."""

    def test_stays_unknown_before_confirm_bars(self):
        """Default confirm_bars=3: regime stays 'unknown' after first two calls."""
        policy = RegimeSwitchPolicy(
            confirm_bars=3,
            min_hold_ms=0,        # disable time gate for this test
            fast_override_score=2.0,  # disable fast override
        )
        svc = MarketRegimeService(RegimeConfig(score_hi=0.2, score_lo=-0.2, switch_policy=policy))
        f = _trend_features()

        svc.update_regime(f, symbol="BTCUSDT", ts_event_ms=1_000)
        assert svc.get_current_regime().regime == "unknown", "Should still be unknown after 1 bar"

        svc.update_regime(f, symbol="BTCUSDT", ts_event_ms=2_000)
        assert svc.get_current_regime().regime == "unknown", "Should still be unknown after 2 bars"

        svc.update_regime(f, symbol="BTCUSDT", ts_event_ms=3_000)
        # After 3rd consecutive bar → switch allowed
        assert svc.get_current_regime().regime in (
            "trending_bull", "trending_bear", "trend"
        ), "Should have switched after 3 confirm bars"

    def test_counter_resets_on_regime_change(self):
        """If candidate changes mid-stream, counter resets."""
        policy = RegimeSwitchPolicy(
            confirm_bars=3,
            min_hold_ms=0,
            fast_override_score=2.0,
        )
        svc = MarketRegimeService(RegimeConfig(score_hi=0.2, score_lo=-0.2, switch_policy=policy))

        # 2 trend bars, then 1 range bar
        svc.update_regime(_trend_features(), symbol="BTCUSDT", ts_event_ms=1_000)
        svc.update_regime(_trend_features(), symbol="BTCUSDT", ts_event_ms=2_000)
        # Switch to range candidate — resets counter
        svc.update_regime(_range_features(), symbol="BTCUSDT", ts_event_ms=3_000)
        assert svc.get_current_regime().regime == "unknown", "Counter reset: still unknown"


# ─────────────────────────────────────────────────────────────────────────────
# P1: fast_override switches immediately regardless of confirm_bars
# ─────────────────────────────────────────────────────────────────────────────

class TestFastOverride:
    def test_fast_override_switches_on_first_bar(self):
        """If |score| >= fast_override_score, switch on bar 1 regardless of confirm_bars."""
        policy = RegimeSwitchPolicy(
            confirm_bars=10,         # high threshold
            min_hold_ms=0,
            fast_override_score=0.5,
        )
        svc = MarketRegimeService(
            RegimeConfig(score_hi=0.2, score_lo=-0.2, switch_policy=policy)
        )
        # Features that produce |score| well above 0.5
        f = RegimeFeatures(atr_q=0.95, adx_q=0.95, delta_ema=5.0, hold_side_score=0.99)
        svc.update_regime(f, symbol="BTCUSDT", ts_event_ms=1_000)
        regime = svc.get_current_regime().regime
        assert regime in ("trending_bull", "trending_bear", "trend"), (
            f"fast_override should have switched on bar 1, got: {regime}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# P1: exit_band_score holds trend regime when transitioning to range/mixed
# ─────────────────────────────────────────────────────────────────────────────

class TestExitBandHold:
    def test_exit_band_holds_when_score_in_band(self):
        """Trend → range/mixed should be blocked while |score| <= exit_band_score."""
        policy = RegimeSwitchPolicy(
            enter_trend_score=0.40,
            enter_range_score=-0.40,
            exit_band_score=0.30,     # wide band
            confirm_bars=1,
            min_hold_ms=0,
            fast_override_score=2.0,  # disable fast override
        )
        # score=0.20 is inside exit_band (0.30) → should block
        do_switch, reason = should_switch(
            prev_label="trending_bull",
            next_label="range",
            score=0.20,
            confirm_count=5,
            now_ms=200_000,
            last_switch_ms=0,
            policy=policy,
        )
        assert not do_switch, f"Expected exit_band_hold, got: {reason}"
        assert reason == "exit_band_hold"

    def test_exit_band_allows_when_score_outside_band(self):
        """When |score| > exit_band_score, switch is allowed."""
        policy = RegimeSwitchPolicy(
            exit_band_score=0.10,
            confirm_bars=1,
            min_hold_ms=0,
            fast_override_score=2.0,
        )
        do_switch, reason = should_switch(
            prev_label="trending_bull",
            next_label="range",
            score=-0.45,  # well outside exit_band
            confirm_count=5,
            now_ms=200_000,
            last_switch_ms=0,
            policy=policy,
        )
        assert do_switch, f"Expected switch outside band, got: {reason}"


# ─────────────────────────────────────────────────────────────────────────────
# P0: expansion_bull/bear contract
# ─────────────────────────────────────────────────────────────────────────────

class TestExpansionRegimeContract:
    def test_expansion_bull_has_valid_regime_id(self):
        rid = regime_to_id("expansion_bull")
        assert rid > 0, f"expansion_bull id must be positive, got {rid}"
        assert rid != regime_to_id("unknown"), "expansion_bull must differ from unknown"
        assert rid != regime_to_id("trend"), "expansion_bull must differ from trend"

    def test_expansion_bear_has_valid_regime_id(self):
        rid = regime_to_id("expansion_bear")
        assert rid > 0, f"expansion_bear id must be positive, got {rid}"
        assert rid != regime_to_id("unknown")
        assert rid != regime_to_id("trend")

    def test_expansion_ids_distinct_from_each_other(self):
        assert regime_to_id("expansion_bull") != regime_to_id("expansion_bear")

    def test_expansion_bull_in_regime_label_enum(self):
        label = RegimeLabel("expansion_bull")
        assert label == RegimeLabel.EXPANSION_BULL

    def test_expansion_bear_in_regime_label_enum(self):
        label = RegimeLabel("expansion_bear")
        assert label == RegimeLabel.EXPANSION_BEAR

    def test_expansion_bull_is_trend_regime(self):
        from common.market_mode import is_trend_regime
        assert is_trend_regime("expansion_bull") is True

    def test_expansion_bear_is_trend_regime(self):
        from common.market_mode import is_trend_regime
        assert is_trend_regime("expansion_bear") is True


# ─────────────────────────────────────────────────────────────────────────────
# Invariant: unknown != range
# ─────────────────────────────────────────────────────────────────────────────

def test_regime_to_id_unknown_not_range():
    assert regime_to_id("unknown") != regime_to_id("range"), (
        "unknown and range must have different IDs (audit/ML safety)"
    )


def test_regime_to_id_unknown_is_negative():
    assert regime_to_id("unknown") < 0


# ─────────────────────────────────────────────────────────────────────────────
# P1: transition payload contract
# ─────────────────────────────────────────────────────────────────────────────

class TestTransitionPayloadContract:
    """Verify that should_switch() returns the reason field used in Redis payload."""

    def test_confirmed_switch_reason(self):
        policy = RegimeSwitchPolicy(
            confirm_bars=1, min_hold_ms=0, fast_override_score=2.0
        )
        _, reason = should_switch(
            prev_label="range",
            next_label="trending_bull",
            score=0.60,
            confirm_count=1,
            now_ms=200_000,
            last_switch_ms=0,
            policy=policy,
        )
        assert reason == "confirmed_switch"

    def test_fast_override_reason(self):
        policy = RegimeSwitchPolicy(fast_override_score=0.5)
        _, reason = should_switch(
            prev_label="range",
            next_label="trending_bull",
            score=0.80,
            confirm_count=1,
            now_ms=1_000,
            last_switch_ms=0,
            policy=policy,
        )
        assert reason == "fast_override"

    def test_same_regime_reason(self):
        policy = RegimeSwitchPolicy()
        _, reason = should_switch(
            prev_label="range",
            next_label="range",
            score=0.0,
            confirm_count=3,
            now_ms=200_000,
            last_switch_ms=0,
            policy=policy,
        )
        assert reason == "same_regime"

    def test_need_confirm_reason(self):
        policy = RegimeSwitchPolicy(
            confirm_bars=5, min_hold_ms=0, fast_override_score=2.0
        )
        _, reason = should_switch(
            prev_label="range",
            next_label="trending_bull",
            score=0.50,
            confirm_count=2,   # < 5
            now_ms=200_000,
            last_switch_ms=0,
            policy=policy,
        )
        assert reason == "need_confirm"
