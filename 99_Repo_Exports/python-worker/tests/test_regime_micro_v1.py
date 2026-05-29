"""Unit tests for core/regime_micro_v1.py — pure classifier + stateful tracker."""
from __future__ import annotations

from core.regime_micro_v1 import (
    RegimeMicroConfig,
    RegimeMicroState,
    classify_regime_micro,
)

CFG = RegimeMicroConfig()


# ──────────────────────────────────────────────
# classify_regime_micro — pure function
# ──────────────────────────────────────────────

class TestClassifyRegimeMicro:
    def test_shock_via_vol_state(self):
        assert classify_regime_micro(0.0, "shock", 5.0, 0.0, CFG) == "shock_micro"

    def test_shock_via_high_z(self):
        assert classify_regime_micro(0.0, "normal", 5.0, 4.0, CFG) == "shock_micro"

    def test_shock_at_z_threshold(self):
        assert classify_regime_micro(0.0, "normal", 5.0, 3.0, CFG) == "shock_micro"

    def test_shock_wins_over_trend(self):
        # High z AND high ret → shock_micro wins
        assert classify_regime_micro(100.0, "shock", 5.0, 5.0, CFG) == "shock_micro"

    def test_trend_micro_up(self):
        assert classify_regime_micro(20.0, "normal", 30.0, 0.5, CFG) == "trend_micro_up"

    def test_trend_micro_up_at_threshold(self):
        assert classify_regime_micro(15.0, "normal", 30.0, 0.5, CFG) == "trend_micro_up"

    def test_trend_micro_down(self):
        assert classify_regime_micro(-20.0, "normal", 30.0, 0.5, CFG) == "trend_micro_down"

    def test_trend_micro_down_at_threshold(self):
        assert classify_regime_micro(-15.0, "normal", 30.0, 0.5, CFG) == "trend_micro_down"

    def test_squeeze_micro(self):
        assert classify_regime_micro(2.0, "calm", 8.0, 0.2, CFG) == "squeeze_micro"

    def test_squeeze_micro_at_threshold(self):
        assert classify_regime_micro(2.0, "calm", 10.0, 0.2, CFG) == "squeeze_micro"

    def test_range_micro(self):
        assert classify_regime_micro(3.0, "normal", 15.0, 0.5, CFG) == "range_micro"

    def test_range_micro_at_ret_threshold(self):
        assert classify_regime_micro(5.0, "normal", 15.0, 0.5, CFG) == "range_micro"

    def test_mixed_micro(self):
        # Not shock, not trend, not squeeze, not range
        assert classify_regime_micro(8.0, "normal", 25.0, 1.0, CFG) == "mixed_micro"

    def test_mixed_micro_boundary(self):
        # ret just below trend, range just above range_pct
        assert classify_regime_micro(14.9, "normal", 21.0, 1.0, CFG) == "mixed_micro"

    def test_calm_with_wide_range_not_squeeze(self):
        # calm but range > squeeze_pct_bps → no squeeze, check range
        assert classify_regime_micro(3.0, "calm", 15.0, 0.1, CFG) == "range_micro"

    def test_calm_with_wide_range_and_high_ret_trend(self):
        assert classify_regime_micro(20.0, "calm", 30.0, 0.5, CFG) == "trend_micro_up"

    def test_nan_inputs_fall_to_default(self):
        result = classify_regime_micro(float("nan"), "normal", float("nan"), float("nan"), CFG)
        assert result in ("range_micro", "mixed_micro")

    def test_inf_ret_treated_as_zero(self):
        result = classify_regime_micro(float("inf"), "normal", 10.0, 0.1, CFG)
        # inf → _safe_f → 0.0 → not trend
        assert result == "range_micro"

    def test_none_vol_state(self):
        result = classify_regime_micro(0.0, None, 5.0, 0.1, CFG)  # type: ignore[arg-type]
        assert result in ("range_micro", "mixed_micro", "squeeze_micro")

    def test_empty_vol_state(self):
        result = classify_regime_micro(0.0, "", 5.0, 0.1, CFG)
        assert result in ("range_micro", "mixed_micro")

    def test_custom_cfg_trend_threshold(self):
        custom = RegimeMicroConfig(trend_ret_bps=30.0)
        # ret=20 < 30 threshold → not trend
        assert classify_regime_micro(20.0, "normal", 30.0, 0.5, custom) == "mixed_micro"

    def test_custom_cfg_shock_z(self):
        custom = RegimeMicroConfig(shock_abs_z=10.0)
        # z=4 < 10 → not shock
        assert classify_regime_micro(0.0, "normal", 5.0, 4.0, custom) != "shock_micro"

    def test_priority_shock_over_squeeze(self):
        # shock condition AND calm + tight range → shock wins
        assert classify_regime_micro(0.0, "shock", 5.0, 0.0, CFG) == "shock_micro"

    def test_priority_shock_over_range(self):
        assert classify_regime_micro(3.0, "shock", 10.0, 0.0, CFG) == "shock_micro"

    def test_z_just_below_threshold_not_shock(self):
        result = classify_regime_micro(0.0, "normal", 5.0, 2.99, CFG)
        assert result == "range_micro"


# ──────────────────────────────────────────────
# RegimeMicroState — stateful tracker
# ──────────────────────────────────────────────

def _make_state(**kwargs) -> RegimeMicroState:
    cfg = RegimeMicroConfig(**kwargs)
    return RegimeMicroState(cfg=cfg)


def _push_n(state: RegimeMicroState, n: int, close: float, high: float = 0.0, low: float = 0.0, vol: str = "normal") -> str:
    lbl = "na"
    for i in range(n):
        lbl = state.push_bar(close=close + i * 0.01, high=high or close + 0.5, low=low or close - 0.5, vol_state=vol, ts_ms=1_000_000 + i * 60_000)
    return lbl


class TestRegimeMicroState:
    def test_initial_label_na(self):
        s = _make_state()
        assert s.label == "na"

    def test_requires_two_bars_minimum(self):
        s = _make_state()
        lbl = s.push_bar(100.0, 101.0, 99.0, "normal", 1_000_000)
        # Only 1 bar, window needs >= 2 closes
        assert lbl == "na"

    def test_second_bar_produces_label(self):
        s = _make_state()
        s.push_bar(100.0, 101.0, 99.0, "normal", 1_000_000)
        lbl = s.push_bar(100.5, 101.0, 99.5, "normal", 1_060_000)
        assert lbl in {"trend_micro_up", "trend_micro_down", "range_micro", "shock_micro", "squeeze_micro", "mixed_micro"}

    def test_strong_uptrend_detected(self):
        s = _make_state()
        # Equal 1m moves of +30 bps each → cumulative +120 bps; z ≈ 2.5 < 3.0 (not shock)
        prices = [1000.0, 1003.0, 1006.0, 1009.0, 1012.0]
        lbl = "na"
        for i, p in enumerate(prices):
            lbl = s.push_bar(p, p + 1, p - 1, "normal", 1_000_000 + i * 60_000)
        assert lbl == "trend_micro_up"


    def test_strong_downtrend_detected(self):
        s = _make_state()
        # Equal 1m moves of -30 bps each; z ≈ 2.5 < 3.0 (not shock)
        prices = [1012.0, 1009.0, 1006.0, 1003.0, 1000.0]
        lbl = "na"
        for i, p in enumerate(prices):
            lbl = s.push_bar(p, p + 1, p - 1, "normal", 1_000_000 + i * 60_000)
        assert lbl == "trend_micro_down"

    def test_shock_from_vol_state(self):
        s = _make_state()
        prices = [100.0, 100.1, 100.2, 100.3, 100.4]
        lbl = "na"
        for i, p in enumerate(prices):
            lbl = s.push_bar(p, p + 0.1, p - 0.1, "shock", 1_000_000 + i * 60_000)
        assert lbl == "shock_micro"

    def test_squeeze_in_calm_tight_range(self):
        s = _make_state()
        # tiny moves, calm vol
        prices = [1000.0, 1000.02, 1000.01, 1000.03, 1000.02]
        lbl = "na"
        for i, p in enumerate(prices):
            # high=low=price → range_pct ≈ 0
            lbl = s.push_bar(p, p + 0.001, p - 0.001, "calm", 1_000_000 + i * 60_000)
        assert lbl in ("squeeze_micro", "range_micro")

    def test_disabled_returns_na(self):
        s = _make_state(enabled=False)
        for i in range(5):
            lbl = s.push_bar(1000.0 + i, 1001.0 + i, 999.0 + i, "shock", 1_000_000 + i * 60_000)
        assert lbl == "na"

    def test_label_ts_ms_updated(self):
        s = _make_state()
        ts = 9_999_000
        for i in range(5):
            s.push_bar(1000.0 + i * 0.5, 1001.0, 999.0, "normal", ts + i * 60_000)
        assert s.label_ts_ms == ts + 4 * 60_000

    def test_zero_close_price_skipped(self):
        s = _make_state()
        s.push_bar(0.0, 0.0, 0.0, "normal", 1_000_000)
        assert s.label == "na"

    def test_rolling_window_5_bars(self):
        s = _make_state(window_bars=5)
        # Push 10 bars — only last 5 matter
        prices = [1000.0] * 5 + [1001.0, 1002.0, 1003.0, 1004.0, 1005.0]
        lbl = "na"
        for i, p in enumerate(prices):
            lbl = s.push_bar(p, p + 1, p - 1, "normal", 1_000_000 + i * 60_000)
        # Last 5 bars: 1001→1005 = +40 bps → trend_micro_up
        assert lbl == "trend_micro_up"
