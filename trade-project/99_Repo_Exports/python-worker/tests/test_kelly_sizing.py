"""Phase 3.2 — fractional Kelly sizing tests."""
from __future__ import annotations

import math
from core.kelly_sizing import kelly_fraction, size_position


def test_kelly_zero_at_break_even():
    # p = 1/(1+b) is break-even
    assert math.isclose(kelly_fraction(0.5, 1.0), 0.0, abs_tol=1e-9)


def test_kelly_positive_with_edge():
    assert kelly_fraction(0.6, 1.0) > 0


def test_kelly_negative_when_no_edge():
    assert kelly_fraction(0.3, 1.0) < 0


def test_kelly_zero_payoff():
    assert kelly_fraction(0.9, 0.0) == 0.0


def test_kelly_classical_value():
    # p=0.6, b=2 → f* = (0.6*2 - 0.4) / 2 = 0.4
    assert math.isclose(kelly_fraction(0.6, 2.0), 0.4, abs_tol=1e-9)


def test_size_no_trade_when_below_edge_band():
    r = size_position(p_win=0.51, payoff_ratio=1.0, sl_bps=10.0, min_edge_bps=5.0)
    assert r.fraction == 0.0
    assert r.rejected_reason.startswith("edge_below_min")


def test_size_respects_fractional_multiplier():
    r = size_position(
        p_win=0.7, payoff_ratio=2.0, kelly_mult=0.25,
        sl_bps=10.0, min_edge_bps=0.0, max_position_pct=1.0,
    )
    # raw kelly = (0.7*2 - 0.3)/2 = 0.55 → fraction 0.25 → 0.1375
    assert math.isclose(r.fraction, 0.1375, abs_tol=1e-6)
    assert r.kelly_raw > r.fraction


def test_size_capped_by_max_position_pct():
    r = size_position(
        p_win=0.95, payoff_ratio=5.0, kelly_mult=1.0,
        sl_bps=100.0, min_edge_bps=0.0, max_position_pct=0.02,
    )
    assert r.fraction == 0.02
    assert r.kelly_raw > 0.02


def test_size_collapses_to_zero_on_negative_edge():
    r = size_position(
        p_win=0.30, payoff_ratio=1.0, kelly_mult=0.25,
        sl_bps=10.0, min_edge_bps=-1.0e9, max_position_pct=1.0,
    )
    assert r.fraction == 0.0
    assert r.kelly_raw < 0.0
    assert r.rejected_reason == "kelly_non_positive"


def test_size_calib_not_ok_uses_fallback():
    r = size_position(
        p_win=0.9, payoff_ratio=2.0, kelly_mult=0.25,
        sl_bps=10.0, min_edge_bps=0.0, max_position_pct=0.02,
        calib_ok=False, fallback_fixed_pct=0.005,
    )
    assert r.fraction == 0.005
    assert r.rejected_reason == ""


def test_size_calib_not_ok_zero_fallback():
    r = size_position(
        p_win=0.9, payoff_ratio=2.0, calib_ok=False, fallback_fixed_pct=0.0,
    )
    assert r.fraction == 0.0
    assert r.rejected_reason == "calib_not_ok"
