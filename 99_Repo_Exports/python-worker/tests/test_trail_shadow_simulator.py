"""
Tests for TrailShadowSimulator — virtual P&L A/B test.

These tests use mock data and do NOT require Redis.
"""
from __future__ import annotations

import pytest

from services.trail_shadow_simulator import (
    simulate_shadow_exit_r,
    compute_shadow_results,
    ShadowSimResult,
    TrailShadowSimulator,
    _TradeForSim,
)


# ---------------------------------------------------------------------------
# simulate_shadow_exit_r (pure function)
# ---------------------------------------------------------------------------

class TestSimulateShadowExitR:
    def test_trail_not_engaged_returns_actual(self):
        """When MFE < activation threshold, shadow exit = actual exit."""
        result = simulate_shadow_exit_r(
            mfe_r=0.5,
            actual_pnl_r=0.3,
            callback_r=0.2,
            min_profit_lock_r=0.3,
            activate_offset_r=0.3,  # threshold = 0.6 > mfe_r=0.5
        )
        assert result == 0.3  # returns actual_pnl_r

    def test_trail_engaged_exit_at_mfe_minus_callback(self):
        """When MFE >= threshold, exit = MFE - callback."""
        result = simulate_shadow_exit_r(
            mfe_r=3.0,
            actual_pnl_r=1.5,
            callback_r=0.5,
            min_profit_lock_r=0.1,
            activate_offset_r=0.1,  # threshold = 0.2 < mfe_r=3.0
        )
        assert result == pytest.approx(2.5)  # 3.0 - 0.5

    def test_trail_engaged_exit_clamped_at_lock(self):
        """Exit clamped at min_profit_lock_r when callback erases all profit."""
        result = simulate_shadow_exit_r(
            mfe_r=1.0,
            actual_pnl_r=0.2,
            callback_r=1.5,  # bigger than MFE
            min_profit_lock_r=0.3,
            activate_offset_r=0.1,  # threshold = 0.4 < mfe_r=1.0
        )
        assert result == pytest.approx(0.3)  # clamped at min_profit_lock_r

    def test_zero_mfe_returns_actual(self):
        """Zero MFE → trail doesn't engage → return actual."""
        result = simulate_shadow_exit_r(
            mfe_r=0.0,
            actual_pnl_r=-0.5,
            callback_r=0.2,
            min_profit_lock_r=0.1,
            activate_offset_r=0.1,
        )
        assert result == -0.5


# ---------------------------------------------------------------------------
# compute_shadow_results (pure function)
# ---------------------------------------------------------------------------

def _make_trade(
    pnl_net: float, one_r: float, mfe_pnl: float,
    notional: float = 1000.0, giveback: float = 0.0,
) -> _TradeForSim:
    return _TradeForSim(
        symbol="BTCUSDT",
        regime="na",
        pnl_net=pnl_net,
        one_r_money=one_r,
        mfe_pnl=mfe_pnl,
        giveback=giveback,
        entry_price=50000.0,
        notional=notional,
        trailing_started=False,
    )


class TestComputeShadowResults:
    def test_shadow_better_than_actual(self):
        """When calibrated trail captures more MFE, shadow > actual → BETTER."""
        trades = [
            _make_trade(pnl_net=5.0, one_r=10.0, mfe_pnl=30.0),  # actual=0.5R, mfe=3.0R
            _make_trade(pnl_net=8.0, one_r=10.0, mfe_pnl=25.0),  # actual=0.8R, mfe=2.5R
            _make_trade(pnl_net=3.0, one_r=10.0, mfe_pnl=20.0),  # actual=0.3R, mfe=2.0R
            _make_trade(pnl_net=6.0, one_r=10.0, mfe_pnl=18.0),  # actual=0.6R, mfe=1.8R
            _make_trade(pnl_net=4.0, one_r=10.0, mfe_pnl=15.0),  # actual=0.4R, mfe=1.5R
        ]
        result = compute_shadow_results(
            trades=trades,
            callback_atr_mult=1.0,
            activate_offset_bps=5.0,
            min_profit_lock_r=0.1,
            atr_bps=30.0,
        )
        assert result is not None
        assert result.n_trades == 5
        assert result.delta_pnl_r > 0  # shadow better
        assert result.recommendation == "BETTER"

    def test_shadow_trail_not_engaged(self):
        """With very low MFE, trail doesn't engage → shadow ≈ actual → NEUTRAL."""
        trades = [
            _make_trade(pnl_net=1.0, one_r=10.0, mfe_pnl=1.5),  # mfe=0.15R
            _make_trade(pnl_net=0.5, one_r=10.0, mfe_pnl=0.8),
            _make_trade(pnl_net=-2.0, one_r=10.0, mfe_pnl=0.3),
            _make_trade(pnl_net=0.2, one_r=10.0, mfe_pnl=0.5),
            _make_trade(pnl_net=-1.0, one_r=10.0, mfe_pnl=0.1),
        ]
        result = compute_shadow_results(
            trades=trades,
            callback_atr_mult=2.0,
            activate_offset_bps=10.0,
            min_profit_lock_r=0.3,
            atr_bps=30.0,
        )
        assert result is not None
        assert abs(result.delta_pnl_r) <= 0.05  # essentially the same
        assert result.recommendation == "NEUTRAL"

    def test_too_few_trades_returns_none(self):
        """Less than 5 trades → None."""
        trades = [_make_trade(1.0, 10.0, 20.0) for _ in range(3)]
        result = compute_shadow_results(
            trades=trades,
            callback_atr_mult=1.0,
            activate_offset_bps=5.0,
            min_profit_lock_r=0.1,
            atr_bps=30.0,
        )
        assert result is None

    def test_empty_trades_returns_none(self):
        result = compute_shadow_results(
            trades=[],
            callback_atr_mult=1.0,
            activate_offset_bps=5.0,
            min_profit_lock_r=0.1,
            atr_bps=30.0,
        )
        assert result is None

    def test_recommendation_worse(self):
        """When calibrated params hurt, recommendation = WORSE."""
        # Large callback = trail catches less than actual
        trades = [
            _make_trade(pnl_net=15.0, one_r=10.0, mfe_pnl=16.0),  # actual=1.5R, mfe≈1.6R
            _make_trade(pnl_net=12.0, one_r=10.0, mfe_pnl=13.0),
            _make_trade(pnl_net=18.0, one_r=10.0, mfe_pnl=19.0),
            _make_trade(pnl_net=10.0, one_r=10.0, mfe_pnl=11.0),
            _make_trade(pnl_net=14.0, one_r=10.0, mfe_pnl=15.0),
        ]
        result = compute_shadow_results(
            trades=trades,
            callback_atr_mult=3.0,  # very wide callback
            activate_offset_bps=2.0,
            min_profit_lock_r=0.1,
            atr_bps=30.0,
        )
        assert result is not None
        assert result.delta_pnl_r < 0  # shadow worse
        assert result.recommendation == "WORSE"


# ---------------------------------------------------------------------------
# Telegram report formatting
# ---------------------------------------------------------------------------

class TestShadowTelegramReport:
    def test_empty_results_returns_empty(self):
        assert TrailShadowSimulator.format_telegram_report([]) == ""

    def test_report_includes_key_fields(self):
        results = [
            ShadowSimResult(
                symbol="BTCUSDT", regime="na", n_trades=100,
                actual_avg_pnl_r=0.5, shadow_avg_pnl_r=0.8,
                delta_pnl_r=0.3, actual_win_rate=0.45,
                shadow_win_rate=0.52, shadow_sharpe=1.2,
                recommendation="BETTER", computed_at_ms=1000000,
            ),
        ]
        report = TrailShadowSimulator.format_telegram_report(results)
        assert "Shadow A/B" in report
        assert "BTCUSDT" in report
        assert "+0.300R" in report
        assert "✅" in report
