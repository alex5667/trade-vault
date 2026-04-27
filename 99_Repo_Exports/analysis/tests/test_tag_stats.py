# -*- coding: utf-8 -*-
"""
Unit tests for analysis.tag_stats (Trade, TagStats).

Tests are pure-Python — no external dependencies beyond pytest.
"""

import math

import pytest

from analysis.tag_stats import TagStats, Trade


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_trade(**kwargs) -> Trade:
    """Build a Trade with sensible defaults, overriding via kwargs."""
    defaults = dict(
        source="test",
        symbol="XAUUSD",
        exit_ts_ms=1_700_000_000_000,
        pnl_net=0.0,
        pnl_if_fixed_exit=0.0,
        one_r_money=100.0,
        giveback=0.0,
        missed_profit=0.0,
        mfe_pnl=0.0,
        mae_pnl=0.0,
        trailing_started=False,
        trailing_active=False,
        close_reason="TP",
        close_reason_raw="TP",
        close_reason_detail="",
        entry_tag="A",
        strategy="default",
    )
    defaults.update(kwargs)
    return Trade(**defaults)  # type: ignore[arg-type]


def make_stats(tag: str = "A") -> TagStats:
    return TagStats(tag=tag)


# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------

class TestTagStatsEmpty:
    def test_finalize_empty(self):
        ts = make_stats()
        result = ts.finalize()
        assert result == {"tag": "A", "n": 0}

    def test_zero_counters(self):
        ts = make_stats()
        assert ts.n == 0
        assert ts.sum_pnl_net == 0.0
        assert ts.mdd == 0.0


# ---------------------------------------------------------------------------
# Single win trade
# ---------------------------------------------------------------------------

class TestSingleWinTrade:
    def setup_method(self):
        self.ts = make_stats()
        self.trade = make_trade(pnl_net=200.0, pnl_if_fixed_exit=150.0, one_r_money=100.0)
        self.ts.add_trade(self.trade)
        self.res = self.ts.finalize()

    def test_count(self):
        assert self.res["n"] == 1

    def test_pnl_net(self):
        assert self.res["pnl_net_sum"] == pytest.approx(200.0)
        assert self.res["pnl_net_avg"] == pytest.approx(200.0)

    def test_win_rate_managed(self):
        assert self.res["wr_managed"] == pytest.approx(1.0)

    def test_expectancy_r(self):
        # r_m = 200 / 100 = 2.0
        assert self.res["expectancy_r"] == pytest.approx(2.0)

    def test_expectancy_fixed_r(self):
        # r_b = 150 / 100 = 1.5
        assert self.res["expectancy_fixed_r"] == pytest.approx(1.5)

    def test_delta_expectancy_r(self):
        assert self.res["delta_expectancy_r"] == pytest.approx(0.5)

    def test_sharpe_single_trade_is_zero(self):
        # Only one trade → std=0 → sharpe=0 by convention
        assert self.res["sharpe"] == pytest.approx(0.0)

    def test_std_r_single_trade(self):
        assert self.res["std_r"] == pytest.approx(0.0)

    def test_mdd_no_drawdown(self):
        assert self.res["mdd_usd"] == pytest.approx(0.0)

    def test_trailing_share_zero(self):
        assert self.res["trailing_share"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Single loss trade
# ---------------------------------------------------------------------------

class TestSingleLossTrade:
    def setup_method(self):
        self.ts = make_stats()
        self.trade = make_trade(pnl_net=-50.0, pnl_if_fixed_exit=-60.0, one_r_money=100.0)
        self.ts.add_trade(self.trade)
        self.res = self.ts.finalize()

    def test_win_rate_zero(self):
        assert self.res["wr_managed"] == pytest.approx(0.0)

    def test_pnl_negative(self):
        assert self.res["pnl_net_sum"] == pytest.approx(-50.0)

    def test_expectancy_r_negative(self):
        assert self.res["expectancy_r"] == pytest.approx(-0.5)

    def test_mdd(self):
        # peak stays 0, eq drops to -50 → dd = 50
        assert self.res["mdd_usd"] == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# Multiple trades — MDD and Sharpe
# ---------------------------------------------------------------------------

class TestMultipleTrades:
    def setup_method(self):
        self.ts = make_stats()
        trades = [
            make_trade(pnl_net=100.0, one_r_money=100.0),   # +1R
            make_trade(pnl_net=-200.0, one_r_money=100.0),  # -2R
            make_trade(pnl_net=50.0, one_r_money=100.0),    # +0.5R
        ]
        for t in trades:
            self.ts.add_trade(t)
        self.res = self.ts.finalize()

    def test_count(self):
        assert self.res["n"] == 3

    def test_mdd(self):
        # eq progression: 100 → -100 → -50; peak=100, trough=-100, dd=200
        assert self.res["mdd_usd"] == pytest.approx(200.0)

    def test_win_rate(self):
        assert self.res["wr_managed"] == pytest.approx(2 / 3)

    def test_sharpe_computed(self):
        # expectancy_r = (1 + -2 + 0.5) / 3 = -0.5 / 3 ≈ -0.1667
        mean_r = (-0.5) / 3
        r_vals = [1.0, -2.0, 0.5]
        var = sum((r - mean_r) ** 2 for r in r_vals) / 2  # ddof=1
        std = math.sqrt(var)
        expected_sharpe = mean_r / std
        assert self.res["sharpe"] == pytest.approx(expected_sharpe, rel=1e-5)


# ---------------------------------------------------------------------------
# One R money = 0 — no R metrics computed
# ---------------------------------------------------------------------------

class TestZeroOneR:
    def test_no_r_metrics_when_one_r_zero(self):
        ts = make_stats()
        ts.add_trade(make_trade(pnl_net=100.0, one_r_money=0.0))
        res = ts.finalize()
        # n_r stays 0 → expectancy_r defaults to 0
        assert res["expectancy_r"] == pytest.approx(0.0)
        assert res["sharpe"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Trailing flag detection
# ---------------------------------------------------------------------------

class TestTrailingFlag:
    def test_trailing_started_counts(self):
        ts = make_stats()
        ts.add_trade(make_trade(trailing_started=True, pnl_net=100.0))
        res = ts.finalize()
        assert res["trailing_share"] == pytest.approx(1.0)

    def test_trailing_active_counts(self):
        ts = make_stats()
        ts.add_trade(make_trade(trailing_active=True, pnl_net=-10.0))
        res = ts.finalize()
        assert res["trailing_share"] == pytest.approx(1.0)

    def test_trailing_close_detected_via_raw(self):
        ts = make_stats()
        ts.add_trade(make_trade(
            trailing_started=True,
            pnl_net=100.0,
            close_reason_raw="TRAILING_STOP",
        ))
        res = ts.finalize()
        assert res["trailing_close_share"] == pytest.approx(1.0)

    def test_trailing_close_detected_via_detail(self):
        ts = make_stats()
        ts.add_trade(make_trade(
            trailing_active=True,
            pnl_net=50.0,
            close_reason_detail="hit trailing stop level",
        ))
        res = ts.finalize()
        assert res["trailing_close_share"] == pytest.approx(1.0)

    def test_not_trailing_close_when_tp(self):
        ts = make_stats()
        ts.add_trade(make_trade(trailing_started=True, pnl_net=100.0, close_reason_raw="TP"))
        res = ts.finalize()
        # trailing flag set but close reason is TP, not trailing
        assert res["trailing_close_share"] == pytest.approx(0.0)

    def test_trailing_wr(self):
        ts = make_stats()
        # win trailing
        ts.add_trade(make_trade(trailing_started=True, pnl_net=100.0, close_reason_raw="TRAILING"))
        # loss trailing
        ts.add_trade(make_trade(trailing_active=True, pnl_net=-50.0, close_reason_raw="TRAILING"))
        res = ts.finalize()
        assert res["trailing_wr"] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Giveback / Missed profit
# ---------------------------------------------------------------------------

class TestGivebackMissed:
    def test_giveback_aggregation(self):
        ts = make_stats()
        ts.add_trade(make_trade(pnl_net=100.0, giveback=20.0, mfe_pnl=100.0, one_r_money=100.0))
        res = ts.finalize()
        assert res["giveback_avg_usd"] == pytest.approx(20.0)
        assert res["giveback_avg_r"] == pytest.approx(0.2)  # 20/100
        assert res["giveback_avg_ratio"] == pytest.approx(0.2)  # 20/100 (mfe)
        assert res["giveback_share"] == pytest.approx(1.0)

    def test_missed_profit_aggregation(self):
        ts = make_stats()
        ts.add_trade(make_trade(pnl_net=50.0, missed_profit=30.0, mfe_pnl=150.0, one_r_money=100.0))
        res = ts.finalize()
        assert res["missed_avg_usd"] == pytest.approx(30.0)
        assert res["missed_avg_r"] == pytest.approx(0.3)
        assert res["missed_share"] == pytest.approx(1.0)

    def test_no_giveback(self):
        ts = make_stats()
        ts.add_trade(make_trade(pnl_net=100.0, giveback=0.0))
        res = ts.finalize()
        assert res["giveback_share"] == pytest.approx(0.0)
        assert res["giveback_avg_usd"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# MFE / MAE excursions
# ---------------------------------------------------------------------------

class TestExcursions:
    def test_mfe_mae_r(self):
        ts = make_stats()
        ts.add_trade(make_trade(pnl_net=100.0, mfe_pnl=300.0, mae_pnl=-50.0, one_r_money=100.0))
        res = ts.finalize()
        assert res["mfe_avg_r"] == pytest.approx(3.0)
        assert res["mae_avg_r"] == pytest.approx(-0.5)


# ---------------------------------------------------------------------------
# Payoff ratios
# ---------------------------------------------------------------------------

class TestPayoff:
    def test_payoff_r_win_loss(self):
        ts = make_stats()
        ts.add_trade(make_trade(pnl_net=200.0, one_r_money=100.0))   # +2R win
        ts.add_trade(make_trade(pnl_net=-100.0, one_r_money=100.0))  # -1R loss
        res = ts.finalize()
        # avg_win_r = 2.0, avg_loss_r = -1.0, payoff = 2.0
        assert res["payoff_r"] == pytest.approx(2.0)

    def test_payoff_usd_no_loss(self):
        ts = make_stats()
        ts.add_trade(make_trade(pnl_net=100.0))
        res = ts.finalize()
        assert res["payoff_usd"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# R reuse correctness (regression: trailing block must not recompute r_m/r_b)
# ---------------------------------------------------------------------------

class TestRReuseRegression:
    """
    Verify that r_m/r_b used in trailing block equal the same values
    as those used in the R-metrics block.
    """
    def test_trailing_r_matches_main_r(self):
        ts = make_stats()
        ts.add_trade(make_trade(
            pnl_net=150.0, pnl_if_fixed_exit=100.0,
            one_r_money=50.0,
            trailing_started=True,
        ))
        res = ts.finalize()
        # r_m = 150/50 = 3.0
        assert res["trailing_expectancy_r"] == pytest.approx(3.0)
        # r_b = 100/50 = 2.0
        assert res["trailing_expectancy_fixed_r"] == pytest.approx(2.0)
        assert res["trailing_delta_expectancy_r"] == pytest.approx(1.0)
