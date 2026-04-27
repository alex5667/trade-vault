"""
Tests for tools/cron_demo_reconcile.py  —  SL/TP coverage + reconcile logic.
"""
from __future__ import annotations

import pytest
from typing import Any, Dict, List

from tools.cron_demo_reconcile import (
    TestnetAccount,
    ReconcileResult,
    SymbolPnlRow,
    ClosedPnlSummary,
    classify_sl_tp_coverage,
    compare_closed_pnl,
    reconcile,
    build_reconcile_text,
    _SymbolCoverage,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pos(symbol: str, amt: float = 1.0, entry: float = 100.0,
         mark: float = 105.0, upnl: float = 5.0) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "positionAmt": amt,
        "entryPrice": entry,
        "markPrice": mark,
        "unrealizedProfit": upnl,
        "positionSide": "BOTH",
    }


def _order(symbol: str, otype: str, stop_price: float = 0.0,
           price: float = 0.0, price_rate: float = 0.0) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "type": otype,
        "stopPrice": str(stop_price),
        "price": str(price),
        "activatePrice": "0",
        "priceRate": str(price_rate),
    }


# ---------------------------------------------------------------------------
# classify_sl_tp_coverage
# ---------------------------------------------------------------------------

class TestClassifySlTpCoverage:
    def test_position_with_sl_and_tp(self):
        positions = [_pos("BTCUSDT")]
        orders = [
            _order("BTCUSDT", "STOP_MARKET", stop_price=48000.0),
            _order("BTCUSDT", "TAKE_PROFIT_MARKET", stop_price=55000.0),
        ]
        cov = classify_sl_tp_coverage(positions, orders)
        assert "BTCUSDT" in cov
        sc = cov["BTCUSDT"]
        assert sc.has_sl is True
        assert sc.has_tp is True
        assert sc.sl_price == 48000.0
        assert sc.tp_price == 55000.0
        assert sc.sl_type == "STOP_MARKET"
        assert sc.tp_type == "TAKE_PROFIT_MARKET"

    def test_position_with_trailing_only(self):
        positions = [_pos("ETHUSDT")]
        orders = [
            _order("ETHUSDT", "TRAILING_STOP_MARKET", price_rate=1.5),
        ]
        cov = classify_sl_tp_coverage(positions, orders)
        sc = cov["ETHUSDT"]
        assert sc.has_sl is False
        assert sc.has_tp is False
        assert sc.has_trailing is True
        assert sc.trailing_delta == 1.5

    def test_position_with_no_orders(self):
        positions = [_pos("SOLUSDT")]
        orders = []
        cov = classify_sl_tp_coverage(positions, orders)
        sc = cov["SOLUSDT"]
        assert sc.has_sl is False
        assert sc.has_tp is False
        assert sc.has_trailing is False

    def test_order_for_unknown_symbol_ignored(self):
        positions = [_pos("BTCUSDT")]
        orders = [
            _order("XYZUSDT", "STOP_MARKET", stop_price=10.0),  # no position
        ]
        cov = classify_sl_tp_coverage(positions, orders)
        assert "XYZUSDT" not in cov
        sc = cov["BTCUSDT"]
        assert sc.has_sl is False

    def test_stop_type_recognized(self):
        """STOP (non-market) is also SL."""
        positions = [_pos("BNBUSDT")]
        orders = [_order("BNBUSDT", "STOP", stop_price=300.0)]
        cov = classify_sl_tp_coverage(positions, orders)
        assert cov["BNBUSDT"].has_sl is True
        assert cov["BNBUSDT"].sl_type == "STOP"

    def test_take_profit_type_recognized(self):
        """TAKE_PROFIT (non-market) is also TP."""
        positions = [_pos("BNBUSDT")]
        orders = [_order("BNBUSDT", "TAKE_PROFIT", stop_price=700.0)]
        cov = classify_sl_tp_coverage(positions, orders)
        assert cov["BNBUSDT"].has_tp is True
        assert cov["BNBUSDT"].tp_type == "TAKE_PROFIT"

    def test_multiple_positions(self):
        positions = [_pos("BTCUSDT"), _pos("ETHUSDT"), _pos("SOLUSDT")]
        orders = [
            _order("BTCUSDT", "STOP_MARKET", stop_price=48000.0),
            _order("BTCUSDT", "TAKE_PROFIT_MARKET", stop_price=55000.0),
            # ETHUSDT has only SL
            _order("ETHUSDT", "STOP_MARKET", stop_price=2800.0),
            # SOLUSDT has nothing
        ]
        cov = classify_sl_tp_coverage(positions, orders)
        assert cov["BTCUSDT"].has_sl and cov["BTCUSDT"].has_tp
        assert cov["ETHUSDT"].has_sl and not cov["ETHUSDT"].has_tp
        assert not cov["SOLUSDT"].has_sl and not cov["SOLUSDT"].has_tp


# ---------------------------------------------------------------------------
# reconcile() — SL/TP section
# ---------------------------------------------------------------------------

class TestReconcileSlTp:
    def _account(self, positions, orders) -> TestnetAccount:
        return TestnetAccount(
            total_wallet_balance=1000.0,
            total_unrealized_profit=10.0,
            positions=positions,
            open_orders=orders,
        )

    def test_unprotected_count(self):
        positions = [_pos("BTCUSDT"), _pos("SOLUSDT")]
        orders = [
            _order("BTCUSDT", "STOP_MARKET", stop_price=48000.0),
            _order("BTCUSDT", "TAKE_PROFIT_MARKET", stop_price=55000.0),
            # SOLUSDT has nothing → unprotected
        ]
        result = reconcile([], self._account(positions, orders), [])
        assert result.unprotected_count == 1
        assert len(result.sl_tp_coverage_lines) == 2

    def test_all_protected(self):
        positions = [_pos("BTCUSDT")]
        orders = [
            _order("BTCUSDT", "STOP_MARKET", stop_price=48000.0),
            _order("BTCUSDT", "TAKE_PROFIT_MARKET", stop_price=55000.0),
        ]
        result = reconcile([], self._account(positions, orders), [])
        assert result.unprotected_count == 0

    def test_trailing_counts_as_tp(self):
        """Position with SL + trailing → protected (trailing substitutes TP)."""
        positions = [_pos("ETHUSDT")]
        orders = [
            _order("ETHUSDT", "STOP_MARKET", stop_price=2800.0),
            _order("ETHUSDT", "TRAILING_STOP_MARKET", price_rate=1.0),
        ]
        result = reconcile([], self._account(positions, orders), [])
        assert result.unprotected_count == 0

    def test_no_positions_empty(self):
        result = reconcile([], self._account([], []), [])
        assert result.sl_tp_coverage_lines == []
        assert result.unprotected_count == 0


# ---------------------------------------------------------------------------
# build_reconcile_text() — SL/TP section rendering
# ---------------------------------------------------------------------------

class TestBuildReconcileTextSlTp:
    def _minimal_result(self, sl_tp_lines=None, unprotected=0) -> ReconcileResult:
        return ReconcileResult(
            project_orders_n=0,
            project_unique_symbols=0,
            project_exec_price_avg=0.0,
            project_qty_total=0.0,
            testnet_wallet_balance=1000.0,
            testnet_unrealized_pnl=10.0,
            testnet_open_positions=[],
            testnet_open_orders_n=0,
            testnet_realized_pnl=0.0,
            position_diffs=[],
            slippage_lines=[],
            orphaned_positions=[],
            missing_positions=[],
            sl_tp_coverage_lines=sl_tp_lines or [],
            unprotected_count=unprotected,
        )

    def test_section_appears_when_coverage_exists(self):
        r = self._minimal_result(
            sl_tp_lines=["<code>BTCUSDT</code>: amt=+1.0 | ✅ SL | ✅ TP"],
            unprotected=0,
        )
        txt = build_reconcile_text(r, since_hours=24, ts="20260316")
        assert "🛡️ SL/TP Coverage" in txt
        assert "BTCUSDT" in txt

    def test_section_hidden_when_no_positions(self):
        r = self._minimal_result(sl_tp_lines=[], unprotected=0)
        txt = build_reconcile_text(r, since_hours=24, ts="20260316")
        assert "SL/TP Coverage" not in txt

    def test_unprotected_warning_shown(self):
        r = self._minimal_result(
            sl_tp_lines=["<code>SOLUSDT</code>: amt=+1.0 | ❌ SL | ❌ TP ⚠️"],
            unprotected=1,
        )
        txt = build_reconcile_text(r, since_hours=24, ts="20260316")
        assert "⚠️ 1 unprotected" in txt


# ---------------------------------------------------------------------------
# compare_closed_pnl()
# ---------------------------------------------------------------------------

def _sql_row(symbol: str, n_trades: int = 5, wins: int = 3,
             pnl_net_sum: float = 10.0, fees_sum: float = 0.5) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "n_trades": n_trades,
        "wins": wins,
        "pnl_net_sum": pnl_net_sum,
        "fees_sum": fees_sum,
    }


def _income(symbol: str, income: float) -> Dict[str, Any]:
    return {"symbol": symbol, "income": str(income), "incomeType": "REALIZED_PNL"}


class TestComparePnl:
    def test_matched_pnl(self):
        """proj ≈ testnet → delta close to zero."""
        sql = [_sql_row("BTCUSDT", pnl_net_sum=10.0)]
        inc = [_income("BTCUSDT", 10.0)]
        cp = compare_closed_pnl(sql, inc)
        assert len(cp.rows) == 1
        r = cp.rows[0]
        assert r.symbol == "BTCUSDT"
        assert abs(r.delta_pnl) < 1e-8
        assert abs(cp.delta_total) < 1e-8

    def test_delta_calculated(self):
        """proj PnL > testnet → positive delta."""
        sql = [_sql_row("ETHUSDT", pnl_net_sum=12.0)]
        inc = [_income("ETHUSDT", 10.0)]
        cp = compare_closed_pnl(sql, inc)
        assert abs(cp.rows[0].delta_pnl - 2.0) < 1e-6
        assert abs(cp.delta_total - 2.0) < 1e-6

    def test_negative_delta(self):
        """proj PnL < testnet → negative delta."""
        sql = [_sql_row("SOLUSDT", pnl_net_sum=8.0)]
        inc = [_income("SOLUSDT", 10.0)]
        cp = compare_closed_pnl(sql, inc)
        assert abs(cp.rows[0].delta_pnl - (-2.0)) < 1e-6

    def test_winrate(self):
        """3 wins out of 5 trades → win_rate=60%."""
        sql = [_sql_row("BTCUSDT", n_trades=5, wins=3, pnl_net_sum=5.0)]
        inc = []
        cp = compare_closed_pnl(sql, inc)
        assert cp.proj_total_trades == 5
        assert cp.proj_total_wins == 3
        assert abs(cp.proj_win_rate_pct - 60.0) < 1e-6

    def test_symbol_in_testnet_only(self):
        """Income exists but no SQL row → proj=0, delta=-tn_pnl."""
        sql: List[Dict[str, Any]] = []
        inc = [_income("BNBUSDT", 5.0)]
        cp = compare_closed_pnl(sql, inc)
        assert len(cp.rows) == 1
        r = cp.rows[0]
        assert r.symbol == "BNBUSDT"
        assert r.proj_trades == 0
        assert abs(r.proj_pnl_net) < 1e-8
        assert abs(r.delta_pnl - (-5.0)) < 1e-6

    def test_symbol_in_sql_only(self):
        """SQL row exists but no income → tn_pnl=0, delta=proj_pnl."""
        sql = [_sql_row("XRPUSDT", pnl_net_sum=7.0)]
        inc: List[Dict[str, Any]] = []
        cp = compare_closed_pnl(sql, inc)
        r = cp.rows[0]
        assert abs(r.tn_pnl) < 1e-8
        assert abs(r.delta_pnl - 7.0) < 1e-6

    def test_empty_inputs(self):
        """Both empty → ClosedPnlSummary with all zeros."""
        cp = compare_closed_pnl([], [])
        assert cp.rows == []
        assert abs(cp.proj_total_pnl) < 1e-8
        assert abs(cp.tn_total_pnl) < 1e-8
        assert cp.proj_total_trades == 0
        assert abs(cp.proj_win_rate_pct) < 1e-8

    def test_multi_symbol_totals(self):
        """Totals aggregate across symbols correctly."""
        sql = [
            _sql_row("BTCUSDT", n_trades=3, wins=2, pnl_net_sum=9.0),
            _sql_row("ETHUSDT", n_trades=2, wins=1, pnl_net_sum=3.0),
        ]
        inc = [_income("BTCUSDT", 8.5), _income("ETHUSDT", 2.5)]
        cp = compare_closed_pnl(sql, inc)
        assert cp.proj_total_trades == 5
        assert cp.proj_total_wins == 3
        assert abs(cp.proj_total_pnl - 12.0) < 1e-6
        assert abs(cp.tn_total_pnl  - 11.0) < 1e-6
        assert abs(cp.delta_total   -  1.0) < 1e-6

    def test_income_aggregated_per_symbol(self):
        """Multiple income events for same symbol are summed."""
        sql = [_sql_row("BTCUSDT", pnl_net_sum=0.0)]
        inc = [_income("BTCUSDT", 3.0), _income("BTCUSDT", 2.0)]
        cp = compare_closed_pnl(sql, inc)
        assert abs(cp.rows[0].tn_pnl - 5.0) < 1e-6


# ---------------------------------------------------------------------------
# build_reconcile_text() — 💹 Closed Trades PnL section
# ---------------------------------------------------------------------------

class TestBuildReconcileTextPnl:
    """Verify that the 💹 Closed Trades PnL section renders and hides correctly."""

    def _base_result(self, closed_pnl=None,
                     testnet_realized_pnl: float = 0.0) -> ReconcileResult:
        return ReconcileResult(
            project_orders_n=0,
            project_unique_symbols=0,
            project_exec_price_avg=0.0,
            project_qty_total=0.0,
            testnet_wallet_balance=1000.0,
            testnet_unrealized_pnl=0.0,
            testnet_open_positions=[],
            testnet_open_orders_n=0,
            testnet_realized_pnl=testnet_realized_pnl,
            position_diffs=[],
            slippage_lines=[],
            orphaned_positions=[],
            missing_positions=[],
            closed_pnl=closed_pnl,
        )

    def test_section_appears_when_trades_exist(self):
        cp = compare_closed_pnl(
            [_sql_row("BTCUSDT", n_trades=10, wins=6, pnl_net_sum=20.0)],
            [_income("BTCUSDT", 19.0)],
        )
        r = self._base_result(closed_pnl=cp)
        txt = build_reconcile_text(r, since_hours=24, ts="20260316")
        assert "💹 Closed Trades PnL" in txt
        assert "BTCUSDT" in txt
        assert "10" in txt  # trades count

    def test_section_hidden_when_no_trades(self):
        """closed_pnl=None and no testnet income → section absent."""
        r = self._base_result(closed_pnl=None, testnet_realized_pnl=0.0)
        txt = build_reconcile_text(r, since_hours=24, ts="20260316")
        assert "💹 Closed Trades PnL" not in txt
        assert "TRADES_DB_DSN" not in txt

    def test_hint_when_db_not_configured_but_income_exists(self):
        """closed_pnl=None but testnet has realized PnL → hint to configure DB."""
        r = self._base_result(closed_pnl=None, testnet_realized_pnl=25.5)
        txt = build_reconcile_text(r, since_hours=24, ts="20260316")
        assert "TRADES_DB_DSN" in txt

    def test_delta_sign_shown_positive(self):
        """Positive delta (+) is shown explicitly."""
        cp = compare_closed_pnl(
            [_sql_row("ETHUSDT", pnl_net_sum=12.0)],
            [_income("ETHUSDT", 10.0)],
        )
        r = self._base_result(closed_pnl=cp)
        txt = build_reconcile_text(r, since_hours=24, ts="20260316")
        assert "+2.00" in txt or "Δ=<code>+2.00" in txt

    def test_win_rate_shown(self):
        """Win rate percentage appears in the rendered text."""
        cp = compare_closed_pnl(
            [_sql_row("SOLUSDT", n_trades=4, wins=3, pnl_net_sum=8.0)],
            [_income("SOLUSDT", 7.0)],
        )
        r = self._base_result(closed_pnl=cp)
        txt = build_reconcile_text(r, since_hours=24, ts="20260316")
        assert "75%" in txt

