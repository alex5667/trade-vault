#!/usr/bin/env python3
"""
Simple test for the trailing analyzer with mock data.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from analyze_trailing_vs_baseline_postgres import (
    TradeRow, TagStats, analyze_global, analyze_by_tag
    print_global_report, print_tag_report, mean, stddev, max_drawdown
)


def create_mock_trades():
    """Create mock trades for testing."""
    return [
        TradeRow(
            symbol="ETHUSDT"
            source="CryptoOrderFlow"
            entry_tag="bullish_signal"
            pnl_net=150.0
            pnl_fixed=100.0
            one_r=100.0
            mfe_pnl=200.0
            mae_pnl=-50.0
            giveback=50.0
            missed_profit=0.0
            trailing_started=True
            trailing_active=False
            close_reason="TRAILING_PROFIT"
            close_reason_raw="trailing_profit"
            close_reason_detail="TRAILING_PROFIT"
            notional_usd=1000.0
            exit_ts_ms=1700000000000
        )
        TradeRow(
            symbol="ETHUSDT"
            source="CryptoOrderFlow"
            entry_tag="bullish_signal"
            pnl_net=-80.0
            pnl_fixed=-100.0
            one_r=100.0
            mfe_pnl=50.0
            mae_pnl=-150.0
            giveback=0.0
            missed_profit=130.0
            trailing_started=False
            trailing_active=False
            close_reason="STOP_LOSS"
            close_reason_raw="stop_loss"
            close_reason_detail="STOP_LOSS"
            notional_usd=1000.0
            exit_ts_ms=1700000100000
        )
        TradeRow(
            symbol="ETHUSDT"
            source="CryptoOrderFlow"
            entry_tag="bearish_signal"
            pnl_net=120.0
            pnl_fixed=90.0
            one_r=100.0
            mfe_pnl=180.0
            mae_pnl=-30.0
            giveback=60.0
            missed_profit=0.0
            trailing_started=True
            trailing_active=True
            close_reason="TRAILING_PROFIT"
            close_reason_raw="trailing_profit"
            close_reason_detail="TRAILING_PROFIT"
            notional_usd=1000.0
            exit_ts_ms=1700000200000
        )
    ]


def test_basic_calculations():
    """Test basic calculation functions."""
    print("Testing basic calculations...")

    # Test mean
    assert mean([1, 2, 3, 4, 5]) == 3.0
    assert mean([]) == 0.0
    print("✓ Mean function works")

    # Test stddev
    assert stddev([1, 3, 5]) > 0
    assert stddev([1, 1, 1]) == 0.0
    print("✓ Stddev function works")

    # Test max_drawdown
    equity = [100, 110, 105, 95, 120, 115]
    mdd = max_drawdown(equity)
    assert mdd == 15.0  # From 110 to 95
    print("✓ Max drawdown function works")


def test_trade_properties():
    """Test TradeRow properties."""
    print("Testing TradeRow properties...")

    trade = TradeRow(
        symbol="ETHUSDT"
        source="CryptoOrderFlow"
        entry_tag="test"
        pnl_net=150.0
        pnl_fixed=100.0
        one_r=100.0
        mfe_pnl=200.0
        mae_pnl=-50.0
        giveback=50.0
        missed_profit=0.0
        trailing_started=True
        trailing_active=False
        close_reason="TRAILING_PROFIT"
        close_reason_raw="trailing_profit"
        close_reason_detail="TRAILING_PROFIT"
        notional_usd=1000.0
        exit_ts_ms=1700000000000
    )

    assert trade.r_managed == 1.5  # 150/100
    assert trade.r_baseline == 1.0  # 100/100
    assert trade.mfe_r == 2.0  # 200/100
    assert trade.mae_r == -0.5  # -50/100
    assert trade.giveback_r == 0.5  # 50/100
    assert trade.missed_r == 0.0  # 0/100
    assert trade.giveback_ratio == 0.25  # 50/200
    assert trade.is_trailing_trade == True
    assert trade.is_trailing_close == True
    assert trade.is_win == True
    assert trade.is_loss == False

    print("✓ TradeRow properties work")


def test_analyzer():
    """Test the main analyzer functions."""
    print("Testing analyzer functions...")

    trades = create_mock_trades()

    # Test global analysis
    global_stats = analyze_global(trades)
    assert global_stats["n"] == 3
    assert global_stats["wins"] == 2
    assert global_stats["losses"] == 1
    assert "sharpe_r" in global_stats
    assert "sortino_r" in global_stats
    print("✓ Global analysis works")

    # Test tag analysis
    tag_stats = analyze_by_tag(trades, min_trades=1)
    assert len(tag_stats) == 2  # bullish_signal and bearish_signal
    assert tag_stats[0]["tag"] == "bullish_signal"
    assert tag_stats[0]["n"] == 2
    print("✓ Tag analysis works")


def test_reporting():
    """Test reporting functions (without actual output)."""
    print("Testing reporting functions...")

    trades = create_mock_trades()
    global_stats = analyze_global(trades)
    tag_stats = analyze_by_tag(trades, min_trades=1)

    # Just call the functions to ensure they don't crash
    print_global_report("ETHUSDT", "CryptoOrderFlow", global_stats)
    print_tag_report(tag_stats, max_tags=5)

    print("✓ Reporting functions work")


def main():
    """Run all tests."""
    print("Running tests for trailing analyzer...")
    print("=" * 50)

    try:
        test_basic_calculations()
        print()
        test_trade_properties()
        print()
        test_analyzer()
        print()
        test_reporting()
        print()
        print("🎉 All tests passed!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
