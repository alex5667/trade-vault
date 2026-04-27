#!/usr/bin/env python3
"""
Test Signal Execution System

Demonstrates the complete signal execution pipeline:
1. Signal generation
2. Execution planning
3. Performance tracking
4. TTD analysis
"""

import sys
from datetime import datetime, timedelta
from typing import List

# Add project root to path
sys.path.insert(0, '/home/alex/front/trade/scanner_infra/python-worker')

from signal_exec import (
    ExtendedSignalContext,
    ExecutionPlanner,
    SignalPerformanceTracker,
    SymbolSetupConfig,
    AccountState,
    Side,
    SwingPoint,
    HTFLevel,
    Bar1m,
)


def create_test_signal_context() -> ExtendedSignalContext:
    """Create a test signal context for breakout scenario."""
    return ExtendedSignalContext(
        signal_id="test-signal-001",
        symbol="XAUUSD",
        side=Side.LONG,
        setup_type="breakout_R1",
        ts_signal=datetime.now(),
        price_at_signal=1950.0,
        atr_1m=2.5,
        atr_5m=3.2,
        final_score=85.0,
        tick_size=0.01,
        contract_size=100.0,

        # Mock account state
        account_state=AccountState(
            equity_usd=10000.0,
            open_risk_usd=200.0,
            max_risk_per_trade_pct=0.5,  # 0.5% per trade
            max_portfolio_risk_pct=5.0,   # 5% total
        ),

        # Mock swing points for stop selection
        local_swings=[
            SwingPoint(
                ts=datetime.now() - timedelta(minutes=5),
                price=1940.0,
                type="low",
                volume=150.0,
                delta=-25.0,
            ),
            SwingPoint(
                ts=datetime.now() - timedelta(minutes=3),
                price=1945.0,
                type="low",
                volume=200.0,
                delta=-15.0,
            ),
        ],

        # Mock HTF levels for TP targets
        htf_levels=[
            HTFLevel(
                ts=datetime.now(),
                price=1960.0,
                kind="H1_high",
                strength=0.8,
            ),
            HTFLevel(
                ts=datetime.now(),
                price=1970.0,
                kind="D_high",
                strength=0.9,
            ),
        ],
    )


def create_test_bars(signal_ts: datetime) -> List[Bar1m]:
    """Create test 1m bars for performance analysis."""
    bars = []
    current_ts = signal_ts

    # Create 60 bars (1 hour) of mock data
    for i in range(60):
        # Simulate price movement after signal
        if i < 10:  # First 10 bars: consolidation
            price_change = 0.5
        elif i < 20:  # Next 10: breakout
            price_change = 2.0 + (i - 10) * 0.5
        elif i < 35:  # Next 15: profit taking
            price_change = 5.0 - (i - 20) * 0.3
        else:  # Final: sideways
            price_change = 2.0

        bars.append(Bar1m(
            ts=current_ts,
            open=1950.0 + price_change - 1.0,
            high=1950.0 + price_change + 0.5,
            low=1950.0 + price_change - 0.5,
            close=1950.0 + price_change,
        ))
        current_ts += timedelta(minutes=1)

    return bars


def test_execution_planning():
    """Test execution plan generation."""
    print("🔧 Testing Execution Planning")
    print("-" * 40)

    # Create signal context
    ctx = create_test_signal_context()

    # Setup configurations
    setup_configs = {
        ("XAUUSD", "breakout_R1"): SymbolSetupConfig(
            symbol="XAUUSD",
            setup_type="breakout_R1",
            expiry_bars=5,
            score_buckets=(0.4, 0.7, 0.85),
            risk_multipliers=(0.5, 1.0, 1.5, 2.0),
        ),
    }

    # Create planner and generate plan
    planner = ExecutionPlanner(setup_configs)
    plan = planner.build_plan(ctx)

    if plan:
        print("✅ Execution plan generated successfully!")
        print(f"   Signal ID: {plan.signal_id}")
        print(f"   Entry Zone: {plan.entry_zone_low:.2f} - {plan.entry_zone_high:.2f}")
        print(f"   Stop Price: {plan.stop_price:.2f}")
        print(f"   Risk R: {plan.pos_risk_R:.2f}")
        print(f"   TP Levels: {[round(tp, 2) for tp in plan.tp_levels]}")
        print(f"   Risk USD: ${plan.risk_usd:.4f}")
        print(f"   Position Size: {plan.position_size:.2f}")
        print(f"   Expiry: {plan.expiry_bars} bars")
        return plan
    else:
        print("❌ Failed to generate execution plan")
        return None


def test_performance_tracking():
    """Test performance analysis."""
    print("\n📊 Testing Performance Tracking")
    print("-" * 40)

    ctx = create_test_signal_context()
    bars = create_test_bars(ctx.ts_signal)

    # Simulate trade execution
    entry_ts = ctx.ts_signal + timedelta(minutes=2)  # Enter after 2 bars
    entry_price = 1952.0  # Enter slightly above signal price
    stop_price = 1945.0   # Based on swing low

    # Simulate profitable exit
    exit_ts = ctx.ts_signal + timedelta(minutes=25)
    exit_price = 1958.0  # +6 points profit

    # Create performance tracker
    tracker = SignalPerformanceTracker(r_target=1.0, max_ttd_bars=30)
    performance = tracker.build_performance(
        ctx=ctx,
        bars=bars,
        entry_ts=entry_ts,
        exit_ts=exit_ts,
        entry_price=entry_price,
        exit_price=exit_price,
        stop_price=stop_price,
    )

    print("✅ Performance analysis completed!")
    print(f"   Outcome: {performance.outcome}")
    print(f"   Realized R: {performance.realized_R:.2f}")
    print(f"   MFE R: {performance.mfe_R:.1f}")
    print(f"   MAE R: {performance.mae_R:.1f}")
    ttd_sec_str = f"{performance.ttd_seconds:.0f}" if performance.ttd_seconds else "N/A"
    print(f"   TTD: {performance.ttd_bars} bars ({ttd_sec_str} sec)")
    print(f"   Bars to Entry: {performance.bars_to_entry}")
    print(f"   Bars to Exit: {performance.bars_to_exit}")

    return performance


def main():
    """Main test function."""
    print("🎯 Signal Execution System Test")
    print("=" * 50)

    try:
        # Test execution planning
        plan = test_execution_planning()

        # Test performance tracking
        performance = test_performance_tracking()

        print("\n🎉 All tests completed successfully!")
        print("=" * 50)

        # Summary
        if plan and performance:
            print("📋 Summary:")
            print(f"   • Signal confidence: {plan.pos_risk_R * 50:.0f}% (mapped from score)")
            print(f"   • Risk per trade: ${plan.risk_usd:.0f}")
            print(f"   • Position size: {plan.position_size:.4f} lots")
            print(f"   • Realized P&L: {performance.realized_R:.2f}R")
            print(f"   • Time to target: {performance.ttd_bars} bars")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
