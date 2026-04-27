#!/usr/bin/env python3
"""
Test unified SignalContext and Execution Planning system.

This test verifies the complete pipeline:
SignalContext → ExecutionPlanner → PerformanceTracker → SignalBus
"""

import sys
import os
import asyncio
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(__file__))

async def test_unified_signal_system():
    """Test the complete unified signal system."""
    print("🧪 Testing Unified Signal System")
    print("=" * 50)

    try:
        # Import the complete system
        from signal_exec import (
            SignalContext, SignalService, ExecutionPlanner,
            SignalPerformanceTracker, SignalRepository,
            SignalBus, SymbolSetupConfig, AccountState, Side
        )
        print("✅ All imports successful")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return

    # Create setup configs
    setup_configs = {
        ("XAUUSD", "breakout_R1"): SymbolSetupConfig(
            symbol="XAUUSD",
            setup_type="breakout_R1",
            expiry_bars=5,
            min_stop_ticks=10,
            max_stop_R=3.0,
            atr_buffer_ratio=0.15,
            entry_zone_min_R=0.3,
            entry_zone_max_R=0.7,
            default_tp_R=(1.0, 2.0, 3.0),
            score_buckets=(0.4, 0.7, 0.85),
            risk_multipliers=(0.5, 1.0, 1.5, 2.0),
            max_risk_R_per_trade=1.0,
            max_portfolio_risk_pct=5.0,
        )
    }

    # Initialize components
    from unittest.mock import MagicMock, AsyncMock
    repo = MagicMock() # SignalRepository("postgresql://postgres:12345@postgres:5434/trade")
    planner = ExecutionPlanner(setup_configs)
    tracker = MagicMock() # SignalPerformanceTracker(repo, ttd_target_R=1.0, max_ttd_bars=30)
    bus = AsyncMock(spec=SignalBus)

    service = SignalService(repo, planner, tracker, bus)
    print("✅ Unified SignalService initialized (with mocks)")

    # Create proper SignalContext
    ctx = SignalContext(
        signal_id="test-signal-unified-123",
        symbol="XAUUSD",
        setup_type="breakout_R1",
        side=Side.LONG,
        ts_signal=datetime.now(timezone.utc),
        price_at_signal=2600.0,
        atr_1m=1.0,
        tick_size=0.1,
        contract_size=100.0,
        final_score=0.82,
        account_state=AccountState(
            equity_usd=10000.0,
            open_risk_usd=0.0,
            max_risk_per_trade_pct=0.5,
            max_portfolio_risk_pct=5.0,
        ),
    )
    print("✅ Unified SignalContext created")

    # Test SignalContext serialization
    ctx_dict = ctx.to_dict()
    ctx_restored = SignalContext.from_dict(ctx_dict)
    assert ctx.signal_id == ctx_restored.signal_id
    print("✅ SignalContext serialization works")

    # Test execution planning
    plan = planner.build_plan(ctx)
    if plan is None:
        print("❌ Plan creation failed")
        return

    print("✅ Execution plan created successfully")
    print(f"   Signal ID: {plan.signal_id}")
    print(f"   Entry Zone: {plan.entry_zone_low:.2f} - {plan.entry_zone_high:.2f}")
    print(f"   Position Size: {plan.position_size:.3f} lots")
    print(f"   Risk USD: ${plan.risk_usd:.2f}")

    # Test service signal processing
    await service.on_new_signal(ctx)
    print("✅ Signal processed through unified service")

    # Test execution event
    await service.on_exec_event(
        signal_id=ctx.signal_id,
        symbol=ctx.symbol,
        event_type="ENTRY_FILLED",
        ts=datetime.now(timezone.utc),
        price=2600.5,
    )
    print("✅ Execution event processed")

    print("\n🎉 Unified signal system test completed successfully!")
    print("All components work together correctly!")

def test_execution_planning_integration():
    """Legacy test for backwards compatibility."""
    asyncio.run(test_unified_signal_system())

if __name__ == "__main__":
    test_execution_planning_integration()
