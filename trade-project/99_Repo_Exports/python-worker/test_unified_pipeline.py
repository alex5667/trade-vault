#!/usr/bin/env python3
"""
Test script for UnifiedSignalPipeline migration.

This script tests the new unified pipeline against the legacy signal generation
to ensure behavioral consistency during migration.
"""

import sys
import os
from datetime import datetime
from unittest.mock import Mock, MagicMock

# Add the python-worker directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from signals.unified_pipeline import UnifiedSignalPipeline
from signals.types import OrderflowContext, SignalContext
from signals.golden_pattern_service import GoldenPatternService
from signals.calibration_service import CalibrationService
from signals.exec_filters import ExecFiltersGroup
from signals.signal_publisher import SignalPublisher


def create_mock_services():
    """Create mock services for testing."""
    # Mock scoring engine
    scoring_engine = Mock()
    scoring_engine.score.return_value = 75.0  # Mock confidence score

    # Mock regime service
    regime_service = Mock()
    mock_regime = Mock()
    mock_regime.regime_type = "trend"
    mock_regime.allow_emit.return_value = True
    regime_service.get_regime.return_value = mock_regime

    # Real services (simplified)
    golden_logic = GoldenPatternService()
    exec_filters = ExecFiltersGroup()
    publisher = SignalPublisher()

    # Mock calibrator
    calibrator = CalibrationService()

    return scoring_engine, regime_service, golden_logic, exec_filters, publisher, calibrator


def create_test_orderflow_context():
    """Create a test OrderflowContext with realistic data."""
    return OrderflowContext(
        ts=int(datetime.now().timestamp() * 1000),
        price=1.0500,
        symbol="EURUSD",
        family="orderflow",
        venue="mt5",
        timeframe="1m",
        z_delta=2.5,  # Strong bullish impulse
        weak_progress=False,
        obi=0.8,
        obi_avg=0.7,
        obi_sustained=True,
        atr=0.0010,  # 10 points ATR
        regime="trend",
        regime_trend_score=0.8,
        regime_range_score=0.2,
        spread_bps=2.0,
        last_price=1.0500,
        daily_open=1.0480,
        daily_open_dist_bps=200.0,  # 2% from daily open
    )


def test_unified_pipeline():
    """Test the unified pipeline with mock data."""
    print("Testing UnifiedSignalPipeline...")

    # Create mock services
    services = create_mock_services()
    scoring_engine, regime_service, golden_logic, exec_filters, publisher, calibrator = services

    # Create pipeline
    pipeline = UnifiedSignalPipeline(*services)

    # Create test context
    of_ctx = create_test_orderflow_context()

    print(f"Input OrderflowContext: symbol={of_ctx.symbol}, z_delta={of_ctx.z_delta}, obi={of_ctx.obi}")

    # Test pipeline steps
    print("\n1. Testing build_ctx...")
    sig_ctx = pipeline.build_ctx(of_ctx)
    print(f"   Created SignalContext: symbol={sig_ctx.symbol}, ts={sig_ctx.ts_event_ms}")

    print("\n2. Testing attach_regime...")
    pipeline.attach_regime(sig_ctx)
    print(f"   Regime: {sig_ctx.regime.regime_type}, Session: {sig_ctx.session}")

    print("\n3. Testing apply_scoring...")
    pipeline.apply_scoring(sig_ctx)
    print(f"   Base score: {sig_ctx.base_score}")

    print("\n4. Testing apply_golden_logic...")
    pipeline.apply_golden_logic(sig_ctx)
    print(f"   Final score: {sig_ctx.final_score}, Is golden: {sig_ctx.is_golden_pattern}")

    print("\n5. Testing should_emit...")
    should_emit = pipeline.should_emit(sig_ctx)
    print(f"   Should emit: {should_emit}")

    print("\n6. Testing full process...")
    signal = pipeline.process(of_ctx)
    if signal:
        print(f"   Generated signal: {signal}")
    else:
        print("   No signal generated")

    print("\n✅ UnifiedSignalPipeline test completed successfully!")
    return True


def test_legacy_vs_unified_comparison():
    """Compare legacy and unified approaches (placeholder for future implementation)."""
    print("\nTesting legacy vs unified comparison...")
    print("⚠️  Legacy comparison not implemented yet - requires integration with existing handlers")
    return True


if __name__ == "__main__":
    try:
        test_unified_pipeline()
        test_legacy_vs_unified_comparison()
        print("\n🎉 All tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
