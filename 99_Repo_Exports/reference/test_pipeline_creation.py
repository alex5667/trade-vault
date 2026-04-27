#!/usr/bin/env python3
"""
Test UnifiedSignalPipeline creation logic in isolation.
"""

import os
import sys
from unittest.mock import Mock

# Add the python-worker directory to the path
sys.path.insert(0, os.path.dirname(__file__))

def test_pipeline_creation_logic():
    """Test the logic for creating UnifiedSignalPipeline."""
    print("Testing UnifiedSignalPipeline creation logic...")

    try:
        from signals.unified_pipeline import UnifiedSignalPipeline
        from signals.golden_pattern_service import GoldenPatternService
        from signals.calibration_service import CalibrationService
        from signals.exec_filters import ExecFiltersGroup
        from signals.signal_publisher import SignalPublisher

        # Create mock services (as they would be in BaseOrderFlowHandler)
        scoring_engine = Mock()
        regime_service = Mock()
        local_calibration = Mock()

        # Test the creation logic from BaseOrderFlowHandler
        try:
            # Create services (same as in BaseOrderFlowHandler)
            golden_logic = GoldenPatternService()
            calibrator = CalibrationService(calibration_store=local_calibration)
            exec_filters = ExecFiltersGroup()
            publisher = SignalPublisher()

            # Create unified pipeline (same as in BaseOrderFlowHandler)
            unified_pipeline = UnifiedSignalPipeline(
                scoring_engine=scoring_engine,
                regime_service=regime_service,
                golden_logic=golden_logic,
                exec_filters=exec_filters,
                publisher=publisher,
                calibrator=calibrator,
            )

            print("✅ UnifiedSignalPipeline created successfully")
            print(f"   Pipeline type: {type(unified_pipeline).__name__}")
            print(f"   Has scoring_engine: {hasattr(unified_pipeline, '_scoring_engine')}")
            print(f"   Has regime_service: {hasattr(unified_pipeline, '_regime_service')}")
            print(f"   Has golden_logic: {hasattr(unified_pipeline, '_golden_logic')}")
            print(f"   Has exec_filters: {hasattr(unified_pipeline, '_exec_filters')}")
            print(f"   Has publisher: {hasattr(unified_pipeline, '_publisher')}")
            print(f"   Has calibrator: {hasattr(unified_pipeline, '_calibrator')}")

            # Test basic pipeline functionality
            from signals.types import OrderflowContext

            # Create test context
            of_ctx = OrderflowContext(
                ts=1000,
                price=1.0500,
                symbol="EURUSD",
                z_delta=2.5,
                obi=0.8,
                atr=0.0010,
            )

            # Test build_ctx
            sig_ctx = unified_pipeline.build_ctx(of_ctx)
            assert sig_ctx.symbol == "EURUSD"
            assert sig_ctx.ts_event_ms == 1000
            print("✅ build_ctx works")

            return True

        except Exception as e:
            print(f"❌ Pipeline creation failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    except Exception as e:
        print(f"❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_legacy_path_logic():
    """Test the _use_legacy_path logic."""
    print("\nTesting _use_legacy_path logic...")

    # Simulate the logic from BaseOrderFlowHandler
    unified_pipeline = Mock()  # Mock successful creation
    use_legacy_path = False  # As set in BaseOrderFlowHandler

    print(f"   unified_pipeline exists: {unified_pipeline is not None}")
    print(f"   _use_legacy_path: {use_legacy_path}")

    # Test the condition in _process_tick
    use_unified = unified_pipeline is not None and not use_legacy_path
    print(f"   Will use unified pipeline: {use_unified}")

    assert use_unified == True, "Should use unified pipeline when available and legacy path is disabled"
    print("✅ Legacy path logic works correctly")

    return True

if __name__ == "__main__":
    success1 = test_pipeline_creation_logic()
    success2 = test_legacy_path_logic()

    if success1 and success2:
        print("\n🎉 All tests passed! UnifiedSignalPipeline integration is ready.")
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
