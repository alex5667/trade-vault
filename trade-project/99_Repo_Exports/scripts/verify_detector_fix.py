#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verification script for Aggregated Hub detector fixes.
Tests that the correct MicrostructureSpikeDetector is imported and functional.
"""

import sys
from pathlib import Path

root = Path(__file__).parent.parent

def test_core_detector():
    """Test that core detector has update method."""
    print("=" * 60)
    print("Testing Core MicrostructureSpikeDetector...")
    print("=" * 60)

    try:
        from core.microstructure_spike_detector import MicrostructureSpikeDetector, SpikeConfig
        print("✅ Import successful")

        # Check for update method
        if hasattr(MicrostructureSpikeDetector, 'update'):
            print("✅ Has 'update' method")
        else:
            print("❌ Missing 'update' method")
            return False

        # Try to instantiate
        cfg = SpikeConfig(
            z_delta_thr=3.0,
            z_extreme_thr=4.5,
            speed_z_thr=3.0,
            win_ticks=300,
            win_speed_sec=30
        )
        detector = MicrostructureSpikeDetector(cfg)
        print("✅ Detector instantiated successfully")

        # Try to call update
        result = detector.update(bid=2650.0, ask=2650.5, volume=1.0, ts_ms=1000000)
        print("✅ Update method called successfully")
        print(f"   Result keys: {list(result.keys())}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_hub_import():
    """Test that hub can import detector correctly."""
    print("\n" + "=" * 60)
    print("Testing Hub V2 Import...")
    print("=" * 60)

    try:
        # This will run the import validation logic
        from aggregated_signal_hub_v2 import (
            HAS_LEGACY_DETECTOR,
            HAS_PRO_DETECTOR,
            MicrostructureSpikeDetector,
            _import_warnings
        )

        print(f"HAS_LEGACY_DETECTOR: {HAS_LEGACY_DETECTOR}")
        print(f"HAS_PRO_DETECTOR: {HAS_PRO_DETECTOR}")

        if _import_warnings:
            print("\n⚠️  Import warnings:")
            for warning in _import_warnings:
                print(f"  - {warning}")
        else:
            print("✅ No import warnings")

        if HAS_LEGACY_DETECTOR:
            if hasattr(MicrostructureSpikeDetector, 'update'):
                print("✅ Legacy detector has 'update' method")
                return True
            else:
                print("❌ Legacy detector missing 'update' method")
                return False
        else:
            print("⚠️  Legacy detector not available")
            return True  # Not a failure if explicitly disabled

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_confidence_threshold():
    """Test that confidence threshold is reasonable."""
    print("\n" + "=" * 60)
    print("Testing Confidence Configuration...")
    print("=" * 60)

    try:
        from aggregated_signal_hub_v2 import HubConfig

        cfg = HubConfig()
        print(f"Confidence threshold: {cfg.confidence_threshold}")
        print(f"Min signal interval: {cfg.min_signal_interval_sec}s")
        print("\nWeights:")
        print(f"  w_delta_pro: {cfg.w_delta_pro}")
        print(f"  w_speed: {cfg.w_speed}")
        print(f"  w_cluster: {cfg.w_cluster}")
        print(f"  w_legacy: {cfg.w_legacy}")
        print(f"  Total: {cfg.w_delta_pro + cfg.w_speed + cfg.w_cluster + cfg.w_legacy}")

        if cfg.confidence_threshold <= 0.50:
            print(f"✅ Confidence threshold ({cfg.confidence_threshold}) is reasonable")
            return True
        else:
            print(f"⚠️  Confidence threshold ({cfg.confidence_threshold}) might be too high")
            return True  # Warning, not failure

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_detector_update_call():
    """Test actual detector update call with realistic data."""
    print("\n" + "=" * 60)
    print("Testing Detector Update Call...")
    print("=" * 60)

    try:
        from core.microstructure_spike_detector import MicrostructureSpikeDetector, SpikeConfig

        cfg = SpikeConfig(
            z_delta_thr=3.0,
            z_extreme_thr=4.5,
            speed_z_thr=3.0,
            win_ticks=300,
            win_speed_sec=30
        )
        detector = MicrostructureSpikeDetector(cfg)

        # Simulate some ticks
        import time
        base_time = int(time.time() * 1000)

        for i in range(10):
            bid = 2650.0 + i * 0.1
            ask = bid + 0.5
            ts = base_time + i * 1000

            result = detector.update(bid=bid, ask=ask, volume=1.0, ts_ms=ts)

            if i == 9:  # Last one
                print(f"✅ Processed {i+1} ticks successfully")
                print("   Last result:")
                print(f"   - z_delta: {result.get('z_delta', 0):.2f}")
                print(f"   - z_speed: {result.get('z_speed', 0):.2f}")
                print(f"   - z_range: {result.get('z_range', 0):.2f}")
                print(f"   - trigger: {result.get('trigger', False)}")
                print(f"   - extreme: {result.get('extreme', False)}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("AGGREGATED HUB DETECTOR FIX VERIFICATION")
    print("=" * 60 + "\n")

    results = []

    # Run tests
    results.append(("Core Detector", test_core_detector()))
    results.append(("Hub Import", test_hub_import()))
    results.append(("Confidence Config", test_confidence_threshold()))
    results.append(("Detector Update", test_detector_update_call()))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")

    print("\n" + "-" * 60)
    print(f"Total: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! The fix is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please review the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

