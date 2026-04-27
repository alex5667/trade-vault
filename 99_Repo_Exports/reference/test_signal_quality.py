#!/usr/bin/env python3
"""
Test script for Signal Quality System
"""

import os
import sys
from datetime import datetime

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_imports():
    """Test that all signal quality modules can be imported."""
    print("🧪 Testing signal quality imports...")

    try:
        # Test basic imports that don't require database
        from signal_quality import make_feature_bucket
        from signal_quality.bucketing import get_bucket_quality_description
        print("✅ Basic signal quality modules imported successfully!")

        # Test database-dependent imports separately
        try:
            from signal_quality import SignalQualityEstimator, QualityEstimate
            print("✅ Database-dependent modules also available!")
        except ImportError as e:
            print(f"⚠️ Database modules not available (expected): {e}")

        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"⚠️ Other error: {e}")
        return False


def test_feature_bucketing():
    """Test feature bucketing functionality."""
    print("\n🧪 Testing feature bucketing...")

    from signal_quality import make_feature_bucket

    # Test case 1: Normal values
    bucket1 = make_feature_bucket(
        delta_spike_z=2.1,
        obi=1.5,
        weak_progress=0.1,
        atr_quantile=0.8,
    )
    expected1 = "dz:<3.0|obi:<2.0|wp:<0.15|atr:<0.9"
    print(f"  Bucket 1: {bucket1}")
    assert bucket1 == expected1, f"Expected {expected1}, got {bucket1}"

    # Test case 2: Edge values
    bucket2 = make_feature_bucket(
        delta_spike_z=-0.1,
        obi=0.2,
        weak_progress=0.8,
        atr_quantile=0.1,
    )
    expected2 = "dz:<0.5|obi:<0.5|wp:>=0.5|atr:<0.3"
    print(f"  Bucket 2: {bucket2}")
    assert bucket2 == expected2, f"Expected {expected2}, got {bucket2}"

    # Test case 3: None values
    bucket3 = make_feature_bucket(
        delta_spike_z=None,
        obi=1.0,
        weak_progress=None,
        atr_quantile=0.5,
    )
    expected3 = "dz:na|obi:<1.5|wp:na|atr:<0.7"
    print(f"  Bucket 3: {bucket3}")
    assert bucket3 == expected3, f"Expected {expected3}, got {bucket3}"

    print("✅ Feature bucketing tests passed!")
    return True


def test_signal_context_quality():
    """Test SignalContext with quality fields."""
    print("\n🧪 Testing SignalContext quality fields...")

    from signal_scoring import SignalContext

    ctx = SignalContext(
        ts=datetime.now(),
        symbol="XAUUSD",
        side="buy",
        session="asia",
        regime="trend",
        pattern_name="breakout_r1",
        confidence=85,
        min_confidence_used=20,
        is_golden_pattern=True,
        golden_pattern_label="breakout_r1_golden",
        quality_offline=78.5,
        quality_online=82.3,
        quality_combined=80.2,
        quality_status="ok",
        final_score=83.0,
        is_disabled_by_quality=False,
    )

    print(f"  Symbol: {ctx.symbol}")
    print(f"  Confidence: {ctx.confidence}")
    print(f"  Quality Combined: {ctx.quality_combined}")
    print(f"  Final Score: {ctx.final_score}")
    print(f"  Quality Status: {ctx.quality_status}")
    print(f"  Is Golden: {ctx.is_golden_pattern}")
    print(f"  Disabled by Quality: {ctx.is_disabled_by_quality}")

    # Verify fields
    assert ctx.quality_offline == 78.5
    assert ctx.quality_online == 82.3
    assert ctx.quality_combined == 80.2
    assert ctx.quality_status == "ok"
    assert ctx.final_score == 83.0
    assert not ctx.is_disabled_by_quality

    print("✅ SignalContext quality fields tests passed!")
    return True


def test_quality_payload():
    """Test quality fields in signal payload."""
    print("\n🧪 Testing quality payload structure...")

    from signal_scoring import SignalContext

    ctx = SignalContext(
        ts=datetime(2024, 1, 15, 10, 30, 0),
        symbol="XAUUSD",
        side="buy",
        session="asia",
        regime="trend",
        pattern_name="breakout_r1",
        confidence=85,
        quality_offline=78.5,
        quality_online=82.3,
        quality_combined=80.2,
        quality_status="ok",
        final_score=83.0,
        is_disabled_by_quality=False,
    )

    # Simulate payload creation
    payload = {
        "symbol": ctx.symbol,
        "confidence": ctx.confidence,
        "quality": {
            "offline": ctx.quality_offline,
            "online": ctx.quality_online,
            "combined": ctx.quality_combined,
            "status": ctx.quality_status,
        },
        "finalScoreWithQuality": ctx.final_score,
        "isDisabledByQuality": ctx.is_disabled_by_quality,
    }

    print(f"  Payload: {payload}")

    # Verify structure
    assert payload["symbol"] == "XAUUSD"
    assert payload["confidence"] == 85
    assert "quality" in payload
    assert payload["quality"]["offline"] == 78.5
    assert payload["quality"]["online"] == 82.3
    assert payload["quality"]["combined"] == 80.2
    assert payload["quality"]["status"] == "ok"
    assert payload["finalScoreWithQuality"] == 83.0
    assert not payload["isDisabledByQuality"]

    print("✅ Quality payload structure tests passed!")
    return True


def main():
    """Run all tests."""
    print("🚀 Testing Signal Quality System")
    print("=" * 50)

    tests = [
        test_imports,
        test_feature_bucketing,
        test_signal_context_quality,
        test_quality_payload,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Test {test_func.__name__} failed with exception: {e}")
            failed += 1

    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed} passed, {failed} failed")

    if failed == 0:
        print("🎉 All Signal Quality System tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    exit(main())
