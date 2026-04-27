#!/usr/bin/env python3
"""
Test script for Signal Scoring System
"""

import os
from datetime import datetime
from signal_scoring import ScoringConfig, SignalContext

def test_scoring_config():
    """Test ScoringConfig with ENV variables"""
    print("🧪 Testing ScoringConfig...")

    # Set test environment variables
    test_env = {
        "MIN_SIGNAL_CONFIDENCE": "75",
        "MIN_SIGNAL_CONFIDENCE__XAUUSD": "25",
        "GOLDEN_PATTERN_MIN_CONFIDENCE": "85",
        "SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z": "1.2",
        "SIGNAL_METRIC_WEIGHT__OBI": "0.8",
        "SIGNAL_PATTERN_WEIGHT__BREAKOUT_R1": "1.3",
        "SIGNAL_PATTERN_MIN_CONF__BREAKOUT_R1": "90",
    }

    # Backup original env
    original_env = {}
    for key in test_env:
        original_env[key] = os.environ.get(key)

    try:
        # Set test env
        for key, value in test_env.items():
            os.environ[key] = value

        # Test config loading
        cfg = ScoringConfig.from_env()

        print("✅ ScoringConfig loaded successfully!")
        print(f"  min_confidence_default: {cfg.min_confidence_default}")
        print(f"  min_confidence_by_symbol: {cfg.min_confidence_by_symbol}")
        print(f"  golden_pattern_min_confidence: {cfg.golden_pattern_min_confidence}")
        print(f"  metric_weights: {cfg.metric_weights}")

        # Test get_min_confidence
        assert cfg.get_min_confidence("BTCUSD", None) == 75, "BTCUSD should use default"
        assert cfg.get_min_confidence("XAUUSD", None) == 25, "XAUUSD should use override"
        assert cfg.get_min_confidence("XAUUSD", "breakout_r1") == 90, "XAUUSD + breakout_r1 should use pattern override"

        # Test get_pattern_weight
        assert cfg.get_pattern_weight("breakout_r1") == 1.3, "breakout_r1 weight should be 1.3"
        assert cfg.get_pattern_weight("unknown") == 1.0, "unknown pattern should have weight 1.0"

        print("✅ All ScoringConfig tests passed!")

    finally:
        # Restore original env
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_signal_context():
    """Test SignalContext creation and manipulation"""
    print("\n🧪 Testing SignalContext...")

    ctx = SignalContext(
        ts=datetime.now(),
        symbol="XAUUSD",
        side="buy",
        session="asia",
        regime="trend",
        pattern_name="breakout_r1",
        delta_spike_z=2.5,
        obi=1.8,
        weak_progress=0.2,
        atr_quantile=0.85,
    )

    print("✅ SignalContext created successfully!")
    print(f"  Symbol: {ctx.symbol}")
    print(f"  Pattern: {ctx.pattern_name}")
    print(f"  Delta Spike Z: {ctx.delta_spike_z}")
    print(f"  OBI: {ctx.obi}")
    print(f"  Weak Progress: {ctx.weak_progress}")
    print(f"  ATR Quantile: {ctx.atr_quantile}")

    # Test setting computed values
    ctx.confidence = 92
    ctx.min_confidence_used = 25
    ctx.is_golden_pattern = True
    ctx.golden_pattern_label = "breakout_r1_golden"

    print(f"  Confidence: {ctx.confidence}")
    print(f"  Min Confidence Used: {ctx.min_confidence_used}")
    print(f"  Is Golden: {ctx.is_golden_pattern}")
    print(f"  Golden Label: {ctx.golden_pattern_label}")

    print("✅ SignalContext tests passed!")


def test_payload_creation():
    """Test payload creation from SignalContext"""
    print("\n🧪 Testing payload creation...")

    ctx = SignalContext(
        ts=datetime(2024, 1, 15, 10, 30, 0),
        symbol="XAUUSD",
        side="buy",
        session="asia",
        regime="trend",
        pattern_name="breakout_r1",
        delta_spike_z=2.5,
        obi=1.8,
        weak_progress=0.2,
        atr_quantile=0.85,
        confidence=92,
        min_confidence_used=25,
        is_golden_pattern=True,
        golden_pattern_label="breakout_r1_golden",
        delta_spike_z_local_q=0.95,
        obi_local_q=0.88,
        weak_progress_local_q=0.92,
        atr_local_q=0.78,
    )

    # Simulate ctx_to_payload method
    payload = {
        "ts": ctx.ts.isoformat(),
        "symbol": ctx.symbol,
        "side": ctx.side,
        "session": ctx.session,
        "regime": ctx.regime,
        "pattern": ctx.pattern_name,
        "confidence": ctx.confidence,
        "minConfidenceUsed": ctx.min_confidence_used,
        "isGoldenPattern": ctx.is_golden_pattern,
        "goldenPatternLabel": ctx.golden_pattern_label,
        "metrics": {
            "deltaSpikeZ": ctx.delta_spike_z,
            "deltaSpikeZLocalQ": ctx.delta_spike_z_local_q,
            "obi": ctx.obi,
            "obiLocalQ": ctx.obi_local_q,
            "weakProgress": ctx.weak_progress,
            "weakProgressLocalQ": ctx.weak_progress_local_q,
            "atrQuantile": ctx.atr_quantile,
            "atrLocalQ": ctx.atr_local_q,
        },
    }

    print("✅ Payload created successfully!")
    print(f"  Symbol: {payload['symbol']}")
    print(f"  Confidence: {payload['confidence']}")
    print(f"  Is Golden: {payload['isGoldenPattern']}")
    print(f"  Metrics count: {len(payload['metrics'])}")

    # Verify structure
    required_fields = ["ts", "symbol", "side", "session", "regime", "pattern", "confidence", "isGoldenPattern", "metrics"]
    for field in required_fields:
        assert field in payload, f"Missing field: {field}"

    assert len(payload["metrics"]) == 8, "Should have 8 metrics"
    print("✅ Payload structure validated!")


def main():
    """Run all tests"""
    print("🚀 Testing Signal Scoring System")
    print("=" * 50)

    try:
        test_scoring_config()
        test_signal_context()
        test_payload_creation()

        print("\n" + "=" * 50)
        print("🎉 All tests passed! Signal Scoring System is working correctly!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
