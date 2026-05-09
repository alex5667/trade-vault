#!/usr/bin/env python3
"""
Unit test for cancellation spike evidence integration in CryptoSignalFormatter.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter


def test_cancellation_spike_veto_display():
    """Test that vetoed signals display cancellation spike diagnostics correctly."""
    signal = CryptoSignal(
        sid="test-signal-1",
        symbol="BTCUSDT",
        side="LONG",
        entry=50000.0,
        sl=49000.0,
        tp_levels=[51000.0, 52000.0, 53000.0],
        lot=0.01,
        atr=500.0,
        confidence=0.75,
        ts=1705228800000,
        source="CryptoOrderFlow",
        reason_mix={"p_delta": 0.15, "p_speed": 0.10},
        confirmations=["weak_progress", "obi_stable"],
        indicators={
            "cancel_spike_veto": 1,
            "cancel_spike_reason": "bid_support_pulled",
            "cancel_spike_ratio_support": 3.45,
            "cancel_spike_z_support": 2.87,
            "cancel_spike_bid_rate_ema": 12.5,
            "cancel_spike_ask_rate_ema": 8.3,
        }
    )

    message = CryptoSignalFormatter.format_telegram_message(signal)

    # Verify veto section is present
    assert "🚫" in message, "Veto emoji should be present"
    assert "Cancellation Spike Veto" in message, "Veto header should be present"
    assert "bid_support_pulled" in message, "Veto reason should be displayed"

    # Verify metrics are present
    assert "Ratio=3.45" in message, "Ratio metric should be displayed"
    assert "Z=2.87" in message, "Z-score metric should be displayed"
    assert "Bid EMA=12.50" in message, "Bid EMA should be displayed"
    assert "Ask EMA=8.30" in message, "Ask EMA should be displayed"

    print("✅ Test passed: Veto display is correct")
    print("\n" + "="*80)
    print("Generated message:")
    print("="*80)
    print(message)
    print("="*80)


def test_cancellation_spike_monitor_display():
    """Test that monitor mode signals display cancellation spike diagnostics correctly."""
    signal = CryptoSignal(
        sid="test-signal-2",
        symbol="ETHUSDT",
        side="SHORT",
        entry=3000.0,
        sl=3100.0,
        tp_levels=[2900.0, 2800.0, 2700.0],
        lot=0.1,
        atr=50.0,
        confidence=0.85,
        ts=1705228800000,
        source="CryptoOrderFlow",
        reason_mix={"p_delta": 0.20, "p_cluster": 0.12},
        confirmations=["sweep", "reclaim_recent"],
        indicators={
            "cancel_spike_veto": 0,
            "cancel_spike_reason": "ok_no_spike",
            "cancel_spike_ratio_support": 1.2,
            "cancel_spike_z_support": 0.5,
            "cancel_spike_bid_rate_ema": 5.2,
            "cancel_spike_ask_rate_ema": 4.8,
        }
    )

    message = CryptoSignalFormatter.format_telegram_message(signal)

    # Verify monitor section is present
    assert "🔍" in message, "Monitor emoji should be present"
    assert "Cancellation Spike Monitor" in message, "Monitor header should be present"
    assert "ok_no_spike" in message, "Monitor status should be displayed"

    # Verify metrics are present
    assert "Ratio=1.20" in message, "Ratio metric should be displayed"
    assert "Z=0.50" in message, "Z-score metric should be displayed"

    print("✅ Test passed: Monitor display is correct")
    print("\n" + "="*80)
    print("Generated message:")
    print("="*80)
    print(message)
    print("="*80)


def test_no_cancellation_spike_data():
    """Test that signals without cancellation spike data don't show the section."""
    signal = CryptoSignal(
        sid="test-signal-3",
        symbol="BNBUSDT",
        side="LONG",
        entry=400.0,
        sl=390.0,
        tp_levels=[410.0, 420.0, 430.0],
        lot=1.0,
        atr=10.0,
        confidence=0.65,
        ts=1705228800000,
        source="CryptoOrderFlow",
        reason_mix={"p_legacy": 0.08},
        confirmations=[],
        indicators={}  # No cancellation spike data
    )

    message = CryptoSignalFormatter.format_telegram_message(signal)

    # Verify cancellation spike section is NOT present
    assert "Cancellation Spike" not in message, "Cancellation spike section should not be present when no data"

    print("✅ Test passed: No cancellation spike section when data is absent")
    print("\n" + "="*80)
    print("Generated message:")
    print("="*80)
    print(message)
    print("="*80)


if __name__ == "__main__":
    print("Running cancellation spike evidence integration tests...\n")

    test_cancellation_spike_veto_display()
    print()

    test_cancellation_spike_monitor_display()
    print()

    test_no_cancellation_spike_data()
    print()

    print("\n🎉 All tests passed!")
