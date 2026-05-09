
import os
import sys

# Ensure python-worker root is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from regime.signal_snapshot import SignalSnapshot


def verify_signal_snapshot_extra():
    print("🚀 Verifying SignalSnapshot extra field preservation...")

    # Mock payload with indicators (confidence metrics)
    payload = {
        "signal_id": "test-signal-123",
        "symbol": "BTCUSDT",
        "ts_ms": 1678886400000,
        "direction": 1,
        "signal_family": "orderflow",
        "conf_score": 85.5,
        "indicators": {
            "confidence_v1": 85.5,
            "confidence_v2": 82.3,
            "confidence_cal": 0.88,
            "other_metric": "value"
        },
        "extra": {
            "debug_flag": True
        }
    }

    # Create snapshot
    snapshot = SignalSnapshot.from_dict(payload)

    # Verify extra field population
    print(f"\nSnapshot extra field: {snapshot.extra}")

    assert "indicators" in snapshot.extra, "❌ 'indicators' missing from snapshot.extra"
    assert snapshot.extra["indicators"]["confidence_v1"] == 85.5, "❌ confidence_v1 mismatch"
    assert snapshot.extra["indicators"]["confidence_v2"] == 82.3, "❌ confidence_v2 mismatch"
    assert snapshot.extra.get("debug_flag") is True, "❌ 'debug_flag' missing from snapshot.extra"

    print("✅ SignalSnapshot correctly populated 'extra' from payload.")

    # Verify to_dict output
    output = snapshot.to_dict()
    print(f"\nSnapshot to_dict output keys: {list(output.keys())}")

    assert "extra" in output, "❌ 'extra' missing from to_dict output"
    assert output["extra"]["indicators"]["confidence_v2"] == 82.3, "❌ confidence_v2 missing in output"

    print("✅ SignalSnapshot.to_dict() correctly includes 'extra'.")
    print("\n🎉 Verification SUCCESS!")

if __name__ == "__main__":
    verify_signal_snapshot_extra()
