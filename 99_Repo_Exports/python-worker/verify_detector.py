import sys
import os
import time
from unittest.mock import MagicMock

# Mock dependencies
sys.modules["redis"] = MagicMock()
sys.modules["redis.exceptions"] = MagicMock()
sys.modules["common.decision_trace"] = MagicMock()
sys.modules["services.outbox.atomic_outbox"] = MagicMock()
sys.modules["services.outbox.envelope_builder"] = MagicMock()
sys.modules["services.signal_publisher"] = MagicMock()
sys.modules["core.order_builder"] = MagicMock()

# Mock dependencies
sys.modules["redis"] = MagicMock()
sys.modules["redis.exceptions"] = MagicMock()
sys.modules["common.decision_trace"] = MagicMock()
sys.modules["services.outbox.atomic_outbox"] = MagicMock()
sys.modules["services.outbox.envelope_builder"] = MagicMock()
sys.modules["services.signal_publisher"] = MagicMock()
sys.modules["core.order_builder"] = MagicMock()

# Import the function to test
from services.binance_iceberg_detector import _build_iceberg_signal_payload, BestLevelState

def verify():
    print("Verifying Iceberg Detector Payload...")
    
    # Test case 1: Long with ATR
    price = 50000.0
    atr = 100.0
    payload = _build_iceberg_signal_payload(
        symbol="BTCUSDT",
        direction="LONG",
        price=price,
        state=BestLevelState(price, 1.0, time.time()),
        level_info={"kind": "test", "price": price},
        atr=atr
    )
    
    print(f"Payload keys: {payload.keys()}")
    if "sl" in payload and "tp_levels" in payload:
        print("✅ SL and TP_LEVELS present")
        expected_sl = price - (2 * atr)
        if payload["sl"] == expected_sl:
             print(f"✅ SL correct: {payload['sl']} (Expected: {expected_sl})")
        else:
             print(f"❌ SL incorrect: {payload['sl']} (Expected: {expected_sl})")
    else:
        print("❌ SL or TP_LEVELS missing")

    # Test case 2: Short without ATR (default)
    payload_no_atr = _build_iceberg_signal_payload(
        symbol="BTCUSDT",
        direction="SHORT",
        price=price,
        state=BestLevelState(price, 1.0, time.time()),
        level_info={"kind": "test", "price": price},
        atr=0.0
    )
    
    if "sl" in payload_no_atr:
        expected_sl_default = price * 1.005 # 0.5%
        print(f"Default SL check: {payload_no_atr['sl']} (Expected approx {expected_sl_default})")
        # Allow small float diff
        if abs(payload_no_atr["sl"] - expected_sl_default) < 1.0:
            print("✅ Default SL correct")
        else:
            print("❌ Default SL incorrect")

if __name__ == "__main__":
    verify()
