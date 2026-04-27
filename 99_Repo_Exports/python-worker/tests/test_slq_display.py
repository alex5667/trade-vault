import sys
import os

# Adjust path to include python-worker
# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter

def test_slq_display():
    # Setup a mock signal with SLQ enabled
    # Entry: 100
    # ATR: 1.0
    # Original Mult: 1.0 -> Default SL = 99.0
    # SLQ Added 0.5 -> Final Mult 1.5 -> Final SL = 98.5
    
    entry = 100.0
    atr = 1.0
    
    # Final SL (adjusted)
    final_sl = 98.5 
    
    config_params = {
        "slq_used": 1,
        "slq_original_mult": 1.0
    }
    
    signal = CryptoSignal(
        sid="test:slq:1",
        symbol="BTCUSDT",
        side="LONG",
        entry=entry,
        sl=final_sl,
        tp_levels=[102.0],
        lot=0.1,
        atr=atr,
        confidence=0.85,
        ts=1700000000000,
        source="Test",
        config_params=config_params
    )

    message = CryptoSignalFormatter.format_telegram_message(signal)
    
    print("\n--- Formatted Message (SLQ) ---")
    print(message)
    print("-------------------------------\n")

    # Expect: "SL 98.50 (1.50 ATR) (def 99.00)"
    expected_substr = "SL 98.50 (1.50 ATR) (def 99.00)"
    
    if expected_substr in message:
        print("✅ SUCCESS: Found SLQ comparison string")
        return True
    else:
        print(f"❌ FAILURE: Expected '{expected_substr}' not found")
        return False

if __name__ == "__main__":
    if test_slq_display():
        sys.exit(0)
    else:
        sys.exit(1)
