import sys

# Adjust path to include python-worker
# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")
from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter


def test_formatter_dynamic_tp1():
    # Setup a mock signal using rocket_v1 profile
    # Entry: 100, ATR: 1.0
    # TP1: 101.58 (should trigger 1.58 ATR)

    entry = 100.0
    atr = 1.0
    # 1.58 ATR distance
    tp1 = entry + (atr * 1.58)

    signal = CryptoSignal(
        sid="test:123",
        symbol="BTCUSDT",
        side="LONG",
        entry=entry,
        sl=99.0,
        tp_levels=[tp1, 102.0, 103.0],
        lot=0.1,
        atr=atr,
        confidence=0.85,
        ts=1700000000000,
        source="Test",
        trail_profile="rocket_v1"
    )

    message = CryptoSignalFormatter.format_telegram_message(signal)

    print("\n--- Formatted Message ---")
    print(message)
    print("-------------------------\n")

    # Assert logic
    expected_substr = "(1.58 ATR)"
    if expected_substr in message:
        print("✅ SUCCESS: Found correctly formatted ATR multiplier")
        return True
    else:
        print(f"❌ FAILURE: Expected '{expected_substr}' not found in message")
        return False

if __name__ == "__main__":
    if test_formatter_dynamic_tp1():
        sys.exit(0)
    else:
        sys.exit(1)
