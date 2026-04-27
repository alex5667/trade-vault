
import sys
import os

# Adjust path to include python-worker
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter

def test_pepe_formatting():
    # Simulate a 1000PEPEUSDT signal
    # Prices are around 0.00001 (but 1000PEPE is 0.01)
    # Let's use the values from the user report:
    # 1000PEPEUSDT SHORT @ 0.01
    # ATR=0.00
    # p_delta=787769.00
    
    signal = CryptoSignal(
        sid="crypto-of:1000PEPEUSDT:1767762339475",
        symbol="1000PEPEUSDT",
        side="SHORT",
        entry=0.0066560, # User provided example
        sl=0.0066000,
        tp_levels=[0.0067000, 0.0068000, 0.0070000],
        lot=500.0, # USDT volume
        atr=0.00005, # Appropriate ATR for this price
        confidence=0.86,
        ts=1767762339475,
        source="CryptoOrderFlow",
        reason_mix={
            "p_delta": 787769.00,
            "p_speed": 4.68,
            "p_cluster": -1.00,
            "absorption": 1815773.00,
            "obi": -1.00
        },
        confirmations=["obi=-1.00", "absorption=1815773.00", "iceberg_refresh=4"],
        position_size_usd=5.0, # Margin
        leverage=100.0
    )

    print("--- Formatted Signal ---")
    formatted = CryptoSignalFormatter.format_telegram_message(signal)
    print(formatted)
    print("------------------------")

if __name__ == "__main__":
    test_pepe_formatting()
