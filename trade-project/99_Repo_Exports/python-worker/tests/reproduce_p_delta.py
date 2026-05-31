
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "python-worker"))

from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter


def reproduce():
    # Helper to create a dummy signal
    def make_signal(sid, symbol, p_delta_val, p_speed_val):
        mix = {
            "p_delta": p_delta_val,
            "p_speed": p_speed_val,
            "p_cluster": 0.5,
            "p_legacy": 2.0
        }
        return CryptoSignal(
            sid=sid,
            symbol=symbol,
            side="LONG",
            entry=100.0,
            sl=99.0,
            tp_levels=[101.0],
            lot=1.0,
            atr=1.0,
            confidence=0.8,
            ts=1700000000000,
            source="Test",
            reason_mix=mix,
            confirmations=[]
        )

    # Case 1: Raw Volume (Current behavior)
    sig_raw = make_signal("test:1", "SUIUSDT", 5100.0, 4.08)
    formatted_raw = CryptoSignalFormatter.format_telegram_message(sig_raw)
    print("--- Case 1: Raw Volume (Current) ---")
    print("p_delta input: 5100.0")
    print("Formatted output extract:")
    for line in formatted_raw.split("\n"):
        if "mix:" in line:
            print(line)

    # Case 2: Score (Expected?)
    sig_score = make_signal("test:2", "SUIUSDT", 0.95, 4.08)
    formatted_score = CryptoSignalFormatter.format_telegram_message(sig_score)
    print("\n--- Case 2: Score 0.95 (Expected?) ---")
    print("p_delta input: 0.95")
    print("Formatted output extract:")
    for line in formatted_score.split("\n"):
        if "mix:" in line:
            print(line)

    # Case 3: Small Delta (BNB)
    sig_bnb = make_signal("test:3", "BNBUSDT", 13.64, 4.18)
    formatted_bnb = CryptoSignalFormatter.format_telegram_message(sig_bnb)
    print("\n--- Case 3: BNB Small Delta ---")
    print("p_delta input: 13.64")
    print("Formatted output extract:")
    for line in formatted_bnb.split("\n"):
        if "mix:" in line:
            print(line)

    # Case 4: New Proposed Structure (p_delta score + raw delta)
    mix_new = {
        "p_delta": 0.96,        # Normalized score
        "p_speed": 4.08,        # Z-score
        "delta": 5100.0,        # Raw volume
        "p_cluster": 0.5,
        "p_legacy": 2.0
    }
    sig_new = CryptoSignal(
        sid="test:4",
        symbol="SUIUSDT",
        side="LONG",
        entry=1.8444,
        sl=1.84,
        tp_levels=[1.85],
        lot=1.0,
        atr=0.003,
        confidence=0.83,
        ts=1700000000000,
        source="Test",
        reason_mix=mix_new,
        confirmations=[]
    )
    formatted_new = CryptoSignalFormatter.format_telegram_message(sig_new)
    print("\n--- Case 4: New Structure (p_delta=Score, delta=Volume) ---")
    print("Formatted output extract:")
    for line in formatted_new.split("\n"):
        if "mix:" in line:
            print(line)

if __name__ == "__main__":
    reproduce()
