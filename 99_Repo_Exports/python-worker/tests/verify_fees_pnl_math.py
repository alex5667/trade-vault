import os
import sys

# Ensure we can import from the current directory
sys.path.append(os.getcwd())

from services.pnl_math import get_symbol_info


def test_pnl_math_defaults():
    print("Testing pnl_math defaults...")

    # Ensure no environment variable is set
    if "CRYPTO_COMMISSION_RATE" in os.environ:
        del os.environ["CRYPTO_COMMISSION_RATE"]

    info = get_symbol_info("BTCUSDT")
    rate = info.get("commission_rate")
    print(f"Default CRYPTO_COMMISSION_RATE: {rate}")
    assert rate == 0.0005, f"Expected 0.0005, got {rate}"

    # Test override
    os.environ["CRYPTO_COMMISSION_RATE"] = "0.0001"
    info = get_symbol_info("BTCUSDT")
    rate = info.get("commission_rate")
    print(f"Overridden CRYPTO_COMMISSION_RATE: {rate}")
    assert rate == 0.0001, f"Expected 0.0001, got {rate}"

    print("Verification passed!")

if __name__ == "__main__":
    try:
        test_pnl_math_defaults()
    except Exception as e:
        print(f"Verification failed: {e}")
        sys.exit(1)
