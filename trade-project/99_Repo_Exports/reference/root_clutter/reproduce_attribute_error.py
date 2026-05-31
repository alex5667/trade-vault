
import sys
import os

# Robust path setup
BASE_DIR = "/home/alex/front/trade/scanner_infra/python-worker"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Import from core and services
try:
    from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter
    from core.smt_symbol_snapshot import SymbolSnapshot
    from services.smt_logic import leader_confirm_reject
    print("✅ All imports successful!")
except Exception as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

def test_formatter_with_int_side():
    print("\nTesting CryptoSignalFormatter with integer side...")
    signal = CryptoSignal(
        sid="test-id",
        symbol="BTCUSDT",
        side=1,  # Integer instead of "LONG"
        entry=50000.0,
        sl=49000.0,
        tp_levels=[51000.0, 52000.0],
        lot=1.0,
        atr=100.0,
        confidence=0.85,
        ts=1625523464260,
        source="Test",
        validation_status=2 # Integer status
    )
    
    try:
        msg = CryptoSignalFormatter.format_telegram_message(signal)
        print("✅ Formatter handled integer side/status successfully!")
        # print("Output line 0:", msg.splitlines()[0])
    except AttributeError as e:
        print(f"❌ Formatter failed with AttributeError: {e}")
    except Exception as e:
        print(f"❌ Formatter failed with unexpected error: {e}")

def test_smt_logic_with_int_trend():
    print("\nTesting smt_logic with integer trend_dir...")
    leader = SymbolSnapshot(
        symbol="BTCUSDT",
        trend_dir=1,  # Integer instead of "UP"
        of_dir=2,     # Integer instead of "LONG"
        sweep_dir=0   # Integer instead of "NONE"
    )
    cfg = {"smt_zone_max_bp": 15.0}
    
    try:
        confirm, reject, _, _, _, _ = leader_confirm_reject(leader, cfg)
        print("✅ SMT logic handled integer fields successfully!")
    except AttributeError as e:
        print(f"❌ SMT logic failed with AttributeError: {e}")
    except Exception as e:
        print(f"❌ SMT logic failed with unexpected error: {e}")

if __name__ == "__main__":
    test_formatter_with_int_side()
    test_smt_logic_with_int_trend()
