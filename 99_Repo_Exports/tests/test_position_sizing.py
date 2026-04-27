
import os
import sys

# Add python-worker to path
sys.path.append(os.path.abspath("python-worker"))

from services.pnl_math import calculate_position_size

def test_btc_sizing(lot_step=0.01):
    symbol = "BTCUSDT"
    entry = 90500.0
    sl = 89500.0 # 1000 pts distance
    deposit = 100.0
    risk_pct = 5.0 # 5 USDT risk
    leverage = 100.0
    
    print(f"\nTesting {symbol} at {entry} with lot_step={lot_step}:")
    print(f"Goal: Margin <= 5 USDT (Notional <= 500 USDT)")
    
    lot, margin, dep, lev = calculate_position_size(
        symbol=symbol,
        entry_price=entry,
        sl_price=sl,
        deposit=deposit,
        risk_percent=risk_pct,
        leverage=leverage,
        lot_step=lot_step
    )
    
    notional = lot * entry
    print(f"Result: Lot={lot}, Margin={margin:.2f}, Notional={notional:.2f}")
    
    if notional > 505: # allow small float error
        print("❌ Notional too high!")
    else:
        print("✅ Notional is correct!")

if __name__ == "__main__":
    # Current state (default lot_step=0.01)
    print("Manual lot_step override (0.01):")
    test_btc_sizing(0.01)
    
    # Proposed state (manual override 0.001)
    print("\nManual lot_step override (0.001):")
    test_btc_sizing(0.001)

    # State with dynamic detection (no param passed)
    print("\nDynamic lot_step detection (default param):")
    symbol = "BTCUSDT"
    entry = 90500.0
    sl = 89500.0 
    deposit = 100.0
    risk_pct = 5.0 
    leverage = 100.0
    lot, margin, dep, lev = calculate_position_size(
        symbol=symbol,
        entry_price=entry,
        sl_price=sl,
        deposit=deposit,
        risk_percent=risk_pct,
        leverage=leverage
    )
    notional = lot * entry
    print(f"Result: Lot={lot}, Margin={margin:.2f}, Notional={notional:.2f}")
    if abs(notional - 452.5) < 1.0:
        print("✅ Dynamic detection worked!")
    else:
        print("❌ Dynamic detection failed!")
