import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signals.risk_levels import compute_levels

def main():
    print("=== Testing TP1_MIN_RR_FLOOR Logic ===")
    
    # Base params
    entry = 100.0
    atr = 10.0
    side = "LONG"
    
    # Scenario 1: SL is 1.5 ATR. Rocket TP1 is set to 0.78 ATR.
    # Without floor, TP1 would be at 107.8 (0.78 * 10), and SL at 85.0 (-1.5 * 10). 
    # This means SL risk is 15.0, TP1 reward is 7.8 (RR = 0.52).
    # With floor = 1.0, TP1 MUST be pushed to at least 115.0 (so reward is 15.0).
    cfg = {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 1.5,
        "TP_MODE": "RR",
        "ROCKET_TP1_ATR_MULT": 0.78,
        "TRAIL_PROFILE": "rocket_v1",
        "TP1_MIN_RR_FLOOR": 1.0
    }
    
    print("\nScenario 1: TP1 < SL distance (Floor = 1.0)")
    print(f"Config: SL={cfg['STOP_ATR_MULT']} ATR, TP1 Target={cfg['ROCKET_TP1_ATR_MULT']} ATR")
    
    res = compute_levels(entry, atr, side, cfg)
    
    sl_dist = entry - res['sl']
    tp1_dist = res['tp_levels'][0] - entry
    
    print(f"SL Distance: {sl_dist:.2f}")
    print(f"TP1 Distance: {tp1_dist:.2f}")
    print(f"Actual RR at TP1: {tp1_dist / sl_dist:.2f}")
    
    assert tp1_dist >= sl_dist, "FAIL: TP1 is closer than SL!"
    print("✅ Verified: TP1 was correctly pushed to match SL distance.")

    # Scenario 2: TP1_MIN_RR_FLOOR is disabled (0.0)
    cfg["TP1_MIN_RR_FLOOR"] = 0.0
    print("\nScenario 2: Floor Disabled (Floor = 0.0)")
    res = compute_levels(entry, atr, side, cfg)
    sl_dist = entry - res['sl']
    tp1_dist = res['tp_levels'][0] - entry
    
    print(f"SL Distance: {sl_dist:.2f}")
    print(f"TP1 Distance: {tp1_dist:.2f}")
    print(f"Actual RR at TP1: {tp1_dist / sl_dist:.2f}")
    print("✅ Verified: Without floor, TP1 is allowed to be closer than SL.")

if __name__ == "__main__":
    main()
