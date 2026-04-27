
import os
import sys
import time
from collections import deque

# Add python-worker to path
sys.path.append(os.path.abspath("python-worker"))

from core.atr_sanity import ATRSanity

def run_simulation(jump_rel_threshold="0.8", window=60):
    print(f"--- Simulation: Threshold={jump_rel_threshold}, Window={window} ---")
    os.environ["ATR_JUMP_MAX_REL"] = jump_rel_threshold
    os.environ["ATR_SANITY_WINDOW"] = str(window)
    
    sanity = ATRSanity(window=window)
    now_ms = int(time.time() * 1000)
    
    # 1. Warmup with stable ATR (10.0) -> 100 bps at price 10000
    print("Warming up with ATR=10.0...")
    for i in range(window + 10):
        sanity.update(atr=10.0, px=10000.0, age_ms=10, now_ms=now_ms, symbol="TEST")
        now_ms += 60000 # 1 minute
        
    # 2. Step jump to ATR=20.0 (2x jump) -> 200 bps
    print("Injecting 2x jump (ATR=20.0)...")
    bad_count = 0
    consecutive_bad = 0
    max_consecutive_bad = 0
    
    for i in range(100):
        res = sanity.update(atr=20.0, px=10000.0, age_ms=10, now_ms=now_ms, symbol="TEST")
        now_ms += 60000
        
        if res.bad:
            bad_count += 1
            consecutive_bad += 1
            # print(f"Tick {i+1}: BAD ({res.reason})")
        else:
            if consecutive_bad > 0:
                print(f"Recovered after {consecutive_bad} bad ticks.")
                max_consecutive_bad = max(max_consecutive_bad, consecutive_bad)
            consecutive_bad = 0
            # print(f"Tick {i+1}: OK")
            
    if consecutive_bad > 0:
        max_consecutive_bad = max(max_consecutive_bad, consecutive_bad)
        
    print(f"Total bad ticks: {bad_count}")
    print(f"Max consecutive bad: {max_consecutive_bad}")
    print(f"Outage duration: {max_consecutive_bad} minutes")
    print("-" * 40)

if __name__ == "__main__":
    # Test 1: Current default
    run_simulation(jump_rel_threshold="0.8", window=60)
    
    # Test 2: Proposed relaxed threshold
    run_simulation(jump_rel_threshold="2.0", window=60)
    
    # Test 3: Proposed huge threshold (just in case)
    run_simulation(jump_rel_threshold="3.0", window=60)
