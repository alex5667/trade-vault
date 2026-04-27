
import sys
import os
import random
sys.path.append(os.getcwd())

from core.crypto_orderflow_detectors import DeltaSpikeDetector

def test_zero_threshold():
    print("Testing DeltaSpikeDetector with z_threshold=0.0")
    detector = DeltaSpikeDetector(window=10, z_threshold=0.0, min_abs_volume=0.0)
    
    # Fill window with constant values (std=0)
    for i in range(20):
        tick = {"ts_ms": 1000+i, "side": "BUY", "qty": 1.0, "price": 100.0}
        res = detector.push(tick)
        if res:
            print(f"Tick {i} result: {res}")
        else:
            # print(f"Tick {i} result: None (warming up)")
            pass

    print("\nTesting DeltaSpikeDetector with z_threshold=3.0")
    detector3 = DeltaSpikeDetector(window=10, z_threshold=3.0, min_abs_volume=0.0)
    
    # Fill window with constant values (std=0)
    for i in range(20):
        tick = {"ts_ms": 1000+i, "side": "BUY", "qty": 1.0, "price": 100.0}
        res = detector3.push(tick)
        if res:
            print(f"Tick {i} result: {res}")

    print("\nTesting partial window warmup")
    
if __name__ == "__main__":
    test_zero_threshold()
