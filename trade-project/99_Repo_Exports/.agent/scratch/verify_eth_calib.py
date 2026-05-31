import sys
import os

# Add python-worker to path
sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

from confidence_calculation.instrument_config import get_config

def verify_ethusdt():
    cfg = get_config("ETHUSDT", use_env=False)
    print(f"Symbol: {cfg.symbol}")
    print(f"Stop Mode: {cfg.stop_mode}")
    print(f"Stop ATR Mult: {cfg.stop_atr_mult}")
    print(f"TP Mode: {cfg.tp_mode}")
    print(f"TP RR: {cfg.tp_rr}")
    
    # Check if Stop ATR Mult is 0.7 as calibrated
    if cfg.stop_atr_mult == 0.7:
        print("Verification SUCCESS: ETHUSDT is calibrated.")
    else:
        print(f"Verification FAILURE: Expected 0.7, got {cfg.stop_atr_mult}")

if __name__ == "__main__":
    verify_ethusdt()
