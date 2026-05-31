#!/usr/bin/env python3
"""
Quick test script to send manual signal to go-gateway
"""

import requests
import sys

GATEWAY_URL = "http://127.0.0.1:8090"

def send_test_signal():
    """Send test signal"""
    signal = {
        "sid": "test-manual-signal",
        "symbol": "XAUUSD",
        "side": "LONG",
        "lot": 0.01,
        "sl": 2758.50,
        "tp_levels": [2773.50, 2778.50, 2783.50]
    }
    
    print("Sending test signal to go-gateway...")
    print(f"URL: {GATEWAY_URL}/orders/enqueue")
    print(f"Payload: {signal}")
    print()
    
    try:
        resp = requests.post(f"{GATEWAY_URL}/orders/enqueue", json=signal, timeout=5)
        
        if resp.status_code == 200:
            print(f"✅ Success: {resp.json()}")
            print("\nCheck Telegram bot for notification!")
            return 0
        else:
            print(f"❌ Error: HTTP {resp.status_code}")
            print(f"Response: {resp.text}")
            return 1
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(send_test_signal())

