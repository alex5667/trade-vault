
import redis
import json
import time
import os

def inject_signals():
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    stream = "stream:signals:outbox"
    sid = "test_verification_sid"
    
    print(f"Injecting 15 signals for SID={sid} into {stream}...")
    
    for i in range(1, 16):
        payload = {
            "sid": sid,
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "signal_type": "test_signal",
            "timestamp": time.time(),
            "confidence": 0.95, # High confidence to test skipping logic if any, but gate should apply first
            "index": i
        }
        envelope = {
            "sid": sid,
            "payload": payload,
            "ts": time.time()
        }
        
        # SignalDispatcher expects "data" field with JSON
        r.xadd(stream, {"data": json.dumps(envelope)}, maxlen=50000)
        print(f"Injected signal {i}")
        time.sleep(0.1)

if __name__ == "__main__":
    inject_signals()
