
import json
import os
import time

import redis
from core.redis_keys import RedisStreams as RS


def inject_signals():
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    stream = RS.SIGNAL_OUTBOX

    print(f"Injecting 15 signals with UNIQUE SIDs into {stream}...")

    base_sid = f"test_{int(time.time())}"

    for i in range(1, 16):
        sid = f"{base_sid}_{i}"
        payload = {
            "sid": sid,
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "signal_type": "test_signal",
            "timestamp": time.time(),
            "confidence": 0.95,
            "index": i
        }

        # Proper envelope structure
        envelope = {
            "sid": sid,
            "meta": {
                "source": "manual_injection",
                "ts": time.time()
            },
            "targets": {
                "notify": payload
            },
            "ts": time.time()
        }

        r.xadd(stream, {"data": json.dumps(envelope)}, maxlen=50000)
        print(f"Injected signal {i} SID={sid}")
        time.sleep(0.1)

if __name__ == "__main__":
    inject_signals()
