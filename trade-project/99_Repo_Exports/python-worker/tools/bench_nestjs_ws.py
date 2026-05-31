import asyncio
import json
import os
import time

# We'll use redis.asyncio since we need to blast messages fast
import redis.asyncio as redis

from utils.time_utils import get_ny_time_millis


async def run_benchmark():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    print(f"Connecting to {redis_url}")
    r = redis.from_url(redis_url, decode_responses=True)

    channel = "events:ws:broadcast"
    num_messages = 5000

    print(f"Generating {num_messages} messages for WebSocket broadcast stress test...")
    start_t = time.time()

    # We pipeline the PubSub publish commands to maximize throughput
    pipe = r.pipeline()
    for i in range(num_messages):
        # Emulate a typical virtual trade update payload
        payload = {
            "type": "VIRTUAL_TRADE_UPDATE",
            "data": {
                "id": f"vt_{i}",
                "symbol": "BTCUSDT",
                "side": "LONG",
                "qty": 0.5,
                "price": 50000.0 + (i * 0.1),
                "unrealized_pnl": 15.5,
                "status": "OPEN",
                "is_virtual": True,
                "timestamp": get_ny_time_millis()
            }
        }
        pipe.publish(channel, json.dumps(payload))

        # Execute in chunks of 500 to avoid excessive memory / socket blobs
        if i % 500 == 0 and i > 0:
            await pipe.execute()
            # Small yield to let event loop breathe
            await asyncio.sleep(0.01)

    # Execute remaining
    await pipe.execute()

    end_t = time.time()
    elapsed = end_t - start_t
    print(f"✅ Published {num_messages} messages to {channel} in {elapsed:.3f} seconds.")
    print(f"Throughput: {num_messages / elapsed:.0f} msg/sec.")
    print("Test Complete. Check NestJS gateway (scanner-nestjs) logs to verify backpressure handling and peak memory consumption.")

    await r.aclose()

if __name__ == "__main__":
    asyncio.run(run_benchmark())
