
import asyncio
import json
import os

import redis.asyncio as aioredis
from core.redis_keys import RedisStreams as RS


async def main():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True)

    stream = RS.CRYPTO_RAW
    print(f"Reading from {stream}...")

    try:
        entries = await r.xrevrange(stream, count=20)
        if not entries:
            print("Stream is empty.")
            return

        for msg_id, fields in entries:
            payload_str = fields.get("payload", "{}")
            payload = json.loads(payload_str)

            symbol = payload.get("symbol")
            direction = payload.get("direction")
            ts = payload.get("ts_ms")
            val_status = payload.get("validation_status")
            val_reason = payload.get("validation_reason")

            indicators = payload.get("indicators", {})
            of_confirm_ok = indicators.get("of_confirm_ok")
            strong_gate_ok = indicators.get("strong_gate_ok")

            print(f"[{msg_id}] {symbol} {direction} ts={ts}")
            print(f"   Validation: status={val_status}, reason={val_reason}")
            print(f"   Indicators: of_confirm_ok={of_confirm_ok}, strong_gate_ok={strong_gate_ok}")
            print("-" * 40)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await r.aclose()

if __name__ == "__main__":
    asyncio.run(main())
