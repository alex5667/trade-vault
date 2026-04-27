import asyncio
import os
import json
import redis.asyncio as aioredis

from core.microbar_streams import read_microbars

async def main():
    r = aioredis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    
    # Check BTC microbars (split-streams aware)
    print("--- BTC Bars Sample ---")
    btc_bars = await read_microbars(r, sym="BTCUSDT", count=5000, reverse=True)
    for b in btc_bars[:5]:
        ts = b.get('ts_ms')
        close = b.get('close')
        vol = b.get('vol') or b.get('volume')
        cvd = b.get('cvd')
        print(f"TS: {ts} | Close: {close} | Vol: {vol} | CVD: {cvd}")

    # Check Book Depth for BTC
    print("\n--- BTC Book Sample ---")
    books = await r.xrevrange("stream:book_BTCUSDT", count=1)
    if books:
        payload = books[0][1]
        print(f"Keys: {list(payload.keys())}")
        if "bids" in payload:
             b = json.loads(payload["bids"])
             a = json.loads(payload["asks"])
             print(f"Bids: {len(b)}, Asks: {len(a)}")

    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
