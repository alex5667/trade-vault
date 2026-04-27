import asyncio
import os
import json
import logging
import time
import sys
sys.path.append("/app")
import redis.asyncio as aioredis

from core.microbar_streams import LEGACY_STREAM, SYMBOLS_SET, list_symbols, pick_stream_key

async def main():
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    print(f"Connecting to {url}...")
    try:
        r = aioredis.from_url(url, decode_responses=True, socket_connect_timeout=2)
        await r.ping()
        print("Connected.")
    except Exception as e:
        print(f"Failed: {e}")
        return

    print("\n--- Discovery ---\n")
    
    # 1. Check Microbar Streams (legacy vs split)
    try:
        legacy_exists = await r.exists(LEGACY_STREAM)
        print(f"{LEGACY_STREAM} exists: {bool(legacy_exists)}")
        if legacy_exists:
            events = await r.xrevrange(LEGACY_STREAM, count=5)
            print(f"{LEGACY_STREAM} (Last 5):")
            for eid, fields in events:
                keys = list(fields.keys())
                print(f"  ID: {eid} | Fields: {keys}")
    except Exception as e:
        print(f"Error checking legacy microbar stream: {e}")

    # 2. Check symbols set (split-streams)
    try:
        syms = await list_symbols(r, fallback=[])
        print(f"Symbols set: {SYMBOLS_SET} | n={len(syms)}")
        if syms:
            print(f"Sample symbols: {syms[:10]}")
            for sym in syms[:3]:
                k = await pick_stream_key(r, sym)
                last = await r.xrevrange(k, count=3)
                print(f"  {sym}: stream={k} | last_n={len(last)}")
    except Exception as e:
        print(f"Error checking symbols set: {e}")

    # 3. Key Scan for BTCUSDT
    print("\nScanning keys for BTCUSDT...")
    keys = []
    async for k in r.scan_iter("*BTCUSDT*"):
        keys.append(k)
        if len(keys) > 50: break
    
    print(f"Found {len(keys)} keys matching *BTCUSDT*:")
    for k in keys:
        print(f"  {k} ({await r.type(k)})")

    # 3. Check Stream Book
    print("\nChecking stream:book_BTCUSDT payload...")
    try:
        books = await r.xrevrange("stream:book_BTCUSDT", count=1)
        if books:
            print(f"  ID: {books[0][0]}")
            print(f"  Fields: {books[0][1]}")
    except Exception as e:
         print(f"  Error: {e}")

    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
