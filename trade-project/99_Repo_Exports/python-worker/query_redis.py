import asyncio
import redis.asyncio as redis
import os

async def main():
    r = redis.Redis(host='localhost', port=6379, db=0)
    try:
        val = await r.hget("config:orderflow:BTCUSDT", "obi_stable_score_min")
        print(f"Redis config:orderflow:BTCUSDT:obi_stable_score_min = {val}")
    except Exception as e:
        print(f"Error reading redis: {e}")

asyncio.run(main())
