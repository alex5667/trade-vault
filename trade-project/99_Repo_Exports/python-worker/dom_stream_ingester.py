# dom_stream_ingester.py
"""
DOM Stream Ingester - читает DOM из адаптера и публикует нормализованные уровни в Redis.
Async version. Supports multiple symbols.
"""
from __future__ import annotations
import asyncio
import os
import json
import signal
import sys
from typing import Dict, List
from common.log import setup_logger
from dom_feed_adapter import build_adapter
import redis.asyncio as aioredis

log = setup_logger("dom_ingester")

async def run_symbol_ingestion(r: aioredis.Redis, symbol: str):
    """
    Запускает ингестию для одного символа.
    """
    stream = f"stream:book_{symbol}"
    last_key = f"book:levels:{symbol}"
    adapter = build_adapter(symbol)
    
    log.info(
        "DOM Ingester task started | vendor=%s symbol=%s stream=%s",
        os.getenv("DOM_VENDOR", ""),
        symbol,
        stream
    )
    
    while True:
        try:
            async for snap in adapter.iter_levels():
                payload = {
                    "ts": snap["ts"],
                    "symbol": snap["symbol"],
                    "mid": snap["mid"],
                    "bids": snap["bids"],
                    "asks": snap["asks"],
                    "provider": snap["provider"]
                }
                payload_json = json.dumps(payload)
                
                # Pipeline optimizations could be done here if needed, but for DOM updates 
                # (usually 100ms-500ms), sequential await is acceptable for readability.
                await r.set(last_key, payload_json)
                await r.xadd(stream, {"data": payload_json}, maxlen=3000)
        except Exception as e:
            log.error(f"Error processing {symbol}: {e}")
            await asyncio.sleep(5) # Backoff before restarting adapter

async def main():
    """Main function - читает DOM и публикует в Redis для списка символов."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=False)
    
    # Support SYMBOLS env var (comma separated) or fallback to single SYMBOL
    symbols_env = os.getenv("SYMBOLS", "")
    if symbols_env:
        symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    else:
        symbols = [os.getenv("SYMBOL", "XAUUSD").strip().upper()]

    log.info(f"Starting DOM Ingester for symbols: {symbols}")

    try:
        async with asyncio.TaskGroup() as tg:
            for sym in symbols:
                tg.create_task(run_symbol_ingestion(r, sym))
    except Exception as e:
        log.error(f"Fatal error in task group: {e}")
    finally:
        await r.close()

if __name__ == "__main__":
    try:
        # Check for Python 3.11+ for TaskGroup
        if sys.version_info < (3, 11):
            # Fallback for older python if needed, but we assume 3.11+ per user persona context (latest tools)
            # Actually python-worker uses 3.12 (see Dockerfile)
            asyncio.run(main())
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        pass
