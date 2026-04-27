
import os
import sys
import asyncio
import logging
import requests
import json
from datetime import datetime, timezone

# Add python-worker to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.persistence_manager import get_persistence_manager
from core.redis_client import get_redis

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backfill_daily")

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

async def fetch_binance_klines(symbol: str, limit: int = 30):
    """Fetch daily klines from Binance."""
    try:
        url = f"{BINANCE_API_URL}?symbol={symbol}&interval=1d&limit={limit}"
        logger.info(f"Fetching {limit} daily klines for {symbol} from {url}...")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # Binance kline: [open_time, open, high, low, close, volume, close_time, ...]
        return data
    except Exception as e:
        logger.error(f"Failed to fetch klines for {symbol}: {e}")
        return []

async def backfill_symbol(pm, redis, symbol: str):
    klines = await fetch_binance_klines(symbol, limit=40)
    if not klines:
        return

    logger.info(f"Processing {len(klines)} candles for {symbol}...")
    
    # Sort by time just in case
    klines.sort(key=lambda x: x[0])

    latest_hlc = None
    
    for k in klines:
        ts_ms = int(k[0])
        o = float(k[1])
        h = float(k[2])
        l = float(k[3])
        c = float(k[4])
        v = float(k[5])
        
        # Date string from timestamp (UTC)
        date_str = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
        
        # Save to PG
        await pm.save_daily_ohlc(symbol, date_str, o, h, l, c, v)
        logger.debug(f"Saved {symbol} {date_str} to PG")
        
        latest_hlc = {
            "date": date_str,
            "high": h,
            "low": l,
            "close": c,
            # optional but good for consistency
            "open": o,
            "volume": v
        }

    # If we have a latest candle, verify if it is "yesterday" relative to NOW
    # Ideally we populate 'yesterday_hlc:{symbol}' with the previous completed day.
    
    # Let's find the completed candle for "yesterday" (UTC)
    now_utc = datetime.now(timezone.utc)
    yesterday_date = now_utc.date().strftime("%Y-%m-%d") # This is TODAY in date string, wait.
    # Actually 'yesterday_hlc' usually means the closes of the PREVIOUS day.
    # Binance returns completed candles mostly? No, the last one might be open.
    # 'daily_hlc' usually means completed days.
    
    # We will iterate and find the one that matches (now - 1 day)
    
    # Correction: 'yesterday_hlc' key is used for Pivot calculations for the CURRENT day.
    # So if today is 2026-01-17, we need HLC of 2026-01-16.
    
    target_yesterday = None
    # We want the last CLOSED candle.
    # If the last candle in list is "today" (still open), we ignore it for yesterday_hlc pivot calculation usually.
    # Binance includes the current open candle as the last element? Yes.
    
    # Helper to check date
    today_str = now_utc.strftime("%Y-%m-%d")
    
    found_yesterday = None
    
    for k in reversed(klines):
        ts_ms = int(k[0])
        k_date = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
        
        if k_date < today_str:
            # This is the latest COMPLETED candle (yesterday or earlier)
            found_yesterday = {
                "date": k_date,
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "open": float(k[1]),
                "volume": float(k[5])
            }
            break
            
    if found_yesterday:
        key = f"yesterday_hlc:{symbol}"
        redis.setex(key, 172800, json.dumps(found_yesterday))
        logger.info(f"✅ Set Redis {key} -> {found_yesterday['date']}")
    else:
        logger.warning(f"⚠️ Could not find a completed yesterday candle for {symbol}")

async def main():
    dsns = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
    if not dsns:
        # Fallback for local run
        os.environ["PG_DSN"] = "postgresql://trading:trading_password@postgres:5432/scanner_analytics"
        
    pm = get_persistence_manager()
    redis = get_redis()
    
    # Try to load keys from env or default
    symbols_env = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,SUIUSDT,APTUSDT,ARBUSDT")
    symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
    
    logger.info(f"Backfilling daily candles for: {symbols}")
    
    for s in symbols:
        await backfill_symbol(pm, redis, s)

    logger.info("Done.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
