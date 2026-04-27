#!/usr/bin/env python3
"""
Candles Archiver Service.

Reads completed candles from Redis stream `candles:data` and archives them
to PostgreSQL TimescaleDB hypertable `candles_archive`.

Features:
- Consumer Group processing (at-least-once delivery)
- Batch inserts for performance
- Deduplication via ON CONFLICT DO NOTHING
- Metadata tracking in `archive_metadata` table
"""

import os
import sys
import time
import json
import signal
import logging
from datetime import datetime, timezone
from typing import Dict, Any

import redis
import psycopg2
from psycopg2.extras import execute_batch

# Configuration
REDIS_URL = os.getenv("REDIS_URL")
PG_DSN = os.getenv("ANALYTICS_DSN")

CANDLES_STREAM = "candles:data"
ARCHIVE_GROUP = os.getenv("ARCHIVE_GROUP", "candles-archiver-group")
ARCHIVE_CONSUMER = os.getenv("ARCHIVE_CONSUMER", "archiver-1")
BATCH_SIZE = int(os.getenv("ARCHIVE_BATCH_SIZE", "1000"))
BLOCK_MS = 2000

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("candles_archiver")

# Global flags
running = True
_archive_log_counter = 0

def handle_signal(signum, frame):
    global running
    logger.info(f"Received signal {signum}, stopping...")
    running = False

def get_redis_client():
    return redis.from_url(REDIS_URL, decode_responses=False)

def get_pg_connection():
    return psycopg2.connect(
        PG_DSN,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

def _first_value(obj: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = obj.get(key)
        if value is not None and value != "":
            return value
    return default

def safe_int(val, default=0):
    if val is None or val == "":
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

def safe_float(val, default=0.0):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def parse_candle(data: Dict[bytes, bytes]) -> Dict[str, Any]:
    """Parse candle data from Redis stream format."""
    try:
        # Decode bytes keys/values
        d = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
        
        # Determine source format (JSON or fields)
        if d.get('type') == 'init':
            return None
            
        ts_fallback = safe_int(d.get('ts'))
            
        json_data = d.get('data') or d.get('payload')
        if json_data:
            # JSON format
            try:
                raw = json.loads(json_data)
                if isinstance(raw, str):
                    raw = json.loads(raw)
            except json.JSONDecodeError:
                raw = {}
                
            open_time_ms = safe_int(_first_value(raw, 'k_t', 't', 'open_time', 'openTime'), ts_fallback)
            close_time_ms = safe_int(_first_value(raw, 'k_Tw', 'T', 'close_time', 'closeTime'), ts_fallback)
            
            return {
                'symbol': _first_value(raw, 's', 'symbol') or _first_value(d, 'symbol', 's', default='UNKNOWN'),
                'timeframe': _first_value(raw, 'tf', 'timeframe', 'i') or _first_value(d, 'tf', 'timeframe', 'i', default='1m'),
                'open_time': datetime.fromtimestamp(open_time_ms / 1000.0, timezone.utc),
                'close_time': datetime.fromtimestamp(close_time_ms / 1000.0, timezone.utc),
                'open': safe_float(_first_value(raw, 'o', 'open')),
                'high': safe_float(_first_value(raw, 'h', 'high')),
                'low': safe_float(_first_value(raw, 'l', 'low')),
                'close': safe_float(_first_value(raw, 'c', 'close')),
                'volume': safe_float(_first_value(raw, 'v', 'volume')),
                'quote_volume': safe_float(_first_value(raw, 'q', 'quote_volume', 'quoteVolume')),
                'trades': safe_int(_first_value(raw, 'n', 'trades', 'numberOfTrades')),
                'taker_buy_base': safe_float(_first_value(raw, 'V', 'taker_buy_base', 'takerBuyVolume')),
                'taker_buy_quote': safe_float(_first_value(raw, 'Q', 'taker_buy_quote', 'takerBuyQuote', 'takerBuyQuoteVolume')),
            }
        else:
            # Fields format (go-worker standard)
             # Expected keys: s, i, t, T, o, h, l, c, v, q, n, V, Q
             # Or full names
            open_time_ms = safe_int(_first_value(d, 't', 'open_time'), ts_fallback)
            close_time_ms = safe_int(_first_value(d, 'T', 'close_time'), ts_fallback)
             
            return {
                'symbol': _first_value(d, 's', 'symbol', default='UNKNOWN'),
                'timeframe': _first_value(d, 'i', 'tf', 'timeframe', default='1m'),
                'open_time': datetime.fromtimestamp(open_time_ms / 1000.0, timezone.utc),
                'close_time': datetime.fromtimestamp(close_time_ms / 1000.0, timezone.utc),
                'open': safe_float(_first_value(d, 'o', 'open')),
                'high': safe_float(_first_value(d, 'h', 'high')),
                'low': safe_float(_first_value(d, 'l', 'low')),
                'close': safe_float(_first_value(d, 'c', 'close')),
                'volume': safe_float(_first_value(d, 'v', 'volume')),
                'quote_volume': safe_float(_first_value(d, 'q', 'quote_volume')),
                'trades': safe_int(_first_value(d, 'n', 'trades', 'numberOfTrades')),
                'taker_buy_base': safe_float(_first_value(d, 'V', 'taker_buy_base', 'takerBuyVolume')),
                'taker_buy_quote': safe_float(_first_value(d, 'Q', 'taker_buy_quote', 'takerBuyQuote', 'takerBuyQuoteVolume')),
            }
            
    except Exception as e:
        logger.error(f"Failed to parse candle data: {e} - Data: {data}")
        return None

def ensure_consumer_group(r, stream, group):
    try:
        r.xgroup_create(stream, group, id='0', mkstream=True)
        logger.info(f"Created consumer group {group}")
    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' in str(e):
            logger.info(f"Consumer group {group} already exists")
        else:
            logger.error(f"Failed to create consumer group: {e}")
            raise e

def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    
    logger.info("Starting Candles Archiver Service...")
    logger.info(f"Stream: {CANDLES_STREAM}, Group: {ARCHIVE_GROUP}, Batch: {BATCH_SIZE}")
    
    r = get_redis_client()
    
    # Ensure consumer group exists
    try:
        ensure_consumer_group(r, CANDLES_STREAM, ARCHIVE_GROUP)
    except Exception as e:
        logger.error(f"Startup error: {e}")
        sys.exit(1)
            
    pg_conn = None

    while running:
        try:
            # Read batch from Redis
            messages = r.xreadgroup(
                ARCHIVE_GROUP, ARCHIVE_CONSUMER,
                {CANDLES_STREAM: '>'},
                count=BATCH_SIZE,
                block=BLOCK_MS
            )
            
            if not messages:
                continue
                
            batch_data = []
            msg_ids = []
            last_id = ""
            
            # Process messages
            for stream, msgs in messages:
                for msg_id, data in msgs:
                    candle = parse_candle(data)
                    if candle and candle.get('symbol') not in (None, '', 'UNKNOWN') and candle.get('open', 0.0) > 0.0:
                        batch_data.append((
                            candle['symbol'],
                            candle['timeframe'],
                            candle['open_time'],
                            candle['close_time'],
                            candle['open'],
                            candle['high'],
                            candle['low'],
                            candle['close'],
                            candle['volume'],
                            candle['quote_volume'],
                            candle['trades'],
                            candle['taker_buy_base'],
                            candle['taker_buy_quote']
                        ))
                    msg_ids.append(msg_id)
                    last_id = msg_id.decode('utf-8')
            
            if not batch_data:
                # Still ACK if we got messages but couldn't parse them (to skip bad data)
                if msg_ids:
                    r.xack(CANDLES_STREAM, ARCHIVE_GROUP, *msg_ids)
                continue
                
            # Insert into Postgres with an infinite retry loop to avoid data loss
            while True:
                # Maintain persistent Postgres connection
                if pg_conn is None or pg_conn.closed:
                    try:
                        pg_conn = get_pg_connection()
                        logger.info("Connected to PostgreSQL successfully.")
                    except Exception as e:
                        logger.error(f"Failed to connect to PostgreSQL: {e}")
                        time.sleep(5)
                        continue

                try:
                    with pg_conn:
                        with pg_conn.cursor() as cur:
                            execute_batch(cur, """
                                INSERT INTO candles_archive (
                                    symbol, timeframe, open_time, close_time,
                                    open, high, low, close,
                                    volume, quote_volume, trades,
                                    taker_buy_base, taker_buy_quote
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (symbol, timeframe, open_time) DO NOTHING
                            """, batch_data, page_size=1000)
                            
                            # Update metadata
                            cur.execute("""
                                UPDATE archive_metadata 
                                SET last_archived_id = %s,
                                    last_archived_at = NOW(),
                                    records_archived = records_archived + %s
                                WHERE stream_name = %s
                            """, (last_id, len(batch_data), CANDLES_STREAM))
                    # Batch successfully inserted, break out of retry loop
                    break
                except psycopg2.OperationalError as e:
                    logger.error(f"PostgreSQL OperationalError (connection dropped): {e}")
                    if pg_conn:
                        pg_conn.close()
                    pg_conn = None
                    time.sleep(2)
                    continue
                except Exception as e:
                    logger.error(f"PostgreSQL Error during batch insert: {e}")
                    # Don't drop connection on basic IntegrityError etc, but let with-block rollback
                    raise

            
            # ACK messages in Redis
            r.xack(CANDLES_STREAM, ARCHIVE_GROUP, *msg_ids)
            
            global _archive_log_counter
            _archive_log_counter += 1
            if _archive_log_counter % 10000 == 0:
                logger.info(f"✅ Archived {len(batch_data)} candles. Last ID: {last_id}")
            
        except redis.exceptions.ResponseError as e:
            if 'NOGROUP' in str(e):
                logger.warning(f"Consumer group missing (NOGROUP), attempting to recreate: {e}")
                try:
                    ensure_consumer_group(r, CANDLES_STREAM, ARCHIVE_GROUP)
                    time.sleep(1)
                except Exception as ex:
                    logger.error(f"Failed to recreate group: {ex}")
                    time.sleep(5)
            else:
                logger.error(f"Redis error in main loop: {e}")
                time.sleep(5)

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(5)  # Backoff on error

    if pg_conn and not pg_conn.closed:
        pg_conn.close()
        
    logger.info("Service stopped.")

if __name__ == "__main__":
    main()
