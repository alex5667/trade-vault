import os
import json
import time
import logging
import asyncio
from utils.task_manager import safe_create_task

from typing import Any, Dict, List, Optional

# NOTE: asyncpg is an optional dependency for unit-test environments.
# Importing it at module import time makes unrelated unit tests fail.
# We fail-fast *only when* persistence is actually used.
try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore

logger = logging.getLogger("persistence_manager")

class PersistenceManager:
    """
    Handles redundant storage and restoration of calibration states and microbar history in PostgreSQL.
    """
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN")) or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN")) or "postgresql://postgres:12345@scanner-postgres:5432/scanner_analytics"
        self._pool: Optional[asyncpg.Pool] = None
        
        # Batching state
        self._microbar_buffer: List[tuple] = []
        self._microbar_lock: Optional[asyncio.Lock] = None
        self._last_flush_time = 0.0
        self._flush_task: Optional[asyncio.Task] = None

    async def _get_pool(self) -> asyncpg.Pool:
        """Lazy initialization of the high-concurrency connection pool."""
        if self._pool is None:
            logger.info(f"🔌 Initializing asyncpg pool for {self.dsn.split('@')[-1]}")
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=2,
                max_size=20,           # was 1000 — caused PG connection exhaustion
                max_inactive_connection_lifetime=300,
                timeout=60
            )
        return self._pool

    async def close(self):
        """Close the connection pool and flush remaining microbars."""
        await self._flush_microbars(force=True)
        if self._flush_task is not None:
            self._flush_task.cancel()
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _flush_loop(self):
        while True:
            await asyncio.sleep(0.5)
            await self._flush_microbars()

    async def _flush_microbars(self, force: bool = False):
        if self._microbar_lock is None:
            return

        async with self._microbar_lock:
            if not self._microbar_buffer:
                return
            
            # Flush if enough items OR enough time has passed OR forced
            current_time = time.time()
            if not force and len(self._microbar_buffer) < 500 and (current_time - self._last_flush_time < 0.5):
                return
                
            batch = self._microbar_buffer[:]
            self._microbar_buffer.clear()
            self._last_flush_time = current_time

        if not batch:
            return

        # 1. Deduplicate by (symbol, ts_ms), keeping the latest
        # batch element format:
        # (symbol, ts_ms, o, h, l, c, v, cvd)
        dedup_map = {}
        for row in batch:
            k = (row[0], row[1])
            dedup_map[k] = row
            
        unique_batch = list(dedup_map.values())
        
        # 2. Sort by symbol, then by time to heavily prevent Postgres deadlocks
        unique_batch.sort(key=lambda x: (x[0], x[1]))

        sql = """
        INSERT INTO microbars (symbol, ts_ms, o, h, l, c, v, cvd, inserted_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
        ON CONFLICT (symbol, ts_ms) DO NOTHING;
        """
        
        chunk_size = 2000
        chunks = [unique_batch[i:i + chunk_size] for i in range(0, len(unique_batch), chunk_size)]

        for chunk in chunks:
            for attempt in range(1, 4):
                try:
                    pool = await self._get_pool()
                    async with pool.acquire(timeout=5.0) as conn:
                        await asyncio.wait_for(conn.executemany(sql, chunk), timeout=15.0)
                    break # Success for this chunk
                except (asyncio.TimeoutError, asyncpg.exceptions.PostgresError) as e:
                    logger.warning(f"⚠️ Pool timeout/error saving microbar chunk of size {len(chunk)} (attempt {attempt}/3): {e}")
                    if attempt < 3:
                         await asyncio.sleep(attempt * 0.5)
                except Exception as e:
                    logger.error(f"❌ Failed to save microbar chunk: {type(e).__name__} - {e}", exc_info=attempt==3)
                    if attempt < 3:
                         await asyncio.sleep(attempt * 0.5)
            else:
                # Fallback to direct connection if pool fails consistently for this chunk
                try:
                     logger.warning(f"🔄 Falling back to direct connection for microbar chunk")
                     # We use a 10s connection timeout for fallback
                     conn = await asyncpg.connect(self.dsn, timeout=10.0)
                     try:
                         # We use a larger timeout here if it's the absolute last fallback
                         await asyncio.wait_for(conn.executemany(sql, chunk), timeout=30.0)
                     finally:
                         await conn.close()
                except Exception as e:
                     logger.error(f"❌ Final explicit failure saving microbar chunk: {type(e).__name__} - {e}")

    async def save_calibration_state(self, symbol: str, regime: str, kind: str, ts_ms: int, state: Dict[str, Any]) -> bool:
        """Saves or updates calibration state in PG."""
        try:
            pool = await self._get_pool()
            sql = """
            INSERT INTO calibration_state (symbol, regime, kind, ts_ms, state_json, updated_at)
            VALUES ($1, $2, $3, $4, $5, now())
            ON CONFLICT (symbol, regime, kind) DO UPDATE SET
                ts_ms = EXCLUDED.ts_ms,
                state_json = EXCLUDED.state_json,
                updated_at = now();
            """
            await pool.execute(sql, symbol, regime, kind, ts_ms, json.dumps(state))
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save calibration state for {symbol}:{regime}:{kind}: {e}")
            return False

    async def load_calibration_states(self, symbol: str) -> List[Dict[str, Any]]:
        """Loads all calibration states for a symbol from PG."""
        try:
            pool = await self._get_pool()
            sql = "SELECT symbol, regime, kind, ts_ms, state_json FROM calibration_state WHERE symbol = $1"
            rows = await pool.fetch(sql, symbol)
            result = []
            for r in rows:
                item = dict(r)
                if isinstance(item['state_json'], str):
                    item['state_json'] = json.loads(item['state_json'])
                result.append(item)
            return result
        except Exception as e:
            logger.error(f"❌ Failed to load calibration states for {symbol}: {e}")
            return []

    async def save_microbar(self, symbol: str, bar_data: Dict[str, Any]) -> bool:
        """Queues a closed microbar for batched saving to PG."""
        if self._microbar_lock is None:
            self._microbar_lock = asyncio.Lock()
            # Start the background flusher only once
            if self._flush_task is None:
                self._flush_task = safe_create_task(self._flush_loop())

        args = (
            symbol,
            int(bar_data['ts_ms']),
            float(bar_data['open']),
            float(bar_data['high']),
            float(bar_data['low']),
            float(bar_data['close']),
            float(bar_data['vol']),
            float(bar_data['cvd'])
        )
        
        async with self._microbar_lock:
            self._microbar_buffer.append(args)
            
            # If buffer is getting too large, force flush without waiting
            if len(self._microbar_buffer) >= 2000:
                safe_create_task(self._flush_microbars(force=True))
                
        return True

    async def load_microbar_history(self, symbol: str, limit: int = 300) -> List[Dict[str, Any]]:
        """Loads historical microbars for a symbol from PG, sorted by time."""
        try:
            pool = await self._get_pool()
            sql = """
            SELECT ts_ms, o as open, h as high, l as low, c as close, v as vol, cvd as cvd_close
            FROM microbars 
            WHERE symbol = $1 
            ORDER BY ts_ms DESC 
            LIMIT $2
            """
            rows = await pool.fetch(sql, symbol, limit)
            result = [dict(r) for r in rows]
            return sorted(result, key=lambda x: x['ts_ms'])
        except Exception as e:
            logger.error(f"❌ Failed to load microbar history for {symbol}: {e}")
            return []

    async def save_daily_ohlc(self, symbol: str, date: str, o: float, h: float, l: float, c: float, v: float) -> bool:
        """Saves a daily OHLC candle to PG."""
        try:
            pool = await self._get_pool()
            sql = """
            INSERT INTO market_daily_ohlc (symbol, date, open, high, low, close, volume, inserted_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, now())
            ON CONFLICT (symbol, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                inserted_at = now();
            """
            await pool.execute(sql, symbol, date, o, h, l, c, v)
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save daily OHLC for {symbol} at {date}: {e}")
            return False

    async def get_latest_daily_ohlc(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Loads the most recent daily OHLC candle for a symbol from PG."""
        try:
            pool = await self._get_pool()
            sql = """
            SELECT date, open, high, low, close, volume
            FROM market_daily_ohlc 
            WHERE symbol = $1 
            ORDER BY date DESC 
            LIMIT 1
            """
            row = await pool.fetchrow(sql, symbol)
            if row:
                item = dict(row)
                item['date'] = str(item['date'])
                item['high'] = float(item['high'])
                item['low'] = float(item['low'])
                item['close'] = float(item['close'])
                if item.get('open') is not None: item['open'] = float(item['open'])
                if item.get('volume') is not None: item['volume'] = float(item['volume'])
                return item
            return None
        except Exception as e:
            logger.error(f"❌ Failed to load latest daily OHLC for {symbol}: {e}")
            return None

    def get_latest_daily_ohlc_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Synchronous version of get_latest_daily_ohlc for legacy sync services.
        WARNING: This uses a temporary connection and is NOT efficient.
        """
        import psycopg2
        from psycopg2.extras import RealDictCursor
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    sql = """
                    SELECT date, open, high, low, close, volume
                    FROM market_daily_ohlc 
                    WHERE symbol = %s 
                    ORDER BY date DESC 
                    LIMIT 1
                    """
                    cur.execute(sql, (symbol,))
                    row = cur.fetchone()
                    if row:
                        row['date'] = str(row['date'])
                        row['high'] = float(row['high'])
                        row['low'] = float(row['low'])
                        row['close'] = float(row['close'])
                        if row.get('open') is not None: row['open'] = float(row['open'])
                        if row.get('volume') is not None: row['volume'] = float(row['volume'])
                        return dict(row)
                    return None
        except Exception as e:
            logger.error(f"❌ Failed to load latest daily OHLC (sync) for {symbol}: {e}")
            return None

# Singleton helper
_pm_instance = None
def get_persistence_manager() -> PersistenceManager:
    global _pm_instance
    if _pm_instance is None:
        _pm_instance = PersistenceManager()
    return _pm_instance
