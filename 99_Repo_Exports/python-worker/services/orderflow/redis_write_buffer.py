from __future__ import annotations
"""
Redis Write-Behind Buffer and Local TTL Cache for hot-path optimization.

Provides two utilities:
1. WriteBehindBuffer — coalesces fire-and-forget SET/INCR/EXPIRE into periodic pipeline flushes,
   reducing Redis round-trips by ~10-50x for frequently-overwritten keys.
2. LocalTTLCache — in-process TTL cache for slow-changing Redis keys (config, state),
   eliminating redundant GET/HGETALL round-trips.

Usage:
    # Write-behind (for cfg:last_px, cfg:atr_bad, metrics counters)
    wb = WriteBehindBuffer(redis_client, flush_interval_sec=2.0)
    wb.set("cfg:last_px:BTCUSDT", "65432.10", ex=600)
    wb.incr("metrics:atr_bad_total:BTCUSDT", 1)
    wb.sadd("cfg:atr_bad:symbols", "BTCUSDT", ex=86400)
    # ... periodic flush via asyncio task

    # Read cache (for config:orderflow:*, trailing configs)
    cache = LocalTTLCache(ttl_ms=500, max_size=256)
    val = cache.get("config:orderflow:BTCUSDT")
    if val is None:
        val = await redis.hgetall(...)
        cache.put("config:orderflow:BTCUSDT", val)
"""


import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger("redis_write_buffer")


class WriteBehindBuffer:
    """Coalesces fire-and-forget Redis writes into periodic pipeline flushes.

    For keys that are overwritten on every tick (e.g. cfg:last_px:*),
    this reduces ~900 SET/sec → ~9 pipeline execs/sec (at flush_interval=2s).

    Thread-safety: designed for single-threaded asyncio event loop.
    """
    __slots__ = (
        '_redis', '_flush_sec', '_last_flush_ns',
        '_pending_set', '_pending_incr', '_pending_sadd',
        '_pending_expire', '_flush_task', '_closed',
    )

    def __init__(self, redis_client: Any, flush_interval_sec: float = 2.0):
        self._redis = redis_client
        self._flush_sec = flush_interval_sec
        self._last_flush_ns = time.monotonic_ns()
        self._pending_set: Dict[str, Tuple[str, Optional[int]]] = {}  # key → (value, ex_sec|None)
        self._pending_incr: Dict[Tuple[str, str], int] = {}  # (hash_key, field) → delta
        self._pending_sadd: Dict[str, set] = {}  # key → set of members
        self._pending_expire: Dict[str, int] = {}  # key → ttl_sec (last wins)
        self._flush_task: Optional[asyncio.Task] = None
        self._closed = False

    def set(self, key: str, value: str, ex: int = 0) -> None:
        """Queue a SET. Last-write-wins for the same key."""
        self._pending_set[key] = (value, ex if ex > 0 else None)
        self._maybe_schedule_flush()

    def incr(self, key: str, amount: int = 1) -> None:
        """Queue an INCR (or INCRBY). Accumulates for the same key."""
        self._pending_incr[key] = self._pending_incr.get(key, 0) + amount
        self._maybe_schedule_flush()

    def hincrby(self, key: str, field: str, amount: int = 1) -> None:
        """Queue an HINCRBY. Accumulates for the same (key, field)."""
        k = (key, field)
        self._pending_incr[k] = self._pending_incr.get(k, 0) + amount
        self._maybe_schedule_flush()

    def sadd(self, key: str, *members: str, ex: int = 0) -> None:
        """Queue an SADD. Accumulates members for the same key."""
        s = self._pending_sadd.get(key)
        if s is None:
            s = set()
            self._pending_sadd[key] = s
        s.update(members)
        if ex > 0:
            self._pending_expire[key] = ex
        self._maybe_schedule_flush()

    def expire(self, key: str, ttl_sec: int) -> None:
        """Queue an EXPIRE. Last-write-wins."""
        self._pending_expire[key] = ttl_sec

    @property
    def pending_count(self) -> int:
        return len(self._pending_set) + len(self._pending_incr) + len(self._pending_sadd)

    def _maybe_schedule_flush(self) -> None:
        """Schedule flush if not already scheduled and we're not closed."""
        if self._closed:
            return
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.ensure_future(self._deferred_flush())

    async def _deferred_flush(self) -> None:
        """Wait for flush interval, then flush."""
        try:
            await asyncio.sleep(self._flush_sec)
            await self.flush()
        except asyncio.CancelledError:
            # Flush on cancel to avoid data loss
            await self.flush()
        except Exception as exc:
            logger.warning("WriteBehindBuffer flush error: %s", exc)

    async def flush(self) -> int:
        """Flush all pending writes via a single Redis pipeline. Returns ops count."""
        if not self._pending_set and not self._pending_incr and not self._pending_sadd:
            return 0

        ops = 0
        try:
            pipe = self._redis.pipeline(transaction=False)

            # SET operations
            for key, (value, ex) in self._pending_set.items():
                if ex:
                    pipe.set(key, value, ex=ex)
                else:
                    pipe.set(key, value)
                ops += 1
            self._pending_set.clear()

            # INCR / HINCRBY operations
            for k, delta in self._pending_incr.items():
                if isinstance(k, tuple):
                    # HINCRBY
                    pipe.hincrby(k[0], k[1], delta)
                else:
                    # INCRBY
                    pipe.incrby(k, delta)
                ops += 1
            self._pending_incr.clear()

            # SADD operations
            for key, members in self._pending_sadd.items():
                if members:
                    pipe.sadd(key, *members)
                    ops += 1
            self._pending_sadd.clear()

            # EXPIRE operations
            for key, ttl in self._pending_expire.items():
                pipe.expire(key, ttl)
                ops += 1
            self._pending_expire.clear()

            await pipe.execute()
            self._last_flush_ns = time.monotonic_ns()
        except Exception as exc:
            logger.warning("WriteBehindBuffer pipeline error (%d ops): %s", ops, exc)

        return ops

    async def close(self) -> None:
        """Final flush and prevent further writes."""
        self._closed = True
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
        await self.flush()


class LocalTTLCache:
    """In-process LRU-like cache with millisecond-resolution TTL.

    Eliminates Redis round-trips for slow-changing keys (config, state hashes).
    At TTL=500ms and 18 symbols polling at 10/sec, this saves ~180 Redis ops/sec.

    Thread-safety: designed for single-threaded asyncio event loop.
    """
    __slots__ = ('_cache', '_ttl_ms', '_max_size')

    def __init__(self, ttl_ms: int = 500, max_size: int = 256):
        self._cache: Dict[str, Tuple[int, Any]] = {}
        self._ttl_ms = ttl_ms
        self._max_size = max_size

    def get(self, key: str) -> Optional[Any]:
        """Return cached value if TTL is still valid, else None."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        ts_ms, value = entry
        now_ms = time.monotonic_ns() // 1_000_000
        if (now_ms - ts_ms) < self._ttl_ms:
            return value
        # Expired — remove
        del self._cache[key]
        return None

    def put(self, key: str, value: Any) -> None:
        """Store value with current timestamp."""
        now_ms = time.monotonic_ns() // 1_000_000
        if len(self._cache) >= self._max_size:
            # Evict oldest entry
            oldest_key = min(self._cache, key=lambda k: self._cache[k][0])
            del self._cache[oldest_key]
        self._cache[key] = (now_ms, value)

    def invalidate(self, key: str) -> None:
        """Remove a specific key from cache."""
        self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear entire cache."""
        self._cache.clear()

    @property
    def size(self) -> int:
        return len(self._cache)
