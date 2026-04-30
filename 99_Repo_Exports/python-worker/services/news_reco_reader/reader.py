"""services.news_reco_reader.reader

Asyncio background reader that periodically fetches *one* Redis key:

    trade:cache:news_reco_map

and updates an in-memory TTL cache. Stage-5 gates/policies consume the
in-memory cache only (no await / no IO).

Fail-open behavior
------------------
- Any Redis/JSON errors do NOT stop the trade-core.
- If last-good refresh is older than STALE_FAIL_OPEN_MS, cache is cleared.
  That disables tighten/hard effects based on stale information.

Backends
--------
- Prefer redis.asyncio when available (redis-py >= 4.x).
- Fallback to sync redis client executed via asyncio.to_thread.
"""

from __future__ import annotations

import asyncio
from utils.task_manager import safe_create_task

import json
import os
import random
import time
from typing import Any, Optional

from .cache import NewsRecoCache, now_ms
from .metrics import build_metrics


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


class NewsRecoReader:
    def __init__(
        self
        *
        redis_url: str
        map_key: str
        poll_ms: int
        jitter_ms: int
        stale_fail_open_ms: int
        max_symbols: int
    ) -> None:
        self._redis_url = redis_url
        self._map_key = map_key
        self._poll_ms = max(50, int(poll_ms))
        self._jitter_ms = max(0, int(jitter_ms))
        self._stale_fail_open_ms = max(0, int(stale_fail_open_ms))
        self.cache = NewsRecoCache(max_symbols=max_symbols)

        self._task: Optional[asyncio.Task[None]] = None
        self._stop_evt = asyncio.Event()
        self._last_ok_ms: int = 0
        self._start_ms: int = now_ms()

        self.metrics = build_metrics()

        self._redis_async = None
        self._redis_sync = None

    @property
    def started(self) -> bool:
        return self._task is not None and not self._task.done()

    def get(self, symbol: str) -> Optional[dict]:
        snap = self.cache.get(symbol)
        if snap is None:
            self.metrics.miss_total.inc()
            return None
        self.metrics.hits_total.inc()
        return snap.payload

    async def start(self) -> None:
        if self.started:
            return
        self._stop_evt.clear()
        self._task = safe_create_task(self._run(), name="news_reco_reader")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_evt.set()
        self._task.cancel()
        try:
            await self._task
        except Exception:
            pass
        self._task = None

        # Close redis clients if any.
        try:
            if self._redis_async is not None:
                await self._redis_async.close()
        except Exception:
            pass
        try:
            if self._redis_sync is not None:
                self._redis_sync.close()
        except Exception:
            pass

    async def _init_redis(self) -> None:
        if self._redis_async is not None or self._redis_sync is not None:
            return

        connect_timeout_ms = int(os.getenv("TRADE_NEWS_RECO_REDIS_CONNECT_TIMEOUT_MS", "200"))
        socket_timeout_ms = int(os.getenv("TRADE_NEWS_RECO_REDIS_SOCKET_TIMEOUT_MS", "50"))
        max_conn = int(os.getenv("TRADE_NEWS_RECO_REDIS_MAX_CONNECTIONS", "20"))

        # Defensive clamps
        connect_timeout_s = max(0.01, min(connect_timeout_ms / 1000.0, 2.0))
        socket_timeout_s = max(0.005, min(socket_timeout_ms / 1000.0, 2.0))
        max_conn = max(2, min(max_conn, 200))

        # Preferred: redis.asyncio
        try:
            import redis.asyncio as redis_async  # type: ignore

            self._redis_async = redis_async.from_url(
                self._redis_url
                socket_connect_timeout=connect_timeout_s
                socket_timeout=socket_timeout_s
                max_connections=max_conn
                decode_responses=True
                retry_on_timeout=False
            )
            return
        except Exception:
            self._redis_async = None

        # Fallback: sync redis in a thread (still non-blocking for event-loop)
        try:
            import redis  # type: ignore

            self._redis_sync = redis.from_url(
                self._redis_url
                socket_connect_timeout=connect_timeout_s
                socket_timeout=socket_timeout_s
                decode_responses=True
                retry_on_timeout=False
            )
        except Exception as exc:
            raise RuntimeError(f"redis client init failed: {exc}") from exc

    async def _redis_get(self, key: str) -> Optional[str]:
        await self._init_redis()
        if self._redis_async is not None:
            return await self._redis_async.get(key)
        assert self._redis_sync is not None
        return await asyncio.to_thread(self._redis_sync.get, key)

    async def _run(self) -> None:
        # Startup jitter to avoid thundering herd on restarts.
        if self._jitter_ms > 0:
            await asyncio.sleep(random.random() * (self._jitter_ms / 1000.0))

        while not self._stop_evt.is_set():
            t0 = now_ms()
            # Update staleness metric regardless of success/fail
            reference_ms = self._last_ok_ms if self._last_ok_ms > 0 else self._start_ms
            self.metrics.stale_seconds.set(float(max(0, t0 - reference_ms) / 1000.0))

            try:
                raw = await self._redis_get(self._map_key)
                if raw:
                    updated, invalid, expired = self.cache.update_from_map_json(raw, now=t0)
                    self.metrics.update_total.inc()
                    self.metrics.symbols.set(self.cache.size)
                    self._last_ok_ms = t0
                    self.metrics.last_ok_ts_ms.set(float(t0))

                    # Lag metric (now - map.ts_ms) if present
                    try:
                        obj = json.loads(raw)
                        ts_ms = int(obj.get("ts_ms", 0))
                        if ts_ms > 0:
                            self.metrics.lag_ms.set(float(max(0, t0 - ts_ms)))
                    except Exception:
                        pass

                else:
                    # No value: key absent in Redis — this is a cache miss, not a Redis error.
                    # The news_agent may not have populated trade:cache:news_reco_map yet.
                    self.metrics.miss_total.inc()

                # Fail-open stale guard
                if self._stale_fail_open_ms > 0 and self._last_ok_ms > 0:
                    if (t0 - self._last_ok_ms) > self._stale_fail_open_ms:
                        dropped = self.cache.sweep_expired(now=t0)
                        # Additionally clear everything, to avoid tighten on stale data.
                        self.cache._by_symbol.clear()  # noqa: SLF001  (internal clear)
                        self.metrics.stale_total.inc()
                        self.metrics.symbols.set(0)

            except asyncio.CancelledError:
                raise
            except ValueError:
                self.metrics.parse_errors_total.inc()
            except Exception:
                self.metrics.redis_errors_total.inc()

            # Next poll
            sleep_ms = self._poll_ms
            if self._jitter_ms > 0:
                sleep_ms += int(random.random() * self._jitter_ms)
            await asyncio.sleep(sleep_ms / 1000.0)


# -------------------------------------------------------------------------
# Singleton helpers (drop-in integration for trade core)
# -------------------------------------------------------------------------

_SINGLETON: Optional[NewsRecoReader] = None
_SINGLETON_LOCK = asyncio.Lock()


def get_reco(symbol: str) -> Optional[dict]:
    """Hot-path getter: returns reco payload or None (fail-open)."""
    if _SINGLETON is None:
        return None
    return _SINGLETON.get(symbol)


async def ensure_started() -> Optional[NewsRecoReader]:
    """Ensure the background reader is running (idempotent).

    Controlled by:
        TRADE_NEWS_RECO_READER_ENABLE=1

    If disabled, returns None and does nothing.
    """
    if not _env_bool("TRADE_NEWS_RECO_READER_ENABLE", "0"):
        return None

    global _SINGLETON
    if _SINGLETON is not None and _SINGLETON.started:
        return _SINGLETON

    async with _SINGLETON_LOCK:
        if _SINGLETON is not None and _SINGLETON.started:
            return _SINGLETON

        redis_url = os.getenv("TRADE_NEWS_RECO_REDIS_URL") or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        map_key = os.getenv("TRADE_NEWS_RECO_MAP_KEY", "trade:cache:news_reco_map")
        poll_ms = int(os.getenv("TRADE_NEWS_RECO_POLL_MS", "250"))
        jitter_ms = int(os.getenv("TRADE_NEWS_RECO_POLL_JITTER_MS", "50"))
        stale_ms = int(os.getenv("TRADE_NEWS_RECO_STALE_FAIL_OPEN_MS", "5000"))
        max_symbols = int(os.getenv("TRADE_NEWS_RECO_MAX_SYMBOLS", "2000"))

        _SINGLETON = NewsRecoReader(
            redis_url=redis_url
            map_key=map_key
            poll_ms=poll_ms
            jitter_ms=jitter_ms
            stale_fail_open_ms=stale_ms
            max_symbols=max_symbols
        )
        await _SINGLETON.start()
        return _SINGLETON


async def shutdown() -> None:
    global _SINGLETON
    if _SINGLETON is None:
        return
    try:
        await _SINGLETON.stop()
    finally:
        _SINGLETON = None
