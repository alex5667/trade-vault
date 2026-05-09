import json
import logging
import os
from typing import Any

from services.orderflow.metrics import log_silent_error
from utils.atr_cache import ATRCache

logger = logging.getLogger("of_market_state")

class MarketStateService:
    def __init__(self, redis_client, atr_cache: ATRCache):
        self.redis = redis_client
        self.atr_cache = atr_cache

        # Caches
        self._rq_cache: dict[str, tuple[int, dict[str, Any]]] = {}
        self._adx_cache: dict[str, tuple[int, float]] = {}

    async def get_regime_quantiles(self, symbol: str, tf: str, now_ms: int) -> dict[str, Any] | None:
        """
        Read regime quantiles JSON from Redis:
          key = regime:q:{SYMBOL}:{tf}
        Cache in-memory for rq_cache_ms (default 60000ms).
        Fail-open: returns None.
        """
        sym = (symbol or "").upper()
        if not sym:
            return None

        tf = (tf or "1m")
        cache_ms = int(os.getenv("RQ_CACHE_MS", "60000"))
        key = f"{sym}:{tf}"

        # 1. Check in-memory cache
        cur = self._rq_cache.get(key)
        if cur is not None:
            ts0, d0 = cur
            if 0 <= now_ms - int(ts0) <= cache_ms:
                return d0

        # 2. Fetch from Redis
        try:
            raw = await self.redis.get(f"regime:q:{sym}:{tf}")
            if not raw:
                return None
            d = json.loads(raw)
            if not isinstance(d, dict):
                return None
            self._rq_cache[key] = (now_ms, d)
            return d
        except Exception as exc:
            log_silent_error(exc, 'redis_read_failure', sym, 'market_state.get_regime_quantiles')
            return None

    async def get_adx(self, symbol: str, now_ms: int) -> float:
        """
        Read ADX14 from Redis:
          key = adx:{SYMBOL}
        Cache in-memory for adx_cache_ms (default 300ms).
        Fail-open: returns 0.0.
        """
        sym = (symbol or "").upper()
        if not sym:
            return 0.0

        cache_ms = int(os.getenv("ADX_CACHE_MS", "300"))

        # 1. Check in-memory cache
        cur = self._adx_cache.get(sym)
        if cur is not None:
            ts0, v0 = cur
            if 0 <= now_ms - int(ts0) <= cache_ms:
                return float(v0 or 0.0)

        # 2. Fetch from Redis
        try:
            raw = await self.redis.get(f"adx:{sym}")
            v = float(raw) if raw is not None else 0.0
            if v < 0:
                v = 0.0
            self._adx_cache[sym] = (now_ms, float(v))
            return float(v)
        except Exception as exc:
            log_silent_error(exc, 'redis_read_failure', sym, 'market_state.get_adx')
            # Fallback to stale cache if available
            return float(self._adx_cache.get(sym, (0, 0.0))[1] or 0.0)

    def get_atr(self, symbol: str, tf: str) -> float:
        """
        Unified ATR retrieval using underlying ATRCache.
        Fail-open: returns 0.0 on error or miss.
        """
        try:
            return float(self.atr_cache.get(symbol, tf) or 0.0)
        except Exception:
            return 0.0

    def get_atr_with_meta(self, symbol: str, tf: str, now_ms: int = 0, prefer_src: str = "") -> tuple[float, Any]:
        """
        Retrieve ATR with full metadata (age, source, consistency).
        """
        try:
            return self.atr_cache.get_with_meta(
                symbol=symbol,
                timeframe=tf,
                now_ms=(now_ms if now_ms > 0 else None),
                prefer_src=prefer_src
            )
        except Exception:
            return 0.0, None
