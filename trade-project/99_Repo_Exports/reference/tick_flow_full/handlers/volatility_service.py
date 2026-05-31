# volatility_service.py
"""
Volatility and ATR calculation functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Optional, Dict, Any, Tuple
import time

# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)


class VolatilityService:
    """
    Service for ATR and volatility calculations.
    """

    def __init__(
        self,
        redis_client: Any,
        symbol: str,
        *,
        atr_cache_ttl_ms: int = 2000,
        atr_estimate_ratio: float = 0.0003,
    ):
        self.redis = redis_client
        self.symbol = symbol
        self.logger = setup_logger(f"VolatilityService:{symbol}")
        self._atr_cache_ttl_ms = int(atr_cache_ttl_ms)
        self._atr_estimate_ratio = float(atr_estimate_ratio)
        # cache: tf -> (atr_value, loaded_ts_ms)
        self._atr_cache: Dict[str, Tuple[float, int]] = {}

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _to_float(self, raw: Any) -> Optional[float]:
        if raw is None:
            return None
        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="ignore")
            v = float(raw)
            if v > 0.0 and v != float("inf"):
                return v
        except Exception:
            return None
        return None

    def _normalize_timeframe(self, tf: str) -> str:
        """Normalize timeframe string."""
        tf = tf.lower().strip()

        # Handle common variations
        if tf in ['1m', '1min', '1minute', '60s', '60sec']:
            return '1m'
        elif tf in ['5m', '5min', '5minute', '300s', '300sec']:
            return '5m'
        elif tf in ['15m', '15min', '15minute', '900s']:
            return '15m'
        elif tf in ['1h', '1hour', '60m', '3600s']:
            return '1h'
        elif tf in ['4h', '4hour', '240m']:
            return '4h'
        elif tf in ['1d', '1day', '24h', '1440m']:
            return '1d'

        # Return as-is if already normalized
        return tf

    def _timeframe_to_ms(self, tf: str) -> int:
        """Convert timeframe to milliseconds."""
        tf = self._normalize_timeframe(tf)

        multipliers = {
            '1m': 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000,
        }

        return multipliers.get(tf, 60 * 1000)  # Default to 1m

    def _cache_ttl_ms(self, tf: str) -> int:
        """Get cache TTL in milliseconds for timeframe."""
        return {
            "1m": 1500,   # 1-2 sec
            "5m": 4000,   # 2-5 sec
            "15m": 8000,
            "1h": 20000,  # 10-30 sec
            "4h": 30000,
            "1d": 60000,
        }.get(tf, 2000)

    def _max_stale_ms(self, tf: str) -> int:
        """Get maximum staleness in milliseconds for ATR data."""
        return {
            "1m": 5 * 60_000,     # 5 minutes
            "5m": 30 * 60_000,    # 30 minutes
            "15m": 90 * 60_000,
            "1h": 6 * 3600_000,   # 6 hours
            "4h": 24 * 3600_000,
            "1d": 7 * 24 * 3600_000,
        }.get(tf, 30 * 60_000)

    def _load_atr_hash(self, tf: str) -> Optional[float]:
        """Load ATR from hash Redis store with staleness check."""
        if not self.redis:
            return None

        key = f"atrh:{self.symbol}:{tf}"
        try:
            d = self.redis.hgetall(key)
            if not d:
                return None

            # Redis may return bytes keys/values
            v_raw = d.get(b"v") or d.get("v")
            ts_raw = d.get(b"ts") or d.get("ts")
            v = self._to_float(v_raw)
            ts = self._to_float(ts_raw)

            if v is None or ts is None:
                return None

            now_ms = self._now_ms()
            if (now_ms - int(ts)) > self._max_stale_ms(tf):
                return None

            return float(v)
        except Exception as e:
            self.logger.warning(f"Failed to load ATR hash from Redis: {e}")
            return None

    def _estimate_atr(self, price: float) -> float:
        """Estimate ATR when no historical data available."""
        # Simple estimation based on price level
        return float(price) * self._atr_estimate_ratio

    def _load_tracker_atr_from_redis(self, timeframe: str, current_ts: int) -> Optional[float]:
        """Load ATR from Redis with hash-first approach and staleness check."""
        tf = self._normalize_timeframe(timeframe)
        now_ms = self._now_ms()

        # 1) Check cache first
        cached = self._atr_cache.get(tf)
        if cached:
            v, loaded_ms = cached
            if (now_ms - loaded_ms) <= self._cache_ttl_ms(tf) and v > 0.0:
                return float(v)

        # 2) Load from hash (atrh:...) - new source of truth
        v = self._load_atr_hash(tf)
        if v is not None and v > 0:
            self._atr_cache[tf] = (float(v), now_ms)
            return float(v)

        # 3) Legacy string fallback (atr:...)
        try:
            if not self.redis:
                return None
            key = f"atr:{self.symbol}:{tf}"
            raw = self.redis.get(key)
            v2 = self._to_float(raw)
            if v2 is not None:
                self._atr_cache[tf] = (float(v2), now_ms)
                return float(v2)
        except Exception as e:
            self.logger.warning(f"Failed to load ATR legacy from Redis: {e}")

        return None

    def _load_tracker_atr_from_redis_with_ts(self, timeframe: str, current_ts: int) -> Tuple[Optional[float], Optional[int]]:
        """
        Load ATR value AND its timestamp from Redis.

        Redis hash ATR:{symbol}:{timeframe} stores:
          - atr (value)
          - lastCloseTime (ms)  <-- this is what we use as atr_ts_ms

        Returns:
          (atr_value, atr_ts_ms)
        """
        if not timeframe:
            return None, None
        key = f"ATR:{self.symbol}:{timeframe}"
        try:
            atr_str, last_close_str = self.redis.hmget(key, "atr", "lastCloseTime")
            atr_v = float(atr_str) if atr_str is not None and str(atr_str) != "" else None
            ts_v = int(float(last_close_str)) if last_close_str is not None and str(last_close_str) != "" else None
            if atr_v is not None and (not math.isfinite(atr_v) or atr_v <= 0):
                atr_v = None
            return atr_v, ts_v
        except Exception:
            return None, None

    def _load_legacy_atr_from_redis(self) -> Optional[float]:
        """Load legacy ATR from Redis."""
        try:
            if not self.redis:
                return None
            # Try different legacy keys
            keys = [
                f"atr:{self.symbol}:5m",
                f"atr:{self.symbol}:1m",
                f"atr:{self.symbol}",
            ]

            for key in keys:
                raw = self.redis.get(key)
                v = self._to_float(raw)
                if v is not None:
                    return v

        except Exception as e:
            self.logger.warning(f"Failed to load legacy ATR: {e}")

        return None

    def _get_atr_for_timeframe(self, price: float, ts: int, timeframe: str) -> float:
        """Get ATR for specific timeframe."""
        tf = self._normalize_timeframe(timeframe)
        now_ms = self._now_ms()

        # 1) Fast path: local cache
        cached = self._atr_cache.get(tf)
        if cached:
            v, loaded_ms = cached
            if (now_ms - loaded_ms) <= self._atr_cache_ttl_ms and v > 0.0:
                return float(v)

        # 2) Load from Redis (tracker key)
        atr = self._load_tracker_atr_from_redis(tf, ts)

        if atr is not None and atr > 0:
            self._atr_cache[tf] = (float(atr), now_ms)
            return atr

        # 3) Fallback to legacy ATR
        atr = self._load_legacy_atr_from_redis()

        if atr is not None and atr > 0:
            self._atr_cache[tf] = (float(atr), now_ms)
            return atr

        # 4) Final fallback to estimation (cache too, but short-lived)
        est = self._estimate_atr(price)
        self._atr_cache[tf] = (float(est), now_ms)
        return est

    def _get_atr(self, price: float, ts: int) -> float:
        """Get ATR for default timeframe (5m)."""
        return self._get_atr_for_timeframe(price, ts, "5m")

    # Optional explicit public wrapper (if you want to stop calling underscore methods from handlers)
    def get_atr(self, price: float, ts: int, timeframe: str = "5m") -> float:
        return self._get_atr_for_timeframe(price, ts, timeframe)
