# atr_redis_publisher.py
from __future__ import annotations

"""
ATR Redis Publisher - publishes ATR values to Redis with hash schema and legacy compatibility.
"""
from typing import Any

from utils.time_utils import get_ny_time_millis


class AtrRedisPublisher:
    """
    Publishes ATR values into Redis in a forward-compatible way:
      - hash key:  atrh:{symbol}:{tf}  fields: v, ts
      - legacy key: atr:{symbol}:{tf} (string) for backward compatibility
    """

    def __init__(self, redis_client: Any, symbol: str) -> None:
        self.redis = redis_client
        self.symbol = symbol

    def _ttl_s(self, tf: str) -> int:
        tf = (tf or "").lower().strip()
        # store enough history to tolerate short outages and allow staleness checks
        return {
            "1m": 10 * 60,          # 10 minutes
            "5m": 2 * 60 * 60,      # 2 hours
            "15m": 6 * 60 * 60,     # 6 hours
            "1h": 24 * 60 * 60,     # 1 day
            "4h": 3 * 24 * 60 * 60, # 3 days
            "1d": 30 * 24 * 60 * 60,# 30 days
        }.get(tf, 2 * 60 * 60)

    def publish(self, tf: str, atr: float, ts_ms: int | None = None) -> None:
        if not self.redis:
            return
        try:
            atr_f = atr
        except Exception:
            return
        if atr_f <= 0.0:
            return

        tf_n = (tf or "").lower().strip()
        now_ms = get_ny_time_millis()
        ts_ms_i = int(ts_ms or now_ms)

        ttl_s = self._ttl_s(tf_n)

        hash_key = f"atrh:{self.symbol}:{tf_n}"
        legacy_key = f"atr:{self.symbol}:{tf_n}"

        pipe = self.redis.pipeline()
        # New schema (hash with timestamp)
        pipe.hset(hash_key, mapping={"v": f"{atr_f:.10f}", "ts": str(ts_ms_i)})
        pipe.expire(hash_key, ttl_s)
        # Legacy schema (string)
        pipe.set(legacy_key, f"{atr_f:.10f}", ex=ttl_s)
        pipe.execute()
