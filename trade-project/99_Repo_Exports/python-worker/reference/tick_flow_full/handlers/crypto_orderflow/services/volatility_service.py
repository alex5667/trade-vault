from __future__ import annotations

import math
from typing import Any


class VolatilityService:
    # NOTE: this is a patch template; apply to your real file that already contains
    # _load_tracker_atr_from_redis() and reads ATR:{symbol}:{timeframe} hash.

    def __init__(self, redis: Any):
        self.redis = redis

    def _load_tracker_atr_from_redis_with_ts(self, key: str) -> tuple[float | None, int | None]:
        """
        Load ATR value AND its timestamp from Redis.

        Redis hash ATR:{symbol}:{timeframe} stores:
          - atr
          - lastCloseTime (ms)  <-- this is what we use as atr_ts_ms

        Returns:
          (atr_value, atr_ts_ms)
        """
        try:
            atr_str, last_close_str = self.redis.hmget(key, "atr", "lastCloseTime")
            atr_v = float(atr_str) if atr_str is not None and str(atr_str) != "" else None
            ts_v = int(float(last_close_str)) if last_close_str is not None and str(last_close_str) != "" else None
            if atr_v is not None and (not math.isfinite(atr_v) or atr_v <= 0):
                atr_v = None
            return atr_v, ts_v
        except Exception:
            return None, None

    def _load_tracker_atr_from_redis(self, key: str) -> float | None:
        """
        Backward-compatible old method (returns only ATR).
        Prefer _load_tracker_atr_from_redis_with_ts() in code that builds ctx.
        """
        atr_v, _ = self._load_tracker_atr_from_redis_with_ts(key)
        return atr_v
