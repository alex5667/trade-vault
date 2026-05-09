from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from core.redis_client import get_redis
from services.persistence_manager import get_persistence_manager
from utils.task_manager import safe_create_task

logger = logging.getLogger("daily_tracker")

class DailyCandleTracker:
    """
    Tracks daily High/Low/Close for a specific symbol in memory.
    Detects UTC day changes and persists completed candles to Postgres + Redis.
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.current_date = self._get_utc_date_str()

        # In-memory state for the current day
        self.day_high = -1.0
        self.day_low = -1.0
        self.day_close = -1.0
        self.day_open = -1.0
        self.day_vol = 0.0

        # Dependencies
        self.pm = get_persistence_manager()
        self.redis = get_redis()

        # Redis key for fallback/init
        self.redis_key_daily = f"daily_ohlc:{self.symbol}"

    def _get_utc_date_str(self) -> str:
        return datetime.now(UTC).strftime("%Y-%m-%d")

    def update(self, bar: Any) -> None:
        """
        Update tracker with a closed microbar.
        """
        try:
            # Check for day switch
            today = self._get_utc_date_str()
            if today != self.current_date:
                # Day rolled over -> persist previous day
                safe_create_task(self._persist_completed_day(self.current_date))

                # Reset state for new day
                self.current_date = today
                self.day_high = -1.0
                self.day_low = -1.0
                self.day_open = -1.0
                self.day_vol = 0.0

            # Parse bar data
            h = float(getattr(bar, "high", 0.0))
            l = float(getattr(bar, "low", 0.0))
            c = float(getattr(bar, "close", 0.0))
            o = float(getattr(bar, "open", 0.0))
            v = float(getattr(bar, "vol", 0.0))

            if c <= 0:
                return

            # Init if needed
            if self.day_high < 0:
                self.day_high = h
                self.day_low = l
                self.day_open = o
                self.day_close = c
                self.day_vol = v
            else:
                self.day_high = max(self.day_high, h)
                self.day_low = min(self.day_low, l)
                self.day_close = c
                self.day_vol += v

        except Exception as e:
            logger.warning(f"Error updating daily tracker for {self.symbol}: {e}")

    async def _persist_completed_day(self, date_str: str):
        """
        Persist the completed day's OHLC to PG and Redis (yesterday_hlc).
        """
        if self.day_high <= 0:
            return

        h, l, c, o, v = self.day_high, self.day_low, self.day_close, self.day_open, self.day_vol

        try:
            # 1. Save to Postgres
            ok = await self.pm.save_daily_ohlc(
                self.symbol, date_str, o, h, l, c, v
            )
            if ok:
                logger.info(f"📅 Persisted daily OHLC for {self.symbol} ({date_str}): H={h:.2f} L={l:.2f} C={c:.2f}")

            # 2. Update Redis 'yesterday_hlc' for Pivot calculations
            import json
            payload = {
                "date": date_str,
                "high": h,
                "low": l,
                "close": c,
                "open": o,
                "volume": v
            }
            # key used by cache_service.py
            key = f"yesterday_hlc:{self.symbol}"
            self.redis.setex(key, 172800, json.dumps(payload))

        except Exception as e:
            logger.error(f"❌ Failed to persist daily OHLC for {self.symbol}: {e}")
