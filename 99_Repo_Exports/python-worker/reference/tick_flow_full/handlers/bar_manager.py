# bar_manager.py
from __future__ import annotations
"""
Bar range and pivot management functionality extracted from base_orderflow_handler.py
"""


from typing import Optional, Dict, Any

# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)


class BarManager:
    """
    Timeframe utilities only.
    NOTE: 1m bars are built by BarBuilder1m in data_processor.py (single source of truth).
    NOTE: pivots are owned by CacheService (single source of truth).
    """

    def __init__(self, symbol: str, cache_service: Any):
        self.symbol = symbol
        self.cache_service = cache_service
        self.logger = setup_logger(f"BarManager:{symbol}")

        # kept only for API compatibility if someone still instantiates BarManager

    def _normalize_timeframe(self, tf: str) -> str:
        """
        Normalize timeframe string to standard format.
        """
        mapping = {
            "1m": "M1", "m1": "M1",
            "5m": "M5", "m5": "M5",
            "15m": "M15", "m15": "M15",
            "30m": "M30", "m30": "M30",
            "1h": "H1", "h1": "H1",
            "4h": "H4", "h4": "H4",
            "1d": "D1", "d1": "D1",
        }
        tf_normalized = (tf or "1m").strip().lower()
        return mapping.get(tf_normalized, (tf or "M1").strip().upper())

    def _timeframe_to_ms(self, tf: str) -> int:
        """
        Convert normalized timeframe to milliseconds.
        """
        mapping = {
            "M1": 60_000,      # 1 minute
            "M5": 300_000,     # 5 minutes
            "M15": 900_000,    # 15 minutes
            "M30": 1_800_000,  # 30 minutes
            "H1": 3_600_000,   # 1 hour
            "H4": 14_400_000,  # 4 hours
            "D1": 86_400_000,  # 1 day
        }
        return mapping.get(tf, 180_000)  # Default to 3 minutes

    def get_timeframe_ms(self, tf: str) -> int:
        """Public method to get timeframe in milliseconds."""
        normalized = self._normalize_timeframe(tf)
        return self._timeframe_to_ms(normalized)
