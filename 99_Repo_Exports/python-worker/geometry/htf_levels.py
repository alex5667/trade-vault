# geometry/htf_levels.py

from __future__ import annotations

from typing import Dict, List, Optional, Any, Protocol
from collections import defaultdict
from collections import defaultdict
from datetime import datetime

from common.log import setup_logger
from .structures import Level, LevelType, GeometrySnapshot


class HTFLevelsProvider(Protocol):
    """Interface for HTF levels providers."""

    def get_levels(self, symbol: str) -> Optional[Any]:  # HTFLevels
        """Get HTF levels for symbol."""
        ...


class HTFLevelsService:
    """
    Service for managing Higher Time Frame levels and geometry.

    Handles:
    - Daily/weekly/monthly high/low levels
    - Session levels (Asia/Europe/US)
    - Order blocks, FVGs, liquidity pools
    - Distance calculations to nearest levels
    """

    def __init__(self, htf_provider: Optional[HTFLevelsProvider] = None, config: Optional[Dict[str, Any]] = None):
        self._htf_provider = htf_provider
        self._cfg = config or self._default_config()
        self._levels_by_symbol: Dict[str, List[Level]] = defaultdict(list)
        self._session_data: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.logger = setup_logger("HTFLevelsService")

    def _default_config(self) -> Dict[str, Any]:
        return {
            "max_levels_per_symbol": 100
            "level_validity_days": 30
            "session_definitions": {
                "asia": {"start_hour": 0, "end_hour": 8}
                "europe": {"start_hour": 8, "end_hour": 16}
                "us": {"start_hour": 16, "end_hour": 24}
            }
        }

    def on_bar(self, bar: Any) -> None:
        """
        Update levels when new bar arrives.
        If bar closes a day/week, fixate high/low/close as levels.
        Maintain list of active levels.
        """
        symbol = getattr(bar, 'symbol', None) or 'unknown'

        # Check if this bar closes a daily candle
        if self._is_daily_close(bar):
            self._add_daily_levels(symbol, bar)

        # Check if this bar closes a weekly candle
        if self._is_weekly_close(bar):
            self._add_weekly_levels(symbol, bar)

        # Update session data
        self._update_session_data(symbol, bar)

        # Clean up expired levels
        self._cleanup_expired_levels(symbol)

    def _is_daily_close(self, bar: Any) -> bool:
        """Check if bar closes a daily candle"""
        # This is a simplified check - in real implementation
        # you'd check against trading sessions or time boundaries
        ts = getattr(bar, 'ts_event_ms', 0)
        dt = datetime.fromtimestamp(ts / 1000)
        return dt.hour == 23 and dt.minute >= 55  # Close to market close

    def _is_weekly_close(self, bar: Any) -> bool:
        """Check if bar closes a weekly candle"""
        ts = getattr(bar, 'ts_event_ms', 0)
        dt = datetime.fromtimestamp(ts / 1000)
        return dt.weekday() == 4 and dt.hour == 23 and dt.minute >= 55  # Friday close

    def _add_daily_levels(self, symbol: str, bar: Any) -> None:
        """Add daily high/low/close levels"""
        ts = getattr(bar, 'ts_event_ms', 0)
        ts_valid = ts + (self._cfg["level_validity_days"] * 24 * 60 * 60 * 1000)

        levels = [
            Level(symbol, LevelType.DAILY_HIGH, getattr(bar, 'high', 0), ts, ts_valid, 0.8)
            Level(symbol, LevelType.DAILY_LOW, getattr(bar, 'low', 0), ts, ts_valid, 0.8)
        ]

        self._levels_by_symbol[symbol].extend(levels)
        self._limit_levels_count(symbol)

    def _add_weekly_levels(self, symbol: str, bar: Any) -> None:
        """Add weekly high/low/close levels"""
        ts = getattr(bar, 'ts_event_ms', 0)
        ts_valid = ts + (self._cfg["level_validity_days"] * 7 * 24 * 60 * 60 * 1000)

        levels = [
            Level(symbol, LevelType.WEEKLY_HIGH, getattr(bar, 'high', 0), ts, ts_valid, 0.9)
            Level(symbol, LevelType.WEEKLY_LOW, getattr(bar, 'low', 0), ts, ts_valid, 0.9)
        ]

        self._levels_by_symbol[symbol].extend(levels)
        self._limit_levels_count(symbol)

    def _update_session_data(self, symbol: str, bar: Any) -> None:
        """Update session high/low/open data"""
        ts = getattr(bar, 'ts_event_ms', 0)
        dt = datetime.fromtimestamp(ts / 1000)
        current_session = self._get_session_name(dt.hour)

        session_key = f"{symbol}_{current_session}"
        session_info = self._session_data[session_key]

        if not session_info or 'open' not in session_info:
            session_info['open'] = getattr(bar, 'open', 0)
            session_info['start_ts'] = ts

        session_info['high'] = max(session_info.get('high', getattr(bar, 'high', 0)), getattr(bar, 'high', 0))
        session_info['low'] = min(session_info.get('low', getattr(bar, 'low', 0)), getattr(bar, 'low', 0))
        session_info['last_update'] = ts

    def _get_session_name(self, hour: int) -> str:
        """Get session name based on hour"""
        if 0 <= hour < 8:
            return "asia"
        elif 8 <= hour < 16:
            return "europe"
        else:
            return "us"

    def _cleanup_expired_levels(self, symbol: str) -> None:
        """Remove expired levels"""
        current_ts = int(datetime.now().timestamp() * 1000)
        levels = self._levels_by_symbol[symbol]
        valid_levels = [
            level for level in levels
            if level.ts_valid_until_ms is None or level.ts_valid_until_ms > current_ts
        ]
        self._levels_by_symbol[symbol] = valid_levels

    def _limit_levels_count(self, symbol: str) -> None:
        """Limit number of levels per symbol"""
        levels = self._levels_by_symbol[symbol]
        if len(levels) > self._cfg["max_levels_per_symbol"]:
            # Keep most recent levels
            levels.sort(key=lambda l: l.ts_created_ms, reverse=True)
            self._levels_by_symbol[symbol] = levels[:self._cfg["max_levels_per_symbol"]]

    def get_geometry(self, symbol: str, ts_event_ms: int, price: float) -> GeometrySnapshot:
        """
        Calculate geometry for current price:
        - nearest level above/below
        - distances in basis points
        - additional geometry features
        """
        levels = self._levels_by_symbol.get(symbol, [])

        # Try to get HTF levels from provider if available
        htf_levels = None
        if self._htf_provider:
            htf_levels = self._htf_provider.get_levels(symbol)

        if htf_levels:
            # Convert HTF levels to our Level format
            levels.extend(self._convert_htf_levels(htf_levels, symbol, ts_event_ms))

        # Find nearest levels above and below
        above_levels = [l for l in levels if l.price > price]
        below_levels = [l for l in levels if l.price < price]

        nearest_above = min(above_levels, key=lambda l: l.price - price) if above_levels else None
        nearest_below = max(below_levels, key=lambda l: price - l.price) if below_levels else None

        # Calculate distances in basis points
        distance_above = None
        distance_below = None

        if nearest_above:
            distance_above = ((nearest_above.price - price) / price) * 10000

        if nearest_below:
            distance_below = ((price - nearest_below.price) / price) * 10000

        distance_to_nearest = min(
            (d for d in [distance_above, distance_below] if d is not None)
            default=None
        )

        # Get session info
        dt = datetime.fromtimestamp(ts_event_ms / 1000)
        current_session = self._get_session_name(dt.hour)
        session_key = f"{symbol}_{current_session}"
        session_info = self._session_data.get(session_key, {})

        return GeometrySnapshot(
            symbol=symbol
            ts_event_ms=ts_event_ms
            levels=levels
            nearest_level_above=nearest_above
            nearest_level_below=nearest_below
            distance_to_nearest_level_bp=distance_to_nearest
            nearest_resistance_bp=distance_above
            nearest_support_bp=distance_below
            levels_above_count=len(above_levels)
            levels_below_count=len(below_levels)
            current_session=current_session
            session_open_price=session_info.get('open')
            session_high_price=session_info.get('high')
            session_low_price=session_info.get('low')
        )

    def _convert_htf_levels(self, htf_levels: Any, symbol: str, ts_event_ms: int) -> List[Level]:
        """Convert HTFLevels to our Level format"""
        levels = []

        # Daily levels
        ts_valid = ts_event_ms + (self._cfg["level_validity_days"] * 24 * 60 * 60 * 1000)
        levels.extend([
            Level(symbol, LevelType.DAILY_HIGH, getattr(htf_levels, 'pdh', 0), ts_event_ms, ts_valid, 0.7)
            Level(symbol, LevelType.DAILY_LOW, getattr(htf_levels, 'pdl', 0), ts_event_ms, ts_valid, 0.7)
        ])

        # Weekly levels
        ts_valid_weekly = ts_event_ms + (self._cfg["level_validity_days"] * 7 * 24 * 60 * 60 * 1000)
        levels.extend([
            Level(symbol, LevelType.WEEKLY_HIGH, getattr(htf_levels, 'week_hi', 0), ts_event_ms, ts_valid_weekly, 0.9)
            Level(symbol, LevelType.WEEKLY_LOW, getattr(htf_levels, 'week_lo', 0), ts_event_ms, ts_valid_weekly, 0.9)
        ])

        # Session opens
        levels.extend([
            Level(symbol, LevelType.SESSION_HIGH, getattr(htf_levels, 'asia_open', 0), ts_event_ms, ts_valid, 0.5)
            Level(symbol, LevelType.SESSION_LOW, getattr(htf_levels, 'europe_open', 0), ts_event_ms, ts_valid, 0.6)
        ])

        return levels
    def get_levels(self, symbol: str, ts_event_ms: int = 0) -> List[Level]:
        """
        Get all active levels for symbol (from provider + local).
        """
        levels = list(self._levels_by_symbol.get(symbol, []))

        # Try to get HTF levels from provider if available
        htf_levels = None
        if self._htf_provider:
            htf_levels = self._htf_provider.get_levels(symbol)

        if htf_levels:
             # Convert HTF levels to our Level format
             levels.extend(self._convert_htf_levels(htf_levels, symbol, ts_event_ms))
        
        return levels
