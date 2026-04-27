# geometry/htf_levels.py

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol

from common.log import setup_logger
from .structures import Level, LevelType, GeometrySnapshot

# Pre-computed millisecond durations for validity calculations
_MS_PER_DAY: int = 24 * 60 * 60 * 1000
_MS_PER_WEEK: int = 7 * _MS_PER_DAY


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
            "max_levels_per_symbol": 100,
            "level_validity_days": 30,
            "session_definitions": {
                "asia": {"start_hour": 0, "end_hour": 8},
                "europe": {"start_hour": 8, "end_hour": 16},
                "us": {"start_hour": 16, "end_hour": 24},
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
        """Check if bar closes a daily candle (UTC)."""
        ts = getattr(bar, 'ts_event_ms', 0)
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        return dt.hour == 23 and dt.minute >= 55  # Close to UTC market close

    def _is_weekly_close(self, bar: Any) -> bool:
        """Check if bar closes a weekly candle (UTC, Friday)."""
        ts = getattr(bar, 'ts_event_ms', 0)
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        return dt.weekday() == 4 and dt.hour == 23 and dt.minute >= 55  # Friday UTC close

    def _add_daily_levels(self, symbol: str, bar: Any) -> None:
        """Add daily high/low levels."""
        ts = getattr(bar, 'ts_event_ms', 0)
        ts_valid = ts + self._cfg["level_validity_days"] * _MS_PER_DAY

        self._levels_by_symbol[symbol].extend([
            Level(symbol, LevelType.DAILY_HIGH, getattr(bar, 'high', 0), ts, ts_valid, 0.8),
            Level(symbol, LevelType.DAILY_LOW, getattr(bar, 'low', 0), ts, ts_valid, 0.8),
        ])
        self._limit_levels_count(symbol)

    def _add_weekly_levels(self, symbol: str, bar: Any) -> None:
        """Add weekly high/low levels."""
        ts = getattr(bar, 'ts_event_ms', 0)
        ts_valid = ts + self._cfg["level_validity_days"] * _MS_PER_WEEK

        self._levels_by_symbol[symbol].extend([
            Level(symbol, LevelType.WEEKLY_HIGH, getattr(bar, 'high', 0), ts, ts_valid, 0.9),
            Level(symbol, LevelType.WEEKLY_LOW, getattr(bar, 'low', 0), ts, ts_valid, 0.9),
        ])
        self._limit_levels_count(symbol)

    def _update_session_data(self, symbol: str, bar: Any) -> None:
        """Update session high/low/open data (UTC hours)."""
        ts = getattr(bar, 'ts_event_ms', 0)
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        current_session = self._get_session_name(dt.hour)

        session_key = f"{symbol}_{current_session}"
        session_info = self._session_data[session_key]

        if not session_info or 'open' not in session_info:
            session_info['open'] = getattr(bar, 'open', 0)
            session_info['start_ts'] = ts

        bar_high = getattr(bar, 'high', 0)
        bar_low = getattr(bar, 'low', 0)
        session_info['high'] = max(session_info.get('high', bar_high), bar_high)
        session_info['low'] = min(session_info.get('low', bar_low), bar_low)
        session_info['last_update'] = ts

    def _get_session_name(self, hour: int) -> str:
        """Get session name based on UTC hour."""
        if hour < 8:
            return "asia"
        if hour < 16:
            return "europe"
        return "us"

    def _cleanup_expired_levels(self, symbol: str) -> None:
        """Remove expired levels."""
        import time
        current_ts = int(time.time() * 1000)
        self._levels_by_symbol[symbol] = [
            level for level in self._levels_by_symbol[symbol]
            if level.ts_valid_until_ms is None or level.ts_valid_until_ms > current_ts
        ]

    def _limit_levels_count(self, symbol: str) -> None:
        """Limit number of levels per symbol, keeping most recent."""
        levels = self._levels_by_symbol[symbol]
        max_lvl = self._cfg["max_levels_per_symbol"]
        if len(levels) > max_lvl:
            levels.sort(key=lambda lv: lv.ts_created_ms, reverse=True)
            self._levels_by_symbol[symbol] = levels[:max_lvl]

    def _get_provider_levels(self, symbol: str, ts_event_ms: int) -> List[Level]:
        """Fetch and convert HTF levels from external provider (if configured)."""
        if self._htf_provider is None:
            return []
        htf = self._htf_provider.get_levels(symbol)
        if not htf:
            return []
        return self._convert_htf_levels(htf, symbol, ts_event_ms)

    def get_geometry(self, symbol: str, ts_event_ms: int, price: float) -> GeometrySnapshot:
        """
        Calculate geometry for current price:
        - nearest level above/below
        - distances in basis points
        - additional geometry features
        """
        levels: List[Level] = list(self._levels_by_symbol.get(symbol, []))
        levels.extend(self._get_provider_levels(symbol, ts_event_ms))

        # Find nearest levels above and below
        above_levels = [lv for lv in levels if lv.price > price]
        below_levels = [lv for lv in levels if lv.price < price]

        nearest_above = min(above_levels, key=lambda lv: lv.price - price) if above_levels else None
        nearest_below = max(below_levels, key=lambda lv: price - lv.price) if below_levels else None

        # Calculate distances in basis points
        distance_above: Optional[float] = None
        distance_below: Optional[float] = None

        if nearest_above:
            distance_above = ((nearest_above.price - price) / price) * 10_000.0

        if nearest_below:
            distance_below = ((price - nearest_below.price) / price) * 10_000.0

        distance_to_nearest = min(
            (d for d in [distance_above, distance_below] if d is not None),
            default=None,
        )

        # Get session info (UTC)
        dt = datetime.fromtimestamp(ts_event_ms / 1000, tz=timezone.utc)
        current_session = self._get_session_name(dt.hour)
        session_key = f"{symbol}_{current_session}"
        session_info = self._session_data.get(session_key, {})

        return GeometrySnapshot(
            symbol=symbol,
            ts_event_ms=ts_event_ms,
            levels=levels,
            nearest_level_above=nearest_above,
            nearest_level_below=nearest_below,
            distance_to_nearest_level_bp=distance_to_nearest,
            nearest_resistance_bp=distance_above,
            nearest_support_bp=distance_below,
            levels_above_count=len(above_levels),
            levels_below_count=len(below_levels),
            current_session=current_session,
            session_open_price=session_info.get('open'),
            session_high_price=session_info.get('high'),
            session_low_price=session_info.get('low'),
        )

    def _convert_htf_levels(self, htf_levels: Any, symbol: str, ts_event_ms: int) -> List[Level]:
        """Convert HTFLevels object to our Level format."""
        ts_valid_daily = ts_event_ms + self._cfg["level_validity_days"] * _MS_PER_DAY
        ts_valid_weekly = ts_event_ms + self._cfg["level_validity_days"] * _MS_PER_WEEK

        return [
            # Daily levels
            Level(symbol, LevelType.DAILY_HIGH, getattr(htf_levels, 'pdh', 0), ts_event_ms, ts_valid_daily, 0.7),
            Level(symbol, LevelType.DAILY_LOW, getattr(htf_levels, 'pdl', 0), ts_event_ms, ts_valid_daily, 0.7),
            # Weekly levels
            Level(symbol, LevelType.WEEKLY_HIGH, getattr(htf_levels, 'week_hi', 0), ts_event_ms, ts_valid_weekly, 0.9),
            Level(symbol, LevelType.WEEKLY_LOW, getattr(htf_levels, 'week_lo', 0), ts_event_ms, ts_valid_weekly, 0.9),
            # Session opens (mismatched level types preserved for back-compat)
            Level(symbol, LevelType.SESSION_HIGH, getattr(htf_levels, 'asia_open', 0), ts_event_ms, ts_valid_daily, 0.5),
            Level(symbol, LevelType.SESSION_LOW, getattr(htf_levels, 'europe_open', 0), ts_event_ms, ts_valid_daily, 0.6),
        ]

    def get_levels(self, symbol: str, ts_event_ms: int = 0) -> List[Level]:
        """Get all active levels for symbol (local + from provider)."""
        levels: List[Level] = list(self._levels_by_symbol.get(symbol, []))
        levels.extend(self._get_provider_levels(symbol, ts_event_ms))
        return levels
