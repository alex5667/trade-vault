# geometry/structures.py

from dataclasses import dataclass
from enum import Enum


class LevelType(Enum):
    DAILY_HIGH = "daily_high"
    DAILY_LOW = "daily_low"
    WEEKLY_HIGH = "weekly_high"
    WEEKLY_LOW = "weekly_low"
    ORDER_BLOCK = "order_block"
    FVG = "fvg"
    LIQUIDITY = "liquidity"
    SESSION_HIGH = "session_high"
    SESSION_LOW = "session_low"
    PIVOT_POINT = "pivot_point"
    RESISTANCE = "resistance"
    SUPPORT = "support"


@dataclass
class Level:
    symbol: str
    level_type: LevelType
    price: float
    ts_created_ms: int
    ts_valid_until_ms: int | None = None
    strength: float = 1.0  # Level strength/confidence 0-1
    metadata: dict | None = None  # Additional data (session, etc.)


@dataclass
class GeometrySnapshot:
    symbol: str
    ts_event_ms: int
    levels: list[Level]
    nearest_level_above: Level | None = None
    nearest_level_below: Level | None = None
    distance_to_nearest_level_bp: float | None = None
    # Additional geometry features
    nearest_resistance_bp: float | None = None
    nearest_support_bp: float | None = None
    levels_above_count: int = 0
    levels_below_count: int = 0
    # Session info
    current_session: str | None = None
    session_open_price: float | None = None
    session_high_price: float | None = None
    session_low_price: float | None = None
