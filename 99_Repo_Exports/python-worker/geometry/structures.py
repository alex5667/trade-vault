# geometry/structures.py

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


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
    ts_valid_until_ms: Optional[int] = None
    strength: float = 1.0  # Level strength/confidence 0-1
    metadata: Optional[dict] = None  # Additional data (session, etc.)


@dataclass
class GeometrySnapshot:
    symbol: str
    ts_event_ms: int
    levels: List[Level]
    nearest_level_above: Optional[Level] = None
    nearest_level_below: Optional[Level] = None
    distance_to_nearest_level_bp: Optional[float] = None
    # Additional geometry features
    nearest_resistance_bp: Optional[float] = None
    nearest_support_bp: Optional[float] = None
    levels_above_count: int = 0
    levels_below_count: int = 0
    # Session info
    current_session: Optional[str] = None
    session_open_price: Optional[float] = None
    session_high_price: Optional[float] = None
    session_low_price: Optional[float] = None
