from __future__ import annotations
import hashlib
from typing import Union, Optional, Dict, Any
from dataclasses import dataclass
from common.enums.trading import Direction, Side


def _ev(x: Any) -> str:
    """Extract string value from enum or plain string.

    str(Direction.LONG) → "Direction.LONG" in Python 3.12 (StrEnum quirk).
    Direction.LONG.value → "LONG" — always correct.
    """
    if hasattr(x, "value"):
        return str(x.value)
    return str(x) if x is not None else ""


@dataclass(frozen=True, slots=True)
class NormalizedSide:
    direction: Direction   # LONG | SHORT
    side: Side             # BUY | SELL
    side_int: int          # 1 | -1


_DIRECTION_LONG_ALIASES = frozenset(("LONG", "L", "1", "BUY"))
_DIRECTION_SHORT_ALIASES = frozenset(("SHORT", "S", "-1", "SELL"))
_SIDE_BUY_ALIASES = frozenset(("BUY", "B", "1", "LONG"))
_SIDE_SELL_ALIASES = frozenset(("SELL", "S", "-1", "SHORT"))


def normalize_direction(
    d: Union[str, Direction, None],
    *,
    default: Optional[Direction] = None,
) -> Direction:
    """Normalize direction to Direction.LONG|SHORT.

    Raises ValueError for unknown input unless *default* is given.
    Use normalize_direction_safe() for hot-paths that must never raise.
    """
    val = _ev(d).upper().strip()
    if val in _DIRECTION_LONG_ALIASES:
        return Direction.LONG
    if val in _DIRECTION_SHORT_ALIASES:
        return Direction.SHORT
    if default is not None:
        return default
    raise ValueError(f"unknown_direction:{d!r}")


def normalize_direction_safe(
    d: Union[str, Direction, None],
) -> Optional[Direction]:
    """Hot-path wrapper: returns None instead of raising for unknown input."""
    try:
        return normalize_direction(d)
    except ValueError:
        return None


def normalize_side(
    s: Union[str, Side, None],
    *,
    default: Optional[Side] = None,
) -> Side:
    """Normalize side to Side.BUY|SELL.

    Raises ValueError for unknown input unless *default* is given.
    """
    val = _ev(s).upper().strip()
    if val in _SIDE_BUY_ALIASES:
        return Side.BUY
    if val in _SIDE_SELL_ALIASES:
        return Side.SELL
    if default is not None:
        return default
    raise ValueError(f"unknown_side:{s!r}")


def normalize_side_safe(
    s: Union[str, Side, None],
) -> Optional[Side]:
    """Hot-path wrapper: returns None instead of raising for unknown input."""
    try:
        return normalize_side(s)
    except ValueError:
        return None


def normalize_side_3(value: Union[str, int, Direction, Side, None]) -> NormalizedSide:
    """Unified side/direction normalizer.

    Returns (Direction, Side, side_int). Raises ValueError for unknown input.
    """
    d = normalize_direction(value)
    s = Side.BUY if d == Direction.LONG else Side.SELL
    si = 1 if d == Direction.LONG else -1
    return NormalizedSide(direction=d, side=s, side_int=si)


def normalize_side_3_safe(
    value: Union[str, int, Direction, Side, None],
) -> Optional[NormalizedSide]:
    """Hot-path wrapper for normalize_side_3: returns None on unknown input."""
    try:
        return normalize_side_3(value)
    except ValueError:
        return None


def get_side_int(d_or_s: Union[str, Direction, Side, int, None]) -> int:
    """Return 1 for LONG/BUY, -1 for SHORT/SELL. Raises ValueError on unknown input."""
    if isinstance(d_or_s, int):
        if d_or_s > 0:
            return 1
        if d_or_s < 0:
            return -1
        raise ValueError(f"unknown_side_int:0")
    val = _ev(d_or_s).upper().strip()
    if val in _SIDE_BUY_ALIASES | _DIRECTION_LONG_ALIASES:
        return 1
    if val in _SIDE_SELL_ALIASES | _DIRECTION_SHORT_ALIASES:
        return -1
    raise ValueError(f"unknown_side_int:{d_or_s!r}")


def get_side_int_safe(d_or_s: Union[str, Direction, Side, int, None]) -> Optional[int]:
    """Hot-path wrapper for get_side_int: returns None on unknown input."""
    try:
        return get_side_int(d_or_s)
    except ValueError:
        return None


def direction_to_side(d: Direction) -> Side:
    return Side.BUY if d == Direction.LONG else Side.SELL


def side_to_direction(s: Side) -> Direction:
    return Direction.LONG if s == Side.BUY else Direction.SHORT


SIGNAL_ID_ALGO_V1 = "v1"


def generate_signal_id(
    symbol: str,
    ts_ms: int,
    direction: Union[str, Direction],
    kind: str = "crypto-of",
) -> str:
    """Unified signal_id algorithm (v1).

    Format: {kind}:{symbol}:{ts_ms}:{direction_short}
    Example: crypto-of:BTCUSDT:1714234567890:L
    """
    d_norm = normalize_direction(direction)
    d_short = "L" if d_norm == Direction.LONG else "S"
    symbol_norm = str(symbol or "UNKNOWN").upper().strip()
    return f"{kind}:{symbol_norm}:{int(ts_ms)}:{d_short}"
