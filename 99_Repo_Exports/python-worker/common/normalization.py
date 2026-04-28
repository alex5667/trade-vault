from __future__ import annotations
import hashlib
from typing import Union, Optional, Dict, Any
from dataclasses import dataclass
from common.enums.trading import Direction, Side

@dataclass(frozen=True)
class NormalizedSide:
    direction: Direction   # LONG | SHORT
    side: Side             # BUY | SELL
    side_int: int          # 1 | -1

def normalize_direction(d: Union[str, Direction, None]) -> Direction:
    """Normalizes direction to LONG|SHORT."""
    val = str(d or "").upper().strip()
    if val in ("LONG", "L", "1", "BUY"):
        return Direction.LONG
    if val in ("SHORT", "S", "-1", "SELL"):
        return Direction.SHORT
    return Direction.LONG  # Default fallback

def normalize_side(s: Union[str, Side, None]) -> Side:
    """Normalizes side to BUY|SELL."""
    val = str(s or "").upper().strip()
    if val in ("BUY", "B", "1", "LONG"):
        return Side.BUY
    if val in ("SELL", "S", "-1", "SHORT"):
        return Side.SELL
    return Side.BUY  # Default fallback

def normalize_side_3(value: Union[str, int, Direction, Side, None]) -> NormalizedSide:
    """
    P0: Unified side/direction normalizer.
    Returns a triple: Direction, Side, side_int.
    """
    d = normalize_direction(value)
    s = Side.BUY if d == Direction.LONG else Side.SELL
    si = 1 if d == Direction.LONG else -1
    return NormalizedSide(direction=d, side=s, side_int=si)

def get_side_int(d_or_s: Union[str, Direction, Side, int, None]) -> int:
    """Returns 1 for LONG/BUY, -1 for SHORT/SELL."""
    if isinstance(d_or_s, int):
        return 1 if d_or_s >= 0 else -1
    
    val = str(d_or_s or "").upper().strip()
    if val in ("LONG", "BUY", "L", "B", "1"):
        return 1
    if val in ("SHORT", "SELL", "S", "-1"):
        return -1
    return 1

def direction_to_side(d: Direction) -> Side:
    return Side.BUY if d == Direction.LONG else Side.SELL

def side_to_direction(s: Side) -> Direction:
    return Direction.LONG if s == Side.BUY else Direction.SHORT

SIGNAL_ID_ALGO_V1 = "v1"

def generate_signal_id(symbol: str, ts_ms: int, direction: Union[str, Direction], kind: str = "crypto-of") -> str:
    """
    Unified signal_id algorithm (v1).
    Format: {kind}:{symbol}:{ts_ms}:{direction_short}
    Example: crypto-of:BTCUSDT:1714234567890:L
    """
    d_norm = normalize_direction(direction)
    d_short = "L" if d_norm == Direction.LONG else "S"
    symbol_norm = str(symbol or "UNKNOWN").upper().strip()
    return f"{kind}:{symbol_norm}:{int(ts_ms)}:{d_short}"
