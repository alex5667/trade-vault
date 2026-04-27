# common/types.py
"""
Common data types for signal processing.
Unified Signal type used across all signal generators.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class Signal:
    """
    Unified signal structure for all signal sources.
    
    Attributes:
        sid: Unique signal identifier
        symbol: Trading symbol (e.g., "XAUUSD")
        side: Trade direction ("LONG" | "SHORT")
        lot: Position size
        sl: Stop Loss price
        tp_levels: List of Take Profit prices
        entry: Entry price (None = market order)
        source: Signal source ("orderflow" | "ta" | "spike" | "cluster" | "aggregated")
        score: Signal quality score (0.0-1.0)
        confidence: Confidence level (0.0-1.0)
        reason: Human-readable reason for signal
        indicators: Dict of technical indicators values
        ts: Timestamp in milliseconds
        push_order: Whether to auto-push order to gateway
    """
    sid: str
    symbol: str
    side: str                   # "LONG" | "SHORT"
    lot: float
    sl: float
    tp_levels: List[float]
    entry: Optional[float] = None
    source: str = "unknown"     # "orderflow" | "ta" | "spike" | "cluster" | "aggregated"
    score: float = 0.0
    confidence: float = 0.0     # 0..1
    reason: str = ""
    indicators: Dict = field(default_factory=dict)
    ts: int = 0                 # ms
    push_order: bool = True     # разрешение на автопуш ордера

