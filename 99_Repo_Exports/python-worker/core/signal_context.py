# core/signal_context.py
"""
Signal context for unified signal processing.

Заглушка для совместимости с существующим кодом.
"""

from typing import Any, Dict, Optional
from dataclasses import dataclass


@dataclass
class SignalContext:
    """Контекст сигнала для unified processing."""

    symbol=""
    confidence: float = 0.0
    score: float = 0.0
    price: Optional[float] = None
    direction: str = ""
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

