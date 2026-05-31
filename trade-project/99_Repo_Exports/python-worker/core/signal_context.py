# core/signal_context.py
"""
Signal context for unified signal processing.

Заглушка для совместимости с существующим кодом.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class SignalContext:
    """Контекст сигнала для unified processing."""

    symbol=""
    confidence: float = 0.0
    score: float = 0.0
    price: float | None = None
    direction: str = ""
    metadata: dict[str, Any] = None  # type: ignore

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

