# signal_types.py
# Shared types and dataclasses for signal processing
# Now re-exports from contexts.py to avoid circular dependencies and ensure single source of truth.
from __future__ import annotations

from contexts import (
    MarketRegime,
    RegimeLabel,
    RegimeDecision,
    SignalKind,
    SignalTypeConf,
    GoldenThresholds
)

__all__ = [
    "MarketRegime",
    "RegimeLabel",
    "RegimeDecision",
    "SignalKind", 
    "SignalTypeConf",
    "GoldenThresholds"
]
