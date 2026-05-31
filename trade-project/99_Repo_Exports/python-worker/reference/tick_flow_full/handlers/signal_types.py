# signal_types.py
# Shared types and dataclasses for signal processing
# Now re-exports from contexts.py to avoid circular dependencies and ensure single source of truth.
from __future__ import annotations

from contexts import GoldenThresholds, MarketRegime, RegimeDecision, RegimeLabel, SignalKind, SignalTypeConf

__all__ = [
    "MarketRegime",
    "RegimeLabel",
    "RegimeDecision",
    "SignalKind",
    "SignalTypeConf",
    "GoldenThresholds"
]
