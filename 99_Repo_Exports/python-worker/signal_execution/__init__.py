"""
Signal Execution Module: Execution Planning and Performance Tracking.

This module provides comprehensive signal execution planning and performance analysis,
including risk management, entry/exit planning, and TTD (Time-To-Decay) calculations.
"""

from .execution_planner import ExecutionPlanner
from .models import (
    AccountState,
    Bar1m,
    ExecutionPlan,
    ExtendedSignalContext,
    HTFLevel,
    OrderBookSnapshot,
    Side,
    SignalPerformance,
    SwingPoint,
    SymbolSetupConfig,
)
from .signal_performance import SignalPerformanceTracker

__all__ = [
    # Enums and basic types
    "Side",

    # Data structures
    "SwingPoint",
    "HTFLevel",
    "OrderBookSnapshot",
    "AccountState",
    "ExtendedSignalContext",
    "ExecutionPlan",
    "Bar1m",
    "SignalPerformance",
    "SymbolSetupConfig",

    # Core classes
    "ExecutionPlanner",
    "SignalPerformanceTracker",
]
