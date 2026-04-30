"""
Signal Execution Module: Execution Planning and Performance Tracking.

This module provides comprehensive signal execution planning and performance analysis
including risk management, entry/exit planning, and TTD (Time-To-Decay) calculations.
"""

from .models import (
    Side
    SwingPoint
    HTFLevel
    OrderBookSnapshot
    AccountState
    ExtendedSignalContext
    ExecutionPlan
    Bar1m
    SignalPerformance
    SymbolSetupConfig
)
from .execution_planner import ExecutionPlanner
from .signal_performance import SignalPerformanceTracker

__all__ = [
    # Enums and basic types
    "Side"

    # Data structures
    "SwingPoint"
    "HTFLevel"
    "OrderBookSnapshot"
    "AccountState"
    "ExtendedSignalContext"
    "ExecutionPlan"
    "Bar1m"
    "SignalPerformance"
    "SymbolSetupConfig"

    # Core classes
    "ExecutionPlanner"
    "SignalPerformanceTracker"
]
