"""
Signal Execution Module

Comprehensive system for signal execution planning and performance analysis.
Integrates with scanner_infra architecture for complete trade lifecycle management.

Components:
- ExecutionPlanner: Risk-based execution planning with TTD expiry
- SignalPerformanceTracker: Post-trade analysis (TTD, MFE/MAE, outcomes)
- SignalRepository: TimescaleDB operations
- Models: Domain objects for execution and performance data

Usage:
    from signal_exec import ExecutionPlanner, SignalPerformanceTracker, SignalRepository
    from signal_exec.context import SignalContext
    from signal_exec.models import ExecutionPlan

    # Create execution plan
    planner = ExecutionPlanner(setup_configs)
    plan = planner.build_plan(signal_context)

    # Analyze performance
    tracker = SignalPerformanceTracker(repo, ttd_target_R=1.0)
    tracker.register_signal(ctx, plan)

    # Save to database
    repo = SignalRepository(dsn)
    repo.insert_signal(signal_ctx)
    repo.insert_execution_plan(plan)
    repo.insert_signal_performance(performance)
"""

from .context import SignalContext
from .models import (
    AccountState,
    Bar1m,
    ExecutionPlan,
    HTFLevel,
    OrderBookSnapshot,
    Side,
    SwingPoint,
    SymbolSetupConfig,
)
from .performance_tracker import SignalPerformance

# Backward-compat alias: ExtendedSignalContext == SignalContext in signal_exec
# (signal_execution used a different ExtendedSignalContext type which is not part
# of this module; callers should migrate to SignalContext)
ExtendedSignalContext = SignalContext

from .bus import SignalBus
from .execution_planner import ExecutionPlanner
from .performance_tracker import SignalPerformanceTracker
from .repository import SignalRepository
from .service import SignalService

__version__ = "1.0.0"
__all__ = [
    # Enums and basic types
    "Side",

    # Data structures
    "SwingPoint",
    "HTFLevel",
    "OrderBookSnapshot",
    "AccountState",
    "SignalContext",
    "ExecutionPlan",
    "ExtendedSignalContext",
    "Bar1m",
    "SignalPerformance",
    "SymbolSetupConfig",

    # Core classes
    "ExecutionPlanner",
    "SignalPerformanceTracker",
    "SignalRepository",
    "SignalBus",
    "SignalService",
]
