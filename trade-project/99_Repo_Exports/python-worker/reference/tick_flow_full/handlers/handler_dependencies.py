from dataclasses import dataclass
from typing import Any


@dataclass
class HandlerDependencies:
    """
    Optional dependencies for BaseOrderFlowHandler.
    Passed via DI container/factory to avoid circular imports and runtime import hell.
    """
    # Core Infrastructure
    health_metrics: Any | None = None
    health_monitor: Any | None = None

    # Analysis & Indicators
    liquidity_analyzer: Any | None = None
    atr_indicator: Any | None = None
    levels_manager: Any | None = None

    # L2/L3 Services
    l3_queue: Any | None = None
    queue_eta: Any | None = None
    burst_tracker: Any | None = None

    # Geometry / Regimes
    extrema_service: Any | None = None
    regime_service: Any | None = None

    # Execution / Signals
    execution_setup: Any | None = None
    outbox_publisher: Any | None = None
    scoring_engine: Any | None = None
    unified_pipeline: Any | None = None

    # Signal Exec Components (Heavy)
    signal_service: Any | None = None
    execution_planner: Any | None = None
    signal_repo: Any | None = None
    signal_bus: Any | None = None
    performance_tracker: Any | None = None

    # GPU
    gpu_processor: Any | None = None

    # Calibration
    local_calibration: Any | None = None
    calibration_service_cls: Any | None = None

    # Service Classes (for late instantiation)
    cooldown_service_cls: Any | None = None

    def __post_init__(self):
        """Helpers to validate or log missing dependencies if needed."""
        pass
