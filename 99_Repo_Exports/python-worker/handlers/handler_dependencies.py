from dataclasses import dataclass
from typing import Optional, Any

@dataclass
class HandlerDependencies:
    """
    Optional dependencies for BaseOrderFlowHandler.
    Passed via DI container/factory to avoid circular imports and runtime import hell.
    """
    # Core Infrastructure
    health_metrics: Optional[Any] = None
    health_monitor: Optional[Any] = None
    
    # Analysis & Indicators
    liquidity_analyzer: Optional[Any] = None
    atr_indicator: Optional[Any] = None
    levels_manager: Optional[Any] = None
    
    # L2/L3 Services
    l3_queue: Optional[Any] = None
    queue_eta: Optional[Any] = None
    burst_tracker: Optional[Any] = None
    
    # Geometry / Regimes
    extrema_service: Optional[Any] = None
    regime_service: Optional[Any] = None
    
    # Execution / Signals
    execution_setup: Optional[Any] = None
    outbox_publisher: Optional[Any] = None
    scoring_engine: Optional[Any] = None
    unified_pipeline: Optional[Any] = None
    
    # Signal Exec Components (Heavy)
    signal_service: Optional[Any] = None
    execution_planner: Optional[Any] = None
    signal_repo: Optional[Any] = None
    signal_bus: Optional[Any] = None
    performance_tracker: Optional[Any] = None
    
    # GPU
    gpu_processor: Optional[Any] = None
    
    # Calibration
    local_calibration: Optional[Any] = None
    calibration_service_cls: Optional[Any] = None

    # Service Classes (for late instantiation)
    cooldown_service_cls: Optional[Any] = None
    
    def __post_init__(self):
        """Helpers to validate or log missing dependencies if needed."""
        pass
