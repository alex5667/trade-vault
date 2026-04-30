
import time
import logging
from typing import Dict, Any, Optional

from common.resiliency import safe_call_fail_open

class HealthMonitorService:
    """
    Service responsible for aggregating health checks and metrics 
    for the OrderFlow handler and its components.
    
    Responsibilities:
    - Perform active health checks (Redis ping, stream existence).
    - Aggregate status from other services (is_initialized).
    - Collect active metrics (tick counts, uptime).
    - Emit health snapshots to pipeline/logging if needed.
    """
    
    def __init__(self, logger: Optional[Any] = None):
        self.logger = logger or logging.getLogger("HealthMonitor")
        self._start_time = time.time()
        
        # Metrics storage
        self._processed_ticks = 0
        self._processed_books = 0
        self._published_signals = 0
        
    def increment_ticks(self, count: int = 1) -> None:
        self._processed_ticks += count
        
    def increment_books(self, count: int = 1) -> None:
        self._processed_books += count
        
    def increment_signals(self, count: int = 1) -> None:
        self._published_signals += count
        
    def health_check(self, handler_ref: Any) -> Dict[str, Any]:
        """
        Perform comprehensive health check given a handler reference.
        
        Args:
            handler_ref: Reference to the handler to inspect dependencies.
                         (We pass it in to avoid circular dependency or complex binding)
                         
        Returns:
            Dict containing health status and components.
        """
        health = {
            "healthy": True
            "checks": {}
            "timestamp": time.time()
        }

        # 1. Infrastructure Checks
        try:
            if hasattr(handler_ref, "redis") and handler_ref.redis:
                 # Check connection if possible, or just presence
                 # Pinging on every health check might be spam, but is standard for health endpoints.
                 handler_ref.redis.ping()
                 health["checks"]["redis"] = {"status": "healthy", "message": "Redis connection OK"}
            else:
                health["checks"]["redis"] = {"status": "unhealthy", "message": "Redis not available"}
                health["healthy"] = False
        except Exception as e:
             health["checks"]["redis"] = {"status": "unhealthy", "message": f"Redis error: {e}"}
             health["healthy"] = False
             
        # 2. Stream Configuration Checks
        for stream_name in ['tick_stream', 'book_stream', 'l3_stream']:
            val = getattr(handler_ref, stream_name, None)
            if val:
                health["checks"][stream_name] = {"status": "healthy", "message": f"{stream_name} configured"}
            else:
                health["checks"][stream_name] = {"status": "unhealthy", "message": f"{stream_name} not configured"}
                health["healthy"] = False
                
        # 3. Service Initialization Checks
        # Using soft checks on private attributes
        services_to_check = [
            '_cooldown_service', '_signal_generator', '_signal_processing'
            '_data_processor', '_cache_service', '_config_manager'
        ]
        
        for service_name in services_to_check:
            svc = getattr(handler_ref, service_name, None)
            if svc is not None:
                health["checks"][service_name] = {"status": "healthy", "message": f"{service_name} initialized"}
            else:
                 # Some services might be optional? Assuming critical here based on legacy code.
                 health["checks"][service_name] = {"status": "unhealthy", "message": f"{service_name} not initialized"}
                 health["healthy"] = False
                 
        # 4. Metrics
        health["metrics"] = {
            "processed_ticks": self._processed_ticks
            "processed_books": self._processed_books
            "published_signals": self._published_signals
            "uptime_seconds": time.time() - self._start_time
        }
        
        return health
        
    def on_tick_health_emit(self, health_metrics_extern: Any, symbol: str, ctx: Any) -> None:
        """
        Forward health metrics to external metrics collector if available.
        This replaces `_emit_health_on_tick`.
        """
        if health_metrics_extern is None:
            return
        
        # Refactoring Phase 5: Use HealthMetricsMapper for safe extraction
        from common.health_mapper import HealthMetricsMapper
        
        # Prepare kwarg dict cleanly
        metrics = HealthMetricsMapper.extract(symbol, ctx)
        
        safe_call_fail_open(
            self.logger
            key="health_metrics.on_tick"
            fn=health_metrics_extern.on_tick
            # We assume on_tick signature matches or accepts these kwargs. 
            # If on_tick signature is strictly (symbol, l2_age_ms, ...), we should unpack.
            # BaseOrderFlowHandler previously called it with kwargs=dict(...).
            # So we pass kwargs=metrics.
            kwargs=metrics
            dq_flag="HEALTH_METRIC_FAIL"
        )
