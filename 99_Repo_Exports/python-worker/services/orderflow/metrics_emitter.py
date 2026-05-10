import logging
import time
from typing import Any

logger = logging.getLogger("crypto_orderflow_metrics")

class MetricsEmitter:
    def __init__(self, facade: Any):
        self.facade = facade

    def log_metrics(self, runtime: Any) -> None:
        """
        Периодический сброс метрик в Prometheus.
        """
        now = time.time()
        if now - runtime.last_metrics_ts < 30:
            return
        runtime.last_metrics_ts = now

        # Count how many times _log_metrics has been called
        call_count = getattr(runtime, "_metrics_call_count", 0) + 1
        setattr(runtime, "_metrics_call_count", call_count)

        # Only log every 10000th call
        if call_count % 10000 != 0:
            return

        logger.info(
            "METRICS symbol=%s ticks=%d delta_trig=%d signals=%d",
            runtime.symbol,
            runtime.tick_count,
            runtime.delta_triggers,
            runtime.signal_count,
        )
