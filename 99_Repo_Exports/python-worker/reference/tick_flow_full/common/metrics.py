from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class _NoopMetrics:
    """
    Minimal metrics facade.
    Replace with your Prometheus/StatsD impl later without changing call sites.
    """
    def inc(self, name: str, value: int = 1, labels: Optional[dict[str, str]] = None) -> None:
        return None

    def observe(self, name: str, value: float, labels: Optional[dict[str, str]] = None) -> None:
        return None


METRICS: Any = _NoopMetrics()


# --- OK/OF-gate metrics emission health (telemetry about telemetry) ---
ok_metrics_emitted_total = _get_or_create_prom_counter(
    "ok_metrics_emitted_total"
    "Total decision/ok metric rows emitted to Redis streams"
    ["src"]
)
ok_metrics_skipped_total = _get_or_create_prom_counter(
    "ok_metrics_skipped_total"
    "Total decision/ok metric rows skipped (sampling/disabled/invalid)"
    ["src", "why"]
)
ok_metrics_error_total = _get_or_create_prom_counter(
    "ok_metrics_error_total"
    "Total decision/ok metric emission errors"
    ["src", "where"]
)

