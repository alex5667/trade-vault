"""Prometheus metrics for P6 ExecutionHealthGate.

Cardinality policy:
- Prefer {symbol} only.
- Reason label is a finite set.
"""

from __future__ import annotations

import logging
from typing import Sequence, Type, TypeVar

try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY  # type: ignore
    from prometheus_client.registry import Collector  # type: ignore
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = object  # type: ignore
    REGISTRY = None  # type: ignore
    Collector = object  # type: ignore

logger = logging.getLogger("orderflow_metrics_exec_health")

TCollector = TypeVar("TCollector", bound="Collector")


def _get_or_create(
    name: str
    ctor: Type[TCollector]
    documentation: str
    labelnames: Sequence[str] = ()
    **kwargs
):
    if REGISTRY is None:  # pragma: no cover
        return None
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing is not None:
        return existing
    try:
        return ctor(name, documentation, labelnames=tuple(labelnames), **kwargs)
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to create metric %s: %s", name, exc)
        return None


exec_health_apply_total = _get_or_create(
    "exec_health_apply_total"
    Counter
    "ExecutionHealthGate applied (monitor/tighten/veto)"
    labelnames=("symbol", "mode")
)

exec_health_veto_total = _get_or_create(
    "exec_health_veto_total"
    Counter
    "ExecutionHealthGate veto count"
    labelnames=("symbol", "reason")
)

exec_health_tighten_add_bps = _get_or_create(
    "exec_health_tighten_add_bps"
    Histogram
    "Added slippage (bps) due to execution health tighten"
    labelnames=("symbol",)
    buckets=(0.5, 1, 2, 3, 4, 6, 8, 10, 15)
)
