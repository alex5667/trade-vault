"""Prometheus metrics for DefiLlama slow-context layer.

Cardinality policy:
- Prefer {symbol}; exporter may collapse into __all__ for non-allowlisted symbols.
- Flags/reasons are finite enumerations.
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

logger = logging.getLogger("orderflow_metrics_defillama_context")
TCollector = TypeVar("TCollector", bound="Collector")


def _get_or_create(name: str, ctor: Type[TCollector], documentation: str, labelnames: Sequence[str] = (), **kwargs):
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


defillama_ctx_snapshot_age_ms = _get_or_create(
    "defillama_ctx_snapshot_age_ms",
    Histogram,
    "Age of DefiLlama context snapshot in milliseconds",
    labelnames=("symbol",),
    buckets=(60_000, 300_000, 900_000, 1_800_000, 3_600_000, 7_200_000),
)

defillama_ctx_missing_total = _get_or_create(
    "defillama_ctx_missing_total",
    Counter,
    "DefiLlama context snapshot missing at signal publish time",
    labelnames=("symbol",),
)

defillama_ctx_gate_monitor_hit_total = _get_or_create(
    "defillama_ctx_gate_monitor_hit_total",
    Counter,
    "DefiLlama context gate hits (annotate/monitor)",
    labelnames=("symbol", "profile"),
)

defillama_ctx_gate_tighten_total = _get_or_create(
    "defillama_ctx_gate_tighten_total",
    Counter,
    "DefiLlama context tighten count",
    labelnames=("symbol", "reason"),
)

defillama_ctx_gate_veto_total = _get_or_create(
    "defillama_ctx_gate_veto_total",
    Counter,
    "DefiLlama context veto count",
    labelnames=("symbol", "reason"),
)

defillama_ctx_dex_volume_spike_z = _get_or_create(
    "defillama_ctx_dex_volume_spike_z",
    Histogram,
    "DefiLlama DEX volume spike z-score",
    labelnames=("symbol",),
    buckets=(0.5, 1, 1.5, 2, 2.5, 3, 4, 5),
)

defillama_ctx_chain_tvl_delta_1d_pct = _get_or_create(
    "defillama_ctx_chain_tvl_delta_1d_pct",
    Histogram,
    "DefiLlama chain TVL 1d delta percentage",
    labelnames=("symbol",),
    buckets=(-5, -3, -2, -1, -0.5, 0, 0.5, 1, 2, 3, 5),
)
