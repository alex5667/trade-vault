"""Prometheus metrics for normalized derivatives context.

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

logger = logging.getLogger("orderflow_metrics_derivatives_context")
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


deriv_ctx_snapshot_age_ms = _get_or_create(
    "deriv_ctx_snapshot_age_ms",
    Histogram,
    "Age of normalized derivatives context snapshot in milliseconds",
    labelnames=("symbol",),
    buckets=(250, 500, 1000, 2000, 5000, 10000, 30000, 60000, 120000),
)

deriv_ctx_funding_rate_z = _get_or_create(
    "deriv_ctx_funding_rate_z",
    Histogram,
    "Funding-rate robust z-score from derivatives context",
    labelnames=("symbol",),
    buckets=(0.5, 1, 2, 3, 4, 5, 7, 10),
)

deriv_ctx_basis_bps = _get_or_create(
    "deriv_ctx_basis_bps",
    Histogram,
    "Basis / premium in bps from derivatives context",
    labelnames=("symbol",),
    buckets=(1, 2, 4, 8, 12, 20, 30, 50, 80, 120),
)

deriv_ctx_oi_notional_usd = _get_or_create(
    "deriv_ctx_oi_notional_usd",
    Histogram,
    "Open interest notional in USD from derivatives context",
    labelnames=("symbol",),
    buckets=(1e5, 5e5, 1e6, 5e6, 1e7, 5e7, 1e8, 5e8, 1e9, 5e9),
)

deriv_ctx_gate_monitor_hit_total = _get_or_create(
    "deriv_ctx_gate_monitor_hit_total",
    Counter,
    "Derivatives context gate hits (annotate/monitor)",
    labelnames=("symbol", "profile"),
)

deriv_ctx_gate_tighten_total = _get_or_create(
    "deriv_ctx_gate_tighten_total",
    Counter,
    "Derivatives context tighten count",
    labelnames=("symbol", "profile"),
)

deriv_ctx_gate_veto_total = _get_or_create(
    "deriv_ctx_gate_veto_total",
    Counter,
    "Derivatives context veto count",
    labelnames=("symbol", "reason"),
)

deriv_ctx_tighten_add_bps = _get_or_create(
    "deriv_ctx_tighten_add_bps",
    Histogram,
    "Added slippage (bps) due to derivatives context crowding",
    labelnames=("symbol",),
    buckets=(0.25, 0.5, 1, 2, 3, 4, 6, 8, 10),
)

deriv_ctx_missing_total = _get_or_create(
    "deriv_ctx_missing_total",
    Counter,
    "Derivatives context snapshot missing at signal publish time (key expired or collector down)",
    labelnames=("symbol",),
)

deriv_ctx_collector_up = _get_or_create(
    "deriv_ctx_collector_up",
    Gauge,
    "Derivatives context collector liveness",
)

deriv_ctx_collector_errors_total = _get_or_create(
    "deriv_ctx_collector_errors_total",
    Counter,
    "Derivatives context collector errors",
    labelnames=("where",),
)

# ─── V2 Metrics (Liquidation, Breadth, Crowding) ─────────────────────────────

deriv_ctx_liq_imbalance_z = _get_or_create(
    "deriv_ctx_liq_imbalance_z",
    Histogram,
    "Liquidation imbalance robust z-score",
    labelnames=("symbol",),
    buckets=(-10, -5, -3, -2, -1, 0, 1, 2, 3, 5, 10),
)

deriv_ctx_long_short_ratio_z = _get_or_create(
    "deriv_ctx_long_short_ratio_z",
    Histogram,
    "Long/Short ratio robust z-score",
    labelnames=("symbol",),
    buckets=(-5, -3, -2, -1, 0, 1, 2, 3, 5),
)

deriv_ctx_market_breadth_ret = _get_or_create(
    "deriv_ctx_market_breadth_ret",
    Histogram,
    "Market breadth 24h return",
    buckets=(-0.1, -0.05, -0.02, -0.01, 0, 0.01, 0.02, 0.05, 0.1),
)

liq_ctx_worker_up = _get_or_create(
    "liq_ctx_worker_up",
    Gauge,
    "Liquidation context worker liveness",
)

liq_ctx_worker_events_processed_total = _get_or_create(
    "liq_ctx_worker_events_processed_total",
    Counter,
    "Liquidation events processed by worker",
    labelnames=("symbol",),
)

liq_ctx_worker_errors_total = _get_or_create(
    "liq_ctx_worker_errors_total",
    Counter,
    "Liquidation context worker errors",
    labelnames=("where",),
)
