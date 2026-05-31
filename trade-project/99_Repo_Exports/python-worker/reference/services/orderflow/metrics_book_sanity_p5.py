from __future__ import annotations

"""Prometheus metrics for P5 book sanity.

Cardinality policy:
- Only {symbol}.
- No per-price/flag labels.
"""


import logging
from collections.abc import Sequence
from typing import TypeVar

try:
    from prometheus_client import REGISTRY, Counter, Gauge, Histogram  # type: ignore
    from prometheus_client.registry import Collector  # type: ignore
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = object  # type: ignore
    REGISTRY = None  # type: ignore
    Collector = object  # type: ignore

logger = logging.getLogger("orderflow_metrics_book_sanity")

TCollector = TypeVar("TCollector", bound="Collector")


def _get_or_create(
    name: str,
    ctor: type[TCollector],
    documentation: str,
    labelnames: Sequence[str] = (),
    **kwargs,
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


book_crossed_total = _get_or_create(
    "book_crossed_total",
    Counter,
    "Count of crossed BBO detections (best_bid >= best_ask)",
    labelnames=("symbol",),
)

book_sanity_flags_total = _get_or_create(
    "book_sanity_flags_total",
    Counter,
    "Count of any book_sanity_flags set (monitor signal)",
    labelnames=("symbol",),
)

trade_outside_bbo_total = _get_or_create(
    "trade_outside_bbo_total",
    Counter,
    "Count of trades that printed outside current BBO (stale book symptom)",
    labelnames=("symbol",),
)

trade_outside_bbo_dist_bps = _get_or_create(
    "trade_outside_bbo_dist_bps",
    Histogram,
    "Distance (bps) of trade price outside BBO",
    labelnames=("symbol",),
    buckets=(0.5, 1, 2, 5, 10, 20, 50, 100),
)
