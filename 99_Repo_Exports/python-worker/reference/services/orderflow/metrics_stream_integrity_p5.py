from __future__ import annotations
"""Prometheus metrics for P5 stream integrity.

We keep metrics definitions in a dedicated module to avoid duplicate
registration across SoT/mirror import paths.

Cardinality policy:
- By default only {symbol,stream} labels.
- No per-seq labels.
"""


import logging
from typing import Sequence, Type, TypeVar

try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY  # type: ignore
    from prometheus_client.registry import Collector  # type: ignore
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = object  # type: ignore
    REGISTRY = None  # type: ignore
    Collector = object  # type: ignore

logger = logging.getLogger("orderflow_metrics_stream_integrity")

TCollector = TypeVar("TCollector", bound="Collector")


def _get_or_create(
    name: str,
    ctor: Type[TCollector],
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


stream_seq_gap_rate_ema = _get_or_create(
    "stream_seq_gap_rate_ema",
    Gauge,
    "EMA gap-rate for monotone sequences (0..1)",
    labelnames=("symbol", "stream"),
)

stream_seq_dup_rate_ema = _get_or_create(
    "stream_seq_dup_rate_ema",
    Gauge,
    "EMA duplicate-rate for monotone sequences (0..1)",
    labelnames=("symbol", "stream"),
)

stream_seq_max_gap_window = _get_or_create(
    "stream_seq_max_gap_window",
    Gauge,
    "Max seq gap magnitude observed in the current window",
    labelnames=("symbol", "stream"),
)

stream_dup_burst_z = _get_or_create(
    "stream_dup_burst_z",
    Gauge,
    "Robust z-score of per-second duplicate ratio",
    labelnames=("symbol", "stream"),
)

stream_schema_changed_total = _get_or_create(
    "stream_schema_changed_total",
    Counter,
    "Count of detected schema-hash changes (keys-set changed)",
    labelnames=("symbol", "stream"),
)

stream_schema_hash = _get_or_create(
    "stream_schema_hash",
    Gauge,
    "Last seen schema hash as an integer (base16 truncated)",
    labelnames=("symbol", "stream"),
)


def _hash_to_int(h: str) -> float:
    try:
        return float(int(str(h or "0"), 16))
    except Exception:
        return 0.0


def emit_integrity_metrics(*, symbol: str, stream: str, snap) -> None:
    """Best-effort emission."""
    sym = str(symbol)
    st = str(stream)
    try:
        if stream_seq_gap_rate_ema is not None:
            stream_seq_gap_rate_ema.labels(symbol=sym, stream=st).set(float(getattr(snap, "gap_rate_ema", 0.0) or 0.0))
        if stream_seq_dup_rate_ema is not None:
            stream_seq_dup_rate_ema.labels(symbol=sym, stream=st).set(float(getattr(snap, "dup_rate_ema", 0.0) or 0.0))
        if stream_seq_max_gap_window is not None:
            stream_seq_max_gap_window.labels(symbol=sym, stream=st).set(float(getattr(snap, "gap_max_window", 0) or 0))
        if stream_dup_burst_z is not None:
            stream_dup_burst_z.labels(symbol=sym, stream=st).set(float(getattr(snap, "dup_burst_z", 0.0) or 0.0))

        if getattr(snap, "schema_changed", 0) == 1:
            if stream_schema_changed_total is not None:
                stream_schema_changed_total.labels(symbol=sym, stream=st).inc()
        if stream_schema_hash is not None:
            stream_schema_hash.labels(symbol=sym, stream=st).set(_hash_to_int(getattr(snap, "schema_hash", "") or ""))
    except Exception:
        pass
