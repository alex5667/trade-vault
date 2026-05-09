from __future__ import annotations

"""Prometheus metrics for P5 stream integrity.

We keep metrics definitions in a dedicated module to avoid duplicate
registration across SoT/mirror import paths.

Cardinality policy:
- By default only {symbol,stream} labels.
- No per-seq labels.
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

logger = logging.getLogger("orderflow_metrics_stream_integrity")

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


stream_event_lag_ms = _get_or_create(
    "stream_event_lag_ms",
    Histogram,
    "Observed event-time lag in milliseconds (processing_time - ts_event_ms)",
    labelnames=("symbol", "stream"),
    buckets=(1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000),
)

stream_late_events_total = _get_or_create(
    "stream_late_events_total",
    Counter,
    "Count of events that exceeded configured event-lag budget",
    labelnames=("symbol", "stream"),
)

stream_future_ts_total = _get_or_create(
    "stream_future_ts_total",
    Counter,
    "Count of events that arrived with future timestamps beyond skew tolerance",
    labelnames=("symbol", "stream"),
)

stream_out_of_order_total = _get_or_create(
    "stream_out_of_order_total",
    Counter,
    "Count of out-of-order events beyond configured tolerance",
    labelnames=("symbol", "stream"),
)

stream_book_staleness_ms = _get_or_create(
    "stream_book_staleness_ms",
    Gauge,
    "Best-book staleness observed on the tick path (ms)",
    labelnames=("symbol",),
)

stream_book_staleness_hist_ms = _get_or_create(
    "stream_book_staleness_hist_ms",
    Histogram,
    "Distribution of best-book staleness observed on the tick path (ms)",
    labelnames=("symbol",),
    buckets=(1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000),
)


def _hash_to_int(h: str) -> float:
    try:
        return float(int((h or "0"), 16))
    except Exception:
        return 0.0


def emit_integrity_metrics(*, symbol: str, stream: str, snap) -> None:
    """Best-effort emission."""
    sym = symbol
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


def emit_event_time_metrics(*, symbol: str, stream: str, event_lag_ms: float, late: bool = False, future: bool = False, out_of_order: bool = False) -> None:
    """Emit hot-path event-time lag and flag counters. Best-effort, never raises."""
    sym = symbol
    st = str(stream)
    try:
        lag = float(max(0.0, float(event_lag_ms or 0.0)))
        if stream_event_lag_ms is not None:
            stream_event_lag_ms.labels(symbol=sym, stream=st).observe(lag)
        if late and stream_late_events_total is not None:
            stream_late_events_total.labels(symbol=sym, stream=st).inc()
        if future and stream_future_ts_total is not None:
            stream_future_ts_total.labels(symbol=sym, stream=st).inc()
        if out_of_order and stream_out_of_order_total is not None:
            stream_out_of_order_total.labels(symbol=sym, stream=st).inc()
    except Exception:
        pass


def emit_book_staleness_metrics(*, symbol: str, staleness_ms: float) -> None:
    """Emit book staleness gauge and histogram. Best-effort, never raises."""
    sym = symbol
    try:
        val = float(max(0.0, float(staleness_ms or 0.0)))
        if stream_book_staleness_ms is not None:
            stream_book_staleness_ms.labels(symbol=sym).set(val)
        if stream_book_staleness_hist_ms is not None:
            stream_book_staleness_hist_ms.labels(symbol=sym).observe(val)
    except Exception:
        pass


# =============================================================================
# BookTradeConsistencyGate P6next — trade-to-book microstructure metrics
#
# New counters/histograms added for: stale book, adverse cross, hard veto.
# Low cardinality: {symbol, stream} + {reason} on veto counter.
# =============================================================================

stream_trade_to_book_stale_total = _get_or_create(
    "stream_trade_to_book_stale_total",
    Counter,
    "Count of events where book was stale relative to event timestamp",
    labelnames=("symbol", "stream"),
)

stream_trade_to_book_adverse_cross_total = _get_or_create(
    "stream_trade_to_book_adverse_cross_total",
    Counter,
    "Count of trade events where trade_px crossed outside current BBO beyond tolerance",
    labelnames=("symbol", "stream"),
)

stream_trade_to_book_adverse_cross_dist_bps = _get_or_create(
    "stream_trade_to_book_adverse_cross_dist_bps",
    Histogram,
    "Distribution of adverse-cross distance in bps when trade_px is outside BBO",
    labelnames=("symbol", "stream"),
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0),
)

stream_trade_to_book_veto_total = _get_or_create(
    "stream_trade_to_book_veto_total",
    Counter,
    "Count of hard-veto decisions from BookTradeConsistencyGate by reason code",
    labelnames=("symbol", "stream", "reason"),
)


def emit_trade_to_book_metrics(
    *,
    symbol: str,
    stream: str,
    book_staleness_ms: float,
    adverse_cross_bps: float,
    stale_book: bool,
    adverse_cross: bool,
    veto_reason: str,
) -> None:
    """Emit BookTradeConsistencyGate trade-to-book metrics. Best-effort, never raises.

    Called from BookTradeConsistencyGate.evaluate() on every hot-path tick.
    """
    sym = symbol
    st = str(stream)
    try:
        if stale_book and stream_trade_to_book_stale_total is not None:
            stream_trade_to_book_stale_total.labels(symbol=sym, stream=st).inc()

        if adverse_cross:
            if stream_trade_to_book_adverse_cross_total is not None:
                stream_trade_to_book_adverse_cross_total.labels(symbol=sym, stream=st).inc()
            dist = float(max(0.0, float(adverse_cross_bps or 0.0)))
            if stream_trade_to_book_adverse_cross_dist_bps is not None and dist > 0:
                stream_trade_to_book_adverse_cross_dist_bps.labels(symbol=sym, stream=st).observe(dist)

        if veto_reason and stream_trade_to_book_veto_total is not None:
            stream_trade_to_book_veto_total.labels(symbol=sym, stream=st, reason=str(veto_reason)).inc()
    except Exception:
        pass
