"""Prometheus metrics for DecisionSnapshotWriter.

Design goals:
- Low cardinality: no per-sid/order_id labels.
- Fail-open: if prometheus_client is missing, metrics become no-ops.
- Singleton: avoid duplicated timeseries in unit tests.

Exposed metrics (requested):
- decision_snapshot_writer_written_total
- decision_snapshot_writer_db_fail_total
- decision_snapshot_writer_redis_lag_ms (histogram; p95 via PromQL)
- decision_snapshot_writer_pending_count (gauge)

Additional useful counters (low-cardinality):
- decision_snapshot_writer_processed_total
- decision_snapshot_writer_dlq_total
- decision_snapshot_writer_reclaim_total
- decision_snapshot_writer_claim_fail_total
- decision_snapshot_writer_dlq_by_reason_total{reason}
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger("decision_snapshot_writer.metrics")


class _NoOp:
    """Fallback no-op metric when prometheus_client is unavailable."""

    def inc(self, amount: float = 1.0) -> None:
        return

    def observe(self, amount: float) -> None:
        return

    def set(self, amount: float) -> None:
        return

    def labels(self, **_kw: Any) -> "_NoOp":
        # Allow dlq_by_reason_total.labels(reason=...).inc() without errors.
        return self


@dataclass
class WriterMetrics:
    written_total: Any
    db_fail_total: Any
    processed_total: Any
    dlq_total: Any
    dlq_by_reason_total: Any   # Counter with label 'reason' (low-cardinality DLQ breakdown)
    reclaim_total: Any
    claim_fail_total: Any      # XAUTOCLAIM/XCLAIM error counter
    pending_count: Any         # Gauge: current PEL size (polled every N seconds)
    redis_lag_ms: Any


_METRICS: Optional[WriterMetrics] = None
_METRICS_PORT: Optional[int] = None


def build_metrics() -> WriterMetrics:
    """Return singleton metrics objects (or no-ops if prometheus_client unavailable)."""
    global _METRICS
    if _METRICS is not None:
        return _METRICS

    try:
        from prometheus_client import Counter, Histogram, Gauge

        written_total = Counter(
            "decision_snapshot_writer_written_total",
            "Rows successfully upserted into TimescaleDB from decision_snapshot stream.",
        )
        db_fail_total = Counter(
            "decision_snapshot_writer_db_fail_total",
            "DB write failures (batch upsert exceptions).",
        )
        processed_total = Counter(
            "decision_snapshot_writer_processed_total",
            "Redis entries processed (including bad/dlq).",
        )
        dlq_total = Counter(
            "decision_snapshot_writer_dlq_total",
            "Entries sent to DLQ (bad payload/row).",
        )
        dlq_by_reason_total = Counter(
            "decision_snapshot_writer_dlq_by_reason_total",
            "Entries sent to DLQ, by reason (low cardinality).",
            ["reason"],
        )
        reclaim_total = Counter(
            "decision_snapshot_writer_reclaim_total",
            "Pending entries reclaimed via XAUTOCLAIM/XCLAIM.",
        )
        claim_fail_total = Counter(
            "decision_snapshot_writer_claim_fail_total",
            "PEL claim failures (XAUTOCLAIM/XCLAIM errors).",
        )
        pending_count = Gauge(
            "decision_snapshot_writer_pending_count",
            "Current pending entries in the Redis consumer group (PEL size).",
        )

        redis_lag_ms = Histogram(
            "decision_snapshot_writer_redis_lag_ms",
            "Processing lag in ms: now_ms - decision_ts_ms for consumed decision_snapshot entries.",
            buckets=(50, 100, 250, 500, 1000, 2000, 5000, 10_000, 30_000, 60_000, 120_000),
        )

        _METRICS = WriterMetrics(
            written_total=written_total,
            db_fail_total=db_fail_total,
            processed_total=processed_total,
            dlq_total=dlq_total,
            dlq_by_reason_total=dlq_by_reason_total,
            reclaim_total=reclaim_total,
            claim_fail_total=claim_fail_total,
            pending_count=pending_count,
            redis_lag_ms=redis_lag_ms,
        )
        return _METRICS

    except Exception as e:
        logger.warning("prometheus_client not available, metrics disabled: %s", e)
        noop = _NoOp()
        _METRICS = WriterMetrics(
            written_total=noop,
            db_fail_total=noop,
            processed_total=noop,
            dlq_total=noop,
            dlq_by_reason_total=noop,
            reclaim_total=noop,
            claim_fail_total=noop,
            pending_count=noop,
            redis_lag_ms=noop,
        )
        return _METRICS


def start_metrics_server() -> Optional[int]:
    """Start HTTP server for /metrics. Returns bound port or None.

    ENV:
      DECISION_SNAPSHOT_WRITER_METRICS_PORT (default 9825)
      DECISION_SNAPSHOT_WRITER_METRICS_ENABLE (default 1)
    """
    global _METRICS_PORT
    if _METRICS_PORT is not None:
        return _METRICS_PORT

    if os.getenv("DECISION_SNAPSHOT_WRITER_METRICS_ENABLE", "1").lower() not in {"1", "true", "yes", "on"}:
        return None

    try:
        port = int(os.getenv("DECISION_SNAPSHOT_WRITER_METRICS_PORT", "9825"))
    except Exception:
        port = 9825

    try:
        from prometheus_client import start_http_server

        start_http_server(port)
        logger.info("decision_snapshot_writer metrics listening on :%d", port)
        _METRICS_PORT = port
        return port
    except Exception as e:
        logger.warning("failed to start metrics server: %s", e)
        return None
