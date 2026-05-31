from __future__ import annotations

"""Prometheus metrics for OFC contextual decision writer.

Low-cardinality, singleton, fail-open metrics wrapper used by
services/orderflow/ofc_contextual_decision_writer_v1.py.
"""

import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("ofc_contextual_decision_writer.metrics")


class _NoOp:
    def inc(self, amount: float = 1.0) -> None:
        return

    def observe(self, amount: float) -> None:
        return

    def set(self, amount: float) -> None:
        return

    def labels(self, **_kw: Any) -> _NoOp:
        return self


@dataclass
class WriterMetrics:
    written_total: Any
    db_fail_total: Any
    processed_total: Any
    dlq_total: Any
    pending_count: Any
    redis_lag_ms: Any
    last_ok: Any
    last_batch_rows: Any


_METRICS: WriterMetrics | None = None
_METRICS_PORT: int | None = None


def build_metrics() -> WriterMetrics:
    global _METRICS
    if _METRICS is not None:
        return _METRICS
    try:
        from prometheus_client import Counter, Gauge, Histogram

        _METRICS = WriterMetrics(
            written_total=Counter(
                "ofc_contextual_decision_writer_written_total",
                "Rows successfully upserted into ofc_contextual_decisions.",
            ),
            db_fail_total=Counter(
                "ofc_contextual_decision_writer_db_fail_total",
                "DB write failures for OFC contextual decision writer.",
            ),
            processed_total=Counter(
                "ofc_contextual_decision_writer_processed_total",
                "Redis entries processed by OFC contextual decision writer.",
            ),
            dlq_total=Counter(
                "ofc_contextual_decision_writer_dlq_total",
                "Entries sent to DLQ by OFC contextual decision writer.",
                ["reason"],
            ),
            pending_count=Gauge(
                "ofc_contextual_decision_writer_pending_count",
                "Current pending entries in the OFC contextual decision writer consumer group.",
            ),
            redis_lag_ms=Histogram(
                "ofc_contextual_decision_writer_redis_lag_ms",
                "Lag in ms from decision_ts_ms to writer processing time.",
                buckets=(50, 100, 250, 500, 1000, 2000, 5000, 10000, 30000, 60000, 120000),
            ),
            last_ok=Gauge(
                "ofc_contextual_decision_writer_last_ok",
                "1 if last OFC contextual decision writer batch succeeded, 0 otherwise.",
            ),
            last_batch_rows=Gauge(
                "ofc_contextual_decision_writer_last_batch_rows",
                "Rows written in the last successful writer batch.",
            )
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
            pending_count=noop,
            redis_lag_ms=noop,
            last_ok=noop,
            last_batch_rows=noop,
        )
        return _METRICS


def start_metrics_server() -> int | None:
    global _METRICS_PORT
    if _METRICS_PORT is not None:
        return _METRICS_PORT
    if os.getenv("OFC_CTX_DECISION_WRITER_METRICS_ENABLE", "1").lower() not in {"1", "true", "yes", "on"}:
        return None
    try:
        port = int(os.getenv("OFC_CTX_DECISION_WRITER_METRICS_PORT", "9831"))
    except Exception:
        port = 9831
    try:
        from prometheus_client import start_http_server
        start_http_server(port)
        logger.info("ofc_contextual_decision_writer metrics listening on :%d", port)
        _METRICS_PORT = port
        return port
    except Exception as e:
        logger.warning("failed to start metrics server: %s", e)
        return None
