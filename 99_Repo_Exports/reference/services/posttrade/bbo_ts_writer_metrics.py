from __future__ import annotations

"""Prometheus metrics for bbo_ts_writer.

We keep this small and low-cardinality.
"""

import logging
from typing import Any, Dict

try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
except Exception:  # pragma: no cover
    Counter = Histogram = Gauge = None  # type: ignore
    start_http_server = None  # type: ignore


logger = logging.getLogger("bbo_ts_writer.metrics")


def build_metrics() -> Dict[str, Any]:
    if Counter is None:
        return {}

    return {
        "seen_total": Counter(
            "bbo_ts_writer_seen_total",
            "Total stream entries seen by bbo_ts_writer",
        ),
        "written_total": Counter(
            "bbo_ts_writer_written_total",
            "Total bbo_ts rows written",
        ),
        "db_fail_total": Counter(
            "bbo_ts_writer_db_fail_total",
            "DB write failures",
        ),
        "dlq_total": Counter(
            "bbo_ts_writer_dlq_total",
            "Invalid payloads moved to DLQ",
        ),
        "redis_lag_ms": Histogram(
            "bbo_ts_writer_redis_lag_ms",
            "Redis stream lag in ms (now_ms - payload.ts_ms)",
            buckets=(50, 100, 250, 500, 1000, 2000, 5000, 10000, 30000),
        ),
        "pending_count": Gauge(
            "bbo_ts_writer_pending_count",
            "Pending (PEL) size for the consumer group",
        ),
    }


def start_metrics_server(port: int) -> None:
    if start_http_server is None:
        return
    try:
        start_http_server(int(port))
        logger.info("bbo_ts_writer metrics server started on :%s", port)
    except Exception:
        logger.exception("Failed to start metrics server")
