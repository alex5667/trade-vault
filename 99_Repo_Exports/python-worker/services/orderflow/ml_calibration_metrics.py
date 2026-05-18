"""ML calibration Prometheus metrics.

Tracks offline-computed calibration quality (ECE, Brier, precision@top5%)
per schema/bucket, published after each calibration window evaluation.

Usage:
    from services.orderflow.ml_calibration_metrics import (
        record_ece, record_brier, record_precision_top5,
        record_p_edge_drift_psi, record_expected_value_bps,
        record_inference_latency_us, record_schema_mismatch,
        record_calibration_rows,
    )
"""
from __future__ import annotations

import logging
from collections.abc import Sequence

from prometheus_client import REGISTRY, Counter, Gauge, Histogram

logger = logging.getLogger(__name__)


def _counter(name: str, doc: str, labels: Sequence[str] | None = None) -> Counter:
    try:
        return Counter(name, doc, list(labels or []))
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


def _gauge(name: str, doc: str, labels: Sequence[str] | None = None) -> Gauge:
    try:
        return Gauge(name, doc, list(labels or []))
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


def _histogram(
    name: str, doc: str,
    labels: Sequence[str] | None = None,
    buckets: Sequence[float] | None = None,
) -> Histogram:
    try:
        return Histogram(name, doc, list(labels or []), buckets=buckets or Histogram.DEFAULT_BUCKETS)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


# ------------------------------------------------------------------
# Calibration quality gauges (written after offline eval window)
# Labels: schema, bucket (symbol | session | scenario | "all")
# ------------------------------------------------------------------
ml_ece = _gauge(
    "ml_ece",
    "Expected Calibration Error [0,1] per schema/bucket (lower = better)",
    ["schema", "bucket"],
)

ml_brier = _gauge(
    "ml_brier",
    "Brier score per schema/bucket (lower = better)",
    ["schema", "bucket"],
)

ml_precision_top5 = _gauge(
    "ml_precision_top5",
    "Precision at top-5% predicted probability per schema/bucket",
    ["schema", "bucket"],
)

ml_p_edge_drift_psi = _gauge(
    "ml_p_edge_drift_psi",
    "PSI of p_edge distribution vs reference window per schema/bucket",
    ["schema", "bucket"],
)

ml_expected_value_bps = _gauge(
    "ml_expected_value_bps",
    "Expected value (bps) per schema/bucket from calibrated p_edge",
    ["schema", "bucket"],
)

ml_calibration_rows = _gauge(
    "ml_calibration_rows",
    "Number of labeled rows in current calibration window per schema/bucket",
    ["schema", "bucket"],
)

# ------------------------------------------------------------------
# Online inference counters (hot path)
# ------------------------------------------------------------------
ml_inference_requests_total = _counter(
    "ml_inference_requests_total",
    "Total ML gate inference requests",
    ["schema", "mode"],
)

ml_inference_errors_total = _counter(
    "ml_inference_errors_total",
    "Total ML gate inference errors",
    ["schema", "error_type"],
)

ml_schema_mismatch_total = _counter(
    "ml_schema_mismatch_total",
    "Model schema does not match code schema — forced SHADOW",
    ["model_schema", "code_schema"],
)

ml_abstain_total = _counter(
    "ml_abstain_total",
    "Abstain decisions (low confidence / missing critical features)",
    ["schema", "reason"],
)

# ------------------------------------------------------------------
# Latency histogram for hot-path inference (p99 SLO < 5 ms)
# ------------------------------------------------------------------
ml_inference_latency_us = _histogram(
    "ml_inference_latency_us",
    "ML gate inference latency in microseconds",
    ["schema"],
    buckets=[100, 250, 500, 1_000, 2_000, 5_000, 10_000, 25_000, 50_000],
)


# ------------------------------------------------------------------
# Helper functions called from calibration evaluation jobs
# ------------------------------------------------------------------

def record_ece(schema: str, bucket: str, value: float) -> None:
    try:
        ml_ece.labels(schema=schema, bucket=bucket).set(value)
    except Exception:
        logger.debug("record_ece failed", exc_info=True)


def record_brier(schema: str, bucket: str, value: float) -> None:
    try:
        ml_brier.labels(schema=schema, bucket=bucket).set(value)
    except Exception:
        logger.debug("record_brier failed", exc_info=True)


def record_precision_top5(schema: str, bucket: str, value: float) -> None:
    try:
        ml_precision_top5.labels(schema=schema, bucket=bucket).set(value)
    except Exception:
        logger.debug("record_precision_top5 failed", exc_info=True)


def record_p_edge_drift_psi(schema: str, bucket: str, value: float) -> None:
    try:
        ml_p_edge_drift_psi.labels(schema=schema, bucket=bucket).set(value)
    except Exception:
        logger.debug("record_p_edge_drift_psi failed", exc_info=True)


def record_expected_value_bps(schema: str, bucket: str, value: float) -> None:
    try:
        ml_expected_value_bps.labels(schema=schema, bucket=bucket).set(value)
    except Exception:
        logger.debug("record_expected_value_bps failed", exc_info=True)


def record_calibration_rows(schema: str, bucket: str, n: int) -> None:
    try:
        ml_calibration_rows.labels(schema=schema, bucket=bucket).set(float(n))
    except Exception:
        logger.debug("record_calibration_rows failed", exc_info=True)


def record_schema_mismatch(model_schema: str, code_schema: str) -> None:
    try:
        ml_schema_mismatch_total.labels(
            model_schema=model_schema, code_schema=code_schema
        ).inc()
    except Exception:
        logger.debug("record_schema_mismatch failed", exc_info=True)


def record_inference_latency_us(schema: str, latency_us: float) -> None:
    try:
        ml_inference_latency_us.labels(schema=schema).observe(latency_us)
    except Exception:
        logger.debug("record_inference_latency_us failed", exc_info=True)
