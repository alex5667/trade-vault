from __future__ import annotations
"""Prometheus metrics for Book missing-seq + DQ policy (P112).

Why a separate module?
----------------------
In the codebase there are multiple entrypoints (SoT + mirror) and historically
metrics definitions were spread across modules. Import-time re-definition of the
same metric name in prometheus_client *raises* a ValueError.

To keep rollout safe, this module uses a "get-or-create" helper backed by the
global prometheus registry. This makes the module idempotent:
  - If another module already defined the metric, we reuse it.
  - If not, we define it with the canonical name/spec.

DoD constraints:
  - No high-cardinality labels: only {symbol} and {symbol,bucket}.
  - Buckets are finite and sanitized.
"""


import logging
from typing import Optional, Sequence, Type, TypeVar

try:
    # prometheus_client is an optional runtime dependency in some dev setups.
    from prometheus_client import Counter, Gauge, REGISTRY  # type: ignore
    from prometheus_client.registry import Collector  # type: ignore
except Exception:  # pragma: no cover
    Counter = Gauge = object  # type: ignore
    REGISTRY = None  # type: ignore
    Collector = object  # type: ignore


logger = logging.getLogger("orderflow_metrics_bookseq_dq")

TCollector = TypeVar("TCollector", bound="Collector")


def _get_or_create(
    name: str,
    ctor: Type[TCollector],
    documentation: str,
    labelnames: Sequence[str] = (),
):
    """Return an existing collector from the default registry, or create one.

    prometheus_client keeps a global name->collector mapping in REGISTRY.
    Accessing internal REGISTRY fields is intentional here to guarantee
    idempotency across multiple import paths.
    """
    if REGISTRY is None:  # pragma: no cover
        return None

    # pylint: disable=protected-access
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing is not None:
        # Defensive: if the metric exists but has an unexpected type, keep it
        # (to avoid crashing the process) but log loudly.
        # Guard: when prometheus_client is unavailable, ctor may be `object`
        # which is not a valid second arg to isinstance() in Python 3.12+.
        try:
            type_mismatch = isinstance(ctor, type) and not isinstance(existing, ctor)
        except TypeError:
            type_mismatch = False
        if type_mismatch:
            logger.error(
                "Metric name collision: %s exists as %s, expected %s",
                name,
                type(existing).__name__,
                getattr(ctor, "__name__", repr(ctor)),
            )
        return existing

    try:
        return ctor(name, documentation, labelnames=tuple(labelnames))
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to create prometheus metric %s: %s", name, exc)
        return None


# -----------------------------
# Book missing-seq telemetry
# -----------------------------

# Gauge: bounded EMA in [0..1]
book_missing_seq_ema_gauge = _get_or_create(
    "book_missing_seq_ema_gauge",
    Gauge,
    "EMA of book missing-seq events (bounded telemetry for DQ gate)",
    labelnames=("symbol",),
)

# Optional gauge: last gap size (updates count) for runbook/diagnostics.
book_seq_last_gap_gauge = _get_or_create(
    "book_seq_last_gap_gauge",
    Gauge,
    "Last detected book missing update gap (count of missing updateIds)",
    labelnames=("symbol",),
)

# Counter book_missing_seq_events_total is defined in services.orderflow.metrics.
# Do NOT re-define it here to avoid duplicate registration on alternative import orders.


# -----------------------------
# DQ gate telemetry
# -----------------------------

dq_level_gauge = _get_or_create(
    "dq_level_gauge",
    Gauge,
    "DQ gate level (0=ok, 1=warn/soft, 2=hard)",
    labelnames=("symbol",),
)

_DQ_BUCKET_ALLOWED = {
    "book_seq",
    "tick_seq",
    "gap_p95",
    "data_health",
    "other",
}


def sanitize_dq_bucket(bucket: Optional[str]) -> str:
    """Sanitize dq bucket to a finite set to avoid high cardinality labels."""
    if not bucket:
        return "other"
    b = str(bucket).strip().lower()
    if b in _DQ_BUCKET_ALLOWED:
        return b
    # Back-compat mapping
    if b in ("gap", "tick_gap", "tick_gap_p95"):
        return "gap_p95"
    if b in ("book", "book_gap", "book_seq_hard"):
        return "book_seq"
    if b in ("tick", "tick_seq_hard"):
        return "tick_seq"
    if b in ("health", "dq_health"):
        return "data_health"
    return "other"


dq_veto_total = _get_or_create(
    "dq_veto_total",
    Counter,
    "Total count of DQ hard veto events",
    labelnames=("symbol", "bucket"),
)


def emit_dq_metrics(symbol: str, dq_level: int, dq_veto: int, bucket: Optional[str]) -> None:
    """Best-effort emission of dq gate metrics.

    Intended to be called from tick_processor/of_confirm_engine after
    dq_level/dq_veto/bucket are computed.
    """
    sym = str(symbol)
    try:
        if dq_level_gauge is not None:
            dq_level_gauge.labels(symbol=sym).set(int(dq_level))
    except Exception:
        pass

    if int(dq_veto or 0) == 1:
        try:
            if dq_veto_total is not None:
                dq_veto_total.labels(symbol=sym, bucket=sanitize_dq_bucket(bucket)).inc()
        except Exception:
            pass
