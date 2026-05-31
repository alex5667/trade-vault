from __future__ import annotations

"""Signal-quality metrics (Step 6).

This module defines low-cardinality Prometheus metrics used to monitor
data-quality (DQ) signals and continuity issues in production.

Why a dedicated module?
- Keeps the Step-6 metric contract self-contained and mirror-friendly.
- Avoids continuously growing the already-large `services.orderflow.metrics`.

Cardinality rules:
- label `symbol` is allowed.
- label `bucket` MUST be from a small fixed allowlist.
  Never put free-form reasons or IDs into labels.
"""


from prometheus_client import Counter, Gauge

_DQ_BUCKET_ALLOWLIST = {
    # Contract buckets (alerting + dashboards)
    "book_seq",
    "tick_seq",
    "gap_p95",
    "data_health",
    "other",
    # internal / no-veto bucket (kept for safety)
    "ok",
}


def sanitize_dq_bucket(bucket: str) -> str:
    """Clamp bucket to a small allowlist.

    This protects Prometheus from accidental high-cardinality growth when
    runtime code starts emitting new reason strings.
    """
    b = (bucket or "").strip().lower()
    if b in _DQ_BUCKET_ALLOWLIST:
        return b
    return "other"


# ---------------------------------------------------------------------------
# Metrics (names are part of the external contract)
# ---------------------------------------------------------------------------

# DQ gate level (0/1/2) emitted per symbol.
dq_level_gauge = Gauge(
    "dq_level",
    "DQ gate severity level (0/1/2).",
    labelnames=["symbol"],
)

# DQ veto counter bucketed by primary DQ bucket.
dq_veto_total = Counter(
    "dq_veto_total",
    "Number of times DQ gate applied veto (dq_veto==1).",
    labelnames=["symbol", "bucket"],
)

# Tick-gap sample count used to gate alerting (min_samples).
tick_gap_n_gauge = Gauge(
    "tick_gap_n",
    "Rolling sample count for tick_gap_pXX_ms trackers.",
    labelnames=["symbol"],
)
