"""Prometheus metrics for cross-venue context gate (Python-side).

Naming convention: crossvenue_ctx_*  (matches Go-side crossvenue_* naming).

All metrics are lazy-initialized to avoid import-time registration conflicts.
"""

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Missing / stale context
# ---------------------------------------------------------------------------

crossvenue_ctx_missing_total = Counter(
    "crossvenue_ctx_missing_total",
    "Cross-venue context not found in Redis (TTL expired or collector down)",
    ["symbol"],
)

crossvenue_ctx_stale_total = Counter(
    "crossvenue_ctx_stale_total",
    "Cross-venue context found but age exceeds max_age_ms threshold",
    ["symbol"],
)

# ---------------------------------------------------------------------------
# Gate outcomes
# ---------------------------------------------------------------------------

crossvenue_ctx_gate_monitor_hit_total = Counter(
    "crossvenue_ctx_gate_monitor_hit_total",
    "Cross-venue gate triggered at least one flag in monitor mode (annotate-only)",
    ["symbol", "profile"],
)

crossvenue_ctx_gate_tighten_total = Counter(
    "crossvenue_ctx_gate_tighten_total",
    "Cross-venue gate applied tighten_add_bps (strict/tighten mode)",
    ["symbol", "reason"],
)

crossvenue_ctx_gate_veto_total = Counter(
    "crossvenue_ctx_gate_veto_total",
    "Cross-venue gate issued hard veto (veto mode, ≥2 adverse flags)",
    ["symbol", "reason"],
)

# ---------------------------------------------------------------------------
# Feature observability (gauge per symbol for Grafana)
# ---------------------------------------------------------------------------

crossvenue_ctx_mid_spread_bps = Gauge(
    "crossvenue_ctx_mid_spread_bps",
    "Latest cross-venue mid spread in bps (max - min across active venues)",
    ["symbol"],
)

crossvenue_ctx_direction_agree = Gauge(
    "crossvenue_ctx_direction_agree",
    "Latest cross-venue direction agreement fraction [0, 1]",
    ["symbol"],
)

crossvenue_ctx_dislocation_z = Gauge(
    "crossvenue_ctx_dislocation_z",
    "Latest venue dislocation robust-z score",
    ["symbol"],
)

crossvenue_ctx_snapshot_age_ms = Histogram(
    "crossvenue_ctx_snapshot_age_ms",
    "Age of cross-venue context snapshot at read time (ms)",
    ["symbol"],
    buckets=[100, 250, 500, 1000, 2000, 5000, 10000],
)
