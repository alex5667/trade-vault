from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ------------------------------------------------------------
# Core health metrics (minimum set for SRE gating)
# ------------------------------------------------------------

atr_bad_total = Counter(
    "atr_bad_total"
    "Count of ATR sanity failures"
    ["symbol", "reason"]
)

atr_bad_active = Gauge(
    "atr_bad_active"
    "ATR bad flag currently active (best-effort from Redis keys)"
    ["symbol"]
)

cvd_quarantine_active = Gauge(
    "cvd_quarantine_active"
    "CVD quarantine active (best-effort from Redis keys)"
    ["symbol"]
)

delta_fallback_mode = Gauge(
    "delta_fallback_mode"
    "Delta fallback mode (1=cvd, 2=volume)"
    ["symbol"]
)

microbar_stream_xlen = Gauge(
    "microbar_stream_xlen"
    "Stream length (XLEN) for microbar streams"
    ["stream"]
)

microbar_symbols_active = Gauge(
    "microbar_symbols_active"
    "Number of active symbols in microbar symbols set"
)

redis_used_memory_mb = Gauge(
    "redis_used_memory_mb"
    "Redis used_memory in MB (INFO used_memory)"
)

of_engine_build_seconds = Histogram(
    "of_engine_build_seconds"
    "Latency of OFConfirmEngine.build() in seconds"
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0)
)

ml_missing_critical_total = Counter(
    "ml_missing_critical_total"
    "Count of missing critical ML fields"
    ["field"]
)

lcb_winner_changes_total = Counter(
    "lcb_winner_changes_total"
    "Number of times LCB winner changed"
    ["symbol", "regime", "scenario"]
)

lcb_margin = Gauge(
    "lcb_margin"
    "LCB margin between winner and runner-up"
    ["symbol", "regime", "scenario"]
)

# ------------------------------------------------------------
# ML Confirm Gate SRE metrics (low cardinality)
# ------------------------------------------------------------

ml_confirm_events_total = Counter(
    "ml_confirm_events_total"
    "ML confirm gate decisions/events"
    ["kind", "outcome"],  # outcome: ALLOW/DENY/SHADOW
)

ml_confirm_errors_total = Counter(
    "ml_confirm_errors_total"
    "ML confirm gate errors"
    ["kind", "reason"],  # reason: no_cfg/load_fail/bad_json/invalid_cfg/timeout/exception
)

ml_confirm_cfg_present = Gauge(
    "ml_confirm_cfg_present"
    "Whether cfg:ml_confirm:champion exists in Redis (1/0)"
    ["kind"]
)

ml_confirm_cfg_valid = Gauge(
    "ml_confirm_cfg_valid"
    "Whether cfg:ml_confirm:champion passed validation (1/0)"
    ["kind"]
)

ml_confirm_enforce_share = Gauge(
    "ml_confirm_enforce_share"
    "Current enforce_share from validated champion cfg"
    ["kind"]
)

ml_confirm_model_loaded = Gauge(
    "ml_confirm_model_loaded"
    "Whether champion model is loaded (1/0)"
    ["kind"]
)

ml_confirm_model_load_seconds = Histogram(
    "ml_confirm_model_load_seconds"
    "Time to load the ML model"
    ["kind"]
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10)
)

ml_confirm_latency_seconds = Histogram(
    "ml_confirm_latency_seconds"
    "End-to-end latency of ML confirm gate per event"
    ["kind"]
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1)
)

# labels:tb health — ONLY exported by ml-confirm-sre-poller.
# Do NOT register tb_labels_xlen here: zero-value gauge from python-worker
# and of-confirm-service triggers false TBLabelsEmpty critical alerts.
# See: prometheus/ml_confirm_alerts.yml, services/observability/ml_confirm_sre_poller.py