from __future__ import annotations

from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, Histogram

# ------------------------------------------------------------
# Idempotent Metric Factory
# ------------------------------------------------------------

def _metric(factory: Any, name: str, documentation: str, labelnames: list[str] | None = None, **kwargs):
    """Returns an existing metric if already registered, otherwise creates a new one."""
    try:
        if labelnames:
            return factory(name, documentation, labelnames, **kwargs)
        return factory(name, documentation, **kwargs)
    except ValueError:
        # Check if already registered in the default registry
        return getattr(REGISTRY, '_names_to_collectors', {}).get(name)

# ------------------------------------------------------------
# Core health metrics (minimum set for SRE gating)
# ------------------------------------------------------------

atr_bad_total = _metric(Counter, "atr_bad_total", "Count of ATR sanity failures", ["symbol", "reason"])
atr_bad_active = _metric(Gauge, "atr_bad_active", "ATR bad flag currently active", ["symbol"])

cvd_quarantine_active = _metric(Gauge, "cvd_quarantine_active", "CVD quarantine active", ["symbol"])
delta_fallback_mode = _metric(Gauge, "delta_fallback_mode", "Delta fallback mode (1=cvd, 2=volume)", ["symbol"])

microbar_stream_xlen = _metric(Gauge, "microbar_stream_xlen", "Stream length (XLEN) for microbar streams", ["stream"])
microbar_symbols_active = _metric(Gauge, "microbar_symbols_active", "Number of active symbols in microbar symbols set")

redis_used_memory_mb = _metric(Gauge, "redis_used_memory_mb", "Redis used_memory in MB (INFO used_memory)")

of_engine_build_seconds = _metric(
    Histogram,
    "of_engine_build_seconds",
    "Latency of OFConfirmEngine.build() in seconds",
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0)
)

ml_missing_critical_total = _metric(Counter, "ml_missing_critical_total", "Count of missing critical ML fields", ["field"])

lcb_winner_changes_total = _metric(
    Counter, "lcb_winner_changes_total", "Number of times LCB winner changed", ["symbol", "regime", "scenario"]
)
lcb_margin = _metric(Gauge, "lcb_margin", "LCB margin between winner and runner-up", ["symbol", "regime", "scenario"])

# ------------------------------------------------------------
# ML Confirm Gate SRE metrics (low cardinality)
# ------------------------------------------------------------

ml_confirm_events_total = _metric(
    Counter, "ml_confirm_events_total", "ML confirm gate decisions/events", ["ab_variant", "kind", "outcome"]
)
ml_confirm_errors_total = _metric(
    Counter, "ml_confirm_errors_total", "ML confirm gate errors", ["kind", "reason"]
)
ml_confirm_cfg_present = _metric(Gauge, "ml_confirm_cfg_present", "Whether ML config exists", ["kind"])
ml_confirm_cfg_valid = _metric(Gauge, "ml_confirm_cfg_valid", "Whether ML config is valid", ["kind"])
ml_confirm_enforce_share = _metric(Gauge, "ml_confirm_enforce_share", "Current enforce_share", ["kind"])
ml_confirm_model_loaded = _metric(Gauge, "ml_confirm_model_loaded", "Whether ML model is loaded", ["kind"])

ml_confirm_model_load_seconds = _metric(
    Histogram, "ml_confirm_model_load_seconds", "Time to load the ML model", ["kind"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10)
)
ml_confirm_latency_seconds = _metric(
    Histogram, "ml_confirm_latency_seconds", "End-to-end latency of ML confirm gate", ["kind"],
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1)
)

# ------------------------------------------------------------
# ATR Disaster Recovery & Promotion (P4.5)
# ------------------------------------------------------------

atr_restore_safe_mode = _metric(Gauge, "atr_restore_safe_mode", "Active safe modes during DR", ["state"])
atr_promotion_hold_total = _metric(Counter, "atr_promotion_hold_total", "Count of promotion holds", ["status"])
atr_promotion_rollback_review_total = _metric(Counter, "atr_promotion_rollback_review_total", "Rollback reviews triggered")

# ------------------------------------------------------------
# Latency & Performance (P4.1 Remediation)
# ------------------------------------------------------------

worker_lag_ms_p99 = _metric(Gauge, "worker_lag_ms_p99", "99th percentile of worker lag (ms)", ["symbol"])
signal_emit_latency_us = _metric(
    Histogram, "signal_emit_latency_us", "Signal emit latency (us)", ["symbol", "stream"],
    buckets=(50, 100, 250, 500, 1000, 2000, 5000, 8000, 15000, 30000)
)

ml_inference_time_us = _metric(
    Histogram, "ml_inference_time_us", "Time spent performing MetaModelLR inference (us)", ["symbol", "model"],
    buckets=(500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 250000, 500000)
)

ml_telemetry_io_time_us = _metric(
    Histogram, "ml_telemetry_io_time_us", "Time spent in Redis IO for telemetry continuation (us)", ["symbol"],
    buckets=(100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000)
)

# ------------------------------------------------------------
# Rules Bundle Health
# ------------------------------------------------------------

of_prom_rules_bundle_last_ok = _metric(Gauge, "of_prom_rules_bundle_last_ok", "Rules bundle health status (1=OK)")
of_prom_rules_bundle_last_error_n = _metric(Counter, "of_prom_rules_bundle_last_error_n", "Rules bundle error count")

# ------------------------------------------------------------
# Data-quality metrics
# ------------------------------------------------------------

ts_rejected_total = _metric(
    Counter, "ts_rejected_total", "Timestamps rejected by normalization", ["source", "reason"]
)

dq_flag_rate = _metric(Gauge, "dq_flag_rate", "Rate of DQ flags applied", ["flag", "symbol"])

notional_clamped_total = _metric(Counter, "notional_clamped_total", "Count of trade quantity clampings", ["symbol"])

pipeline_stage_ms = _metric(
    Histogram, "pipeline_stage_ms", "Latency of individual pipeline stages", ["kind", "symbol", "stage"],
    buckets=(0.5, 1, 2, 5, 10, 20, 50, 100, 250, 500, 1000)
)

pipeline_candidates_total = _metric(Counter, "pipeline_candidates_total", "Total pipeline candidates", ["kind", "symbol"])
pipeline_veto_total = _metric(Counter, "pipeline_veto_total", "Total pipeline vetoes", ["kind", "symbol", "reason_code", "cfg_hash"])
pipeline_emit_ok_total = _metric(Counter, "pipeline_emit_ok_total", "Total pipeline emissions", ["kind", "symbol", "cfg_hash"])

signal_dq_flag_total = _metric(
    Counter, "signal_dq_flag_total", "Count of DQ flags in signal pipeline", ["flag", "symbol"]
)

# ------------------------------------------------------------
# ML Governance & Monitoring (P5-P6)
# ------------------------------------------------------------

ml_psi_drift = _metric(Gauge, "ml_psi_drift", "Feature distribution drift (PSI)", ["feature", "model_v"])
ml_signal_expectation = _metric(Gauge, "ml_signal_expectation", "Signal expected value (expectation)", ["side", "kind", "regime"])
ml_calibration_brier_score = _metric(Gauge, "ml_calibration_brier_score", "Model calibration Brier score", ["model_v"])
ml_calibration_ece = _metric(Gauge, "ml_calibration_ece", "Model calibration Expected Calibration Error (ECE)", ["model_v"])
ml_feature_missing_rate = _metric(Gauge, "ml_feature_missing_rate", "Feature missing rate in inference", ["feature"])

# P1: DLQ emission failures visibility
dlq_xadd_errors_total = _metric(
    Counter, "dlq_xadd_errors_total", "Total signal veto DLQ emission failures", ["symbol", "kind"]
)

# P2: Schema versioning fallback visibility
schema_version_fallback_total = _metric(
    Counter, "schema_version_fallback_total", "Total signals falling back to schema version 1", ["symbol", "kind"]
)

strong_gate_veto_total = _metric(Counter, "strong_gate_veto_total", "Total vetoes by OFConfirm Strong Gate", ["symbol", "scenario", "reason", "mode"])

# Shadow telemetry for burst gate penalty→enforce promotion.
# Incremented whenever veto conditions are met regardless of mode (penalty/shadow/enforce).
# Collect ≥7 days before promoting burst_gate_mode to enforce.
burst_gate_would_veto_total = _metric(
    Counter,
    "burst_gate_would_veto_total",
    "Burst gate would-veto events (shadow telemetry, all modes)",
    ["symbol", "reason", "mode"],
)
