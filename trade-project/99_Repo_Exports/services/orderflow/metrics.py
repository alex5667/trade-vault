
from typing import List, Optional
import random
import logging
from prometheus_client import Counter, Gauge, Histogram, REGISTRY

def _get_or_create_prom_counter(name: str, documentation: str, labelnames: List[str] = None):
    try:
        if labelnames:
            return Counter(name, documentation, labelnames)
        else:
            return Counter(name, documentation)
    except ValueError:
        # Check if already registered
        for collector in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[collector]:
                return collector
        raise

def _get_or_create_prom_gauge(name: str, documentation: str, labelnames: List[str] = None):
    try:
        if labelnames:
            return Gauge(name, documentation, labelnames)
        else:
            return Gauge(name, documentation)
    except ValueError:
        for collector in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[collector]:
                return collector
        raise

def _get_or_create_prom_histogram(name: str, documentation: str, labelnames: List[str] = None, buckets: List[float] = None):
    try:
        if labelnames:
            return Histogram(name, documentation, labelnames, buckets=buckets or Histogram.DEFAULT_BUCKETS)
        else:
            return Histogram(name, documentation, buckets=buckets or Histogram.DEFAULT_BUCKETS)
    except ValueError:
        for collector in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[collector]:
                return collector
        raise

# Metrics for silent errors
silent_errors_total = _get_or_create_prom_counter(
    "silent_errors_total",
    "Total silent errors (except: pass blocks)",
    ["kind", "symbol", "where"]
)

_SILENT_COUNTS = {}
_SILENT_LAST_FP = {}

# Helper for logging silent exceptions
def log_silent_error(exc: Exception, kind: str, symbol: str = "unknown", context: str = "", sample_rate: int = 100, where: str = ""):
    """
    Log and track silent exceptions that would otherwise be suppressed.
    Args:
        exc: The exception
        kind: Error category (e.g., 'ack_failure', 'persist_failure', 'publish_failure')
        symbol: Symbol name for metrics
        context: Additional context for debugging
        sample_rate: Log every Nth occurrence (default: 100)
        where: Location/component where error occurred (e.g., 'consume_books:parse', 'process_tick:data_health')
    """
    try:
        w = where or context or "unknown"
        silent_errors_total.labels(kind=kind, symbol=symbol, where=w).inc()
        
        key = (kind, symbol)
        c = _SILENT_COUNTS.get(key, 0) + 1
        _SILENT_COUNTS[key] = c
        
        # Deterministic: every Nth OR if fingerprint changed
        fp = (type(exc).__name__, str(exc)[:120])
        last_fp = _SILENT_LAST_FP.get(key)
        
        is_new = last_fp != fp
        is_sampled = sample_rate > 0 and (c % sample_rate == 0)
        
        if is_new or is_sampled:
            _SILENT_LAST_FP[key] = fp
            logging.getLogger("crypto_orderflow").debug(
                "Silent error [%s] for %s: %r ctx=%s n=%d", 
                kind, symbol, exc, context, c
            )
    except Exception:
        pass  # Don't let error tracking itself fail

# Prometheus Metrics
atr_gate_veto_total = _get_or_create_prom_counter(
    'atr_gate_veto_total', 
    'Total signals vetoed by ATR gate', 
    ['symbol', 'reason', 'mode']
)
tp1_net_margin_bps_gauge = _get_or_create_prom_gauge(
    'tp1_net_margin_bps',
    'Net profit margin at TP1 after fees and buffer (bps)',
    ['symbol']
)
tp1_zero_pnl_total = _get_or_create_prom_counter(
    'tp1_zero_pnl_total',
    'Total signals where expected net margin at TP1 is <= 0',
    ['symbol']
)

worker_lag_ms_gauge = _get_or_create_prom_gauge(
    "worker_lag_ms",
    "Lag between wall clock and tick ts_ms at read time (ms)",
    ["symbol"]
)

processing_time_us = _get_or_create_prom_histogram(
    "processing_time_us",
    "Time spent in strategy.process_tick (microseconds)",
    ["symbol"],
    buckets=(50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000)
)

redis_errors_total = _get_or_create_prom_counter(
    "redis_errors_total",
    "Total Redis errors by operation",
    ["op", "symbol"]
)

# Note: histograms are expensive, use separate buckets if critical,
# or just rely on logs/indicators for distribution analysis.

book_rate_ema_gauge = _get_or_create_prom_gauge("book_rate_ema_hz", "Per-symbol book update rate EMA (Hz)", ["symbol"])
book_rate_z_gauge = _get_or_create_prom_gauge("book_rate_z", "Per-symbol book update rate robust z", ["symbol"])
book_stale_ms_gauge = _get_or_create_prom_gauge("book_stale_ms", "Per-symbol book staleness vs tick ts (ms)", ["symbol"])

# ATR Sanity & Floors metrics
atr_sanity_stale_total = _get_or_create_prom_counter("atr_sanity_stale_total", "Count of stale ATR reads", ["symbol"])
atr_sanity_missing_total = _get_or_create_prom_counter("atr_sanity_missing_total", "Count of missing ATR reads", ["symbol"])
atr_floor_ready_gauge = _get_or_create_prom_gauge("atr_floor_ready", "ATR floor calibration ready (0/1)", ["symbol"])
atr_floor_picked_bps_gauge = _get_or_create_prom_gauge("atr_floor_picked_bps", "ATR floor picked threshold in bps", ["symbol", "regime", "tier"])
atr_gate_dominant_total = _get_or_create_prom_counter("atr_gate_dominant_total", "Which ATR gate component dominated", ["symbol", "dominant"])

signals_total = _get_or_create_prom_counter(
    'signals_total',
    'Total number of signals processed by the worker',
    ['symbol', 'handler']
)
fp_imb_confirm_total = _get_or_create_prom_counter(
    'fp_imb_confirm_total',
    'Total footprint imbalance confirmations',
    ['symbol']
)
fp_absorb_confirm_total = _get_or_create_prom_counter(
    'fp_absorb_confirm_total',
    'Total footprint absorption confirmations',
    ['symbol']
)
fp_buckets_evicted_total = _get_or_create_prom_counter(
    'fp_buckets_evicted_total',
    'Total footprint buckets evicted (LRU)',
    ['symbol']
)

# --- Expert Calibration Metrics ---
book_calib_ready = _get_or_create_prom_gauge('book_calib_ready', 'Book rate calibration ready (1=yes)', ['symbol', 'regime'])
dn_calib_ready = _get_or_create_prom_gauge('dn_calib_ready', 'Delta Notional calibration ready (1=yes)', ['symbol', 'regime'])
book_health_ok = _get_or_create_prom_gauge('book_health_ok', 'Book health status (1=OK, 0=Fail)', ['symbol'])
book_rate_hz = _get_or_create_prom_gauge('book_rate_hz', 'Book update rate (Hz, smoothed)', ['symbol'])
dn_usd = _get_or_create_prom_gauge('dn_usd', 'Delta notional (USD) last bar', ['symbol', 'regime'])

# --- Authoritative DN Tiers ---
dn_tier0_usd = _get_or_create_prom_gauge('dn_tier0_usd', 'Authoritative DN tier0 threshold (USD)', ['symbol'])
dn_tier1_usd = _get_or_create_prom_gauge('dn_tier1_usd', 'Authoritative DN tier1 threshold (USD)', ['symbol'])
dn_tier2_usd = _get_or_create_prom_gauge('dn_tier2_usd', 'Authoritative DN tier2 threshold (USD)', ['symbol'])

# --- Telemetry/Shadow DN Tiers ---
ptier_tier0_usd = _get_or_create_prom_gauge('ptier_tier0_usd', 'Telemetry DN tier0 threshold (USD) from ptier_calib', ['symbol'])
ptier_tier1_usd = _get_or_create_prom_gauge('ptier_tier1_usd', 'Telemetry DN tier1 threshold (USD) from ptier_calib', ['symbol'])
ptier_tier2_usd = _get_or_create_prom_gauge('ptier_tier2_usd', 'Telemetry DN tier2 threshold (USD) from ptier_calib', ['symbol'])

dn_calib_n = _get_or_create_prom_gauge('dn_calib_n', 'Number of samples in DN calibrator', ['symbol', 'regime'])

calib_persist_total = _get_or_create_prom_counter('calib_persist_total', 'Calibration persistence events', ['kind', 'symbol', 'regime'])

# --- New Burst/Time Metrics ---
tick_ts_missing_total = _get_or_create_prom_counter('tick_ts_missing_total', 'Total ticks with missing timestamp', ['symbol'])
tick_ts_backwards_total = _get_or_create_prom_counter('tick_ts_backwards_total', 'Total ticks with backwards timestamp', ['symbol'])
tick_ts_clamped_total = _get_or_create_prom_counter('tick_ts_clamped_total', 'Total ticks with clamped monotonicity', ['symbol'])
tick_ts_quarantined_total = _get_or_create_prom_counter('tick_ts_quarantined_total', 'Total ticks discarded due to huge rollback', ['symbol'])
tick_ts_future_total = _get_or_create_prom_counter('tick_ts_future_total', 'Total ticks with event timestamp in the future vs wall-clock', ['symbol'])

# Tick time distribution (observability only; does NOT affect decisions)
tick_age_ms_hist = _get_or_create_prom_histogram(
    "tick_age_ms",
    "Wall-clock age of tick event timestamp at processing time (ms)",
    ["symbol"],
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000, 120_000)
)

tick_reorder_back_ms_hist = _get_or_create_prom_histogram(
    "tick_reorder_back_ms",
    "How far backwards (ms) an out-of-order tick arrived vs last_ts_ms before clamp/drop",
    ["symbol"],
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 1_500, 2_000, 3_000, 5_000, 10_000)
)

tick_time_action_total = _get_or_create_prom_counter(
    "tick_time_action_total",
    "Tick time policy actions (ok/clamp/drop) with reason",
    ["symbol", "action", "reason"]
)
tick_dedup_drop_total = _get_or_create_prom_counter('tick_dedup_drop_total', 'Total duplicate ticks dropped by tick_uid dedup window', ['symbol'])

# Tick time policy decisions (low-cardinality)
tick_time_decision_total = _get_or_create_prom_counter(
    "tick_time_decision_total",
    "Tick time policy decisions (ok/clamp/drop/reorder)",
    ["symbol", "decision"],
)
tick_dedup_dropped_total = _get_or_create_prom_counter('tick_dedup_dropped_total', 'Total ticks dropped by dedupe', ['symbol', 'reason'])
redis_pel_pending_gauge = _get_or_create_prom_gauge('redis_pel_pending', 'Pending length (PEL) for tick stream group', ['symbol'])
redis_pel_claim_total = _get_or_create_prom_counter('redis_pel_claim_total', 'Total pending messages claimed from PEL', ['symbol'])
burst_active_gauge = _get_or_create_prom_gauge('burst_active', 'Burst mode active status (1=active)', ['symbol'])
burst_flush_total = _get_or_create_prom_counter('burst_flush_total', 'Total burst flushes', ['symbol', 'mode'])
signals_emitted_total = _get_or_create_prom_counter('signals_emitted_total', 'Total signals actually emitted', ['symbol'])
burst_window_ms_gauge = _get_or_create_prom_gauge('burst_window_ms', 'Current burst window (ms)', ['symbol'])
tick_gap_p50_ms_gauge = _get_or_create_prom_gauge('tick_gap_p50_ms', 'Tick gap p50 (ms)', ['symbol'])


# --- P2/F: strict DQ signals exported to Prometheus ---
# Low-cardinality, per-symbol. Used by DQ gate policy alerts.

tick_gap_p95_ms_gauge = _get_or_create_prom_gauge('tick_gap_p95_ms', 'Tick gap p95 (ms)', ['symbol'])
tick_gap_n_gauge = _get_or_create_prom_gauge('tick_gap_n', 'Tick gap sample count (rolling window)', ['symbol'])

tick_missing_seq_ema_gauge = _get_or_create_prom_gauge('tick_missing_seq_ema', 'EMA of tick trade_id sequence gaps (0..1)', ['symbol'])
book_missing_seq_ema_gauge = _get_or_create_prom_gauge('book_missing_seq_ema', 'EMA of book update-id sequence gaps (0..1)', ['symbol'])

tick_id_gap_events_total = _get_or_create_prom_counter('tick_id_gap_events_total', 'Total tick trade_id GAP events (tid > last_tid+1)', ['symbol'])
tick_id_dup_events_total = _get_or_create_prom_counter('tick_id_dup_events_total', 'Total tick trade_id DUP events (tid == last_tid)', ['symbol'])
tick_id_reorder_events_total = _get_or_create_prom_counter('tick_id_reorder_events_total', 'Total tick trade_id REORDER events (tid < last_tid)', ['symbol'])

dq_level_gauge = _get_or_create_prom_gauge('dq_level', 'DQ gate level (0=ok,1=soft,2=hard)', ['symbol'])
dq_health_score_gauge = _get_or_create_prom_gauge('dq_health_score', 'DQ gate health score (0..1)', ['symbol'])
dq_pen_gauge = _get_or_create_prom_gauge('dq_pen', 'DQ gate penalty applied to score (>=0)', ['symbol'])

dq_reason_bucket_total = _get_or_create_prom_counter('dq_reason_bucket_total', 'Total DQ degraded events by bucket/level (dq_level>0)', ['symbol', 'bucket', 'level'])
dq_veto_total = _get_or_create_prom_counter('dq_veto_total', 'Total DQ hard veto events (dq_veto==1)', ['symbol', 'bucket', 'reason'])

# --- Drain Mode Metrics ---
drain_forced_cancel_total = _get_or_create_prom_counter(
    'drain_forced_cancel_total',
    'Total number of symbol workers forced to cancel due to drain timeout',
    ['symbol', 'kind']
)

ticks_out_of_order_total = _get_or_create_prom_counter(
    "ticks_out_of_order_total",
    "Total ticks received out of temporal order",
    ["symbol"]
)
ticks_side_unknown_total = _get_or_create_prom_counter(
    "ticks_side_unknown_total",
    "Total ticks with unknown side classification",
    ["symbol"]
)
ticks_unknown_side_policy_total = _get_or_create_prom_counter(
    "ticks_unknown_side_policy_total",
    "Total ticks with unknown side classification by policy",
    ["symbol", "policy"],
)
ticks_unknown_side_quarantine_published_total = _get_or_create_prom_counter(
    "ticks_unknown_side_quarantine_published_total",
    "Total unknown-side ticks published to side-quarantine stream (sampled)",
    ["symbol", "reason"],
)
ticks_dropped_total = _get_or_create_prom_counter(
    "ticks_dropped_total",
    "Total ticks dropped (not processed)",
    ["symbol", "reason"],
)
bars_closed_total = _get_or_create_prom_counter(
    "bars_closed_total",
    "Total micro-bars closed",
    ["symbol", "tf"]
)
divergence_detected_total = _get_or_create_prom_counter(
    "divergence_detected_total",
    "Total divergences detected",
    ["symbol", "kind"]
)
sweep_detected_total = _get_or_create_prom_counter(
    "sweep_detected_total",
    "Total sweeps detected",
    ["symbol", "eq_kind"]
)
strong_gate_veto_total = _get_or_create_prom_counter(
    "strong_gate_veto_total",
    "Total signals vetoed by Strong OF Gate",
    ["symbol", "scenario", "reason", "mode"]
)

divergence_confirmed_total = _get_or_create_prom_counter(
    "divergence_confirmed_total",
    "Total signals where divergence was confirmed",
    ["symbol"]
)
divergence_triggered_total = _get_or_create_prom_counter(
    "divergence_triggered_total",
    "Total signals triggered specifically by divergence",
    ["symbol"]
)
divergence_overridden_total = _get_or_create_prom_counter(
    "divergence_overridden_total",
    "Total divergences overridden by other gates",
    ["symbol"]
)
divergence_suppressed_total = _get_or_create_prom_counter(
    "divergence_suppressed_total",
    "Total divergences suppressed by data quality or other factors",
    ["symbol"]
)

pre_publish_veto_total = _get_or_create_prom_counter(
    "pre_publish_veto_total",
    "Total signals vetoed by pre-publish gates (data-quality/regime/session)",
    ["symbol", "kind", "gate", "reason", "mode"]
)

evidence_used_total = _get_or_create_prom_counter(
    "evidence_used_total",
    "Total strong evidence used in signals",
    ["symbol", "key"]
)

ticks_pressure_filtered_total = _get_or_create_prom_counter(
    "ticks_pressure_filtered_total",
    "Total ticks categorized by delta tier/pressure",
    ["symbol", "reason"]
)

# ATR-TF Selection Metrics
atr_tf_switch_total = _get_or_create_prom_counter(
    "atr_tf_switch_total",
    "Total ATR timeframe switches",
    ["symbol"]
)
atr_tf_candidate_diff = _get_or_create_prom_gauge(
    "atr_tf_candidate_diff",
    "1 if candidate TF differs from selected TF, 0 otherwise",
    ["symbol"]
)
atr_tf_target_bps = _get_or_create_prom_gauge(
    "atr_tf_target_bps",
    "Target ATR in basis points for TF selection",
    ["symbol"]
)
atr_tf_candidate_score = _get_or_create_prom_gauge(
    "atr_tf_candidate_score",
    "Score of candidate TF",
    ["symbol"]
)

# --- DN Telemetry Pass-Rate & HOW Scale ---
dn_tier_attempt_total = _get_or_create_prom_counter(
    "of_dn_tier_attempt_total",
    "DN tier filter attempts",
    ["symbol", "tier", "session"],
)
dn_tier_pass_total = _get_or_create_prom_counter(
    "of_dn_tier_pass_total",
    "DN tier filter passes",
    ["symbol", "tier", "session"],
)
dn_tier_passrate_ema_gauge = _get_or_create_prom_gauge(
    "of_dn_tier_passrate_ema",
    "EMA pass-rate of DN tier filter (telemetry)",
    ["symbol", "tier", "session"],
)

dn_how_scale_gauge = _get_or_create_prom_gauge(
    "of_dn_how_scale",
    "Hour-of-week activity scale (telemetry)",
    ["symbol", "regime"],
)
dn_how_diff_ratio_gauge = _get_or_create_prom_gauge(
    "of_dn_how_diff_ratio",
    "Ratio (>=1) between HOW-scaled static tier and dn_calib tier (telemetry)",
    ["symbol", "tier"],
)
dn_how_diff_alert_total = _get_or_create_prom_counter(
    "of_dn_how_diff_alert_total",
    "Alerts emitted when HOW vs dn_calib diverge",
    ["symbol", "tier", "reason"],
)

of_session_outcome_total = _get_or_create_prom_counter(
    "of_session_outcome_total",
    "Orderflow outcomes by session (trigger, veto, buffer, emit)",
    ["symbol", "session", "outcome"],
)

# --- Diagnostic Metrics (Signal Generation Tracking) ---
ticks_read_total = _get_or_create_prom_counter(
    "ticks_read_total", 
    "Total raw tick messages read from Redis stream", 
    ["symbol"]
)
ticks_processed_total = _get_or_create_prom_counter(
    "ticks_processed_total", 
    "Total tick messages successfully parsed and passed to detectors", 
    ["symbol"]
)

ticks_dedup_dropped_total = _get_or_create_prom_counter(
    "ticks_dedup_dropped_total",
    "Total tick messages dropped by in-memory deduper",
    ["symbol", "mode"],
)

ticks_quarantined_total = _get_or_create_prom_counter(
    "ticks_quarantined_total",
    "Total tick messages quarantined (bad schema/time/exception)",
    ["symbol", "reason"],
)

ticks_schema_invalid_total = _get_or_create_prom_counter(
    "ticks_schema_invalid_total",
    "Total tick messages dropped due to schema/field validation",
    ["symbol", "field"],
)

tick_uid_missing_total = _get_or_create_prom_counter(
    "tick_uid_missing_total",
    "Total ticks where tick_uid could not be produced (dedupe disabled for that tick)",
    ["symbol"],
)

tick_trade_id_missing_total = _get_or_create_prom_counter(
    "tick_trade_id_missing_total",
    "Total ticks missing trade_id (source does not guarantee it)",
    ["symbol"],
)

ticks_deduped_total = _get_or_create_prom_counter(
    "ticks_deduped_total",
    "Total duplicate ticks dropped by tick_uid dedupe",
    ["symbol"],
)

redis_pel_claimed_total = _get_or_create_prom_counter(
    "redis_pel_claimed_total",
    "Total pending Redis stream messages claimed from PEL for recovery",
    ["symbol", "kind"],
)

signals_published_total = _get_or_create_prom_counter(
    "signals_published_total", 
    "Total signals successfully published to all targets (Redis/Telegram/etc)", 
    ["symbol"]
)

veto_min_conf_total = _get_or_create_prom_counter(
    "veto_min_conf_total", 
    "Total signals vetoed due to confidence < CRYPTO_SIGNAL_MIN_CONF", 
    ["symbol"]
)

veto_low_conf_total = _get_or_create_prom_counter(
    "veto_low_conf_total",
    "Total signals vetoed due to confidence < CRYPTO_SIGNAL_LOW_CONF",
    ["symbol"]
)


# ---------------------------------------------------------------------------
# Delta-notional tier gate telemetry (session segmented)
# ---------------------------------------------------------------------------
dn_gate_events_total = _get_or_create_prom_counter(
    "of_dn_gate_events_total",
    "Delta-notional tier gate events (pass/veto) segmented by session",
    ["symbol", "tier", "session", "result"],
)

dn_tier_passrate_ema_gauge = _get_or_create_prom_gauge(
    "of_dn_tier_passrate_ema",
    "EMA pass-rate of DN tier filter segmented by session (telemetry)",
    ["symbol", "tier", "session"],
)

of_hidden_divergence_signal_total = _get_or_create_prom_counter(
    "of_hidden_divergence_signal_total",
    "Total signals where trend direction was confirmed by Hidden Divergence",
    ["symbol"]
)



# CVD Reclaim Metrics
cvd_reclaim_eval_total = _get_or_create_prom_counter(
    'cvd_reclaim_eval_total',
    'Total CVD Reclaim evaluations (computed only on reclaim event)',
    ['symbol', 'bias']
)
cvd_reclaim_ok_total = _get_or_create_prom_counter(
    'cvd_reclaim_ok_total',
    'Total CVD Reclaim OK results',
    ['symbol', 'bias']
)
cvd_reclaim_applied_total = _get_or_create_prom_counter(
    'cvd_reclaim_applied_total',
    'Total times CVD Reclaim evidence was applied to a signal (fresh + aligned)',
    ['symbol', 'bias']
)
cvd_reclaim_no_data_total = _get_or_create_prom_counter(
    'cvd_reclaim_no_data_total',
    'CVD Reclaim evaluate had insufficient data',
    ['symbol', 'reason']
)
cvd_reclaim_ratio_gauge = _get_or_create_prom_gauge(
    'cvd_reclaim_ratio',
    'Latest CVD Reclaim ratio (normalized strength)',
    ['symbol', 'bias']
)
cvd_reclaim_age_ms_gauge = _get_or_create_prom_gauge(
    'cvd_reclaim_age_ms',
    'Age of last CVD Reclaim event when applied to signal (ms)',
    ['symbol', 'bias']
)
cvd_reclaim_window_ms_gauge = _get_or_create_prom_gauge(
    'cvd_reclaim_window_ms',
    'Window length used for CVD Reclaim eval (ms)',
    ['symbol', 'bias']
)
obi_stability_score_gauge = _get_or_create_prom_gauge(
    'obi_stability_score',
    'Latest OBI stability quality score (0..1) for symbol',
    ['symbol']
)

# --- OF Inputs Version & Missing Fields Metrics ---
of_inputs_version_total = _get_or_create_prom_counter(
    'of_inputs_version_total',
    'Total OF inputs published by version',
    ['symbol', 'version']
)

of_inputs_missing_ofi_total = _get_or_create_prom_counter(
    'of_inputs_missing_ofi_total',
    'Total OF inputs missing OFI fields (v1 or v2 without OFI)',
    ['symbol']
)

of_inputs_missing_fp_total = _get_or_create_prom_counter(
    'of_inputs_missing_fp_total',
    'Total OF inputs missing FP edge fields (v1 or v2 without FP edge)',
    ['symbol']
)

of_inputs_bad_time_total = _get_or_create_prom_counter(
    'of_inputs_bad_time_total',
    'Total OF inputs skipped due to invalid tick_ts_ms (non-deterministic / bad tick time)',
    ['symbol']
)

# --- OF Inputs V3 downgrade / quarantine / DLQ observability (P96) ---
of_inputs_missing_lob_total = _get_or_create_prom_counter(
    'of_inputs_missing_lob_total',
    'Total OF inputs where v3 was requested but LOB fields were missing/degraded (downgraded to v2)',
    ['symbol', 'reason']
)

of_inputs_downgrade_total = _get_or_create_prom_counter(
    'of_inputs_downgrade_total',
    'Total OF inputs automatic version downgrades (e.g., v3->v2) with reason',
    ['symbol', 'from_version', 'to_version', 'reason']
)

of_inputs_quarantined_total = _get_or_create_prom_counter(
    'of_inputs_quarantined_total',
    'Total OF inputs written to quarantine stream for triage (low-cardinality reasons)',
    ['symbol', 'reason', 'attempt_version', 'published_version']
)

of_inputs_publish_error_total = _get_or_create_prom_counter(
    'of_inputs_publish_error_total',
    'Total OF inputs publish errors (DLQ writes) by stage',
    ['symbol', 'stage']
)


# --- OF Inputs V3 circuit breaker (P100) ---
of_inputs_v3_forced_v2_total = _get_or_create_prom_counter(
    "of_inputs_v3_forced_v2_total",
    "Total times V3 was requested but V2 was published (forced downgrade), by reason",
    ["symbol", "reason"],
)

of_inputs_v3_circuit_trip_total = _get_or_create_prom_counter(
    "of_inputs_v3_circuit_trip_total",
    "Total times OFInputs V3 circuit breaker tripped (set cfg disable), by downgrade reason",
    ["symbol", "reason"],
)

of_inputs_v3_circuit_disabled = _get_or_create_prom_gauge(
    "of_inputs_v3_circuit_disabled",
    "Gauge: OFInputs V3 is currently disabled by circuit breaker (1/0)",
    ["symbol"],
)

of_inputs_v3_circuit_disabled_until_ms = _get_or_create_prom_gauge(
    "of_inputs_v3_circuit_disabled_until_ms",
    "If disabled, wall-clock epoch ms until which V3 remains disabled (best-effort)",
    ["symbol"],
)

of_inputs_v3_circuit_hard_disabled_until_ms = _get_or_create_prom_gauge(
    "of_inputs_v3_circuit_hard_disabled_until_ms",
    "If disabled, epoch ms until which HARD-disable phase ends (before cooldown) (best-effort)",
    ["symbol"],
)

# A1.1: LiqMap snapshot staleness observability — mirror sync with python-worker.
# Gauge (also present above in python-worker): Age in ms; -1=missing, -2=parse error.
liqmap_snapshot_age_ms_gauge = _get_or_create_prom_gauge(
    "liqmap_snapshot_age_ms",
    "Age of the last parsed LiqMap snapshot (ms). Sentinel values: -1=missing, -2=parse/compute error.",
    ["symbol", "window"],
)

# A1.1: Per-(symbol, window, where) parse/compute error counter.
# Labels:
#   symbol: market symbol (e.g., BTCUSDT)
#   window: snapshot window (e.g., 1h, 5m)
#   where:  small enum for failing step — keep low-cardinality (e.g., "parse_or_compute").
liqmap_parse_errors_total = _get_or_create_prom_counter(
    "liqmap_parse_errors_total",
    "Total LiqMap snapshot parse/validation/feature-compute errors (fail-open). "
    "Sentinel values on liqmap_snapshot_age_ms: -1=missing, -2=parse/compute error.",
    ["symbol", "window", "where"],
)
