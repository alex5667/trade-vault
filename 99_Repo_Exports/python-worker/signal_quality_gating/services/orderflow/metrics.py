
from typing import List, Dict, Set, Tuple
import logging
import re
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

# Trade close joiner metrics (P46)
trade_close_joiner_seen_total = _get_or_create_prom_counter(
    "trade_close_joiner_seen_total",
    "Total POSITION_CLOSED events seen by joiner",
    ["symbol"]
)

trade_close_joiner_join_ok_total = _get_or_create_prom_counter(
    "trade_close_joiner_join_ok_total",
    "Total successful decision:{sid} joins on POSITION_CLOSED",
    ["symbol", "where"]
)

trade_close_joiner_missing_decision_total = _get_or_create_prom_counter(
    "trade_close_joiner_missing_decision_total",
    "Total POSITION_CLOSED events missing decision:{sid} (queued for backfill)",
    ["symbol", "where"]
)

trade_close_joiner_written_total = _get_or_create_prom_counter(
    "trade_close_joiner_written_total",
    "Total enriched rows written by joiner",
    ["stream", "symbol"]
)

trade_close_joiner_dedup_skipped_total = _get_or_create_prom_counter(
    "trade_close_joiner_dedup_skipped_total",
    "Total deduplicated (already processed) close events",
    ["symbol", "where"]
)

trade_close_joiner_backfill_ok_total = _get_or_create_prom_counter(
    "trade_close_joiner_backfill_ok_total",
    "Total successful joins from close wait backfill",
    ["symbol"]
)

trade_close_joiner_backfill_drop_total = _get_or_create_prom_counter(
    "trade_close_joiner_backfill_drop_total",
    "Total wait entries dropped during backfill",
    ["symbol", "reason"]
)

# Metrics for silent errors
silent_errors_total = _get_or_create_prom_counter(
    "silent_errors_total",
    "Total silent errors (except: pass blocks)",
    ["kind", "symbol", "where"]
)

# Decision record metrics (P45/P48)
decision_record_written_total = _get_or_create_prom_counter(
    "decision_record_written_total",
    "Total decision records written to Redis (decision:{sid} + decisions:final)",
    ["symbol", "stage", "result"]
)

decision_record_sampled_out_total = _get_or_create_prom_counter(
    "decision_record_sampled_out_total",
    "Total decision records skipped due to sampling",
    ["symbol", "stage"]
)


decision_record_sampled_out_total = _get_or_create_prom_counter(
    "decision_record_sampled_out_total",
    "Total decision records skipped due to sampling",
    ["symbol", "stage"]
)


decision_record_error_total = _get_or_create_prom_counter(
    "decision_record_error_total",
    "Total decision record write errors",
    ["symbol"]
)


# Signal Quality KPI Worker Metrics (P47)
signal_quality_kpi_runs_total = _get_or_create_prom_counter(
    "signal_quality_kpi_runs_total",
    "Total runs of the KPI calculation worker",
    ["result"]
)

signal_quality_kpi_rows = _get_or_create_prom_gauge(
    "signal_quality_kpi_rows",
    "Number of closed trades rows processed in last run",
    []
)

signal_quality_kpi_groups = _get_or_create_prom_gauge(
    "signal_quality_kpi_groups",
    "Number of groups computed in last run",
    []
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

worker_lag_ms_p50_gauge = _get_or_create_prom_gauge(
    "worker_lag_ms_p50",
    "50th percentile of worker lag (ms)",
    ["symbol"]
)

worker_lag_ms_p95_gauge = _get_or_create_prom_gauge(
    "worker_lag_ms_p95",
    "95th percentile of worker lag (ms)",
    ["symbol"]
)

worker_lag_ms_p99_gauge = _get_or_create_prom_gauge(
    "worker_lag_ms_p99",
    "99th percentile of worker lag (ms)",
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

# Bad time quarantine metrics
tick_time_quarantine_active_gauge = _get_or_create_prom_gauge(
    'tick_time_quarantine_active',
    'Bad time quarantine active status (1=active, 0=inactive)',
    ['symbol']
)
tick_time_quarantine_enabled_total = _get_or_create_prom_counter(
    'tick_time_quarantine_enabled_total',
    'Total times bad time quarantine was enabled',
    ['symbol', 'reason']
)
tick_time_hard_drop_total = _get_or_create_prom_counter(
    'tick_time_hard_drop_total',
    'Total hard drops due to bad time (future/past/reorder_hard)',
    ['symbol', 'reason']
)
tick_time_soft_event_total = _get_or_create_prom_counter(
    'tick_time_soft_event_total',
    'Total soft time events (clamped/reorder_soft)',
    ['symbol', 'flag']
)
tick_time_state_freeze_total = _get_or_create_prom_counter(
    'tick_time_state_freeze_total',
    'Total state freezes due to bad time',
    ['symbol']
)
tick_time_recovery_passed_total = _get_or_create_prom_counter(
    'tick_time_recovery_passed_total',
    'Total recovery gates passed after state freeze',
    ['symbol']
)
tick_time_quarantine_score_gauge = _get_or_create_prom_gauge(
    'tick_time_quarantine_score',
    'Current bad time quarantine score',
    ['symbol']
)
burst_active_gauge = _get_or_create_prom_gauge('burst_active', 'Burst mode active status (1=active)', ['symbol'])
burst_flush_total = _get_or_create_prom_counter('burst_flush_total', 'Total burst flushes', ['symbol', 'mode'])
signals_emitted_total = _get_or_create_prom_counter('signals_emitted_total', 'Total signals actually emitted', ['symbol'])
burst_window_ms_gauge = _get_or_create_prom_gauge('burst_window_ms', 'Current burst window (ms)', ['symbol'])
tick_gap_p50_ms_gauge = _get_or_create_prom_gauge('tick_gap_p50_ms', 'Tick gap p50 (ms)', ['symbol'])

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

# Unknown-side policy metrics (ingestion-time policy decisions)
ticks_unknown_side_policy_total = _get_or_create_prom_counter(
    "ticks_unknown_side_policy_total",
    "Total ticks with unknown side classification by policy",
    ["symbol", "policy"]
)

ticks_unknown_side_quarantine_published_total = _get_or_create_prom_counter(
    "ticks_unknown_side_quarantine_published_total",
    "Total unknown-side ticks published to side-quarantine stream (sampled)",
    ["symbol", "reason"]
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

divergence_bias_source_total = _get_or_create_prom_counter(
    "divergence_bias_source_total",
    "Total divergences by effective bias source (cont_ctx/breakout/regime/rsi/div_infer/none)",
    ["symbol", "source", "kind"]
)
divergence_bias_inferred_total = _get_or_create_prom_counter(
    "divergence_bias_inferred_total",
    "Total divergences where direction was inferred (regular divergence inference)",
    ["symbol", "kind"]
)

sweep_detected_total = _get_or_create_prom_counter(
    "sweep_detected_total",
    "Total sweeps detected",
    ["symbol", "eq_kind"]
)
sweep_side_missing_total = _get_or_create_prom_counter(
    "sweep_side_missing_total",
    "Total sweep events where direction/eq_kind is missing or unknown",
    ["symbol"]
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
    ["symbol"],
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

# Tick event-time source distribution (payload vs stream_id vs wall(now))
ticks_ts_source_total = _get_or_create_prom_counter(
    "ticks_ts_source_total",
    "Total ticks by event_ts source (payload/stream_id/now)",
    ["symbol", "ts_source"],
)

# Fast tick-quality EMAs (time-based), computed inside CryptoOrderflowService.
tick_unknown_side_ema_gauge = _get_or_create_prom_gauge(
    "tick_unknown_side_ema",
    "EMA of unknown-side ticks share (0..1)",
    ["symbol"],
)
tick_ts_source_now_ema_gauge = _get_or_create_prom_gauge(
    "tick_ts_source_now_ema",
    "EMA of event_ts sourced from wall/now (0..1)",
    ["symbol"],
)
tick_ts_source_stream_id_ema_gauge = _get_or_create_prom_gauge(
    "tick_ts_source_stream_id_ema",
    "EMA of event_ts sourced from stream_id (0..1)",
    ["symbol"],
)
tick_event_stream_skew_abs_ema_ms_gauge = _get_or_create_prom_gauge(
    "tick_event_stream_skew_abs_ema_ms",
    "EMA of abs(event_ts_ms - stream_ms) in ms",
    ["symbol"],
)
tick_event_age_abs_ema_ms_gauge = _get_or_create_prom_gauge(
    "tick_event_age_abs_ema_ms",
    "EMA of abs(now_ms - event_ts_ms) in ms",
    ["symbol"],
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

# Delta-notional gate pass-rate telemetry by session
dn_gate_events_total = Counter(
    "dn_gate_events_total",
    "Delta-notional tier gate events (pass/veto) segmented by session",
    ["symbol", "tier", "session", "result"],
)

dn_how_scale_gauge = Gauge(
    "dn_how_scale",
    "Hour-of-week liquidity scale factor (telemetry-only)",
    ["symbol", "regime"],
)

of_hidden_divergence_signal_total = _get_or_create_prom_counter(
    "of_hidden_divergence_signal_total",
    "Total signals where trend direction was confirmed by Hidden Divergence",
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

dn_how_scale_gauge = _get_or_create_prom_gauge(
    "of_dn_how_scale",
    "Telemetry hour-of-week liquidity scale from dn_calib (NOT used in decisions)",
    ["symbol", "regime"],
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

# --- Pre-publish gates (data quality / regime-session) ---
pre_publish_gate_veto_total = _get_or_create_prom_counter(
    'pre_publish_gate_veto_total',
    'Total pre-publish gate vetoes (data-quality / regime / spread etc.)',
    ['symbol', 'gate', 'reason', 'mode'],
)

# ✅ P1: Pressure control metrics
ticks_dropped_total = _get_or_create_prom_counter(
    'ticks_dropped_total',
    'Total ticks dropped due to pressure control (lag/drop policy)',
    ['symbol', 'reason']
)

# ✅ P0: PEL recovery metrics
pel_claimed_total = _get_or_create_prom_counter(
    'pel_claimed_total',
    'Total PEL messages claimed by sweeper',
    ['symbol']
)

pel_pending_gauge = _get_or_create_prom_gauge(
    'pel_pending',
    'Current PEL pending count per symbol',
    ['symbol']
)

pel_oldest_idle_ms = _get_or_create_prom_gauge(
    'pel_oldest_idle_ms',
    'Oldest PEL message idle time (ms)',
    ['symbol']
)

# ✅ P0: PEL autoclaim activity (optional sweeper)
pel_autoclaim_total = _get_or_create_prom_counter(
    'pel_autoclaim_total',
    'XAUTOCLAIM recovered pending messages (then ACKed/quarantined)',
    ['symbol', 'kind']
)

# ✅ Step 17: ingest latency histograms (sampled)
# Processing latency per tick (ms) inside consume_ticks loop.
tick_ingest_process_ms = _get_or_create_prom_histogram(
    'tick_ingest_process_ms',
    'Per-tick processing latency in consume_ticks (ms, sampled)',
    ['symbol'],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]
)

# End-to-end delay (ingest_ts_ms - event_ts_ms), ms (sampled).
tick_ingest_e2e_delay_ms = _get_or_create_prom_histogram(
    'tick_ingest_e2e_delay_ms',
    'End-to-end delay ingest_ts_ms - event_ts_ms (ms, sampled)',
    ['symbol'],
    buckets=[10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0, 60000.0, 120000.0]
)

feature_missing_total = _get_or_create_prom_counter(
    "feature_missing_total",
    "Total features missing in signal generation",
    ["feature", "symbol"]
)

# ---------------------------
# Confirmations coverage / schema drift (high-ROI)
# ---------------------------

# Curated keys we explicitly track for coverage (low cardinality, high signal)
_DEFAULT_COVERAGE_KEYS: Tuple[str, ...] = (
    "reclaim",
    "obi_stable",
    "iceberg_strict",
    "ice_strict",
    "fp_edge_absorb",
    "rsi_agree",
    "div_match",
    "sweep",
    "sweep_eqh",
    "sweep_eql",
    "weak_progress",
    "weak_recent",
    "absorption",
    "abs_lvl",
    "iceberg",
)

# Allowlist for schema drift detection (keep reasonably broad; update as you add new confirmations)
_DEFAULT_ALLOW_KEYS: Set[str] = set(_DEFAULT_COVERAGE_KEYS)

# Aliases (schema compat) — for drift tracking & completeness checks
_ALIAS_MAP: Dict[str, str] = {
    "ice_strict": "iceberg_strict",
    "sweep": "sweep",  # kept for legacy detection; canonical are sweep_eqh/sweep_eql
}

_CONF_KEY_RE = re.compile(r"^([a-zA-Z0-9_]+)")

confirmation_seen_total = _get_or_create_prom_counter(
    "confirmation_seen_total",
    "Total signals where a tracked confirmation key is present",
    ["key", "symbol"]
)

confirmation_unknown_total = _get_or_create_prom_counter(
    "confirmation_unknown_total",
    "Total unknown confirmation keys (schema drift)",
    ["key", "symbol"]
)

confirmation_alias_used_total = _get_or_create_prom_counter(
    "confirmation_alias_used_total",
    "Total times an alias confirmation key appeared (compat path used)",
    ["from_key", "to_key", "symbol"]
)

confirmation_incomplete_total = _get_or_create_prom_counter(
    "confirmation_incomplete_total",
    "Total incomplete/mismatched confirmation states",
    ["kind", "symbol"]
)

evidence_used_total_session = _get_or_create_prom_counter(
    "evidence_used_total_session",
    "Total strong evidence used in signals with session label",
    ["symbol", "session", "key"]
)


# -------------------------
# Confirmation canonicalization helpers (contract + dashboards)
# -------------------------

# Canonical key aliases to avoid "dead" confirmations due to typos/renames.
# Keep this small and high-signal; add here only when the confirmation is relied upon by scoring/ML/monitoring.
_CONFIRM_KEY_ALIAS = {
    # Iceberg strict (legacy -> canonical)
    "ice_strict": "iceberg_strict",
    "iceberg_strict": "iceberg_strict",
    # Sweep keys
    "sweep": "sweep",
    "sweep_eqh": "sweep_eqh",
    "sweep_eql": "sweep_eql",
    # RSI / divergence confirmations
    "rsi_agree": "rsi_agree",
    "div_match": "div_match",
    # Microstructure confirmations (used in confidence scorer / ML features)
    "reclaim": "reclaim",
    "obi_stable": "obi_stable",
    "fp_edge_absorb": "fp_edge_absorb",
}

# Evidence keys (subset of confirmations) that we treat as "strong evidence used" in metrics/dashboard.
_EVIDENCE_KEYS = {
    "sweep",
    "sweep_eqh",
    "sweep_eql",
    "iceberg_strict",
    "rsi_agree",
    "div_match",
    "reclaim",
    "obi_stable",
    "fp_edge_absorb",
}

def canonical_confirmation_key(raw: str) -> str:
    """Return canonical key for confirmation strings like 'key=1' or 'key=0.12'."""
    try:
        if not raw:
            return ""
        # fastest: split on '=' if present
        k_raw = str(raw).split("=", 1)[0].strip().lower()
        return _CONFIRM_KEY_ALIAS.get(k_raw, k_raw) or ""
    except Exception:
        return ""

def record_confirmation_seen(symbol: str, conf: str) -> None:
    """Increment confirmation_seen_total and confirmation_unknown_total (best-effort)."""
    try:
        k_raw = (str(conf).split("=", 1)[0].strip().lower()) if conf else ""
        k = _CONFIRM_KEY_ALIAS.get(k_raw, k_raw)
        if not k:
            return
        confirmation_seen_total.labels(symbol=str(symbol), key=str(k)).inc()
        if k_raw not in _CONFIRM_KEY_ALIAS:
            # Note: confirmation_unknown_total handles schema drift detection
            confirmation_unknown_total.labels(symbol=str(symbol), key=str(k_raw)).inc()
    except Exception:
        return

def record_evidence_used(symbol: str, session: str, conf: str) -> None:
    """Increment evidence_used_total (+ session-labeled variant) for strong evidence keys."""
    try:
        k = canonical_confirmation_key(conf)
        if not k:
            return
        if k not in _EVIDENCE_KEYS:
            return
        evidence_used_total.labels(symbol=str(symbol), key=str(k)).inc()
        if session:
            # Session label is critical for dashboard filtering
            evidence_used_total_session.labels(symbol=str(symbol), session=str(session), key=str(k)).inc()
    except Exception:
        return


confirmations_per_signal_hist = _get_or_create_prom_histogram(
    "confirmations_per_signal",
    "Number of confirmations attached per emitted signal",
    ["symbol"],
    buckets=[0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0]
)

# Protect Prometheus from cardinality explosion on truly broken runs
_UNKNOWN_KEYS_MAX = 64
_UNKNOWN_KEYS_SEEN: Set[str] = set()

def _label_unknown_key(key: str) -> str:
    if key in _UNKNOWN_KEYS_SEEN:
        return key
    if len(_UNKNOWN_KEYS_SEEN) < _UNKNOWN_KEYS_MAX:
        _UNKNOWN_KEYS_SEEN.add(key)
        return key
    return "__other__"

def _parse_confirm_key(c: str) -> str:
    """
    confirmations entry formats:
      - "key=1"
      - "key=2.15"
      - "key" (rare)
    We extract key safely.
    """
    try:
        if not c:
            return ""
        # fastest: split on '=' if present
        k = c.split("=", 1)[0].strip()
        if k:
            return k
        m = _CONF_KEY_RE.match(c.strip())
        return m.group(1) if m else ""
    except Exception:
        return ""

def track_confirmations(
    symbol: str,
    confirmations: List[str],
    side: str = "",
    kind: str = "",
    allow_keys: Set[str] = None,
    coverage_keys: Tuple[str, ...] = None,
) -> None:
    """
    High-ROI drift/coverage tracker:
      - coverage of curated keys
      - unknown keys (schema drift)
      - alias usage (compat mode)
      - incomplete states (sweep side missing, sweep mismatch, iceberg strict mismatch)
    """
    try:
        sym = symbol or "unknown"
        confirmations_per_signal_hist.labels(symbol=sym).observe(float(len(confirmations or [])))

        cov = coverage_keys or _DEFAULT_COVERAGE_KEYS
        allow = allow_keys or _DEFAULT_ALLOW_KEYS

        keys: Set[str] = set()
        for c in (confirmations or []):
            k = _parse_confirm_key(str(c))
            if not k:
                continue
            keys.add(k)

            # Coverage metrics (curated list only)
            if k in cov:
                confirmation_seen_total.labels(key=k, symbol=sym).inc()

            # Alias metrics
            if k in _ALIAS_MAP:
                confirmation_alias_used_total.labels(from_key=k, to_key=_ALIAS_MAP[k], symbol=sym).inc()

            # Unknown keys (schema drift)
            if k not in allow:
                confirmation_unknown_total.labels(key=_label_unknown_key(k), symbol=sym).inc()

        # Incomplete/mismatch detectors (cheap but very valuable)
        if "sweep" in keys and ("sweep_eqh" not in keys and "sweep_eql" not in keys):
            confirmation_incomplete_total.labels(kind="sweep_side_missing", symbol=sym).inc()

        s = (side or "").upper()
        if s == "LONG" and "sweep_eqh" in keys:
            confirmation_incomplete_total.labels(kind="sweep_side_mismatch", symbol=sym).inc()
        if s == "SHORT" and "sweep_eql" in keys:
            confirmation_incomplete_total.labels(kind="sweep_side_mismatch", symbol=sym).inc()

        # iceberg strict mismatch: legacy present but canonical absent
        if "ice_strict" in keys and "iceberg_strict" not in keys:
            confirmation_incomplete_total.labels(kind="iceberg_strict_missing", symbol=sym).inc()

    except Exception:
        # never break signal pipeline on telemetry
        pass


# --- OK/OF-gate metrics emission health (telemetry about telemetry) ---
ok_metrics_emitted_total = _get_or_create_prom_counter(
    "ok_metrics_emitted_total",
    "Total decision/ok metric rows emitted to Redis streams",
    ["src"],
)
ok_metrics_skipped_total = _get_or_create_prom_counter(
    "ok_metrics_skipped_total",
    "Total decision/ok metric rows skipped (sampling/disabled/invalid)",
    ["src", "why"],
)
ok_metrics_error_total = _get_or_create_prom_counter(
    "ok_metrics_error_total",
    "Total decision/ok metric emission errors",
    ["src", "where"],
)


of_confirm_build_ms_hist = _get_or_create_prom_histogram(
    "of_confirm_build_ms",
    "Time taken for OFConfirmEngine.build() in ms",
    ["symbol", "tf"]
)
