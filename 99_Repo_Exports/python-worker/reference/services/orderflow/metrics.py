
import logging
import re

from prometheus_client import REGISTRY, Counter, Gauge, Histogram


def _get_or_create_prom_counter(name: str, documentation: str, labelnames: list[str] = None):
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

def _get_or_create_prom_gauge(name: str, documentation: str, labelnames: list[str] = None):
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

def _get_or_create_prom_histogram(name: str, documentation: str, labelnames: list[str] = None, buckets: list[float] = None):
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

# Phase E / P4: Manipulation gate events (quote stuffing / layering / OTR)
# Low-cardinality: {symbol, mode, reason}
# mode: monitor | tighten | veto
# reason: VETO_QUOTE_STUFFING | VETO_LAYERING | VETO_OTR_SPIKE | TIGHTEN | ANNOTATE
manip_gate_events_total = _get_or_create_prom_counter(
    "manip_gate_events_total",
    "Phase E P4: Manipulation gate events (quote stuffing, layering, OTR spike)",
    ["symbol", "mode", "reason"],
)

# Trade close joiner metrics (P46)
trade_close_joiner_seen_total = _get_or_create_prom_counter(
    "trade_close_joiner_seen_total",
    "Total POSITION_CLOSED events seen by joiner",
    ["symbol"],
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
    ["symbol"],
)

trade_close_joiner_backfill_drop_total = _get_or_create_prom_counter(
    "trade_close_joiner_backfill_drop_total",
    "Total wait entries dropped during backfill",
    ["symbol", "reason"],
)

trade_close_joiner_prob_missing_total = _get_or_create_prom_counter(
    "trade_close_joiner_prob_missing_total",
    "Total close events where probability (p) could not be extracted from decision",
    ["symbol", "where"]
)

trade_close_joiner_prob_source_total = _get_or_create_prom_counter(
    "trade_close_joiner_prob_source_total",
    "Total close events by probability extraction source (ml/meta/ensemble/unknown)",
    ["symbol", "source"]
)

# Metrics for silent errors
silent_errors_total = _get_or_create_prom_counter(
    "silent_errors_total",
    "Total silent errors (except Exception: pass blocks)",
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
    ["symbol"],
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
    ['symbol'],
)
tp1_zero_pnl_total = _get_or_create_prom_counter(
    'tp1_zero_pnl_total',
    'Total signals where expected net margin at TP1 is <= 0',
    ['symbol'],
)

worker_lag_ms_gauge = _get_or_create_prom_gauge(
    "worker_lag_ms",
    "Lag between wall clock and tick ts_ms at read time (ms)",
    ["symbol"],
)

worker_lag_ms_p50_gauge = _get_or_create_prom_gauge(
    "worker_lag_ms_p50",
    "50th percentile of worker lag (ms)",
    ["symbol"],
)

worker_lag_ms_p95_gauge = _get_or_create_prom_gauge(
    "worker_lag_ms_p95",
    "95th percentile of worker lag (ms)",
    ["symbol"],
)

worker_lag_ms_p99_gauge = _get_or_create_prom_gauge(
    "worker_lag_ms_p99",
    "99th percentile of worker lag (ms)",
    ["symbol"],
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

# --- Gate accounting & Slippage (P70+) ---
trade_gate_eligible_total = _get_or_create_prom_counter(
    "trade_gate_eligible_total",
    "Total signals eligible for gate evaluation",
    ["gate", "sym", "bucket", "mode"]
)
trade_gate_ok_total = _get_or_create_prom_counter(
    "trade_gate_ok_total",
    "Total signals passing the gate",
    ["gate", "sym", "bucket", "mode", "status"]
)
trade_gate_veto_total = _get_or_create_prom_counter(
    "trade_gate_veto_total",
    "Total signals vetoed by the gate",
    ["gate", "sym", "bucket", "mode", "reason"]
)
trade_gate_shadow_veto_total = _get_or_create_prom_counter(
    "trade_gate_shadow_veto_total",
    "Total signals that would be vetoed in shadow mode",
    ["gate", "sym", "bucket", "reason"]
)

# Taker flow metrics
trade_taker_flow_imb_z = _get_or_create_prom_histogram(
    "trade_taker_flow_imb_z",
    "Taker flow imbalance z-score distribution",
    ["sym", "bucket"],
    buckets=[-5.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 5.0]
)
trade_taker_flow_imb = _get_or_create_prom_histogram(
    "trade_taker_flow_imb",
    "Taker flow imbalance distribution",
    ["sym", "bucket"],
    buckets=[-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0]
)

# Slippage metrics
trade_expected_slippage_bps = _get_or_create_prom_histogram(
    "trade_expected_slippage_bps",
    "Expected slippage (bps)",
    ["sym", "bucket", "model"],
    buckets=[1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 250.0]
)
trade_expected_slippage_ratio = _get_or_create_prom_histogram(
    "trade_expected_slippage_ratio",
    "Expected slippage ratio (expected / max_eff)",
    ["sym", "bucket"],
    buckets=[0.1, 0.5, 0.8, 0.9, 0.95, 1.0, 1.05, 1.1, 1.5, 2.0]
)

# Bid/ask spread at decision-time snapshot (P95/P99-ready, low-cardinality: sym × bucket)
trade_spread_bps = _get_or_create_prom_histogram(
    "trade_spread_bps",
    "Bid/ask spread (bps) at decision time",
    ["sym", "bucket"],
    buckets=[0.05, 0.1, 0.2, 0.3, 0.5, 0.8, 1, 1.5, 2, 3, 5, 8, 13, 21, 34]
)

# --- Execution-risk histograms (P95/P99-ready; low-cardinality: sym × bucket) ---
trade_exec_risk_ref_bps = _get_or_create_prom_histogram(
    "trade_exec_risk_ref_bps",
    "Execution-risk reference (bps) used for normalization",
    ["sym", "bucket"],
    buckets=[0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377]
)

trade_exec_risk_bps = _get_or_create_prom_histogram(
    "trade_exec_risk_bps",
    "Execution-risk (bps) implied by expected slippage / liquidity state",
    ["sym", "bucket"],
    buckets=[0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377]
)

trade_exec_risk_norm = _get_or_create_prom_histogram(
    "trade_exec_risk_norm",
    "Execution-risk normalized to [0..1] (soft-capped)",
    ["sym", "bucket"],
    buckets=[0.0, 0.02, 0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.60, 0.80, 1.0, 1.2]
)

trade_exec_pen = _get_or_create_prom_histogram(
    "trade_exec_pen",
    "Execution penalty used by gate (combines risk + fill)",
    ["sym", "bucket"],
    buckets=[0.0, 0.02, 0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.60, 0.80, 1.0, 1.2]
)

# --- Taker-flow contra gate (P9c) ---
trade_taker_flow_gate_shadow_veto_total = _get_or_create_prom_counter(
    "trade_taker_flow_gate_shadow_veto_total",
    "Count of taker-flow contra gate shadow veto (would-have-veto, shadow mode)",
    ["sym", "bucket", "reason"],
)

trade_taker_flow_gate_veto_total = _get_or_create_prom_counter(
    "trade_taker_flow_gate_veto_total",
    "Count of taker-flow contra gate enforced veto (enforce mode)",
    ["sym", "bucket", "reason"],
)

trade_expected_slippage_limit_exceed_total = _get_or_create_prom_counter(
    "trade_expected_slippage_limit_exceed_total",
    "Total exceedances of expected slippage limit (ratio > 1)",
    ["sym", "bucket"],
)
trade_slippage_residual_bps = _get_or_create_prom_histogram(
    "trade_slippage_residual_bps",
    "Slippage residual (realized_worse - expected) (bps)",
    ["sym", "bucket"],
    buckets=[-100.0, -50.0, -20.0, -10.0, -5.0, 0.0, 5.0, 10.0, 20.0, 50.0, 100.0]
)
trade_edge_minus_expected_bps = _get_or_create_prom_histogram(
    "trade_edge_minus_expected_bps",
    "Edge minus expected slippage (bps)",
    ["sym", "bucket"],
    buckets=[-50.0, -20.0, -10.0, -5.0, 0.0, 5.0, 10.0, 20.0, 50.0, 100.0]
)
trade_edge_negative_total = _get_or_create_prom_counter(
    "trade_edge_negative_total",
    "Total cases where edge minus expected slippage is < 0",
    ["sym", "bucket"],
)
trade_max_expected_slippage_bps_eff = _get_or_create_prom_gauge(
    "trade_max_expected_slippage_bps_eff",
    "Effective max expected slippage limit (bps)",
    ["sym", "bucket"],
)

fp_imb_confirm_total = _get_or_create_prom_counter(
    'fp_imb_confirm_total',
    'Total footprint imbalance confirmations',
    ['symbol'],
)
fp_absorb_confirm_total = _get_or_create_prom_counter(
    'fp_absorb_confirm_total',
    'Total footprint absorption confirmations',
    ['symbol'],
)
fp_buckets_evicted_total = _get_or_create_prom_counter(
    'fp_buckets_evicted_total',
    'Total footprint buckets evicted (LRU)',
    ['symbol'],
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
    ['symbol'],
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
    ['symbol'],
)
tick_time_recovery_passed_total = _get_or_create_prom_counter(
    'tick_time_recovery_passed_total',
    'Total recovery gates passed after state freeze',
    ['symbol'],
)
tick_time_quarantine_score_gauge = _get_or_create_prom_gauge(
    'tick_time_quarantine_score',
    'Current bad time quarantine score',
    ['symbol'],
)
burst_active_gauge = _get_or_create_prom_gauge('burst_active', 'Burst mode active status (1=active)', ['symbol'])
burst_flush_total = _get_or_create_prom_counter('burst_flush_total', 'Total burst flushes', ['symbol', 'mode'])
signals_emitted_total = _get_or_create_prom_counter('signals_emitted_total', 'Total signals actually emitted', ['symbol'])
burst_window_ms_gauge = _get_or_create_prom_gauge('burst_window_ms', 'Current burst window (ms)', ['symbol'])
tick_gap_p50_ms_gauge = _get_or_create_prom_gauge('tick_gap_p50_ms', 'Tick gap p50 (ms)', ['symbol'])
tick_gap_p95_ms_gauge = _get_or_create_prom_gauge(
    "tick_gap_p95_ms", "Tick gap p95 (ms)", ["symbol"]
)

tick_gap_n_gauge = _get_or_create_prom_gauge(
    "tick_gap_n",
    "Number of samples currently in TickGapTracker window (used for tick_gap_pXX snapshots)",
    ["symbol"],
)


# P2/F: missing-seq EMA signals (0..1) and raw gap events count.
book_missing_seq_ema_gauge = _get_or_create_prom_gauge(
    "book_missing_seq_ema", "EMA of book missing-seq events (0..1)", ["symbol"]
)
tick_missing_seq_ema_gauge = _get_or_create_prom_gauge(
    "tick_missing_seq_ema", "EMA of tick missing-seq events (0..1)", ["symbol"]
)

book_missing_seq_events_total = _get_or_create_prom_counter(
    "book_missing_seq_events_total", "Count of detected book sequence gaps", ["symbol"]
)
tick_missing_seq_events_total = _get_or_create_prom_counter(
    "tick_missing_seq_events_total", "Count of detected tick sequence gaps", ["symbol"]
)

dq_level_gauge = _get_or_create_prom_gauge(
    "dq_level",
    "Data quality level: 0=OK, 1=SOFT degrade (penalty), 2=HARD degrade (veto-capable)",
    ["symbol"],
)

dq_veto_total = _get_or_create_prom_counter(
    "dq_veto_total",
    "Number of times DQ entered veto-capable state (edge-triggered per symbol)",
    ["symbol", "bucket"],
)


# --- LiqMap (liquidation map) metrics ---
# Snapshot age is computed as: now_ms - snapshot.ts_ms (from redis payload).
# Special values: -1 = missing snapshot, -2 = parse error.
liqmap_snapshot_age_ms_gauge = _get_or_create_prom_gauge(
    "liqmap_snapshot_age_ms",
    "Age of liqmap snapshot (ms). -1 missing, -2 parse error.",
    ["symbol", "window"],
)
liqmap_snapshot_parse_errors_total = _get_or_create_prom_counter(
    "liqmap_snapshot_parse_errors_total",
    "Total liqmap snapshot parse errors.",
    ["symbol"],
)
liqmap_gate_shadow_hit_total = _get_or_create_prom_counter(
    "liqmap_gate_shadow_hit_total",
    "Total liqmap gate shadow hits (would veto).",
    ["symbol", "dir", "window"],
)
liqmap_gate_veto_total = _get_or_create_prom_counter(
    "liqmap_gate_veto_total",
    "Total liqmap gate enforced vetoes.",
    ["symbol", "dir", "reason"],
)

# A1.1: Per-(symbol, window, where) parse/compute error counter (LiqMap observability).
# Labels:
#   symbol: market symbol (e.g., BTCUSDT)
#   window: snapshot window (e.g., 1h, 5m)
#   where:  small enum identifying the failing step — keep low-cardinality.
#           Currently only "parse_or_compute" is emitted from _inject_liqmap_features().
# Complements liqmap_snapshot_parse_errors_total (symbol-only, legacy) by adding
# window granularity for per-window failure breakdowns in Grafana / alert rules.
liqmap_parse_errors_total = _get_or_create_prom_counter(
    "liqmap_parse_errors_total",
    "Total LiqMap snapshot parse/validation/feature-compute errors (fail-open). "
    "Sentinel values on liqmap_snapshot_age_ms: -1=missing, -2=parse/compute error.",
    ["symbol", "window", "where"],
)

# P2/F: trade_id ordering issue breakdown (diagnostic; GAP is already covered by tick_missing_seq_events_total).
tick_id_gap_events_total = _get_or_create_prom_counter(
    "tick_id_gap_events_total", "Count of tick trade_id gaps (tid > last_tid+1)", ["symbol"]
)
tick_id_dup_events_total = _get_or_create_prom_counter(
    "tick_id_dup_events_total", "Count of duplicate tick trade_id events (tid == last_tid)", ["symbol"]
)
tick_id_reorder_events_total = _get_or_create_prom_counter(
    "tick_id_reorder_events_total", "Count of out-of-order tick trade_id events (tid < last_tid)", ["symbol"]
)


# --- Drain Mode Metrics ---
drain_forced_cancel_total = _get_or_create_prom_counter(
    'drain_forced_cancel_total',
    'Total number of symbol workers forced to cancel due to drain timeout',
    ['symbol', 'kind']
)

ticks_out_of_order_total = _get_or_create_prom_counter(
    "ticks_out_of_order_total",
    "Total ticks received out of temporal order",
    ["symbol"],
)
ticks_side_unknown_total = _get_or_create_prom_counter(
    "ticks_side_unknown_total",
    "Total ticks with unknown side classification",
    ["symbol"],
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
    ["symbol"],
)

strong_gate_veto_total = _get_or_create_prom_counter(
    "strong_gate_veto_total",
    "Total signals vetoed by Strong OF Gate",
    ["symbol", "scenario", "reason", "mode"]
)

divergence_confirmed_total = _get_or_create_prom_counter(
    "divergence_confirmed_total",
    "Total signals where divergence was confirmed",
    ["symbol"],
)
divergence_triggered_total = _get_or_create_prom_counter(
    "divergence_triggered_total",
    "Total signals triggered specifically by divergence",
    ["symbol"],
)
divergence_overridden_total = _get_or_create_prom_counter(
    "divergence_overridden_total",
    "Total divergences overridden by other gates",
    ["symbol"],
)
divergence_suppressed_total = _get_or_create_prom_counter(
    "divergence_suppressed_total",
    "Total divergences suppressed by data quality or other factors",
    ["symbol"],
)
evidence_used_total = _get_or_create_prom_counter(
    "evidence_used_total",
    "Total strong evidence used in signals",
    ["symbol", "key"]
)

ticks_pressure_filtered_total = _get_or_create_prom_counter(
    "ticks_pressure_filtered_total",
    "Total ticks categorized by delta tier/pressure",
    ["symbol", "reason"],
)

# ATR-TF Selection Metrics
atr_tf_switch_total = _get_or_create_prom_counter(
    "atr_tf_switch_total",
    "Total ATR timeframe switches",
    ["symbol"],
)
atr_tf_candidate_diff = _get_or_create_prom_gauge(
    "atr_tf_candidate_diff",
    "1 if candidate TF differs from selected TF, 0 otherwise",
    ["symbol"],
)
atr_tf_target_bps = _get_or_create_prom_gauge(
    "atr_tf_target_bps",
    "Target ATR in basis points for TF selection",
    ["symbol"],
)
atr_tf_candidate_score = _get_or_create_prom_gauge(
    "atr_tf_candidate_score",
    "Score of candidate TF",
    ["symbol"],
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
    ["symbol"],
)
ticks_processed_total = _get_or_create_prom_counter(
    "ticks_processed_total",
    "Total tick messages successfully parsed and passed to detectors",
    ["symbol"],
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
    ["symbol"],
)

veto_min_conf_total = _get_or_create_prom_counter(
    "veto_min_conf_total",
    "Total signals vetoed due to confidence < CRYPTO_SIGNAL_MIN_CONF",
    ["symbol"],
)

veto_low_conf_total = _get_or_create_prom_counter(
    "veto_low_conf_total",
    "Total signals vetoed due to confidence < CRYPTO_SIGNAL_LOW_CONF",
    ["symbol"],
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
    ["symbol"],
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
    ['symbol'],
)

# --- LOB pressure metrics (P91) ---
# Queue imbalance per LOB level {symbol, level=L1..L5}
of_lob_queue_imbalance_gauge = _get_or_create_prom_gauge(
    "of_lob_queue_imbalance",
    "Queue imbalance per LOB level (bid_qty-ask_qty)/(bid_qty+ask_qty)",
    ["symbol", "level"],
)
# Aggregates (mean/max_abs/slope) across L1..L5
of_lob_queue_imbalance_mean_gauge = _get_or_create_prom_gauge(
    "of_lob_queue_imbalance_mean",
    "Mean queue imbalance over L1..L5 (signed)",
    ["symbol"],
)
of_lob_queue_imbalance_max_abs_gauge = _get_or_create_prom_gauge(
    "of_lob_queue_imbalance_max_abs",
    "Max absolute queue imbalance over L1..L5",
    ["symbol"],
)
of_lob_queue_imbalance_slope_gauge = _get_or_create_prom_gauge(
    "of_lob_queue_imbalance_slope",
    "Slope of queue imbalance over depth levels (least squares, signed)",
    ["symbol"],
)

# Microprice vs mid divergence and shift (bps scale)
of_lob_micro_mid_div_bps_gauge = _get_or_create_prom_gauge(
    "of_lob_micro_mid_div_bps",
    "Microprice divergence vs mid (bps)",
    ["symbol"],
)
of_lob_micro_shift_bps_gauge = _get_or_create_prom_gauge(
    "of_lob_micro_shift_bps",
    "Microprice shift vs previous snapshot (bps)",
    ["symbol"],
)

# Depth curve slope and convexity by side {symbol, side=bid/ask/imb}
of_lob_depth_slope_gauge = _get_or_create_prom_gauge(
    "of_lob_depth_slope",
    "Depth curve slope by side (cumulative qty, per level)",
    ["symbol", "side"],
)
of_lob_depth_convexity_gauge = _get_or_create_prom_gauge(
    "of_lob_depth_convexity",
    "Depth curve convexity by side (second-diff of cumulative qty, normalized)",
    ["symbol", "side"],
)

# Depth-weighted OBI and derived stability metrics
of_lob_dw_obi_gauge = _get_or_create_prom_gauge(
    "of_lob_dw_obi",
    "Depth-weighted OBI (weights 1/level)",
    ["symbol"],
)
of_lob_dw_obi_z_gauge = _get_or_create_prom_gauge(
    "of_lob_dw_obi_z",
    "Z-score of depth-weighted OBI (robust rolling)",
    ["symbol"],
)
of_lob_dw_obi_stability_score_gauge = _get_or_create_prom_gauge(
    "of_lob_dw_obi_stability_score",
    "Stability score [0..1] for depth-weighted OBI",
    ["symbol"],
)
of_lob_dw_obi_stable_secs_gauge = _get_or_create_prom_gauge(
    "of_lob_dw_obi_stable_secs",
    "Continuous stable seconds for depth-weighted OBI direction",
    ["symbol"],
)
of_lob_dw_obi_stable_gauge = _get_or_create_prom_gauge(
    "of_lob_dw_obi_stable",
    "Flag: depth-weighted OBI is stable enough (1/0)",
    ["symbol"],
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
    ['symbol'],
)

of_inputs_missing_fp_total = _get_or_create_prom_counter(
    'of_inputs_missing_fp_total',
    'Total OF inputs missing FP edge fields (v1 or v2 without FP edge)',
    ['symbol'],
)

of_inputs_bad_time_total = _get_or_create_prom_counter(
    'of_inputs_bad_time_total',
    'Total OF inputs skipped due to invalid tick_ts_ms (non-deterministic / bad tick time)',
    ['symbol'],
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
    ['symbol'],
)

pel_pending_gauge = _get_or_create_prom_gauge(
    'pel_pending',
    'Current PEL pending count per symbol',
    ['symbol'],
)

pel_oldest_idle_ms = _get_or_create_prom_gauge(
    'pel_oldest_idle_ms',
    'Oldest PEL message idle time (ms)',
    ['symbol'],
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
_DEFAULT_COVERAGE_KEYS: tuple[str, ...] = (
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
_DEFAULT_ALLOW_KEYS: set[str] = set(_DEFAULT_COVERAGE_KEYS)

# Aliases (schema compat) — for drift tracking & completeness checks
_ALIAS_MAP: dict[str, str] = {
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
        confirmation_seen_total.labels(symbol=symbol, key=str(k)).inc()
        if k_raw not in _CONFIRM_KEY_ALIAS:
            # Note: confirmation_unknown_total handles schema drift detection
            confirmation_unknown_total.labels(symbol=symbol, key=str(k_raw)).inc()
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
        evidence_used_total.labels(symbol=symbol, key=str(k)).inc()
        if session:
            # Session label is critical for dashboard filtering
            evidence_used_total_session.labels(symbol=symbol, session=str(session), key=str(k)).inc()
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
_UNKNOWN_KEYS_SEEN: set[str] = set()

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
    confirmations: list[str],
    side: str = "",
    kind: str = "",
    allow_keys: set[str] = None,
    coverage_keys: tuple[str, ...] = None,
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

        keys: set[str] = set()
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


# --- of-gate SLI counters (eligible / ok_hard / ok_soft / quarantined) ---
# These are derived from the sampled metrics:of_gate stream and are safe for ratio SLIs.
# Note: ok_rate ratios are sampling-invariant if sampling is independent of ok/ok_soft.
of_gate_eligible_total = _get_or_create_prom_counter(
    "of_gate_eligible_total",
    "Total eligible of-gate metric rows (post-validation) emitted",
    ["symbol", "scenario_v4"],
)
of_gate_ok_hard_total = _get_or_create_prom_counter(
    "of_gate_ok_hard_total",
    "Total of-gate hard-ok rows (ok==1) emitted",
    ["symbol", "scenario_v4"],
)
of_gate_ok_soft_total = _get_or_create_prom_counter(
    "of_gate_ok_soft_total",
    "Total of-gate soft-ok rows (ok_soft==1) emitted",
    ["symbol", "scenario_v4"],
)
of_gate_quarantined_total = _get_or_create_prom_counter(
    "of_gate_quarantined_total",
    "Total of-gate rows quarantined by DQ validation",
    ["symbol", "why"],
)

# --- World-practice tracker snapshots (low-cardinality gauges) ---
# Labels: sym, bucket (bucket is exec_regime_bucket: NORMAL|LOW_LIQ|HIGH_VOL|HIGH_VOL_LOW_LIQ)
trade_vol_fast_bps = _get_or_create_prom_gauge(
    "trade_vol_fast_bps",
    "Fast EWMA realized vol (bps)",
    ["sym", "bucket"],
)

trade_vol_slow_bps = _get_or_create_prom_gauge(
    "trade_vol_slow_bps",
    "Slow EWMA realized vol (bps)",
    ["sym", "bucket"],
)

trade_vol_ratio = _get_or_create_prom_gauge(
    "trade_vol_ratio",
    "Volatility ratio: fast/slow",
    ["sym", "bucket"],
)

trade_vol_ratio_z = _get_or_create_prom_gauge(
    "trade_vol_ratio_z",
    "Robust z-score of vol_ratio",
    ["sym", "bucket"],
)

trade_res_recovered = _get_or_create_prom_gauge(
    "trade_res_recovered",
    "Book resilience recovered flag (0/1)",
    ["sym", "bucket"],
)

trade_res_recovery_ms = _get_or_create_prom_gauge(
    "trade_res_recovery_ms",
    "Book resilience recovery time (ms)",
    ["sym", "bucket"],
)

trade_res_speed_per_s = _get_or_create_prom_gauge(
    "trade_res_speed_per_s",
    "Book resilience replenishment speed proxy (per second)",
    ["sym", "bucket"],
)

trade_fill_prob = _get_or_create_prom_gauge(
    "trade_fill_prob",
    "Passive fill probability proxy (0..1)",
    ["sym", "bucket"],
)

trade_eta_fill_sec = _get_or_create_prom_gauge(
    "trade_eta_fill_sec",
    "ETA-to-fill proxy for passive execution (seconds)",
    ["sym", "bucket"],
)

trade_exec_fill_pen = _get_or_create_prom_gauge(
    "trade_exec_fill_pen",
    "Execution penalty component derived from fill_prob (0..1)",
    ["sym", "bucket"],
)


# ---------------------------------------------------------------------------
# World-practice flow / L3-lite pressure (v1)
# Low-cardinality gauges (sym × exec_regime_bucket)
# ---------------------------------------------------------------------------

trade_taker_buy_rate_ema = _get_or_create_prom_gauge(
    "trade_taker_buy_rate_ema",
    "Taker buy rate EMA (qty/sec) from L3-lite",
    ["sym", "bucket"],
)

trade_taker_sell_rate_ema = _get_or_create_prom_gauge(
    "trade_taker_sell_rate_ema",
    "Taker sell rate EMA (qty/sec) from L3-lite",
    ["sym", "bucket"],
)

trade_cancel_bid_rate_ema = _get_or_create_prom_gauge(
    "trade_cancel_bid_rate_ema",
    "Bid cancel rate EMA (qty/sec) from L3-lite",
    ["sym", "bucket"],
)

trade_cancel_ask_rate_ema = _get_or_create_prom_gauge(
    "trade_cancel_ask_rate_ema",
    "Ask cancel rate EMA (qty/sec) from L3-lite",
    ["sym", "bucket"],
)

trade_cancel_to_trade_bid = _get_or_create_prom_gauge(
    "trade_cancel_to_trade_bid",
    "Cancel-to-trade ratio (bid) from L3-lite",
    ["sym", "bucket"],
)

trade_cancel_to_trade_ask = _get_or_create_prom_gauge(
    "trade_cancel_to_trade_ask",
    "Cancel-to-trade ratio (ask) from L3-lite",
    ["sym", "bucket"],
)

trade_taker_flow_imb_z = _get_or_create_prom_gauge(
    "trade_taker_flow_imb_z",
    "Signed taker flow imbalance robust z-score (L3-lite)",
    ["sym", "bucket"],
)

trade_book_churn_score = _get_or_create_prom_gauge(
    "trade_book_churn_score",
    "Book churn score (0..1) — quote update intensity proxy",
    ["sym", "bucket"],
)

trade_book_churn_hi = _get_or_create_prom_gauge(
    "trade_book_churn_hi",
    "Book churn high flag (0/1)",
    ["sym", "bucket"],
)

trade_max_expected_slippage_bps_eff = _get_or_create_prom_gauge(
    "trade_max_expected_slippage_bps_eff",
    "Effective max expected slippage cap after regime tightening (bps)",
    ["sym", "bucket"],
)



# ---------------------------------------------------------------------------
# A8 — Observability for additional microstructure features (v1)
# Low-cardinality gauges (sym × exec_regime_bucket, plus small flag enum)
#
# These gauges mirror the new derived features injected into `indicators`:
#   - depth_total_10, gini_depth_10
#   - vwap_roll_diff_bps, price_momentum_bps, realized_vol_bps
#   - liquidity_pressure, info_flow, pressure_per_min
#   - boolean flags flag_* (0/1)
#
# Motivation:
#   - Make the wiring visible in Grafana immediately (no waiting for offline rollups)
#   - Enable quick smoke-checks to detect “feature stuck” and NaN explosions
# ---------------------------------------------------------------------------

trade_depth_total_10 = _get_or_create_prom_gauge(
    "trade_depth_total_10",
    "Total top-10 depth (bid+ask) in native qty units",
    ["sym", "bucket"],
)

trade_gini_depth_10 = _get_or_create_prom_gauge(
    "trade_gini_depth_10",
    "Gini coefficient of top-10 depth distribution (0..1), higher = more concentrated liquidity",
    ["sym", "bucket"],
)

trade_vwap_roll_diff_bps = _get_or_create_prom_gauge(
    "trade_vwap_roll_diff_bps",
    "Rolling VWAP vs mid divergence (bps)",
    ["sym", "bucket"],
)

trade_price_momentum_bps = _get_or_create_prom_gauge(
    "trade_price_momentum_bps",
    "Rolling price momentum (bps) — last_px vs rolling median/close proxy",
    ["sym", "bucket"],
)

trade_realized_vol_bps = _get_or_create_prom_gauge(
    "trade_realized_vol_bps",
    "Realized volatility estimate over rolling micro-bars (bps)",
    ["sym", "bucket"],
)

trade_pressure_per_min = _get_or_create_prom_gauge(
    "trade_pressure_per_min",
    "System pressure proxy (cooldown hits per minute) from rolling trackers",
    ["sym", "bucket"],
)

trade_liquidity_pressure = _get_or_create_prom_gauge(
    "trade_liquidity_pressure",
    "Liquidity pressure proxy (dimensionless) — signed flow vs available depth",
    ["sym", "bucket"],
)

trade_info_flow = _get_or_create_prom_gauge(
    "trade_info_flow",
    "Information flow / toxicity proxy (dimensionless), higher = more toxic flow",
    ["sym", "bucket"],
)

trade_flag_state = _get_or_create_prom_gauge(
    "trade_flag_state",
    "Binary microstructure flags (0/1); label 'flag' enumerates the flag name",
    ["sym", "bucket", "flag"],
)
# ---------------------------------------------------------------------------
# World-practice LOB pressure snapshots v1 (microprice + depth shape + DW OBI)
# Low-cardinality: sym × exec_regime_bucket
# ---------------------------------------------------------------------------

trade_qi_mean = _get_or_create_prom_gauge(
    "trade_qi_mean",
    "Queue imbalance mean across L1..L5 (LOB pressure)",
    ["sym", "bucket"],
)

trade_qi_max_abs = _get_or_create_prom_gauge(
    "trade_qi_max_abs",
    "Queue imbalance max |qi| across L1..L5 (LOB pressure)",
    ["sym", "bucket"],
)

trade_qi_slope = _get_or_create_prom_gauge(
    "trade_qi_slope",
    "Queue imbalance slope across depth levels (positive = bid pressure builds deeper)",
    ["sym", "bucket"],
)

trade_micro_mid_div_bps = _get_or_create_prom_gauge(
    "trade_micro_mid_div_bps",
    "Microprice divergence vs mid (bps); +ve = hidden bid pressure",
    ["sym", "bucket"],
)

trade_micro_shift_bps = _get_or_create_prom_gauge(
    "trade_micro_shift_bps",
    "Microprice shift vs previous snapshot (bps)",
    ["sym", "bucket"],
)

trade_depth_slope_bid = _get_or_create_prom_gauge(
    "trade_depth_slope_bid",
    "Cumulative depth slope (bid) across L1..L5 (units=qty)",
    ["sym", "bucket"],
)

trade_depth_slope_ask = _get_or_create_prom_gauge(
    "trade_depth_slope_ask",
    "Cumulative depth slope (ask) across L1..L5 (units=qty)",
    ["sym", "bucket"],
)

trade_depth_slope_imb = _get_or_create_prom_gauge(
    "trade_depth_slope_imb",
    "Depth slope imbalance (bid - ask) (units=qty)",
    ["sym", "bucket"],
)

trade_depth_slope_imb_norm = _get_or_create_prom_gauge(
    "trade_depth_slope_imb_norm",
    "Depth slope imbalance normalized by |bid|+|ask| (dimensionless)",
    ["sym", "bucket"],
)

trade_depth_convexity_bid = _get_or_create_prom_gauge(
    "trade_depth_convexity_bid",
    "Depth curve convexity (bid), normalized by total depth (dimensionless)",
    ["sym", "bucket"],
)

trade_depth_convexity_ask = _get_or_create_prom_gauge(
    "trade_depth_convexity_ask",
    "Depth curve convexity (ask), normalized by total depth (dimensionless)",
    ["sym", "bucket"],
)

trade_depth_convexity_imb = _get_or_create_prom_gauge(
    "trade_depth_convexity_imb",
    "Depth curve convexity imbalance (bid - ask) (dimensionless)",
    ["sym", "bucket"],
)

trade_dw_obi = _get_or_create_prom_gauge(
    "trade_dw_obi",
    "Depth-weighted order-book imbalance (weights=1/level), [-1..+1]",
    ["sym", "bucket"],
)

trade_dw_obi_z = _get_or_create_prom_gauge(
    "trade_dw_obi_z",
    "Depth-weighted OBI robust z-score (symbol-local)",
    ["sym", "bucket"],
)

trade_dw_obi_stability_score = _get_or_create_prom_gauge(
    "trade_dw_obi_stability_score",
    "DW OBI stability score (0..1) — higher = more persistent pressure",
    ["sym", "bucket"],
)

trade_dw_obi_stable_secs = _get_or_create_prom_gauge(
    "trade_dw_obi_stable_secs",
    "Seconds DW OBI stayed stable (monotone segments)",
    ["sym", "bucket"],
)

trade_dw_obi_stable = _get_or_create_prom_gauge(
    "trade_dw_obi_stable",
    "DW OBI stable flag (0/1)",
    ["sym", "bucket"],
)



# ----------------------------
# World-practice: adverse selection (realized drift) v1
# ----------------------------

adverse_rd_eval_total = _get_or_create_prom_counter(
    "adverse_rd_eval_total",
    "Number of realized-drift horizon evaluations processed (world-practice adverse selection tracker).",
    ["sym", "bucket"],
)

trade_adverse_rd_mean_bps = _get_or_create_prom_gauge(
    "trade_adverse_rd_mean_bps",
    "EWMA mean realized drift in bps (positive=favorable, negative=adverse) over horizon.",
    ["sym", "bucket"],
)
trade_adverse_rd_sigma_bps = _get_or_create_prom_gauge(
    "trade_adverse_rd_sigma_bps",
    "EWMA sigma of realized drift in bps over horizon.",
    ["sym", "bucket"],
)
trade_adverse_rd_z = _get_or_create_prom_gauge(
    "trade_adverse_rd_z",
    "Realized drift z-score (mean/sigma) over horizon.",
    ["sym", "bucket"],
)
trade_adverse_rd_bad_share = _get_or_create_prom_gauge(
    "trade_adverse_rd_bad_share",
    "EWMA share of adverse outcomes (realized drift < 0) over horizon.",
    ["sym", "bucket"],
)
trade_adverse_rd_n = _get_or_create_prom_gauge(
    "trade_adverse_rd_n",
    "Number of realized drift samples processed (not EWMA).",
    ["sym", "bucket"],
)
trade_adverse_rd_veto = _get_or_create_prom_gauge(
    "trade_adverse_rd_veto",
    "Adverse selection veto bit computed from realized drift stats (0/1).",
    ["sym", "bucket"],
)


# ---------------------------------------------------------------------------
# Backward-compat aliases (crypto_orderflow_service.py uses older names)
# ---------------------------------------------------------------------------

# tick_dedup_drop_total was renamed from tick_dedup_dropped_total in SoT
tick_dedup_dropped_total = tick_dedup_drop_total  # noqa: F811

# PEL metrics (older name had 'redis_' prefix)
redis_pel_pending_gauge = pel_pending_gauge  # noqa: F811
redis_pel_claim_total = pel_autoclaim_total  # noqa: F811
redis_pel_claimed_total = pel_claimed_total  # noqa: F811

# Additional missing names (imported by crypto_orderflow_service.py)
tick_uid_missing_total = _get_or_create_prom_counter(
    "tick_uid_missing_total",
    "Total ticks where a UID could not be computed",
    ["symbol"],
)
tick_trade_id_missing_total = _get_or_create_prom_counter(
    "tick_trade_id_missing_total",
    "Total ticks where trade_id was missing",
    ["symbol"],
)
ticks_deduped_total = _get_or_create_prom_counter(
    "ticks_deduped_total",
    "Total ticks deduplicated by tick_uid window",
    ["symbol"],
)
ticks_quarantined_total = _get_or_create_prom_counter(
    "ticks_quarantined_total",
    "Total ticks quarantined (side/time policy)",
    ["symbol", "reason"],
)
ticks_schema_invalid_total = _get_or_create_prom_counter(
    "ticks_schema_invalid_total",
    "Total ticks with invalid schema (missing required fields)",
    ["symbol"],
)

# ---------------------------------------------------------------------------
# Additional backward-compat aliases (strategy.py / other older consumers)
# ---------------------------------------------------------------------------

# conf_feature_* (renamed to confirmation_* in SoT)
conf_feature_seen_total = _get_or_create_prom_counter(
    "conf_feature_seen_total",
    "Backward-compat: confirmation seen per feature/src",
    ["feature", "src"],
)
conf_feature_true_total = _get_or_create_prom_counter(
    "conf_feature_true_total",
    "Backward-compat: confirmation feature true count",
    ["feature", "src"],
)
conf_feature_missing_total = _get_or_create_prom_counter(
    "conf_feature_missing_total",
    "Backward-compat: confirmation feature missing count",
    ["feature", "src"],
)

# of_dn_how_ratio_t1 (telemetry only)
of_dn_how_ratio_t1_gauge = _get_or_create_prom_gauge(
    "of_dn_how_ratio_t1",
    "Backward-compat: DN HOW ratio tier1 telemetry",
    ["symbol"],
)

# metrics_queue_dropped_total
metrics_queue_dropped_total = _get_or_create_prom_counter(
    "metrics_queue_dropped_total",
    "Total metric entries dropped from queue",
    ["reason"],
)

# ---------------------------------------------------------------------------
# Phase C (P2): Liquidity geometry / resiliency (low-cardinality telemetry)
# ---------------------------------------------------------------------------
#
# These are *optional* metrics used for debugging and SRE guardrails.
# We keep cardinality bounded:
#   - label `symbol` is used, but callers should collapse to "__all__" unless the
#     symbol is explicitly allow-listed via LIQ_GEOM_METRICS_SYMBOLS.

liq_geom_monitor_hit_total = _get_or_create_prom_counter(
    "liq_geom_monitor_hit_total",
    "Total monitor hits for liquidity geometry flags (slope/dws/recovery)",
    ["symbol", "profile"],
)

liq_geom_tighten_total = _get_or_create_prom_counter(
    "liq_geom_tighten_total",
    "Total tighten actions applied due to liquidity geometry (expected_slippage add)",
    ["symbol", "profile"],
)

liq_geom_veto_total = _get_or_create_prom_counter(
    "liq_geom_veto_total",
    "Total vetos due to liquidity geometry in hard profile",
    ["symbol", "reason"],
)

liq_geom_dws_bps = _get_or_create_prom_histogram(
    "liq_geom_dws_bps",
    "Depth-weighted spread proxy (bps), bounded top5",
    ["symbol"],
    buckets=[0.0, 0.2, 0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377],
)

liq_geom_book_slope_min_usd_per_bps = _get_or_create_prom_histogram(
    "liq_geom_book_slope_min_usd_per_bps",
    "Min(book_slope_bid, book_slope_ask) in USD/bps (higher is better)",
    ["symbol"],
    buckets=[0.0, 100, 300, 1_000, 3_000, 10_000, 30_000, 100_000, 300_000, 1_000_000, 3_000_000, 10_000_000],
)

liq_geom_recovery_time_ms = _get_or_create_prom_histogram(
    "liq_geom_recovery_time_ms",
    "Liquidity stress recovery time (ms) while stressed, else 0",
    ["symbol"],
    buckets=[0.0, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000, 120_000],
)

# ---------------------------------------------------------------------------
# Phase D (P3): Flow toxicity (OFI normalized by depth + optional VPIN)
# ---------------------------------------------------------------------------
# Same cardinality policy as liq_geom: per-symbol only for allowlist, else symbol="__all__".

flow_toxic_monitor_hit_total = _get_or_create_prom_counter(
    "flow_toxic_monitor_hit_total",
    "Total monitor hits for flow toxicity (ofi_norm_z / vpin_cdf)",
    ["symbol", "profile"],
)

flow_toxic_tighten_total = _get_or_create_prom_counter(
    "flow_toxic_tighten_total",
    "Total tighten actions due to flow toxicity (expected_slippage add)",
    ["symbol", "profile"],
)

flow_toxic_veto_total = _get_or_create_prom_counter(
    "flow_toxic_veto_total",
    "Total vetos due to flow toxicity in hard profile",
    ["symbol", "reason"],
)

flow_toxic_ofi_norm_z = _get_or_create_prom_histogram(
    "flow_toxic_ofi_norm_z",
    "Robust z-score of notional-normalized OFI (higher => more toxic)",
    ["symbol"],
    buckets=[-10, -6, -4, -3, -2, -1, -0.5, 0, 0.5, 1, 2, 3, 4, 6, 10, 20],
)

flow_toxic_vpin_cdf = _get_or_create_prom_histogram(
    "flow_toxic_vpin_cdf",
    "VPIN toxicity proxy (CDF in [0..1])",
    ["symbol"],
    buckets=[0.0, 0.01, 0.05, 0.10, 0.20, 0.35, 0.50, 0.65, 0.80, 0.90, 0.95, 0.99, 1.0],
)

# pre_publish_veto_total → pre_publish_gate_veto_total
pre_publish_veto_total = pre_publish_gate_veto_total  # noqa: F811
