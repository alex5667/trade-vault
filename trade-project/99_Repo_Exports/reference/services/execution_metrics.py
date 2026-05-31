from __future__ import annotations

"""Execution/runtime Prometheus metrics for Binance execution hardening.

Single source of truth for P6/P7 observability.
Names are intentionally stable and low-cardinality.
"""

from typing import Any

try:
    from prometheus_client import Counter, Gauge, REGISTRY
except Exception:  # pragma: no cover
    Counter = Gauge = None  # type: ignore
    REGISTRY = None  # type: ignore


def _metric(factory, name: str, *args, **kwargs):
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name) if REGISTRY is not None else None


EXECUTION_ENTRY_SUBMITTED_TOTAL = _metric(
    Counter,
    "execution_entry_submitted_total",
    "Entry orders submitted by the Binance executor.",
    ["symbol", "venue", "order_type"],
)
EXECUTION_ENTRY_FILLED_TOTAL = _metric(
    Counter,
    "execution_entry_filled_total",
    "Entry orders reaching FILLED or PARTIALLY_FILLED state.",
    ["symbol", "venue", "fill_status"],
)
EXECUTION_MARGIN_GUARD_SKIPPED_TOTAL = _metric(
    Counter,
    "execution_margin_guard_skipped_total",
    "Signals skipped by the pre-order margin ratio safety check (balance / margin < 4).",
    ["symbol", "venue"],
)
EXECUTION_PROTECTION_ARM_TIMEOUT_TOTAL = _metric(
    Counter,
    "execution_protection_arm_timeout_total",
    "Entry filled but protection was not confirmed within the configured deadline.",
    ["symbol", "execution_policy"],
)
EXECUTION_DUPLICATE_PREVENTED_TOTAL = _metric(
    Counter,
    "execution_duplicate_prevented_total",
    "Duplicate open deliveries short-circuited from executor state/replay materialization.",
    ["symbol", "reason"],
)
# P5: active-symbol guard exchange-truth release metrics
EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL = _metric(Counter,
    "execution_active_symbol_guard_exchange_check_total",
    "Number of active symbol guard verification requests sent to the exchange.",
    ["symbol", "reason"]
)

EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL = _metric(Counter,
    "execution_active_symbol_guard_cas_total",
    "Number of active symbol guard CAS (Compare-And-Swap) operations.",
    ["symbol", "writer", "outcome", "reason"]
)
EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_stuck_total",
    "Active-symbol guards that remained blocked because exchange truth still showed exposure or live orders.",
    ["symbol", "reason"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_release_total",
    "Active-symbol guards that were successfully released by the background repair worker.",
    ["symbol", "reason"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_exchange_check_total",
    "Exchange-truth checks performed while deciding whether an active-symbol guard can be released.",
    ["symbol", "result"],
)

EXECUTION_POSITION_UNPROTECTED_SECONDS = _metric(
    Gauge,
    "execution_position_unprotected_seconds",
    "Worst-case naked-position window before emergency flatten after a protection incident.",
    ["symbol"],
)


EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_CONFLICT_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_cas_conflict_total",
    "CAS/store conflicts while updating or releasing active-symbol guard keys.",
    ["writer", "operation", "reason"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASED_TOMBSTONE_AGE_MS = _metric(
    Gauge,
    "execution_active_symbol_guard_released_tombstone_age_ms",
    "Age in milliseconds of released active-symbol tombstones that are still present in Redis.",
    ["symbol"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_RESURRECTION_ATTEMPT_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_resurrection_attempt_total",
    "Suspicious attempts to resurrect an already released active-symbol guard.",
    ["writer", "reason"],
)


EXECUTION_ACTIVE_SYMBOL_GUARD_SNAPSHOT_TOTAL = _metric(
    Gauge,
    "execution_active_symbol_guard_snapshot_total",
    "Current active-symbol guard snapshot totals by semantic status.",
    ["status"],
)


EXECUTION_ACTIVE_SYMBOL_GUARD_WINDOW_HOT_SYMBOLS = _metric(
    Gauge,
    "execution_active_symbol_guard_window_hot_symbols",
    "Windowed active-symbol guard hotness by symbol and conflict window.",
    ["window", "symbol"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_RACE_CHAIN_TOTAL = _metric(
    Gauge,
    "execution_active_symbol_guard_race_chain_total",
    "Current suspicious active-symbol writer race chains detected from guard diagnostics timelines.",
    ["symbol", "chain_type"],
)


EXECUTION_ACTIVE_SYMBOL_GUARD_INCIDENT_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_incident_total",
    "Active-symbol guard incidents evaluated by the policy engine.",
    ["severity", "classification", "decision"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_NOTIFY_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_notify_total",
    "Active-symbol guard notifications attempted by channel and result.",
    ["severity", "channel", "result"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_SUPPRESSION_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_suppression_total",
    "Active-symbol guard incidents suppressed or deduped by policy scope.",
    ["scope", "result"],
)
# P12: runbook execution layer metrics
EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_runbook_action_total",
    "Runbook actions executed against active-symbol guard state.",
    ["action", "result"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_runbook_audit_total",
    "Audit trail records emitted for active-symbol runbook actions.",
    ["action", "result"],
)
# P13: runbook state gauge + ack renew-reminder counter
EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL = _metric(
    Gauge,
    "execution_active_symbol_guard_runbook_state_total",
    "Current active runbook state documents by kind/status.",
    ["kind", "status"],
)
EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL = _metric(
    Counter,
    "execution_active_symbol_guard_renew_reminder_total",
    "Ack-aware renew reminders produced by the policy engine.",
    ["severity", "result"],
)


# ---------------------------------------------------------------------------
# Dust / tail cleanup — exact reduce-only flatten with exchange-truth verify
# ---------------------------------------------------------------------------

EXECUTION_FORCE_FLAT_VERIFY_TOTAL = _metric(
    Counter,
    "execution_force_flat_verify_total",
    "Verification outcomes for exact reduce-only flatten / dust cleanup.",
    ["symbol", "result"],
)
EXECUTION_DUST_CLEANUP_TOTAL = _metric(
    Counter,
    "execution_dust_cleanup_total",
    "Dust/tail cleanup attempts after close / flatten paths.",
    ["symbol", "result"],
)
EXECUTION_DUST_RESIDUAL_QTY = _metric(
    Gauge,
    "execution_dust_residual_qty",
    "Residual absolute position quantity seen after a close/flatten verification pass.",
    ["symbol"],
)

# ---------------------------------------------------------------------------
# Dust sweep worker — periodic background cleanup (sequential patch)
# ---------------------------------------------------------------------------

EXECUTION_DUST_SWEEP_TOTAL = _metric(
    Counter,
    "execution_dust_sweep_total",
    "Periodic dust cleanup worker sweep outcomes by symbol.",
    ["symbol", "result"],
)
EXECUTION_DUST_SWEEP_CANDIDATES = _metric(
    Gauge,
    "execution_dust_sweep_candidates",
    "Current number of dust candidates observed during the latest periodic sweep.",
)
EXECUTION_DUST_SWEEP_LAST_RUN_TS = _metric(
    Gauge,
    "execution_dust_sweep_last_run_ts",
    "Unix timestamp of the latest periodic dust cleanup worker sweep.",
)

# ---------------------------------------------------------------------------
# Dust sweep denylist + cooldown skip metrics (denylist/cooldown patch)
# ---------------------------------------------------------------------------

EXECUTION_DUST_SWEEP_SKIP_TOTAL = _metric(
    Counter,
    "execution_dust_sweep_skip_total",
    "Periodic dust cleanup worker skips by symbol and reason.",
    ["symbol", "reason"],
)
EXECUTION_DUST_SWEEP_COOLDOWN_REMAINING_SEC = _metric(
    Gauge,
    "execution_dust_sweep_cooldown_remaining_sec",
    "Remaining cooldown seconds before the dust cleanup worker may act on the symbol again.",
    ["symbol"],
)

# ---------------------------------------------------------------------------
# Dust cleanup admin control-plane (admin control patch)
# ---------------------------------------------------------------------------

EXECUTION_DUST_ADMIN_ACTION_TOTAL = _metric(
    Counter,
    "execution_dust_admin_action_total",
    "Manual/admin dust cleanup control-plane actions by result.",
    ["action", "result"],
)
EXECUTION_DUST_ADMIN_STATE_TOTAL = _metric(
    Gauge,
    "execution_dust_admin_state_total",
    "Current number of manual/admin dust cleanup control-plane objects by kind.",
    ["kind"],
)

# ---------------------------------------------------------------------------
# Dust cleanup admin notification / reminder worker (admin notify patch)
# ---------------------------------------------------------------------------

EXECUTION_DUST_ADMIN_NOTIFY_TOTAL = _metric(
    Counter,
    "execution_dust_admin_notify_total",
    "Telegram/admin notification actions emitted by the dust admin notifier.",
    ["kind", "result"],
)
EXECUTION_DUST_ADMIN_REMINDER_TOTAL = _metric(
    Counter,
    "execution_dust_admin_reminder_total",
    "Reminder events emitted for long-lived denylist/cooldown dust admin objects.",
    ["kind", "result"],
)
EXECUTION_DUST_ADMIN_OLD_ENTRY_AGE_SEC = _metric(
    Gauge,
    "execution_dust_admin_old_entry_age_sec",
    "Observed age in seconds for dynamic denylist entries and cooldown loops tracked by the dust admin notifier.",
    ["kind", "symbol"],
)

# ---------------------------------------------------------------------------
# Dust cleanup admin ACK workflow (P14 — ack/suppress/renew layer)
# ---------------------------------------------------------------------------

EXECUTION_DUST_ADMIN_ACK_ACTION_TOTAL = _metric(
    Counter,
    "execution_dust_admin_ack_action_total",
    "Dust admin reminder acknowledgement actions.",
    ["action", "result"],
)

EXECUTION_DUST_ADMIN_ACK_STATE_TOTAL = _metric(
    Gauge,
    "execution_dust_admin_ack_state_total",
    "Current count of active dust admin ACK states.",
    ["kind"],
)

EXECUTION_DUST_ADMIN_ACK_TTL_SEC = _metric(
    Gauge,
    "execution_dust_admin_ack_ttl_sec",
    "Remaining TTL for dust admin reminder ACK state.",
    ["kind", "symbol"],
)

EXECUTION_DUST_ADMIN_UNACKED_ITEMS_TOTAL = _metric(
    Gauge,
    "execution_dust_admin_unacked_items_total",
    "Count of old denylist / cooldown loop items that currently have no operator ACK.",
    ["kind"],
)

EXECUTION_DUST_ADMIN_ACK_RENEW_REMINDER_TOTAL = _metric(
    Counter,
    "execution_dust_admin_ack_renew_reminder_total",
    "Dust admin ACK renewal reminders emitted.",
    ["kind", "result"],
)

MARK_CONTRACT_SPREAD_BPS = _metric(
    Gauge,
    "mark_contract_spread_bps",
    "Current mark minus contract price spread in basis points.",
    ["symbol"],
)
TP_TRIGGER_MARK_MINUS_CONTRACT_BPS = _metric(
    Gauge,
    "tp_trigger_mark_minus_contract_bps",
    "Mark minus contract spread in basis points observed when TP trigger semantics were evaluated.",
    ["symbol", "level"],
)
SL_TRIGGER_MARK_MINUS_CONTRACT_BPS = _metric(
    Gauge,
    "sl_trigger_mark_minus_contract_bps",
    "Mark minus contract spread in basis points observed when SL semantics were evaluated.",
    ["symbol"],
)
TRIGGER_MISS_SUSPECTED_TOTAL = _metric(
    Counter,
    "trigger_miss_suspected_total",
    "Trigger touched but exposure did not reduce before watchdog fallback / timeout.",
    ["symbol", "level", "working_type"],
)

TP_LIMIT_TRIGGERED_TOTAL = _metric(
    Counter,
    "tp_limit_triggered_total",
    "Maker TP levels that reached trigger condition.",
    ["symbol", "level"],
)
TP_LIMIT_FILLED_TOTAL = _metric(
    Counter,
    "tp_limit_filled_total",
    "Maker TP levels that fully filled before watchdog fallback.",
    ["symbol", "level"],
)
TP_WATCHDOG_FALLBACK_TOTAL = _metric(
    Counter,
    "tp_watchdog_fallback_total",
    "Maker TP levels that required market watchdog fallback.",
    ["symbol", "level"],
)
MAKER_FILL_RATIO = _metric(
    Gauge,
    "maker_fill_ratio",
    "Filled maker TP levels divided by triggered maker TP levels.",
    ["symbol", "level"],
)
FEE_BPS_SAVED_ESTIMATE = _metric(
    Gauge,
    "fee_bps_saved_estimate",
    "Estimated fee advantage in bps for maker TP levels that filled without fallback.",
    ["symbol", "level"],
)

BINANCE_503_UNKNOWN_TOTAL = _metric(
    Counter,
    "binance_503_unknown_total",
    "Binance HTTP 503 responses where execution result was explicitly unknown.",
    ["endpoint"],
)
BINANCE_503_FAILURE_TOTAL = _metric(
    Counter,
    "binance_503_failure_total",
    "Binance HTTP 503 responses that were ordinary failures rather than unknown execution state.",
    ["endpoint"],
)
BINANCE_429_TOTAL = _metric(
    Counter,
    "binance_429_total",
    "Binance rate-limit responses.",
    ["endpoint"],
)
BINANCE_1008_TOTAL = _metric(
    Counter,
    "binance_1008_total",
    "Binance overload errors (-1008). Metric name is normalised because Prometheus names cannot contain '-'.",
    ["endpoint"],
)
BINANCE_API_ERRORS_TOTAL = _metric(
    Counter,
    "binance_api_errors_total",
    "Binance API/transport errors by endpoint and exchange error code.",
    ["endpoint", "code"],
)
BINANCE_ALGO_RECONCILE_TOTAL = _metric(
    Counter,
    "binance_algo_reconcile_total",
    "Reconcile-first recoveries after ambiguous execution responses.",
    ["action", "source"],
)
LISTENKEY_REFRESH_TOTAL = _metric(
    Counter,
    "listenkey_refresh_total",
    "ListenKey lifecycle operations (start/keepalive/close).",
    ["op", "result"],
)
USER_STREAM_RECONNECT_TOTAL = _metric(
    Counter,
    "user_stream_reconnect_total",
    "User stream reconnect cycles.",
    ["reason"],
)
USER_STREAM_LAST_EVENT_AGE_MS = _metric(
    Gauge,
    "user_stream_last_event_age_ms",
    "Age of the last Binance user-stream event relative to local wall clock.",
)
USER_STREAM_CONNECTED = _metric(
    Gauge,
    "user_stream_connected",
    "Whether the Binance user-stream worker currently considers itself connected (1/0).",
)

REDIS_STREAM_TIMEOUT_TOTAL = _metric(
    Counter,
    "redis_stream_timeout_total",
    "Redis stream timeout bursts observed by the orderflow worker.",
    ["symbol", "stream"],
)
QUEUE_LAG_MS = _metric(
    Gauge,
    "queue_lag_ms",
    "Redis/orderflow queue lag in milliseconds.",
    ["symbol"],
)
BOOK_STALENESS_MS = _metric(
    Gauge,
    "book_staleness_ms",
    "Best-book staleness in milliseconds.",
    ["symbol"],
)
TICK_STALENESS_MS = _metric(
    Gauge,
    "tick_staleness_ms",
    "Tick staleness in milliseconds.",
    ["symbol"],
)
NEGATIVE_AGE_EVENTS_TOTAL = _metric(
    Counter,
    "negative_age_events_total",
    "Negative-age / time-regression events detected by DQ logic.",
    ["symbol"],
)

# ---------------------------------------------------------------------------
# P12: Strict protection verification & repair metrics
# ---------------------------------------------------------------------------

EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL = _metric(
    Counter,
    "execution_protection_verify_fail_total",
    "Protection verification failures (SL/TP not confirmed on exchange after placement).",
    ["phase", "reason"],
)

EXECUTION_PROTECTION_REPAIR_TOTAL = _metric(
    Counter,
    "execution_protection_repair_total",
    "Repair attempts for missing protective orders (SL/TP re-placement after verify fail).",
    ["symbol", "component"],
)

EXECUTION_RECONCILE_PARTIAL_PROTECTION_TOTAL = _metric(
    Counter,
    "execution_reconcile_partial_protection_total",
    "Reconciliation responses where entry was found but protection was incomplete.",
    ["action", "symbol"],
)

# ---------------------------------------------------------------------------
# P3: strict modify/resize protection-replacement metrics
# ---------------------------------------------------------------------------

EXECUTION_PROTECTION_REPLACE_TOTAL = _metric(
    Counter,
    "execution_protection_replace_total",
    "Strict modify/resize protection replacement outcomes.",
    ["symbol", "action", "result"],
)
EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS = _metric(
    Gauge,
    "execution_protection_replace_naked_window_ms",
    "Observed naked-window duration during strict protection replacement.",
    ["symbol", "action"],
)

# ---------------------------------------------------------------------------
# BinanceProtectionAuditor: independent second-control-plane metrics
# ---------------------------------------------------------------------------

EXECUTION_PROTECTION_AUDIT_FINDING_TOTAL = _metric(
    Counter,
    "execution_protection_audit_finding_total",
    "Protection-audit findings detected by the independent BinanceProtectionAuditor.",
    ["venue", "finding", "mode"],
)
EXECUTION_PROTECTION_AUDIT_FLATTEN_TOTAL = _metric(
    Counter,
    "execution_protection_audit_flatten_total",
    "Emergency flatten operations executed by the BinanceProtectionAuditor (flatten mode only).",
    ["venue", "finding"],
)
EXECUTION_PROTECTION_AUDIT_LAST_RUN_TS = _metric(
    Gauge,
    "execution_protection_audit_last_run_ts",
    "Unix timestamp of the last completed BinanceProtectionAuditor scan cycle.",
    ["venue"],
)
EXECUTION_PROTECTION_AUDIT_OPEN_FINDINGS = _metric(
    Gauge,
    "execution_protection_audit_open_findings",
    "Current open protection findings per venue/symbol/finding (1 = active, staleness-tracked by Prometheus).",
    ["venue", "symbol", "finding"],
)
