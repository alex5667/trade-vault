from __future__ import annotations

"""Execution/runtime Prometheus metrics for Binance execution hardening.

Single source of truth for P6/P7 observability.
Names are intentionally stable and low-cardinality.
"""


try:
    from prometheus_client import REGISTRY, Counter, Gauge
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
EXECUTION_POSITION_UNPROTECTED_SECONDS = _metric(
    Gauge,
    "execution_position_unprotected_seconds",
    "Worst-case naked-position window before emergency flatten after a protection incident.",
    ["symbol"],
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
