#!/usr/bin/env python3
from __future__ import annotations

try:
    from utils.time_utils import get_ny_time_millis
except Exception:
    try:
        from time_utils import get_ny_time_millis
    except Exception:
        import time
        def get_ny_time_millis() -> int:
            return int(time.time() * 1000)

"""Binance USDT-M Futures executor (reads Redis queue → places orders → writes exec facts).

Queue contract (orders:queue:binance):
  Required fields: action (open|modify|cancel|resize), sid, symbol
  Open-specific:   side/direction (BUY/SELL or LONG/SHORT), qty/quantity,
                   type (MARKET|LIMIT), entry (for LIMIT),
                   sl, tp_levels (list of floats)
  Trailing:        trail_after_tp1=true, trail_callback_rate/trail_callback_bps/atr+trail_atr_mult

Binance specifics:
  - Entry order: MARKET or LIMIT
  - SL: STOP_MARKET with closePosition=True (one-way) or quantity+positionSide (hedge)
  - TP: TAKE_PROFIT_MARKET with reduceOnly=True (one-way) or positionSide (hedge)
  - Trailing: TRAILING_STOP_MARKET with callbackRate (0.1–5.0%)

Delivery guarantee:
  - BRPOPLPUSH queue → processing list (at-least-once)
  - errors classified: transient (retry up to BINANCE_MAX_RETRY) vs fatal (DLQ)
  - writes orders:exec events for both success and failure

Trailing stop logic (trail_after_tp1):
  - Keep hard SL active until TP1 is touched
  - On TP1 touch: cancel SL, place TRAILING_STOP_MARKET for remainder
  - callbackRate = explicit payload override OR ATR-derived OR default
  - Arming runs in a daemon thread (non-blocking for the main loop)

ENV — required:
  REDIS_URL, BINANCE_API_KEY, BINANCE_API_SECRET

ENV — queue:
  ORDERS_QUEUE_BINANCE=orders:queue:binance
  ORDERS_QUEUE_BINANCE_PROCESSING=orders:queue:binance:processing
  ORDERS_QUEUE_BINANCE_DLQ=orders:queue:binance:dlq
  EXEC_STREAM=orders:exec

ENV — execution:
  BINANCE_POSITION_MODE=oneway|hedge   (default oneway)
  BINANCE_SYMBOL_ALLOWLIST=BTCUSDT,ETHUSDT,...
  BINANCE_INIT_SYMBOL_SETTINGS=1      (auto-set margin/leverage on first open)
  BINANCE_MARGIN_TYPE=ISOLATED|CROSSED
  BINANCE_DEFAULT_LEVERAGE=10
  BINANCE_MAX_RETRY=3
  BINANCE_FILL_TIMEOUT_S=8.0
  BINANCE_FILL_POLL_S=0.25
  BINANCE_ASSUME_LOT_IS_QTY=0         (allow lot field for MT5 compat)

ENV — trailing:
  BINANCE_TRAIL_CALLBACK_MIN=0.1    (% floor for callbackRate)
  BINANCE_TRAIL_CALLBACK_MAX=5.0    (% ceiling)
  BINANCE_TRAIL_CALLBACK_DEFAULT=0.3
  BINANCE_TRAIL_ATR_MULT=1.0        (multiplier for ATR-derived rate)
  BINANCE_TRAIL_ARM_POLL_S=1.0      (mark price polling interval)
  BINANCE_TRAIL_ARM_TIMEOUT_S=7200  (give up arming after 2h)
  BINANCE_TRAIL_NOTIFY=1            (send Telegram on arm)

ENV — telegram (optional, errors only):
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (or BOT_TOKEN, CHAT_ID)
"""

import hashlib
import json
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Any
import contextlib

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from services.binance_futures_client import (
        TRADFI_PERPS_NOT_SIGNED,
        AlgoOrderRef,
        BinanceAPIError,
        BinanceFuturesClient,
        PlainOrderRef,
        is_tradfi_perps_error,
    )
    from services.execution_contracts import ExecutionEvent, build_materialized_state_view
    from services.execution_intent_validator import ExecutionIntent, validate_execution_intent, validate_exit_intent
    from services.execution_journal import ExecutionJournalSink
    from services.execution_policy import (
        MAKER_FIRST,
        SAFETY_FIRST,
        ExecutionPolicyDecision,
        resolve_execution_policy,
    )
    from services.execution_state_replay import (
        persist_state_snapshot,
        project_event_into_state,
        rebuild_state_with_fallback,
    )
    from services.rollout_flags import RolloutFlags
except Exception:  # pragma: no cover - standalone bundle / local tests
    from binance_futures_client import (
        TRADFI_PERPS_NOT_SIGNED,
        AlgoOrderRef,
        BinanceAPIError,
        BinanceFuturesClient,
        PlainOrderRef,
        is_tradfi_perps_error,
    )
    from execution_contracts import ExecutionEvent, build_materialized_state_view
    from execution_intent_validator import ExecutionIntent, validate_execution_intent, validate_exit_intent
    with contextlib.suppress(Exception):
        from execution_policy import (
            MAKER_FIRST,
            SAFETY_FIRST,
            ExecutionPolicyDecision,
            resolve_execution_policy,
        )
    from execution_journal import ExecutionJournalSink
    from execution_state_replay import (
        persist_state_snapshot,
        project_event_into_state,
        rebuild_state_with_fallback,
    )
    from rollout_flags import RolloutFlags
try:
    from common.contracts.registry import ExecutionEventV1
    from common.normalization import get_side_int, normalize_direction, normalize_side
except Exception:
    try:
        from normalization import get_side_int, normalize_direction, normalize_side
    except Exception:
        normalize_side = normalize_direction = get_side_int = None
    try:
        from contracts.registry import ExecutionEventV1
    except Exception:
        ExecutionEventV1 = None

# --- Trailing Orchestrator integration (fail-open) ---
try:
    from services.trailing_profiles import TrailingProfile, TrailingProfilesRegistry
    _HAS_TRAILING_PROFILES = True
except Exception:  # pragma: no cover
    _HAS_TRAILING_PROFILES = False
    TrailingProfilesRegistry = None  # type: ignore
    TrailingProfile = None  # type: ignore

try:
    from services.trailing_condition import TrailingConditionConfig, TrailingConditionEvaluator
    _HAS_TRAILING_CONDITION = True
except Exception:  # pragma: no cover
    _HAS_TRAILING_CONDITION = False
    TrailingConditionEvaluator = None  # type: ignore
    TrailingConditionConfig = None  # type: ignore

try:
    from services.active_symbol_guard_store import ActiveSymbolGuardStore
    from services.telegram.telegram_client import TelegramClient
except Exception:  # pragma: no cover
    try:
        from telegram.telegram_client import TelegramClient
    except Exception:
        from telegram_client import TelegramClient
    from active_symbol_guard_store import ActiveSymbolGuardStore

try:
    from prometheus_client import REGISTRY, Counter, Gauge, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = start_http_server = None  # type: ignore
    REGISTRY = None  # type: ignore


try:
    from services.execution_metrics import (
        BINANCE_ALGO_RECONCILE_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL,
        # P5: exchange-truth guard release metrics
        EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL,
        EXECUTION_DUPLICATE_PREVENTED_TOTAL,
        EXECUTION_DUST_CLEANUP_TOTAL,
        EXECUTION_DUST_RESIDUAL_QTY,
        EXECUTION_ENTRY_FILLED_TOTAL,
        EXECUTION_ENTRY_SUBMITTED_TOTAL,
        EXECUTION_FORCE_FLAT_VERIFY_TOTAL,
        EXECUTION_INTENT_AGE_MS,
        EXECUTION_INTENT_REJECTED_TOTAL,
        EXECUTION_MARGIN_GUARD_SKIPPED_TOTAL,
        EXECUTION_OPERATION_BLOCKED_TOTAL,
        EXECUTION_POSITION_UNPROTECTED_SECONDS,
        EXECUTION_PROTECTION_ARM_TIMEOUT_TOTAL,
        EXECUTION_PROTECTION_REPAIR_TOTAL,
        EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS,
        EXECUTION_PROTECTION_REPLACE_TOTAL,
        EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL,
        EXECUTION_RECONCILE_PARTIAL_PROTECTION_TOTAL,
        FEE_BPS_SAVED_ESTIMATE,
        KILL_SWITCH_ACTIVE,
        KILL_SWITCH_ARMED_TIMESTAMP,
        MAKER_FILL_RATIO,
        MARK_CONTRACT_SPREAD_BPS,
        SL_TRIGGER_MARK_MINUS_CONTRACT_BPS,
        TP_LIMIT_FILLED_TOTAL,
        TP_LIMIT_TRIGGERED_TOTAL,
        TP_TRIGGER_MARK_MINUS_CONTRACT_BPS,
        TP_WATCHDOG_FALLBACK_TOTAL,
        TRIGGER_MISS_SUSPECTED_TOTAL,
    )
except Exception:  # pragma: no cover
    try:
        from execution_metrics import (
            BINANCE_ALGO_RECONCILE_TOTAL,
            EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL,
            # P5: exchange-truth guard release metrics
            EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL,
            EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL,
            EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL,
            EXECUTION_DUPLICATE_PREVENTED_TOTAL,
            EXECUTION_DUST_CLEANUP_TOTAL,
            EXECUTION_DUST_RESIDUAL_QTY,
            EXECUTION_ENTRY_FILLED_TOTAL,
            EXECUTION_ENTRY_SUBMITTED_TOTAL,
            EXECUTION_FORCE_FLAT_VERIFY_TOTAL,
            EXECUTION_INTENT_AGE_MS,
            EXECUTION_INTENT_REJECTED_TOTAL,
            EXECUTION_MARGIN_GUARD_SKIPPED_TOTAL,
            EXECUTION_OPERATION_BLOCKED_TOTAL,
            EXECUTION_POSITION_UNPROTECTED_SECONDS,
            EXECUTION_PROTECTION_ARM_TIMEOUT_TOTAL,
            EXECUTION_PROTECTION_REPAIR_TOTAL,
            EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS,
            EXECUTION_PROTECTION_REPLACE_TOTAL,
            EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL,
            EXECUTION_RECONCILE_PARTIAL_PROTECTION_TOTAL,
            FEE_BPS_SAVED_ESTIMATE,
            KILL_SWITCH_ACTIVE,
            KILL_SWITCH_ARMED_TIMESTAMP,
            MAKER_FILL_RATIO,
            MARK_CONTRACT_SPREAD_BPS,
            SL_TRIGGER_MARK_MINUS_CONTRACT_BPS,
            TP_LIMIT_FILLED_TOTAL,
            TP_LIMIT_TRIGGERED_TOTAL,
            TP_TRIGGER_MARK_MINUS_CONTRACT_BPS,
            TP_WATCHDOG_FALLBACK_TOTAL,
            TRIGGER_MISS_SUSPECTED_TOTAL,
        )
    except Exception:  # pragma: no cover
        BINANCE_ALGO_RECONCILE_TOTAL = EXECUTION_DUPLICATE_PREVENTED_TOTAL = None  # type: ignore
        EXECUTION_DUST_CLEANUP_TOTAL = EXECUTION_DUST_RESIDUAL_QTY = None  # type: ignore
        EXECUTION_ENTRY_FILLED_TOTAL = EXECUTION_ENTRY_SUBMITTED_TOTAL = EXECUTION_OPERATION_BLOCKED_TOTAL = None  # type: ignore
        EXECUTION_INTENT_AGE_MS = EXECUTION_INTENT_REJECTED_TOTAL = None  # type: ignore
        EXECUTION_FORCE_FLAT_VERIFY_TOTAL = EXECUTION_MARGIN_GUARD_SKIPPED_TOTAL = None  # type: ignore
        EXECUTION_POSITION_UNPROTECTED_SECONDS = EXECUTION_PROTECTION_ARM_TIMEOUT_TOTAL = None  # type: ignore
        KILL_SWITCH_ARMED_TIMESTAMP = None  # type: ignore
        KILL_SWITCH_ACTIVE = None  # type: ignore
        EXECUTION_PROTECTION_REPAIR_TOTAL = EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS = None  # type: ignore
        EXECUTION_PROTECTION_REPLACE_TOTAL = EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL = None  # type: ignore
        EXECUTION_RECONCILE_PARTIAL_PROTECTION_TOTAL = None  # type: ignore
        # P5: guard metrics fallback
        EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL = EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL = None  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL = EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL = None  # type: ignore
        FEE_BPS_SAVED_ESTIMATE = MAKER_FILL_RATIO = MARK_CONTRACT_SPREAD_BPS = None  # type: ignore
        SL_TRIGGER_MARK_MINUS_CONTRACT_BPS = TP_LIMIT_FILLED_TOTAL = TP_LIMIT_TRIGGERED_TOTAL = None  # type: ignore
        TP_TRIGGER_MARK_MINUS_CONTRACT_BPS = TP_WATCHDOG_FALLBACK_TOTAL = TRIGGER_MISS_SUSPECTED_TOTAL = None  # type: ignore


def _metric(factory, name: str, *args, **kwargs):
    """Idempotent Prometheus metric factory — returns existing metric if already registered."""
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        collector = getattr(REGISTRY, "_names_to_collectors", {}).get(name) if REGISTRY is not None else None
        return collector

# P3/P4 execution health counters
EXECUTION_RECONCILE_PENDING_TOTAL = _metric(Counter,
    "execution_reconcile_pending_total",
    "Number of executor transitions into PENDING_RECONCILE.",
    ["action", "symbol"],
)
EXECUTION_EMERGENCY_FLATTEN_TOTAL = _metric(Counter,
    "execution_emergency_flatten_total",
    "Number of emergency flatten operations executed by the Binance executor.",
    ["symbol", "reason"],
)
EXECUTION_STATE_TRANSITION_TOTAL = _metric(Counter,
    "execution_state_transition_total",
    "Number of executor finite-state-machine transitions.",
    ["action", "symbol", "next_state"],
)
EXECUTION_USER_STREAM_STALE_TOTAL = _metric(Counter,
    "execution_user_stream_stale_total",
    "Number of execution requests blocked because user stream liveness is stale.",
    ["symbol", "action"],
)
EXECUTION_POSITION_UNPROTECTED_SECONDS = _metric(Gauge,
    "execution_position_unprotected_seconds",
    "Age in seconds of the current unprotected position window.",
    ["symbol"],
)
EXECUTION_USER_STREAM_STALE_TOTAL = _metric(Counter,
    "execution_user_stream_stale_total",
    "Number of execution requests blocked because user stream liveness is stale.",
    ["symbol", "action"],
)
EXECUTION_POSITION_UNPROTECTED_SECONDS = _metric(Gauge,
    "execution_position_unprotected_seconds",
    "Age in seconds of the current unprotected position window.",
    ["symbol"],
)

EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL = _metric(Counter,
    "execution_open_blocked_active_symbol_total",
    "Number of open requests blocked because the symbol already has an active execution.",
    ["symbol", "blocked_state"],
)


class OpenBlockedByActiveSymbolError(RuntimeError):
    def __init__(self, details: dict[str, Any]):
        super().__init__(str((details or {}).get("reason") or "single_active_position_per_symbol"))
        self.details = dict(details or {})



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bool_env(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "on"}


def _ms_now() -> int:
    return get_ny_time_millis()


def _mono_ms() -> int:
    return int(time.monotonic() * 1000)


# Explicit executor finite-state machine. Each state transition is persisted to
# Redis state and mirrored into the execution stream so the worker can recover
# idempotently after a restart.
FSM_PENDING_RECONCILE = "PENDING_RECONCILE"
FSM_RECEIVED = "RECEIVED"
FSM_VALIDATED = "VALIDATED"
FSM_ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
FSM_ENTRY_ACKED = "ENTRY_ACKED"
FSM_ENTRY_PARTIAL = "ENTRY_PARTIAL"
FSM_ENTRY_FILLED = "ENTRY_FILLED"
FSM_PROTECTION_ARMING = "PROTECTION_ARMING"
FSM_PROTECTION_REPLACING = "PROTECTION_REPLACING"  # P3: strict replace in-flight state
FSM_PROTECTED = "PROTECTED"
FSM_TP_POLICY_ARMED = "TP_POLICY_ARMED"
FSM_TRAIL_ARMED = "TRAIL_ARMED"
FSM_EXIT_FILLED = "EXIT_FILLED"
FSM_EMERGENCY_FLATTENED = "EMERGENCY_FLATTENED"
FSM_FAILED = "FAILED"
TERMINAL_FSM_STATES = {FSM_EXIT_FILLED, FSM_EMERGENCY_FLATTENED, FSM_FAILED}

PARTIAL_FILL_CANCEL_REMAINDER_AND_PROTECT_FILLED = "CANCEL_REMAINDER_AND_PROTECT_FILLED"
PARTIAL_FILL_CONVERT_REMAINDER_TO_MARKET = "CONVERT_REMAINDER_TO_MARKET"
PARTIAL_FILL_ABORT_AND_FLATTEN = "ABORT_AND_FLATTEN"
VALID_PARTIAL_FILL_POLICIES = {
    PARTIAL_FILL_CANCEL_REMAINDER_AND_PROTECT_FILLED,
    PARTIAL_FILL_CONVERT_REMAINDER_TO_MARKET,
    PARTIAL_FILL_ABORT_AND_FLATTEN,
}
def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def _sha1_8(s: str) -> str:
    """Short stable hash for building client order IDs without exceeding Binance's 36-char limit."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]


def _make_cid(sid: str, tag: str, r: Any = None) -> str:
    """Build a deterministic clientOrderId ≤36 chars: <base>-<sha1[:8]>-<tag>."""
    token = _sha1_8(sid)
    base = sid.replace(" ", "").replace(":", "-")
    base = base[: max(6, 36 - (len(tag) + len(token) + 2))]
    cid = f"{base}-{token}-{tag}"
    cid = cid[:36]
    if r is not None:
        with contextlib.suppress(Exception):
            r.set(f"orders:cid_to_sid:{cid}", sid, ex=86400 * 3)
    return cid


def _round_down(x: float, step: float) -> float:
    """Round x down to the nearest multiple of step (for LOT_SIZE quantisation)."""
    if step <= 0:
        return x
    return math.floor(x / step) * step


def _truthy(v: Any) -> bool:
    """Check if a value is truthy in payload context (handles string "true"/"1" etc.)."""
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v) != 0.0
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "on"}


def _format_float(x: float, step: float) -> str:
    """Format float exactly to the step dimension without scientific notation."""
    if step <= 0:
        return f"{x:f}".rstrip('0').rstrip('.') if '.' in f"{x:f}" else f"{x:f}"

    s_step = f"{step:f}".rstrip('0').rstrip('.') if '.' in f"{step:f}" else f"{step:f}"
    decimals = 0
    if '.' in s_step:
        decimals = len(s_step.split('.')[1])

    fmt = f"{{:.{decimals}f}}"
    return fmt.format(x)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _round_half_up(x: float, decimals: int = 1) -> float:
    """Round halves up (avoids banker's rounding).

    Binance callbackRate uses 0.1% steps. Python's built-in round(0.35, 1)
    can yield 0.3 due to binary floating-point; this helper keeps it stable.
    """
    p = 10 ** int(decimals)
    return math.floor(x * p + 0.5) / p


def compute_trailing_callback_rate_pct(
    payload: dict[str, Any],
    *,
    min_pct: float,
    max_pct: float,
    default_pct: float,
) -> float:
    """Extract callbackRate (%) for TRAILING_STOP_MARKET from signal payload.

    Executor is a thin execution layer — it does NOT compute callback rates.
    All values (SL, TP, trailing cb) must be pre-calculated by the python-worker
    signal generator before reaching this executor.

    Priority:
      1. Explicit percent in payload: trail_callback_rate / trail_callback_pct
      2. Explicit bps:  trail_callback_bps  (e.g. 30 bps = 0.30%)
      3. ENV default:   BINANCE_TRAIL_CALLBACK_DEFAULT

    Result is clamped to [min, max] and rounded to 0.1% (Binance requirement).
    """
    # 1. Explicit percent from payload
    for k in ("trail_callback_rate", "trail_callback_pct", "trail_callback_percent"):
        v = payload.get(k)
        if v is not None:
            try:
                return _round_half_up(_clamp(float(v), min_pct, max_pct), 1)
            except Exception:
                pass

    # 2. Explicit bps from payload (30 bps = 0.30%)
    bps = payload.get("trail_callback_bps")
    if bps is not None:
        try:
            return _round_half_up(_clamp(float(bps) / 100.0, min_pct, max_pct), 1)
        except Exception:
            pass

    # 3. ENV default
    return _round_half_up(_clamp(float(default_pct), min_pct, max_pct), 1)



def compute_limit_tp_price(tp_trigger_price: float, logical_side: str, *, offset_bps: float, tick_size: float) -> float:
    """Compute the passive limit price used by maker TP ladder.

    For LONG exits (SELL), a positive offset places the limit *above* the
    trigger so the order is less likely to cross the book immediately.
    For SHORT exits (BUY), a positive offset places the limit *below* the
    trigger.
    """
    px = float(tp_trigger_price)
    off = abs(float(offset_bps)) / 10000.0
    raw = px * (1.0 + off) if logical_side == "LONG" else px * (1.0 - off)
    tick = float(tick_size or 0.0)
    if tick <= 0:
        return raw
    if logical_side == "LONG":
        return math.ceil(raw / tick) * tick
    return math.floor(raw / tick) * tick


def compute_trailing_activate_price(
    logical_side: str,
    *,
    latest_price: float,
    tick_size: float,
    buffer_bps: float,
    user_activate_price: float | None = None,
) -> float:
    """Return a valid activatePrice for Binance TRAILING_STOP_MARKET.

    Binance requires:
      * BUY trailing (used to close SHORT): activatePrice < latest price
      * SELL trailing (used to close LONG): activatePrice > latest price

    We encode the same rule locally to avoid -2021 immediately-triggered errors.
    """
    latest = float(latest_price)
    if latest <= 0:
        raise ValueError("latest_price must be > 0 for trailing activation")

    tick = float(tick_size or 0.0)
    buf = abs(float(buffer_bps)) / 10000.0
    if user_activate_price is not None:
        raw = float(user_activate_price)
    else:
        raw = latest * (1.0 + buf) if logical_side == "LONG" else latest * (1.0 - buf)

    if tick > 0:
        if logical_side == "LONG":
            px = math.ceil(raw / tick) * tick
            if px <= latest:
                px += tick
        else:
            px = math.floor(raw / tick) * tick
            if px >= latest:
                px -= tick
        if px <= 0:
            raise ValueError("computed activatePrice <= 0")
    else:
        px = raw

    if logical_side == "LONG" and not (px > latest):
        raise ValueError("activatePrice must be above latest price for LONG trailing exit")
    if logical_side == "SHORT" and not (px < latest):
        raise ValueError("activatePrice must be below latest price for SHORT trailing exit")
    return px


def _tp_state_name(level: int, state: str) -> str:
    return f"TP{int(level)}_{str(state).strip().upper()}"



# ---------------------------------------------------------------------------
# Symbol filter cache (LOT_SIZE / PRICE_FILTER)
# ---------------------------------------------------------------------------

@dataclass
class SymbolFilters:
    tick_size: float     # PRICE_FILTER tickSize (for price quantisation)
    step_size: float     # LOT_SIZE stepSize (for qty quantisation)
    min_qty: float       # LOT_SIZE minQty
    min_notional: float  # MIN_NOTIONAL notional


class FiltersCache:
    """Lazy cache of symbol exchange filters (fetched once per symbol per session)."""

    def __init__(self, client: BinanceFuturesClient):
        self.client = client
        self._cache: dict[str, SymbolFilters] = {}

    def get(self, symbol: str) -> SymbolFilters:
        s = symbol.upper()
        if s in self._cache:
            return self._cache[s]

        info = self.client.get_exchange_info()
        sym_list = info.get("symbols") or []
        by_symbol = {(x.get("symbol")).upper(): x for x in sym_list if x.get("symbol")}
        if s not in by_symbol:
            raise RuntimeError(f"Unknown Binance symbol: {s}")

        filters = by_symbol[s].get("filters") or []
        tick = 0.0
        step = 0.0
        min_qty = 0.0
        min_notional = 0.0
        for f in filters:
            t = (f.get("filterType") or "")
            if t == "PRICE_FILTER":
                tick = _f(f.get("tickSize"), tick)
            elif t == "LOT_SIZE":
                step = _f(f.get("stepSize"), step)
                min_qty = _f(f.get("minQty"), min_qty)
            elif t == "MIN_NOTIONAL":
                min_notional = _f(f.get("notional"), min_notional)

        sf = SymbolFilters(
            tick_size=tick or 0.0, step_size=step or 0.0,
            min_qty=min_qty or 0.0, min_notional=min_notional or 0.0,
        )
        self._cache[s] = sf
        return sf


# ---------------------------------------------------------------------------
# Utility functions for order construction
# ---------------------------------------------------------------------------

def _normalize_side(payload: dict[str, Any]) -> tuple[str, str, int]:
    """Return (binance_side, logical_side, side_int).
    
    internal: logical_side=LONG|SHORT
    execution: binance_side=BUY|SELL
    numeric: side_int=1|-1
    """
    # Prefer explicit fields from payload
    raw = payload.get("side") or payload.get("direction") or ""
    side = normalize_side(raw)
    direction = normalize_direction(raw)
    side_int = get_side_int(raw)

    return str(side.value), str(direction.value), side_int


def _normalize_qty(payload: dict[str, Any], assume_lot_is_qty: bool = True, symbol: str = "") -> float:
    """Extract trade quantity from payload.

    Checks: qty → quantity → lot.
    MT5 payloads use 'lot'; Binance executor explicitly supports it as a fallback.
    If falling back to 'lot', we multiply by the instrument's contract_size (e.g., 100 for XAUUSDT).
    """
    if payload.get("qty") is not None:
        return _f(payload.get("qty"))
    if payload.get("quantity") is not None:
        return _f(payload.get("quantity"))

    if payload.get("lot") is not None:
        lot = _f(payload.get("lot"))
        sym = symbol or (payload.get("symbol") or "")
        try:
            if sym:
                from confidence_calculation.instrument_config import get_specs
                specs = get_specs(sym)
                c_size = getattr(specs, "contract_size", 1.0)
                # Apply contract multiplier so Binance receives native qty
                return lot * float(c_size)
        except Exception:
            pass
        return lot

    raise ValueError(f"missing qty (payload provided no qty/quantity/lot, keys: {list(payload.keys())})")


def _classify_error(e: Exception) -> str:
    """Return 'transient' or 'fatal' for error classification.

    Transient codes (retry-eligible):
      -1021 timestamp out of recvWindow → sync_time() and retry
      -1003 too many requests
      -1001 internal error
      -1007 timeout
      -1100 illegal chars (often transient duplicate)
    Fatal codes (no retry):
      -2021 Order would immediately trigger — stale price, should not retry;
            handle_open performs an automatic MARKET fallback before this point.
      -4411 TradFi-Perps agreement not signed — account-level restriction,
            requires manual action; retrying will always fail.
    All other BinanceAPIError codes, connection issues that look like
    nothing we can retry → fatal.
    """
    if isinstance(e, BinanceAPIError):
        payload = e.payload if isinstance(e.payload, dict) else {}
        code = payload.get("code")
        msg = (payload.get("msg") or "").lower()
        # 503 "Unknown" and ambiguous transport timeouts are reconcile-first,
        # not naive retries — classify as transient so callers can reconcile
        if payload.get("ambiguous") is True or (e.status == 503 and "unknown" in msg):
            return "transient"
        # -4411: TradFi-Perps agreement not signed — always fatal, never retry
        if code == TRADFI_PERPS_NOT_SIGNED:
            return "fatal"
        if code in (-1021, -1003, -1001, -1007, -1100):
            return "transient"
        return "fatal"  # includes -2021, -4045, etc.
    # network
    msg = str(e).lower()
    if "timed out" in msg or "temporary" in msg or "connection" in msg:
        return "transient"
    return "fatal"


def _position_side_for_mode(position_mode: str, logical_side: str) -> str | None:
    """Return positionSide for hedge mode; None for one-way mode."""
    if position_mode != "hedge":
        return None
    return "LONG" if logical_side == "LONG" else "SHORT"


# ---------------------------------------------------------------------------
# Main executor class
# ---------------------------------------------------------------------------

class BinanceExecutor:
    """Consumes orders:queue:binance and executes on Binance USDT-M Futures.

    Lifecycle:
      run_forever() ─ BRPOPLPUSH ─ process_one() ─┬ handle_open()
                                                    ├ handle_modify()
                                                    ├ handle_cancel()
                                                    └ handle_resize() [TODO stub]

    Each successful or failed action results in an event written to
    orders:exec stream for downstream consumers.

    PositionSizer Contract:
      - BinanceExecutor is an execution gateway, NOT a risk/sizing engine.
      - The `qty` field (or `quantity`/`lot`) MUST be pre-calculated by the upstream pre-publish/risk layer.
      - The executor only normalizes and quantizes the received `qty` to exchange LOT_SIZE filters.
      - Margin Guard is a fail-closed safety check, not a sizing calculator.
    """

    def __init__(
        self,
        *,
        redis_client: Any | None = None,
        prod_client: BinanceFuturesClient | None = None,
        demo_client: BinanceFuturesClient | None = None,
        telegram_client: Any | None = None,
    ) -> None:
        if redis_client is None and redis is None:
            raise RuntimeError("redis-py is required (pip install redis)")
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        # Redis connection: injected InMemoryRedis for tests, or real prod connection
        self.r = redis_client if redis_client is not None else redis.from_url(self.redis_url, decode_responses=True)

        # Default queue: orders:queue:binance (separate from MT5 orders:queue:mt5)
        from core.redis_keys import RedisStreams as RS
        self.queue = os.getenv("ORDERS_QUEUE_BINANCE") or os.getenv("ORDERS_QUEUE") or RS.ORDERS_QUEUE_BINANCE
        self.queue_processing = os.getenv("ORDERS_QUEUE_BINANCE_PROCESSING") or f"{self.queue}:processing"
        self.queue_dlq = os.getenv("ORDERS_QUEUE_BINANCE_DLQ") or f"{self.queue}:dlq"
        self.exec_stream = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)
        # Stream size cap: 0 = unlimited (default, backward-compatible).
        # Recommended production value: EXEC_STREAM_MAXLEN=50000 (aligned with janitor policy)
        _maxlen_raw = int(os.getenv("EXEC_STREAM_MAXLEN", "0"))
        self.exec_stream_maxlen: int | None = _maxlen_raw if _maxlen_raw > 0 else None

        # Optional symbol allowlist guard (prevents accidental symbol typos hitting Binance)
        allow = (os.getenv("BINANCE_SYMBOL_ALLOWLIST") or "").strip()
        self.allowlist = {s.strip().upper() for s in allow.split(",") if s.strip()} if allow else set()

        # Position mode: oneway (default) or hedge
        self.position_mode = (os.getenv("BINANCE_POSITION_MODE") or "oneway").strip().lower()
        if self.position_mode not in {"oneway", "hedge"}:
            self.position_mode = "oneway"

        # Safety default: True, seamlessly support MT5 generated payloads
        self.assume_lot_is_qty = _bool_env("BINANCE_ASSUME_LOT_IS_QTY", True)
        self.max_retry = int(os.getenv("BINANCE_MAX_RETRY", "3"))
        self.fill_timeout_s = float(os.getenv("BINANCE_FILL_TIMEOUT_S", "8.0"))
        self.fill_poll_s = float(os.getenv("BINANCE_FILL_POLL_S", "0.25"))

        # Auto-init margin type and leverage on first open per symbol
        self.init_symbol_settings = _bool_env("BINANCE_INIT_SYMBOL_SETTINGS", False)
        self.margin_type = (os.getenv("BINANCE_MARGIN_TYPE") or "ISOLATED").strip().upper()
        # Safe default: 10x. Override per-tier with BINANCE_LEVERAGE_TIER_{A,B,C} or per-symbol.
        self.default_leverage = int(os.getenv("BINANCE_DEFAULT_LEVERAGE", "10"))
        # Policy: force SAFETY_FIRST for Tier-C symbols; limit trailing to post-TP1 only
        self.exec_policy_tier_c_force_safety_first = _bool_env("EXEC_POLICY_TIER_C_FORCE_SAFETY_FIRST", True)
        self.trail_arm_only_after_tp1 = _bool_env("TRAIL_ARM_ONLY_AFTER_TP1", True)

        # Telegram: optional, used only for execution errors and trailing notifications
        self.tg = telegram_client if telegram_client is not None else TelegramClient.from_env()

        # ── Dual-client architecture ──────────────────────────────────────────
        # BINANCE_DEMO_API_KEY  → demo/testnet account (used for virtual trades)
        # BINANCE_API_KEY       → production account   (used for real trades)
        #
        # ENV knobs:
        #   BINANCE_DEMO_API_KEY / BINANCE_DEMO_API_SECRET   — always required for demo client
        #   BINANCE_DEMO_FUTURES_BASE_URL                    — testnet endpoint
        #   BINANCE_API_KEY / BINANCE_API_SECRET             — required for real trades
        #   BINANCE_CLIENT_MODE=demo|real|auto (default auto)
        #     auto: if payload has is_virtual=true → demo client; else prod client
        #     demo: all orders routed to demo regardless of is_virtual flag
        #     real: all orders routed to prod (ignores is_virtual, no demo needed)
        # ─────────────────────────────────────────────────────────────────────
        self._client_mode = (os.getenv("BINANCE_CLIENT_MODE") or "auto").strip().lower()

        # Demo client — built from BINANCE_DEMO_ prefix (testnet), or injected directly
        if demo_client is not None:
            # Injection point: allows test harnesses to supply a mock client
            self.demo_client: BinanceFuturesClient | None = demo_client
            self.demo_filters = FiltersCache(self.demo_client)
        else:
            _demo_key = (os.getenv("BINANCE_DEMO_API_KEY") or "").strip()
            if _demo_key:
                self.demo_client = BinanceFuturesClient.from_env(prefix="BINANCE_DEMO_")
                self.demo_filters = FiltersCache(self.demo_client)
                print(f"   demo_client: base_url={self.demo_client.base_url}")
            else:
                self.demo_client = None
                self.demo_filters = None
                print("   demo_client: not configured (BINANCE_DEMO_API_KEY not set)")

        # Production client — built from BINANCE_ prefix, or injected directly
        if prod_client is not None:
            # Injection point: allows test harnesses to supply a mock client
            self.client: BinanceFuturesClient | None = prod_client
            self.filters = FiltersCache(self.client)
        else:
            _prod_key = (os.getenv("BINANCE_API_KEY") or "").strip()
            if _prod_key:
                self.client = BinanceFuturesClient.from_env(prefix="BINANCE_")
                self.filters = FiltersCache(self.client)
                print(f"   prod_client: base_url={self.client.base_url}")
            else:
                self.client = None
                self.filters = None
                print("   prod_client: not configured (BINANCE_API_KEY not set)")

        if self.demo_client is None and self.client is None:
            raise RuntimeError("At least one of BINANCE_DEMO_API_KEY or BINANCE_API_KEY must be set")

        # --- Trailing stop settings ---
        # Activated only when payload has trail_after_tp1=true
        self.trail_cb_min = float(os.getenv("BINANCE_TRAIL_CALLBACK_MIN", "0.1"))
        self.trail_cb_max = float(os.getenv("BINANCE_TRAIL_CALLBACK_MAX", "5.0"))
        self.trail_cb_default = float(os.getenv("BINANCE_TRAIL_CALLBACK_DEFAULT", "0.3"))
        self.trail_atr_mult_default = float(os.getenv("BINANCE_TRAIL_ATR_MULT", "1.0"))
        self.trail_arm_poll_s = float(os.getenv("BINANCE_TRAIL_ARM_POLL_S", "1.0"))
        self.trail_arm_timeout_s = float(os.getenv("BINANCE_TRAIL_ARM_TIMEOUT_S", "7200"))
        self.trail_notify = _bool_env("BINANCE_TRAIL_NOTIFY", True)
        # BINANCE_TRAIL_ACTIVATE_TP: which TP level triggers trailing (1=TP1, 2=TP2)
        self.trail_activate_tp_level = max(1, int(os.getenv("BINANCE_TRAIL_ACTIVATE_TP", "2")))

        # --- Trailing orchestrator mode ---
        # BINANCE_TRAIL_MODE=orchestrator (default): profile-based continuous SL-move
        # BINANCE_TRAIL_MODE=native: old TRAILING_STOP_MARKET behaviour
        self.trail_mode = (os.getenv("BINANCE_TRAIL_MODE") or "orchestrator").strip().lower()
        if self.trail_mode not in {"native", "orchestrator"}:
            self.trail_mode = "orchestrator"
        self.trail_profile_name = (os.getenv("BINANCE_TRAIL_PROFILE") or "rocket_v1").strip()
        self.trail_sl_move_min_delta_pct = float(os.getenv("BINANCE_TRAIL_SL_MOVE_MIN_DELTA_PCT", "0.05"))
        self.trail_loop_poll_s = float(os.getenv("BINANCE_TRAIL_LOOP_POLL_S", "2.0"))
        self.trail_loop_timeout_s = float(os.getenv("BINANCE_TRAIL_LOOP_TIMEOUT_S", "14400"))

        # Trailing profiles registry (fail-open: fallback to native mode)
        self._trailing_profiles: Any = None
        self._trailing_condition: Any = None
        if self.trail_mode == "orchestrator" and _HAS_TRAILING_PROFILES:
            try:
                self._trailing_profiles = TrailingProfilesRegistry()
                print(f"   trailing profiles: {self._trailing_profiles.list_names()}")
            except Exception as _tpe:
                print(f"   ⚠️ trailing profiles init failed: {_tpe} — fallback to native trail mode")
                self.trail_mode = "native"
        if self.trail_mode == "orchestrator" and _HAS_TRAILING_CONDITION:
            try:
                self._trailing_condition = TrailingConditionEvaluator(
                    redis_client=redis_client if redis_client is not None else self.r,
                )
                print(f"   trailing condition evaluator: enabled={self._trailing_condition.cfg.enabled}")
            except Exception as _tce:
                print(f"   ⚠️ trailing condition evaluator init failed: {_tce} — will skip condition gate")
                self._trailing_condition = None
        print(f"   trail_mode={self.trail_mode} trail_profile={self.trail_profile_name}")

        # --- Explicit trigger/workingType policy ---
        # Binance defaults workingType to CONTRACT_PRICE; we require explicit
        # policy values so risk-critical protection does not silently inherit
        # exchange defaults.
        self.sl_working_type = (os.getenv("SL_WORKING_TYPE") or "MARK_PRICE").strip().upper()
        self.tp_market_working_type = (os.getenv("TP_MARKET_WORKING_TYPE") or "MARK_PRICE").strip().upper()
        self.tp_limit_trigger_working_type = (os.getenv("TP_LIMIT_TRIGGER_WORKING_TYPE") or "MARK_PRICE").strip().upper()
        self.trail_working_type = (os.getenv("TRAIL_WORKING_TYPE") or "MARK_PRICE").strip().upper()

        # Anti-blowup invariant: after entry fill, protection must be confirmed
        # within this window, otherwise the executor will emergency-flatten.
        self.protection_arm_timeout_ms = int(os.getenv("PROTECTION_ARM_TIMEOUT_MS", "2500"))

        # Local protection headroom reserve. Binance does not perform a margin
        # check before algo trigger; we keep a small local reserve to avoid
        # standing a protection order that becomes non-viable at trigger time.
        self.protection_fee_buffer_bps = float(os.getenv("PROTECTION_FEE_BUFFER_BPS", "8.0"))
        self.protection_slippage_bps_a = float(os.getenv("PROTECTION_TIER_A_SLIPPAGE_BUFFER_BPS", os.getenv("PROTECTION_SLIPPAGE_BUFFER_BPS", "15.0")))
        self.protection_slippage_bps_b = float(os.getenv("PROTECTION_TIER_B_SLIPPAGE_BUFFER_BPS", os.getenv("PROTECTION_SLIPPAGE_BUFFER_BPS", "20.0")))
        self.protection_slippage_bps_c = float(os.getenv("PROTECTION_TIER_C_SLIPPAGE_BUFFER_BPS", os.getenv("PROTECTION_SLIPPAGE_BUFFER_BPS", "30.0")))
        self.account_available_floor_usd = float(os.getenv("ACCOUNT_AVAILABLE_FLOOR_USD", "25.0"))

        # Post-close dust/tail cleanup. Binance can leave tiny residual positions
        # when close qty was computed from local intent rather than live positionRisk,
        # or when reduceOnly closes race against lingering plain/algo orders.
        # All exact-flatten paths use these thresholds and timing parameters.
        self.dust_notional_usdt = float(os.getenv("BINANCE_DUST_NOTIONAL_USDT", "3.0"))
        self.dust_margin_usdt = float(os.getenv("BINANCE_DUST_MARGIN_USDT", "1.0"))
        self.dust_close_retries = max(1, int(os.getenv("BINANCE_DUST_CLOSE_RETRIES", "3")))
        self.dust_verify_timeout_ms = max(250, int(os.getenv("BINANCE_DUST_VERIFY_TIMEOUT_MS", "3000")))
        self.dust_verify_poll_ms = max(100, int(os.getenv("BINANCE_DUST_VERIFY_POLL_MS", "250")))

        # --- Explicit trigger/workingType policy ---
        # Binance defaults workingType to CONTRACT_PRICE; we require explicit
        # policy values so risk-critical protection does not silently inherit
        # exchange defaults.
        self.sl_working_type = (os.getenv("SL_WORKING_TYPE") or "MARK_PRICE").strip().upper()
        self.tp_market_working_type = (os.getenv("TP_MARKET_WORKING_TYPE") or "MARK_PRICE").strip().upper()
        self.tp_limit_trigger_working_type = (os.getenv("TP_LIMIT_TRIGGER_WORKING_TYPE") or "MARK_PRICE").strip().upper()
        self.trail_working_type = (os.getenv("TRAIL_WORKING_TYPE") or "MARK_PRICE").strip().upper()

        # Rollout flags centralise which hardened features are enabled in the
        # current deployment. This makes rollback a simple env override rather
        # than a code change.
        self.rollout_flags = RolloutFlags.from_env()

        # Official execution policies
        self.exec_policy_default = (os.getenv("EXEC_POLICY_DEFAULT") or SAFETY_FIRST).strip().replace("-", "_").replace(" ", "_").upper()
        maker_allow = (os.getenv("EXEC_POLICY_MAKER_ALLOWED_SYMBOLS") or "BTCUSDT,ETHUSDT").strip()
        self.exec_policy_maker_allowed_symbols = {s.strip().upper() for s in maker_allow.split(",") if s.strip()}
        self.tp_limit_time_in_force = (os.getenv("TP_LIMIT_TIME_IN_FORCE") or "GTX").strip().upper()
        self.tp_limit_watchdog_enable = self.rollout_flags.exec_maker_tp_enable and _bool_env("TP_LIMIT_WATCHDOG_ENABLE", True)
        self.tp_limit_watchdog_timeout_ms = int(os.getenv("TP_LIMIT_WATCHDOG_TIMEOUT_MS", "6000"))
        self.tp_trigger_monitor_timeout_s = float(os.getenv("TP_TRIGGER_MONITOR_TIMEOUT_S", "7200"))
        self.tp_limit_price_offset_bps = float(os.getenv("TP_LIMIT_PRICE_OFFSET_BPS", "0.0"))
        self.safety_entry_time_in_force = (os.getenv("SAFETY_ENTRY_TIME_IN_FORCE") or "IOC").strip().upper()

        # Trailing activation guard
        self.trail_activate_price_bps = float(os.getenv("TRAIL_ACTIVATE_PRICE_BPS", "5.0"))

        # P0: operator kill-switches for modify/resize (incident containment)
        self.exec_disable_modify_on_binance = _bool_env("EXEC_DISABLE_MODIFY_ON_BINANCE", False)
        self.exec_disable_resize_on_binance = _bool_env("EXEC_DISABLE_RESIZE_ON_BINANCE", False)
        self.exec_blocked_action_reason = os.getenv("EXEC_BLOCKED_ACTION_REASON", "operator_risk_hold")
        self.exec_blocked_action_state_write = _bool_env("EXEC_BLOCKED_ACTION_STATE_WRITE", True)
        # P12: strict open/protect orchestration flags
        self.exec_resume_open_repair = _bool_env("EXEC_RESUME_OPEN_REPAIR", True)
        # EXEC_STRICT_PROTECTION_VERIFY=1: verify on-exchange after placement
        self.exec_strict_protection_verify = _bool_env("EXEC_STRICT_PROTECTION_VERIFY", True)
        # EXEC_RECONCILE_REQUIRE_PROTECTION_COMPLETE=1: reconcile ok only if protection is complete
        self.exec_reconcile_require_protection_complete = _bool_env("EXEC_RECONCILE_REQUIRE_PROTECTION_COMPLETE", True)
        # P3: strict modify/resize replace — max naked window before emergency flatten
        self.protection_replace_max_naked_ms = int(os.getenv("PROTECTION_REPLACE_MAX_NAKED_MS", "3000"))
        # P3: EXEC_MODIFY_RESIZE_STRICT_REPLACE=1 — enforce full cancel+re-arm invariant on modify/resize
        self.exec_modify_resize_strict_replace = _bool_env("EXEC_MODIFY_RESIZE_STRICT_REPLACE", True)


        # Redis connection is now initialized earlier (around line 700)

        # --- orders:state:{sid} — fast lookup of Binance IDs by signal ID ---
        self.state_key_prefix = (os.getenv("ORDERS_STATE_KEY_PREFIX") or "orders:state:").rstrip(":") + ":"
        self.state_ttl = int(os.getenv("ORDERS_STATE_TTL_SEC", "86400"))  # default 24h
        # P3.3: replay/rehydrate knobs. When orders:state:{sid} is absent the
        # executor replays orders:exec to rebuild the snapshot (EXEC_REHYDRATE_ON_STATE_MISS)
        # rather than treating a miss as a fresh signal.
        self.exec_replay_scan_count = int(os.getenv("EXEC_REPLAY_SCAN_COUNT", "20000"))
        self.exec_rehydrate_on_state_miss = _bool_env("EXEC_REHYDRATE_ON_STATE_MISS", True)
        # P1.2.1: journal-primary projection knobs
        # EXEC_JOURNAL_PRIMARY=1   → executor only appends to orders:exec; projection worker writes cache
        # EXEC_STATE_DERIVED_VIEW=1 → orders:state:{sid} is treated as derived, not as SoT
        # EXEC_INLINE_STATE_PROJECTION=0 → disable inline state materialisation from hot-path (default)
        self.exec_journal_primary = _bool_env("EXEC_JOURNAL_PRIMARY", True)
        self.exec_state_derived_view = _bool_env("EXEC_STATE_DERIVED_VIEW", True)
        self.exec_inline_state_projection = _bool_env("EXEC_INLINE_STATE_PROJECTION", False)

        # Counter to limit trailing arm notifications per symbol/side
        self._trail_arm_counts = {}
        self._trail_arm_lock = threading.Lock()

        # Reconcile + user-stream integration. The worker stores the latest
        # normalized ORDER_TRADE_UPDATE / ALGO_UPDATE payloads in Redis so the
        # executor can verify ambiguous submissions before attempting a retry.
        self.reconcile_enable = bool(self.rollout_flags.exec_reconcile_enable and _bool_env("EXEC_RECONCILE_ENABLE", True))
        self.exec_require_user_stream_live = bool(self.rollout_flags.exec_user_stream_enable and _bool_env("EXEC_REQUIRE_USER_STREAM_LIVE", False))
        # P1.2.3: bootstrap gate — executor will not pass startup until both
        # projection cluster and user-stream contour report healthy.
        # Rollback: set EXEC_BOOTSTRAP_REQUIRE_READY=0 (default) to bypass.
        self.exec_bootstrap_require_ready = _bool_env("EXEC_BOOTSTRAP_REQUIRE_READY", False)
        self.exec_bootstrap_timeout_ms = int(os.getenv("EXEC_BOOTSTRAP_TIMEOUT_MS", "0"))
        self.exec_bootstrap_poll_ms = int(os.getenv("EXEC_BOOTSTRAP_POLL_MS", "500"))
        self.exec_single_active_position_per_symbol = _bool_env("EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL", False)
        self.exec_single_active_position_release_on_terminal = _bool_env("EXEC_SINGLE_ACTIVE_POSITION_RELEASE_ON_TERMINAL", True)
        self.exec_single_active_position_stale_timeout_ms = int(os.getenv("EXEC_SINGLE_ACTIVE_POSITION_STALE_TIMEOUT_MS", "900000"))
        # P5: exchange-truth release — guard release only confirmed by Binance position/order state
        self.exec_single_active_position_exchange_truth_release = _bool_env("EXEC_SINGLE_ACTIVE_POSITION_EXCHANGE_TRUTH_RELEASE", True)
        # P5: enable background repair loop (binance_active_symbol_guard_repair_worker)
        self.exec_single_active_position_guard_repair_enable = _bool_env("EXEC_SINGLE_ACTIVE_POSITION_GUARD_REPAIR_ENABLE", True)
        # P5: require both flat position AND no open orders for guard release
        self.exec_single_active_position_require_flat_no_orders = _bool_env("EXEC_SINGLE_ACTIVE_POSITION_REQUIRE_FLAT_NO_ORDERS", True)
        # P5: how stale user-stream must be before we consider it unreliable for guard decisions
        self.exec_active_symbol_user_stream_stale_ms = int(os.getenv("EXEC_ACTIVE_SYMBOL_USER_STREAM_STALE_MS", "30000"))
        self.active_symbol_key_prefix = (os.getenv("ORDERS_ACTIVE_SYMBOL_KEY_PREFIX") or "orders:active_symbol_sid:").rstrip(":") + ":"
        self.active_symbol_guard_tombstone_ttl_sec = int(os.getenv("ACTIVE_SYMBOL_GUARD_TOMBSTONE_TTL_SEC", "120"))
        # P5: Redis key where user-stream worker publishes its liveness doc
        self.user_stream_status_key = os.getenv("USER_STREAM_STATUS_KEY", "orders:user_stream:status")
        self.exec_reconcile_on_503_unknown = _bool_env("EXEC_RECONCILE_ON_503_UNKNOWN", True)
        self.exec_reconcile_prefer_user_stream = _bool_env("EXEC_RECONCILE_PREFER_USER_STREAM", True)
        self.user_stream_cache_prefix = (os.getenv("USER_STREAM_CACHE_PREFIX") or "orders:user_stream:").rstrip(":") + ":"
        self.user_stream_stream = os.getenv("USER_STREAM_STREAM", "orders:user_stream")
        self.orders_quarantine_sids_key = (os.getenv("ORDERS_QUARANTINE_SIDS_KEY") or "orders:quarantine:state:sids").strip()
        self.exec_quarantine_resume_guard_enable = _bool_env("EXEC_QUARANTINE_RESUME_GUARD_ENABLE", True)
        self.binance_recv_window_ms = int(os.getenv("BINANCE_RECV_WINDOW_MS", os.getenv("BINANCE_RECV_WINDOW", "5000")))
        self.binance_time_sync_interval_ms = int(os.getenv("BINANCE_TIME_SYNC_INTERVAL_MS", "30000"))
        self.max_clock_drift_ms = int(os.getenv("MAX_CLOCK_DRIFT_MS", "250"))
        self._next_time_sync_due_ms = 0

        # Best-effort durable SQL mirror for incident analysis. Redis stream
        # remains the online source of truth; Postgres journal is auxiliary.
        # Disabled if EXEC_JOURNAL_SQL_ENABLE=0 or EXECUTION_JOURNAL_DSN is not set.
        self.execution_journal = ExecutionJournalSink() if self.rollout_flags.exec_journal_sql_enable else ExecutionJournalSink(dsn="")
        self.exec_fee_maker_bps = float(os.getenv("EXEC_FEE_MAKER_BPS", "2.0"))
        self.exec_fee_taker_bps = float(os.getenv("EXEC_FEE_TAKER_BPS", "5.0"))
        self._maker_tp_stats: dict[tuple[str, int], dict[str, float]] = {}

        # TradFi-Perps agreement guard (Binance -4411).
        # Symbols blocked here have received a -4411 response from Binance,
        # meaning the account has not signed the TradFi-Perps contract for them.
        # All subsequent opens for the same symbol are immediately rejected
        # without hitting Binance, suppressing spam retries.
        # Reset: executor restart (in-memory only; intentional — operator must
        # sign the agreement and restart to re-enable trading for the symbol).
        self._tradfi_blocked: set[str] = set()

        # --- Margin Guard Latency Optimization ---
        # Cache account balance to avoid redundant REST calls during signal bursts.
        self.margin_guard_cache_s = float(os.getenv("BINANCE_MARGIN_GUARD_CACHE_S", "10.0"))
        self._account_cache: dict[int, dict[str, Any]] = {}  # {id(client): {"balance": float, "ts": float}}
        self._account_cache_lock = threading.Lock()

    def _is_sid_quarantined(self, sid: str) -> bool:
        if not self.exec_quarantine_resume_guard_enable or not sid or not self.orders_quarantine_sids_key:
            return False
        try:
            return bool(self.r.sismember(self.orders_quarantine_sids_key, sid))
        except Exception:
            return False

    def _get_available_balance(self, client: BinanceFuturesClient) -> float:
        """Fetch availableBalance from Binance with TTL caching to avoid REST latency.
        
        The 10s default TTL is a compromise between safety and execution speed.
        A 4x margin leverage guard is resilient to small balance drifts within 10s.
        """
        now = time.time()
        client_id = id(client)

        # 1. Fast path (no lock)
        cache = self._account_cache.get(client_id)
        if cache and (now - cache["ts"] < self.margin_guard_cache_s):
            return float(cache["balance"])

        # 2. Slow path (fetch fresh)
        with self._account_cache_lock:
            # Double check inside lock
            cache = self._account_cache.get(client_id)
            if cache and (now - cache["ts"] < self.margin_guard_cache_s):
                return float(cache["balance"])

            # Perform synchronous REST call (approx 50-200ms)
            account_data = client.get_account()
            balance = float(account_data.get("availableBalance", 0.0))

            # Update cache
            self._account_cache[client_id] = {
                "balance": balance,
                "ts": now
            }
            return balance

    def _guard_sid_not_quarantined(self, sid: str, *, symbol: str, action: str) -> None:
        if not self._is_sid_quarantined(sid):
            return
        self._exec_event({
            'sid': sid, 'symbol': symbol, 'action': action, 'severity': 'warning',
            'msg': 'resume/open blocked: sid is quarantined', 'event_type': 'RESUME_GUARD_BLOCKED',
        })
        raise RuntimeError(f'sid is quarantined: {sid}')

    def _resolve_client(
        self, payload: dict[str, Any]
    ) -> tuple[BinanceFuturesClient, FiltersCache]:
        """Return (client, filters_cache) for this payload.

        Routing logic:
          BINANCE_CLIENT_MODE=demo  → always demo client
          BINANCE_CLIENT_MODE=real  → always prod client
          BINANCE_CLIENT_MODE=auto  → is_virtual=true → demo; else → prod
        """
        use_demo: bool
        if self._client_mode == "demo":
            use_demo = True
        elif self._client_mode == "real":
            use_demo = False
        else:  # auto
            use_demo = _truthy(payload.get("is_virtual")) or _truthy(payload.get("virtual"))

        if use_demo:
            if self.demo_client is None:
                raise RuntimeError(
                    "Virtual/demo order requested but BINANCE_DEMO_API_KEY is not configured"
                )
            return self.demo_client, self.demo_filters  # type: ignore[return-value]

        if self.client is None:
            # Fallback to demo if no prod client
            if self.demo_client is not None:
                return self.demo_client, self.demo_filters  # type: ignore[return-value]
            raise RuntimeError("BINANCE_API_KEY not configured and no demo client available")
        return self.client, self.filters  # type: ignore[return-value]


    # --- P5 Audit chain helpers ---

    def _derive_audit_chain_fields(self, source: dict[str, Any], sid: str) -> dict[str, Any]:
        """Build a stable audit chain carried in Redis state and SQL mirrors.

        `signal_id` and `execution_plan_id` may arrive from the upstream signal
        envelope.  When they do not, we derive deterministic fallbacks so SQL
        joins remain possible after restarts/backfills.
        """
        src = source or {}
        signal_id = str(src.get('signal_id') or src.get('decision_id') or src.get('id') or sid or '').strip()
        execution_plan_id = str(src.get('execution_plan_id') or src.get('decision_id') or signal_id or sid or '').strip()
        return {
            'signal_id': signal_id,
            'execution_plan_id': execution_plan_id,
        }

    @staticmethod
    def _format_order_ref(*, venue: str, kind: str, order_id: Any = None, client_id: Any = None) -> str:
        """Build a compact pipe-delimited order reference string for audit joins.

        Example: 'binance|entry|oid=123456789|cid=abcd1234-entry'
        """
        parts = [(venue or 'binance').strip(), (kind or '').strip()]
        if order_id not in (None, '', 0, '0'):
            parts.append(f"oid={order_id}")
        if client_id not in (None, ''):
            parts.append(f"cid={client_id}")
        return '|'.join([p for p in parts if p])

    def _derive_entry_exit_policies(self, *, execution_policy: str) -> dict[str, str]:
        """Return entry/exit policy names based on execution_policy.

        Stored in execution_orders for analytics joins without parsing state_jsonb.
        """
        if str(execution_policy).upper() == MAKER_FIRST:
            return {
                'entry_policy': 'ENTRY_MARKET_OR_SHORT_IOC',
                'exit_policy': 'SL_STOP_MARKET__TP_LIMIT_LADDER__TRAIL_OPTIONAL',
            }
        return {
            'entry_policy': 'ENTRY_MARKET_OR_SHORT_IOC',
            'exit_policy': 'SL_STOP_MARKET__TP_MARKET__TRAIL_OPTIONAL',
        }

    def _new_closed_trade_id(self, sid: str, *, exit_order_ref: str = '') -> str:
        """Generate a stable closed_trade_id that survives restarts.

        sha1[:12] suffix over (sid|exit_order_ref|ts_ms) gives a unique
        but short identifier for joins in analytics tables.
        """
        import hashlib as _hashlib
        suffix = _hashlib.sha1(f"{sid}|{exit_order_ref}|{_ms_now()}".encode()).hexdigest()[:12]
        return f"closed:{sid}:{suffix}"




    def _exec_event(self, fields: dict[str, Any]) -> None:
        """Write one canonical fact to ``orders:exec``.

        The executor appends to the primary journal synchronously. Projection into
        ``orders:state:{sid}`` is optional and disabled by default so derived
        state materialization can run in a separate deterministic worker.
        """
        raw = dict(fields or {})
        sid = (raw.get('sid') or '').strip()
        symbol = (raw.get('symbol') or '').strip().upper()
        action = str(raw.get('action') or raw.get('event_type') or 'event').strip() or 'event'
        event_type = str(raw.get('event_type') or action).strip() or 'event'
        status = (raw.get('status') or 'ok').strip() or 'ok'
        ts_event_ms = int(raw.get('ts_event_ms') or raw.get('ts_ms') or _ms_now())

        # Determine side_int if not present
        side_int = raw.get('side_int')
        if side_int is None:
            raw_side = raw.get('side') or raw.get('logical_side') or raw.get('direction')
            if raw_side:
                side_int = get_side_int(str(raw_side))

        # P1: Unified ExecutionEventV1
        try:
            # If this is a fill event, use ExecutionEventV1
            if action in {"fill", "entry_filled", "exit_filled", "tp_filled", "sl_filled"}:
                # Map fields to ExecutionEventV1
                ev_v1 = ExecutionEventV1(
                    exec_id=str(raw.get('exec_id') or f"exec:{sid}:{ts_event_ms}"),
                    order_id=str(raw.get('order_id') or raw.get('binance_order_id') or ""),
                    client_order_id=str(raw.get('client_order_id') or raw.get('entry_client_order_id') or ""),
                    symbol=symbol,
                    ts_ms=ts_event_ms,
                    side=Side(normalize_side(str(raw.get('side') or raw.get('logical_side') or '')).value),
                    price=float(raw.get('avg_price') or raw.get('price') or 0.0),
                    qty=float(raw.get('filled_qty') or raw.get('qty') or 0.0),
                    side_int=side_int or 0,
                    status=status.upper(),
                    meta={k: v for k, v in raw.items() if v is not None}
                )
                stream_fields = ev_v1.model_dump()
            else:
                # General event: keep legacy structure but add side_int
                core_keys = {
                    'sid', 'symbol', 'action', 'event_type', 'status', 'severity',
                    'ts_event_ms', 'ts_exec_start_ms', 'ts_queue_ms', 'ts_state_commit_ms',
                    'ts_ms', 'mono_ms',
                }
                payload = {k: v for k, v in raw.items() if k not in core_keys and v is not None}
                if side_int is not None:
                    payload['side_int'] = side_int

                event = ExecutionEvent(
                    sid=sid,
                    symbol=symbol,
                    action=action,
                    event_type=event_type,
                    status=status,
                    ts_event_ms=ts_event_ms,
                    ts_exec_start_ms=_i(raw.get('ts_exec_start_ms')) or None,
                    ts_queue_ms=_i(raw.get('ts_queue_ms')) or None,
                    ts_state_commit_ms=_i(raw.get('ts_state_commit_ms')) or None,
                    severity=(raw.get('severity') or '').strip() or None,
                    payload={**payload, 'mono_ms': str(_mono_ms()), 'venue': 'binance'},
                )
                stream_fields = event.to_stream_fields()
        except Exception as e:
            # Fallback to plain dict if Pydantic fails
            stream_fields = dict(raw)
            stream_fields.update({
                'ts_ms': str(ts_event_ms),
                'error_mapping': str(e)
            })

        stream_fields.setdefault('ts_ms', str(ts_event_ms))

        stream_id = ''
        try:
            stream_id = str(self.r.xadd(
                self.exec_stream,
                {k: str(v) for k, v in stream_fields.items() if v is not None},
                maxlen=getattr(self, 'exec_stream_maxlen', 100000),
                approximate=True,
            ) or '')
        except Exception:
            stream_id = ''
        try:
            sink = getattr(self, "execution_journal", None)
            if sink is not None:
                sink.record_event(stream_fields)
        except Exception:
            pass
        try:
            if sid and getattr(self, 'exec_inline_state_projection', False):
                self._project_materialized_state_from_event(sid, stream_fields, stream_id=stream_id)
        except Exception:
            pass

    def _append_state_patch_event(self, sid: str, patch: dict[str, Any]) -> None:
        """Append a derived-state patch event instead of mutating Redis state inline."""
        doc = dict(patch or {})
        if not sid:
            return
        symbol = (doc.get('symbol') or '').strip().upper()
        action = (doc.get('action') or 'state_patch').strip() or 'state_patch'
        self._exec_event({
            'sid': sid,
            'symbol': symbol,
            'action': action,
            'event_type': 'state_patch',
            'status': (doc.get('status') or 'ok').strip() or 'ok',
            **doc,
        })

    def _dlq(self, raw: str, reason: str) -> None:
        """Push unprocessable message to DLQ list (fail-open)."""
        with contextlib.suppress(Exception):
            self.r.lpush(
                self.queue_dlq,
                json.dumps({"reason": reason, "raw": raw, "ts_ms": _ms_now()}),
            )

    def _ack_processing(self, raw: str) -> None:
        """Remove message from the processing list (BRPOPLPUSH safety net)."""
        with contextlib.suppress(Exception):
            self.r.lrem(self.queue_processing, 1, raw)

    def _requeue(self, payload: dict[str, Any], raw: str, reason: str) -> None:
        """Push back to main queue with incremented retry counter."""
        retry_n = int(payload.get("retry_n") or 0)
        payload["retry_n"] = retry_n + 1
        payload["retry_reason"] = reason
        new_raw = json.dumps(payload, ensure_ascii=False, default=str)
        try:
            self.r.rpush(self.queue, new_raw)
        except Exception:
            # Can't requeue: DLQ the original
            self._dlq(raw, f"requeue_failed:{reason}")

    def _active_symbol_state_key(self, symbol: str) -> str:
        return f"{self.active_symbol_key_prefix}{(symbol or '').strip().upper()}"

    def _guard_store(self) -> ActiveSymbolGuardStore:
        if not hasattr(self, '_active_symbol_guard_store'):
            self._active_symbol_guard_store = ActiveSymbolGuardStore(
                self.r,
                key_prefix=self.active_symbol_key_prefix,
                active_ttl_sec=self.state_ttl,
                tombstone_ttl_sec=self.active_symbol_guard_tombstone_ttl_sec,
            )
        return self._active_symbol_guard_store

    def _record_active_symbol_guard_cas(self, symbol: str, outcome: str, reason: str) -> None:
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL.labels(
                    symbol=(symbol or "").strip().upper(),
                    writer="executor",
                    outcome=(outcome or ""),
                    reason=(reason or "")
                ).inc()
        except Exception:
            pass

    def _state_is_terminalish(self, state: dict[str, Any] | None) -> bool:
        doc = dict(state or {})
        fsm_state = (doc.get("fsm_state") or "").strip().upper()
        if fsm_state in TERMINAL_FSM_STATES:
            return True
        status = (doc.get("status") or "").strip().lower()
        if status in {"closed", "cancelled", "canceled", "failed", "exited", "exit_filled", "emergency_flattened"}:
            return True
        if bool(doc.get("closed")):
            return True
        return False

    def _load_active_symbol_guard(self, symbol: str) -> dict[str, Any]:
        return self._guard_store().load_active(symbol)

    def _clear_active_symbol_guard(self, symbol: str, *, expected_sid: str = "") -> None:
        try:
            res = self._guard_store().mark_released(
                symbol=symbol,
                expected_sid=expected_sid,
                release_reason="executor_terminal_clear",
                writer="executor",
            )
            self._record_active_symbol_guard_cas(
                symbol=symbol, outcome="success" if res.get('applied') else "rejected", reason=res.get('reason') or "unknown"
            )
        except Exception:
            self._record_active_symbol_guard_cas(symbol=symbol, outcome="error", reason="exception")

    def _load_user_stream_status_doc(self) -> dict[str, Any]:
        """Read the user-stream liveness doc from Redis. Returns {} on any error."""
        try:
            raw = self.r.get(getattr(self, "user_stream_status_key", "orders:user_stream:status"))
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _user_stream_is_stale_for_active_guard(self) -> bool:
        """True when user-stream is disconnected or last event is older than threshold."""
        threshold_ms = int(getattr(self, "exec_active_symbol_user_stream_stale_ms", 30000) or 30000)
        if threshold_ms <= 0:
            return False
        doc = self._load_user_stream_status_doc()
        if not doc:
            return True
        if not bool(doc.get("connected", False)):
            return True
        last_ms = _i(doc.get("last_event_ms") or doc.get("last_ingest_ms") or doc.get("updated_at_ms"), 0)
        if last_ms <= 0:
            return True
        return max(0, _ms_now() - int(last_ms)) > threshold_ms

    def _read_active_symbol_exchange_truth(
        self, *, symbol: str, client: BinanceFuturesClient | None
    ) -> dict[str, Any]:
        """Query Binance for real position and open-order state.

        P5: This is the canonical source of truth used to release stuck guards.
        Checks: positionRisk (positionAmt), openOrders, openAlgoOrders.
        Returns a dict with is_flat=True only if all three confirm the symbol is clean.
        """
        truth: dict[str, Any] = {
            "symbol": (symbol or "").strip().upper(),
            "checked_at_ms": _ms_now(),
            "position_amt": 0.0,
            "has_live_position": False,
            "open_plain_orders": 0,
            "open_algo_orders": 0,
            "has_open_orders": False,
            "is_flat": False,
            "is_reliable": False,
            "errors": [],
        }
        if client is None:
            truth["errors"] = ["client_missing"]
            return truth
        errors: list[str] = []
        try:
            risks = client.get_position_risk() or []
            for pos in risks:
                if str((pos or {}).get("symbol") or "").upper() != truth["symbol"]:
                    continue
                amt = _f((pos or {}).get("positionAmt"), 0.0)
                truth["position_amt"] = amt
                truth["has_live_position"] = not math.isclose(float(amt), 0.0, abs_tol=1e-12)
                break
        except Exception as exc:
            errors.append(f"position_risk:{exc.__class__.__name__}")
        try:
            plain = client.get_open_orders(truth["symbol"]) or []
            truth["open_plain_orders"] = len(list(plain))
        except Exception as exc:
            errors.append(f"open_orders:{exc.__class__.__name__}")
        try:
            algo = client.get_open_algo_orders(truth["symbol"]) or []
            truth["open_algo_orders"] = len(list(algo))
        except Exception as exc:
            errors.append(f"open_algo_orders:{exc.__class__.__name__}")
        truth["has_open_orders"] = int(truth["open_plain_orders"]) > 0 or int(truth["open_algo_orders"]) > 0
        truth["errors"] = list(errors)
        require_flat_no_orders = bool(getattr(self, "exec_single_active_position_require_flat_no_orders", True))
        truth["is_reliable"] = not errors
        truth["is_flat"] = (
            not truth["has_live_position"]
            and (not truth["has_open_orders"] if require_flat_no_orders else True)
            and truth["is_reliable"]
        )
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL is not None:
                result = "flat" if truth["is_flat"] else ("active" if truth["is_reliable"] else "error")
                EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL.labels(
                    symbol=truth["symbol"], result=result
                ).inc()
        except Exception:
            pass
        return truth

    def _refresh_active_symbol_guard_from_exchange(
        self,
        *,
        symbol: str,
        blocked_by_sid: str,
        guard: dict[str, Any],
        blocked_state_doc: dict[str, Any],
        exchange_truth: dict[str, Any],
        reason: str,
    ) -> None:
        """Persist exchange-truth snapshot back into the guard key and bump stuck metric.

        P5: When we cannot release the guard (exchange still shows live position or
        open orders), we annotate the guard key with exchange metadata so operators
        and the repair worker can understand why it's still held.
        """
        if not bool(getattr(self, "exec_single_active_position_guard_repair_enable", True)):
            return
        try:
            # P6: evaluate terminal-ish state once for semantic fields
            state_terminalish = bool(self._state_is_terminalish(blocked_state_doc))
            updated = dict(guard or {})
            updated.update({
                "symbol": (symbol or "").strip().upper(),
                "sid": blocked_by_sid,
                "fsm_state": str(
                    (blocked_state_doc or {}).get("fsm_state")
                    or updated.get("fsm_state")
                    or updated.get("state")
                    or ""
                ),
                "state": str(
                    (blocked_state_doc or {}).get("fsm_state")
                    or updated.get("state")
                    or updated.get("fsm_state")
                    or ""
                ),
                "updated_at_ms": _ms_now(),
                "exchange_truth_checked_at_ms": int(exchange_truth.get("checked_at_ms") or _ms_now()),
                "exchange_position_amt": float(exchange_truth.get("position_amt") or 0.0),
                "exchange_open_plain_orders": int(exchange_truth.get("open_plain_orders") or 0),
                "exchange_open_algo_orders": int(exchange_truth.get("open_algo_orders") or 0),
                "exchange_guard_reason": (reason or "exchange_truth_active"),
                # P6 unified semantic fields: same contract as projection worker
                "guard_release_policy": "exchange_truth" if bool(getattr(self, "exec_single_active_position_exchange_truth_release", True)) else "local_terminal",
                "guard_release_pending": bool(state_terminalish and bool(getattr(self, "exec_single_active_position_exchange_truth_release", True))),
                "guard_release_reason": "await_exchange_flat_no_orders" if state_terminalish and bool(getattr(self, "exec_single_active_position_exchange_truth_release", True)) else "",
                "state_terminalish": bool(state_terminalish),
                "user_stream_stale": bool(self._user_stream_is_stale_for_active_guard()),
            })
            res = self._guard_store().acquire_or_refresh(
                symbol=symbol,
                sid=blocked_by_sid,
                payload_patch=updated,
                writer="executor",
            )
            self._record_active_symbol_guard_cas(
                symbol=symbol, outcome="success" if res.get('applied') else "rejected", reason=res.get('reason') or "unknown"
            )
        except Exception:
            self._record_active_symbol_guard_cas(symbol=symbol, outcome="error", reason="exception")
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL.labels(
                    symbol=(symbol or "").strip().upper(),
                    reason=(reason or "exchange_truth_active"),
                ).inc()
        except Exception:
            pass

    def _load_manual_symbol_hold(self, symbol: str) -> dict[str, Any]:
        """Load the manual symbol hold document from Redis.

        Returns {} if:
        - no hold exists
        - hold is expired (expires_at_ms <= now)
        - hold_status is not 'active'
        P12: hold blocks new open orders for the duration of the TTL.
        """
        symbol = (symbol or '').strip().upper()
        if not symbol:
            return {}
        try:
            raw = self.r.get(f'orders:active_symbol_guard:hold:symbol:{symbol}')
            doc = json.loads(raw) if raw else {}
            if not isinstance(doc, dict):
                return {}
            expires_at_ms = _i(doc.get('expires_at_ms'), 0)
            if expires_at_ms and expires_at_ms <= _ms_now():
                return {}
            if (doc.get('hold_status') or 'active').strip().lower() != 'active':
                return {}
            return doc
        except Exception:
            return {}

    def _guard_symbol_not_manually_held(self, *, symbol: str, action: str) -> None:
        """Raise ExecutionActionBlockedError if an active manual hold exists for symbol.

        P12: manual hold blocks execution path for new opens only.
        The hold is TTL-bound and operator/ticket attributed.
        Existing positions are not affected.
        """
        hold = self._load_manual_symbol_hold(symbol)
        if not hold:
            return
        with contextlib.suppress(Exception):
            self._exec_event({
                'symbol': (symbol or '').upper(),
                'action': (action or ''),
                'severity': 'warning',
                'subtype': 'manual_symbol_hold',
                'msg': 'symbol blocked by active manual hold',
                'ticket': (hold.get('ticket') or ''),
                'operator': (hold.get('operator') or ''),
                'reason': (hold.get('reason') or ''),
            })
        raise OpenBlockedByActiveSymbolError({
            'symbol': (symbol or '').upper(),
            'action': (action or ''),
            'event_type': 'open_blocked_by_manual_symbol_hold',
            'status': 'blocked',
            'severity': 'warning',
            'reason': f'{action}_blocked_by_manual_symbol_hold',
            'ticket': (hold.get('ticket') or ''),
            'operator': (hold.get('operator') or ''),
        })

    def _guard_single_active_symbol_open(
        self, *, sid: str, symbol: str, client: BinanceFuturesClient | None = None
    ) -> None:
        """Block a new open if the symbol already has an active execution guard.

        P5 logic:
          - When exec_single_active_position_exchange_truth_release=True AND a client
            is provided, the guard is released ONLY if Binance confirms the symbol is
            completely flat (positionAmt==0 AND no open plain/algo orders).
          - If Binance still shows live exposure or open orders, the guard stays and
            the guard key is annotated with the exchange snapshot + reason.
          - Legacy terminal-state release is preserved as a fast-path fallback but
            only when exchange-truth release is disabled (flag=False).
        """
        if not self.exec_single_active_position_per_symbol:
            return
        guard = self._load_active_symbol_guard(symbol)
        blocked_by_sid = (guard.get("sid") or "").strip()
        if not blocked_by_sid or blocked_by_sid == sid:
            return
        blocked_state = str(guard.get("fsm_state") or guard.get("state") or guard.get("status") or "").strip().upper()
        updated_at_ms = _i(guard.get("updated_at_ms") or guard.get("ts_state_commit_ms") or guard.get("ts_event_ms"), 0)
        age_ms = max(0, _ms_now() - int(updated_at_ms or 0)) if updated_at_ms else 0
        blocked_state_doc = self._load_order_state(blocked_by_sid) if blocked_by_sid else {}
        user_stream_stale = self._user_stream_is_stale_for_active_guard()

        # P5: prioritise exchange-truth check when enabled and client is available.
        # Do not release on terminal/stale cache alone — require exchange confirmation.
        if bool(getattr(self, "exec_single_active_position_exchange_truth_release", True)) and client is not None:
            exchange_truth = self._read_active_symbol_exchange_truth(symbol=symbol, client=client)
            if bool(exchange_truth.get("is_flat")):
                # Exchange confirmed: symbol is flat with no open orders — safe to release
                self._clear_active_symbol_guard(symbol, expected_sid=blocked_by_sid)
                try:
                    if EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL is not None:
                        EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL.labels(
                            symbol=symbol, reason="exchange_flat_no_orders"
                        ).inc()
                except Exception:
                    pass
                return
            # Determine why guard cannot be released — used for metrics/ops visibility
            repair_reason = "exchange_truth_active"
            if exchange_truth.get("errors"):
                repair_reason = "exchange_check_error"
            elif exchange_truth.get("has_live_position"):
                repair_reason = "exchange_open_position"
            elif exchange_truth.get("has_open_orders"):
                repair_reason = "exchange_open_orders"
            elif self._state_is_terminalish(blocked_state_doc):
                repair_reason = "terminal_state_but_exchange_not_flat"
            elif age_ms > int(getattr(self, "exec_single_active_position_stale_timeout_ms", 900000) or 0) and not blocked_state_doc:
                repair_reason = "stale_guard_but_exchange_not_flat"
            # Annotate the guard key with exchange metadata for observability
            self._refresh_active_symbol_guard_from_exchange(
                symbol=symbol,
                blocked_by_sid=blocked_by_sid,
                guard=guard,
                blocked_state_doc=blocked_state_doc,
                exchange_truth=exchange_truth,
                reason=repair_reason,
            )
            final_state = blocked_state or str((blocked_state_doc or {}).get("fsm_state") or "") or "unknown"
            details = {
                "sid": sid,
                "symbol": symbol,
                "action": "open",
                "event_type": "open_blocked_by_active_symbol_position",
                "status": "blocked",
                "severity": "warning",
                "reason": "single_active_position_per_symbol",
                "blocked_by_sid": blocked_by_sid,
                "blocked_by_state": final_state,
                "active_symbol_guard_age_ms": age_ms,
                # P5: exchange snapshot fields in blocked event
                "exchange_position_amt": float(exchange_truth.get("position_amt") or 0.0),
                "exchange_open_plain_orders": int(exchange_truth.get("open_plain_orders") or 0),
                "exchange_open_algo_orders": int(exchange_truth.get("open_algo_orders") or 0),
                "exchange_truth_errors": list(exchange_truth.get("errors") or []),
                "user_stream_stale": bool(user_stream_stale),
            }
            if EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL:
                EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL.labels(symbol=symbol, blocked_state=final_state).inc()
            raise OpenBlockedByActiveSymbolError(details)

        # Fallback: legacy terminal-state release (only when exchange-truth is disabled)
        if self.exec_single_active_position_release_on_terminal and self._state_is_terminalish(blocked_state_doc):
            self._clear_active_symbol_guard(symbol, expected_sid=blocked_by_sid)
            return
        if self.exec_single_active_position_stale_timeout_ms > 0 and age_ms > int(self.exec_single_active_position_stale_timeout_ms) and not blocked_state_doc:
            self._clear_active_symbol_guard(symbol, expected_sid=blocked_by_sid)
            return
        final_state = blocked_state or str((blocked_state_doc or {}).get("fsm_state") or "") or "unknown"
        details = {
            "sid": sid,
            "symbol": symbol,
            "action": "open",
            "event_type": "open_blocked_by_active_symbol_position",
            "status": "blocked",
            "severity": "warning",
            "reason": "single_active_position_per_symbol",
            "blocked_by_sid": blocked_by_sid,
            "blocked_by_state": final_state,
            "active_symbol_guard_age_ms": age_ms,
            "user_stream_stale": bool(user_stream_stale),
        }
        if EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL:
            EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL.labels(symbol=symbol, blocked_state=final_state).inc()
        raise OpenBlockedByActiveSymbolError(details)

    def _load_materialized_state_cache(self, sid: str) -> dict[str, Any]:
        """Return the raw orders:state cache document without replay side effects."""
        try:
            raw = self.r.get(f"{self.state_key_prefix}{sid}")
            if raw:
                doc = json.loads(raw)
                if isinstance(doc, dict):
                    return doc
        except Exception:
            pass
        return {}

    def _persist_materialized_state_cache(self, sid: str, state: dict[str, Any]) -> dict[str, Any]:
        """Persist one derived orders:state snapshot in canonical materialized form."""
        try:
            existing = self._load_materialized_state_cache(sid)
            merged = dict(existing)
            merged.update(state or {})
            if 'created_at_ms' not in merged:
                merged['created_at_ms'] = int(existing.get('created_at_ms') or _ms_now())
            merged['updated_at_ms'] = _ms_now()
            # Build the canonical nested view (entry/protective/trailing) before writing
            doc = build_materialized_state_view({"ts_ms": _ms_now(), "venue": "binance", **merged})
            self.r.set(
                f"{self.state_key_prefix}{sid}",
                json.dumps(doc, ensure_ascii=False, default=str),
                ex=self.state_ttl if self.state_ttl > 0 else None,
            )
            if self.exec_single_active_position_per_symbol:
                symbol = (doc.get("symbol") or "").strip().upper()
                if symbol:
                    if not self._state_is_terminalish(doc):
                        try:
                            # P6: non-terminal state — guard is active, not pending release
                            guard_doc = dict(doc)
                            guard_doc.update({
                                "guard_release_policy": "exchange_truth" if bool(getattr(self, "exec_single_active_position_exchange_truth_release", True)) else "local_terminal",
                                "guard_release_pending": False,
                                "guard_release_reason": "",
                                "state_terminalish": False,
                            })
                            res = self._guard_store().acquire_or_refresh(
                                symbol=symbol,
                                sid=sid,
                                payload_patch=guard_doc,
                                writer="executor",
                            )
                            self._record_active_symbol_guard_cas(
                                symbol=symbol, outcome="success" if res.get('applied') else "rejected", reason=res.get('reason') or "unknown"
                            )
                        except Exception:
                            self._record_active_symbol_guard_cas(symbol=symbol, outcome="error", reason="exception")
                    elif bool(getattr(self, "exec_single_active_position_exchange_truth_release", True)):
                        # P6: terminal state + exchange_truth_release=True — do NOT delete the guard;
                        # mark it as pending-release so repair worker can confirm via exchange truth.
                        try:
                            guard_doc = dict(doc)
                            guard_doc.update({
                                "guard_release_policy": "exchange_truth",
                                "guard_release_pending": True,
                                "guard_release_reason": "await_exchange_flat_no_orders",
                                "state_terminalish": True,
                            })
                            res = self._guard_store().acquire_or_refresh(
                                symbol=symbol,
                                sid=sid,
                                payload_patch=guard_doc,
                                writer="executor",
                            )
                            self._record_active_symbol_guard_cas(
                                symbol=symbol, outcome="success" if res.get('applied') else "rejected", reason=res.get('reason') or "unknown"
                            )
                        except Exception:
                            self._record_active_symbol_guard_cas(symbol=symbol, outcome="error", reason="exception")
                    else:
                        # P5 legacy: when exchange-truth release is disabled, do NOT auto-clear
                        # the guard on terminal state inside _persist_materialized_state_cache.
                        # Release is handled by _guard_single_active_symbol_open (inline)
                        # or the background repair worker after exchange confirmation.
                        if self.exec_single_active_position_release_on_terminal and not bool(
                            getattr(self, "exec_single_active_position_exchange_truth_release", True)
                        ):
                            self._clear_active_symbol_guard(symbol, expected_sid=sid)
            try:
                sink = getattr(self, "execution_journal", None)
                if sink is not None:
                    sink.upsert_order_snapshot(doc)
                    sink.upsert_protection_refs(doc)
            except Exception:
                pass
            return doc
        except Exception:
            return dict(state or {})


    def _save_order_state(self, sid: str, state: dict[str, Any]) -> None:
        """Update the derived orders:state cache from the primary execution journal."""
        try:
            if not getattr(self, 'exec_inline_state_projection', False):
                self._append_state_patch_event(sid, state)
                return
            base: dict[str, Any] = {}
            if getattr(self, 'exec_journal_primary', True):
                base = self._recover_state_from_exec_stream(sid) or {}
            if not base:
                base = self._load_materialized_state_cache(sid)
            merged = dict(base)
            merged.update(state or {})
            self._persist_materialized_state_cache(sid, merged)
        except Exception:
            pass  # fail-open: state is best-effort; exec stream is authoritative

    def _project_materialized_state_from_event(self, sid: str, event_fields: dict[str, Any], *, stream_id: str = '') -> dict[str, Any]:
        """Project one newly appended exec event into the materialized state cache."""
        if not sid or not getattr(self, 'exec_state_derived_view', True):
            return {}
        try:
            base = self._load_materialized_state_cache(sid)
            ev = dict(event_fields or {})
            projected = project_event_into_state(ev, base_state=base, stream_id=stream_id)
            projected['ts_state_commit_ms'] = _ms_now()
            return self._persist_materialized_state_cache(sid, projected)
        except Exception:
            return {}

    def _replay_checkpoint_key(self, sid: str) -> str:
        """Return the Redis key under which the replay checkpoint cursor for *sid* is stored.

        P3.3-ops-complete: fixed to use single-quoted f-string to avoid SyntaxError.
        """
        return f"{getattr(self, 'exec_replay_checkpoint_key_prefix', 'orders:exec:replay:cursor:')}{sid}"

    def _quarantine_sid_for_replay_mismatch(
        self, sid: str, *, mismatch: dict[str, Any], state_doc: dict[str, Any]
    ) -> None:
        """Write quarantine event in Redis + QuarantineLedger for a replay mismatch.

        P3.3-ops-complete: also records in the SQL quarantine ledger when available.
        Fail-open on all errors — Redis exec stream remains the authoritative source.
        """
        if not getattr(self, 'exec_replay_quarantine_on_mismatch', True):
            return
        try:
            qprefix = getattr(self, 'exec_replay_quarantine_prefix', 'orders:quarantine:state:')
            qkey = f"{qprefix}{sid}"
            now_ms = _ms_now()
            payload = dict(state_doc or {})
            payload.update({
                'sid': sid,
                'quarantined_at_ms': now_ms,
                'quarantine_reason': 'replay_mismatch',
                'quarantine_source': 'executor_rehydrate',
                'replay_mismatch': mismatch,
            })
            pipe = self.r.pipeline()
            pipe.set(qkey, json.dumps(payload, ensure_ascii=False, default=str))
            pipe.sadd(f"{qprefix}sids", sid)
            pipe.xadd(f"{qprefix}events", {
                'sid': sid, 'event': 'REPLAY_MISMATCH_QUARANTINED', 'ts_ms': str(now_ms)
            }, maxlen=10000, approximate=True)
            pipe.execute()
            try:
                sink = getattr(self, 'quarantine_ledger', None)
                if sink is not None:
                    sink.record_quarantine_event({
                        'sid': sid,
                        'symbol': str((state_doc or {}).get('symbol') or ''),
                        'action': 'REPLAY_MISMATCH_QUARANTINED',
                        'severity': 'critical' if any(k in {'status', 'fsm_state'} for k in mismatch) else 'warning',
                        'reason': 'replay_mismatch',
                        'source': 'executor_rehydrate',
                        'quarantine_key': qkey,
                        'state': payload,
                        'event_ts_ms': now_ms,
                        'created_at_ms': now_ms,
                    })
            except Exception:
                pass
        except Exception:
            return

    def _recover_state_from_exec_stream(self, sid: str) -> dict[str, Any]:
        """Best-effort rehydrate of ``orders:state:{sid}`` from ``orders:exec``.

        Redis state keys are a materialized view; the stream remains the
        authoritative fact log. When a worker restarts after losing the hot
        state key, we replay the most recent execution facts for that ``sid``
        and persist the rebuilt snapshot back into Redis.

        P3.3-ops-complete: uses rebuild_state_with_fallback (checkpoint-aware),
        publishes retention_guard_triggered and replay_latency_ms into the
        state_rehydrated event so operators can track replay health.
        """
        if not self.exec_rehydrate_on_state_miss:
            return {}
        try:
            checkpoint_id = str(self.r.get(self._replay_checkpoint_key(sid)) or '')
            result = rebuild_state_with_fallback(
                self.r,
                exec_stream=self.exec_stream,
                sid=sid,
                scan_count=self.exec_replay_scan_count,
                checkpoint_id=checkpoint_id,
                sql_dsn=getattr(self, 'execution_journal_dsn', ''),
            )
            state_doc = result.state_doc
            if not state_doc:
                return {}
            persist_state_snapshot(
                self.r,
                state_key=f"{self.state_key_prefix}{sid}",
                state_doc=state_doc,
                ttl_sec=self.state_ttl if self.state_ttl > 0 else 0,
                checkpoint_key=self._replay_checkpoint_key(sid),
            )
            self._exec_event({
                "sid": sid,
                "action": "rehydrate",
                "event_type": "state_rehydrated_from_stream",
                "fsm_state": state_doc.get("fsm_state"),
                "stream_last_id": state_doc.get("stream_last_id"),
                "stream_replayed_events": state_doc.get("stream_replayed_events"),
                "rehydrate_source": result.source,
                "replay_truncated": int(bool(result.truncated)),
                "checkpoint_id": result.checkpoint_id,
                # P3.3-ops-complete: retention guard + latency telemetry
                "retention_guard_triggered": int(bool(result.retention_guard_triggered)),
                "replay_latency_ms": int(result.latency_ms),
            })
            return state_doc
        except Exception:
            return {}

    def _load_order_state(self, sid: str) -> dict[str, Any]:
        """Load execution state, preferring the primary journal over the cache.

        P1.2.1: when exec_journal_primary=True (default), we replay orders:exec
        first to get the most recent SID state; fall back to cache on miss.
        """
        if getattr(self, 'exec_journal_primary', True):
            state = self._recover_state_from_exec_stream(sid)
            if state:
                return build_materialized_state_view(state)
            return self._load_materialized_state_cache(sid)
        try:
            raw = self.r.get(f"{self.state_key_prefix}{sid}")
            if raw:
                doc = json.loads(raw)
                if isinstance(doc, dict):
                    return build_materialized_state_view(doc)
            return self._recover_state_from_exec_stream(sid)
        except Exception:
            return self._recover_state_from_exec_stream(sid)

    def _transition_state(
        self,
        sid: str,
        *,
        symbol: str,
        action: str,
        next_state: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append one idempotent FSM transition to the journal and project the cache.

        P1.2.1: event is written to orders:exec first; cache is updated deterministically
        by the projection worker (or inline when EXEC_INLINE_STATE_PROJECTION=1).
        """
        prev = self._load_order_state(sid)
        prev_state = (prev.get("fsm_state") or "")
        if prev_state == next_state:
            return prev
        event_doc = dict(details or {})
        event_doc.update({
            "sid": sid,
            "symbol": symbol,
            "action": action,
            "event_type": "state_transition",
            "prev_state": prev_state,
            "fsm_prev_state": prev_state,
            "fsm_state": next_state,
            "fsm_ts_ms": _ms_now(),
            "fsm_mono_ms": _mono_ms(),
        })
        if next_state in {FSM_EXIT_FILLED, FSM_EMERGENCY_FLATTENED}:
            rc = (
                event_doc.get("close_reason_tag") or
                event_doc.get("reason_tag") or
                event_doc.get("resize_mode") or
                event_doc.get("cancel_mode") or
                "unknown_exit"
            )
            event_doc["reason_code"] = rc

        if EXECUTION_STATE_TRANSITION_TOTAL:
            EXECUTION_STATE_TRANSITION_TOTAL.labels(action=action, symbol=symbol, next_state=next_state).inc()
        self._exec_event(event_doc)
        merged = dict(prev)
        merged.update(details or {})
        if next_state in {FSM_EXIT_FILLED, FSM_EMERGENCY_FLATTENED} and "reason_code" not in merged:
            merged["reason_code"] = event_doc["reason_code"]
        merged["sid"] = sid
        merged["symbol"] = symbol
        merged["action"] = action
        merged["fsm_prev_state"] = prev_state
        merged["fsm_state"] = next_state
        return build_materialized_state_view(merged)

    def _mark_pending_reconcile(self, sid: str, *, symbol: str, action: str, reason: str) -> None:
        if EXECUTION_RECONCILE_PENDING_TOTAL:
            EXECUTION_RECONCILE_PENDING_TOTAL.labels(action=action, symbol=symbol).inc()
        self._transition_state(
            sid,
            symbol=symbol,
            action=action,
            next_state=FSM_PENDING_RECONCILE,
            details={"reconcile_reason": reason},
        )

    def _user_stream_cache_key(self, ref_kind: str, ref_value: str) -> str:
        return f"{self.user_stream_cache_prefix}{ref_kind}:{ref_value}"

    def _lookup_user_stream_event(self, *, plain_client_id: str | None = None, algo_client_id: str | None = None) -> dict[str, Any]:
        keys = []
        if plain_client_id:
            keys.append(self._user_stream_cache_key("order", plain_client_id))
        if algo_client_id:
            keys.append(self._user_stream_cache_key("algo", algo_client_id))
        for k in keys:
            try:
                raw = self.r.get(k)
                if raw:
                    doc = json.loads(raw)
                    if isinstance(doc, dict):
                        return doc
            except Exception:
                continue
        return {}

    def _normalize_user_stream_plain_order(self, event_doc: dict[str, Any]) -> dict[str, Any]:
        """Normalize raw Binance user-stream ORDER_TRADE_UPDATE payload to REST-like fields.

        The user-stream cache stores the raw WS ``o`` sub-object which uses compact
        field names (i=orderId, c=clientOrderId, X=status, z=executedQty, ap=avgPrice).
        The open-path (and caller assertions) expects REST-style field names.  This
        normalizer maps compact → expanded names without removing the originals so
        callers that already handle either form continue working.
        """
        order = dict((event_doc or {}).get("order") or event_doc or {})
        if not isinstance(order, dict):
            return {}
        if order.get("orderId") in (None, "") and order.get("i") not in (None, ""):
            order["orderId"] = order.get("i")
        if order.get("clientOrderId") in (None, "") and order.get("c") not in (None, ""):
            order["clientOrderId"] = order.get("c")
        if order.get("status") in (None, "") and order.get("X") not in (None, ""):
            order["status"] = order.get("X")
        if order.get("executedQty") in (None, "") and order.get("z") not in (None, ""):
            order["executedQty"] = order.get("z")
        if order.get("avgPrice") in (None, "") and order.get("ap") not in (None, ""):
            order["avgPrice"] = order.get("ap")
        if order.get("symbol") in (None, "") and order.get("s") not in (None, ""):
            order["symbol"] = order.get("s")
        if order.get("side") in (None, "") and order.get("S") not in (None, ""):
            order["side"] = order.get("S")
        if order.get("type") in (None, "") and order.get("o") not in (None, ""):
            order["type"] = order.get("o")
        return order

    def _normalize_user_stream_algo_order(self, event_doc: dict[str, Any]) -> dict[str, Any]:
        """Normalize raw Binance user-stream ALGO_UPDATE payload to REST-like fields.

        The ``ao`` sub-object uses X=status, s=symbol; algoId and clientAlgoId are
        already spelled out in the ALGO_UPDATE payload, so we only map compact extras.
        """
        algo = dict((event_doc or {}).get("algo") or event_doc or {})
        if not isinstance(algo, dict):
            return {}
        if algo.get("status") in (None, "") and algo.get("X") not in (None, ""):
            algo["status"] = algo.get("X")
        if algo.get("symbol") in (None, "") and algo.get("s") not in (None, ""):
            algo["symbol"] = algo.get("s")
        return algo

    def _sync_client_clock_if_due(self, client: BinanceFuturesClient) -> None:
        now_ms = _ms_now()
        if now_ms < self._next_time_sync_due_ms:
            return
        try:
            client.sync_time()
        except Exception:
            return
        self._next_time_sync_due_ms = now_ms + max(1000, self.binance_time_sync_interval_ms)

    def _sync_client_clock(self, client: BinanceFuturesClient) -> None:
        self._sync_client_clock_if_due(client)
        if abs(int(getattr(client, "timestamp_offset_ms", 0))) > int(self.max_clock_drift_ms):
            client.sync_time()
            self._next_time_sync_due_ms = _ms_now() + max(1000, self.binance_time_sync_interval_ms)

    def _submit_plain_order_with_reconcile(self, *, sid: str, symbol: str, action: str, params: dict[str, Any], client: BinanceFuturesClient) -> dict[str, Any]:
        try:
            return client.post_plain_order(params)
        except Exception as exc:
            if not self.reconcile_enable or not getattr(self, "exec_reconcile_on_503_unknown", True) or not client.is_ambiguous_execution_error(exc):
                raise
            self._mark_pending_reconcile(sid, symbol=symbol, action=action, reason=str(exc))
            client_id = (params.get("newClientOrderId") or "").strip() or None
            event_doc = self._lookup_user_stream_event(plain_client_id=client_id) if getattr(self, "exec_reconcile_prefer_user_stream", True) else {}
            if event_doc:
                try:
                    if BINANCE_ALGO_RECONCILE_TOTAL is not None:
                        BINANCE_ALGO_RECONCILE_TOTAL.labels(action=action, source="user_stream").inc()
                except Exception:
                    pass
                # P6.3 fix: normalize raw WS o-payload (i/c/X/z/ap) → REST-like fields
                return self._normalize_user_stream_plain_order(event_doc)
            if client_id:
                q = client.query_plain_order(symbol, client_order_id=client_id)
                try:
                    if BINANCE_ALGO_RECONCILE_TOTAL is not None:
                        BINANCE_ALGO_RECONCILE_TOTAL.labels(action=action, source="rest_query").inc()
                except Exception:
                    pass
                return q
            raise

    def _submit_algo_order_with_reconcile(self, *, sid: str, symbol: str, action: str, params: dict[str, Any], client: BinanceFuturesClient) -> dict[str, Any]:
        try:
            return client.post_algo_order(params)
        except Exception as exc:
            if not self.reconcile_enable or not getattr(self, "exec_reconcile_on_503_unknown", True) or not client.is_ambiguous_execution_error(exc):
                raise
            self._mark_pending_reconcile(sid, symbol=symbol, action=action, reason=str(exc))
            client_algo_id = str(params.get("clientAlgoId") or params.get("newClientOrderId") or "").strip() or None
            event_doc = self._lookup_user_stream_event(algo_client_id=client_algo_id) if getattr(self, "exec_reconcile_prefer_user_stream", True) else {}
            if event_doc:
                try:
                    if BINANCE_ALGO_RECONCILE_TOTAL is not None:
                        BINANCE_ALGO_RECONCILE_TOTAL.labels(action=action, source="user_stream").inc()
                except Exception:
                    pass
                # P6.3 fix: normalize raw WS ao-payload (X/s) → REST-like fields
                return self._normalize_user_stream_algo_order(event_doc)
            if client_algo_id:
                q = client.query_algo_order(symbol, client_algo_id=client_algo_id)
                try:
                    if BINANCE_ALGO_RECONCILE_TOTAL is not None:
                        BINANCE_ALGO_RECONCILE_TOTAL.labels(action=action, source="rest_query").inc()
                except Exception:
                    pass
                return q
            raise

    def _resume_open_from_state(
        self, sid: str, *, symbol: str, client: BinanceFuturesClient,
    ) -> dict[str, Any] | None:
        state = self._load_order_state(sid)
        if not state or (state.get("symbol") or "").upper() != symbol.upper():
            return None
        fsm_state = (state.get("fsm_state") or "")
        if fsm_state in {FSM_PROTECTED, FSM_TP_POLICY_ARMED, FSM_TRAIL_ARMED, FSM_EXIT_FILLED, FSM_EMERGENCY_FLATTENED}:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "resume",
                "event_type": "resume_from_materialized_state",
                "fsm_state": fsm_state,
            })
            return dict(state, recovered_from_state=True)
        order_id = _i(state.get("binance_order_id"), 0)
        client_order_id = (state.get("entry_client_order_id") or "").strip() or None
        if order_id or client_order_id:
            q = client.query_plain_order(symbol, order_id=order_id or None, client_order_id=client_order_id)
            return {"recovered_order": q, **state}
        return None

    # --- Symbol initialisation ---

    def _ensure_symbol_settings(
        self, symbol: str, *, client: BinanceFuturesClient
    ) -> None:
        """Set margin type and leverage for symbol (idempotent, errors ignored).

        Leverage fallback logic:
          1. Try to set self.default_leverage (e.g. 100).
          2. If Binance returns -4028 (leverage exceeds maximum for symbol),
             parse maxLeverage from the error payload and retry with that value.
          3. All other errors are swallowed (margin type already set, etc.).
        """
        if not self.init_symbol_settings:
            return
        try:
            # Binance returns error if marginType already set — safe to swallow
            client.post_margin_type(symbol, self.margin_type)
        except Exception:
            pass
        actual_lev = 0
        try:
            target_leverage = self._resolve_symbol_leverage(symbol)
            client.post_leverage(symbol, target_leverage)
            actual_lev = target_leverage
        except BinanceAPIError as e:
            # -4028: "Leverage 100 is not valid, maximum is N for SYMBOL"
            # payload may have {'maxLeverage': N} or the message contains it.
            max_lev = self._parse_max_leverage(e)
            if max_lev and max_lev > 0:
                capped = min(self._resolve_symbol_leverage(symbol), max_lev)
                try:
                    client.post_leverage(symbol, capped)
                    actual_lev = capped
                    import logging as _logging
                    _logging.getLogger(__name__).info(
                        "leverage fallback: %s requested=%d max_allowed=%d → set=%d",
                        symbol, self._resolve_symbol_leverage(symbol), max_lev, capped,
                    )
                except Exception:
                    pass
        except Exception:
            pass
        # Persist actual leverage to Redis for observability (read by trade_monitor reports)
        if actual_lev > 0:
            with contextlib.suppress(Exception):
                self.redis.hset("exec:leverage:actual", symbol.upper(), str(actual_lev))

    @staticmethod
    def _parse_max_leverage(exc: BinanceAPIError) -> int:
        """Extract maxLeverage from a Binance -4028 error payload.

        Binance returns one of:
          {"code": -4028, "msg": "...", "maxLeverage": 75}
          {"code": -4028, "msg": "Leverage 100 is not valid, maximum is 75 ..."}
        Returns 0 if not parseable.
        """
        import re as _re
        try:
            payload = exc.payload or {}
            if isinstance(payload, dict):
                # Direct field (some Binance versions)
                if payload.get("maxLeverage"):
                    return int(payload["maxLeverage"])
                # Parse from message string
                msg = str(payload.get("msg") or payload.get("message") or "")
                m = _re.search(r"maximum is (\d+)", msg, _re.IGNORECASE)
                if m:
                    return int(m.group(1))
                # Another pattern: "Leverage N is not valid"
                m2 = _re.search(r"max.*?(\d+)", msg, _re.IGNORECASE)
                if m2:
                    return int(m2.group(1))
        except Exception:
            pass
        return 0


    # --- Order quantisation ---

    def _quantize(
        self, symbol: str, qty: float, price: float | None,
        *, filters: FiltersCache,
    ) -> tuple[str, str | None]:
        """Apply LOT_SIZE stepSize and PRICE_FILTER tickSize to qty and price. Returns formatted strings."""
        f = filters.get(symbol)
        q = qty
        if f.step_size and f.step_size > 0:
            q = _round_down(qty, f.step_size)
        if f.min_qty and q < f.min_qty:
            raise ValueError(f"qty below minQty: {q} < {f.min_qty}")
        p = price
        if price is not None and f.tick_size and f.tick_size > 0:
            p = _round_down(price, f.tick_size)

        return _format_float(q, f.step_size), (_format_float(p, f.tick_size) if p is not None else None)

    # --- Fill polling ---

    def _wait_fill(
        self, symbol: str, order_id: int, *, timeout_s: float,
        client: BinanceFuturesClient,
    ) -> dict[str, Any]:
        """Poll order status until FILLED or terminal state or timeout."""
        deadline = time.time() + timeout_s
        last: dict[str, Any] = {}
        while time.time() < deadline:
            j = client.get_order(symbol, order_id=order_id)
            last = j
            st = (j.get("status") or "").upper()
            if st == "FILLED":
                return j
            if st in {"CANCELED", "REJECTED", "EXPIRED"}:
                return j
            time.sleep(max(0.05, self.fill_poll_s))
        return last

    # --- TP qty splitting ---

    def _split_tp_qtys(
        self, symbol: str, total_qty: float, n: int,
        *, filters: FiltersCache,
    ) -> list[float]:
        """Split total_qty evenly across n TP orders respecting stepSize.

        Last TP gets the remainder to avoid rounding dust loss.
        """
        if n <= 0:
            return []
        f = filters.get(symbol)
        step = f.step_size or 0.0
        if n == 1:
            return [total_qty]

        base = total_qty / float(n)
        parts = []
        remaining = total_qty
        for i in range(n):
            if i == n - 1:
                q = remaining
            else:
                q = base
                if step > 0:
                    q = _round_down(q, step)
                remaining -= q
            if q <= 0:
                continue
            parts.append(q)
        # Guard against rounding adding dust above total
        s = sum(parts)
        if s > total_qty and step > 0:
            parts[-1] = _round_down(parts[-1] - (s - total_qty), step)
        return [q for q in parts if q > 0]


    def _local_headroom_check(
        self,
        *,
        client: BinanceFuturesClient,
        symbol: str,
        qty: float,
        reference_price: float | None,
        tier: str = "C",
    ) -> None:
        """Best-effort reserve check before submitting conditional protection.

        Binance Algo Service does not run a full margin check before trigger.
        We therefore require a small local headroom floor so a freshly-opened
        position is not considered protected when the future trigger cannot
        realistically be carried.
        """
        try:
            acct = client.get_account() or {}
            avail = _f(acct.get("availableBalance"), 0.0)
            px = float(reference_price or 0.0)
            notional = abs(float(qty)) * px if px > 0 else 0.0

            t = str(tier).upper()
            if t == "A":
                slip = self.protection_slippage_bps_a
            elif t == "B":
                slip = self.protection_slippage_bps_b
            else:
                slip = self.protection_slippage_bps_c

            reserve = notional * (self.protection_fee_buffer_bps + slip) / 10000.0
            if avail - reserve < self.account_available_floor_usd:
                raise RuntimeError(
                    f"insufficient protection headroom: available={avail:.8f} reserve={reserve:.8f} "
                    f"floor={self.account_available_floor_usd:.8f}"
                )
        except RuntimeError:
            raise
        except Exception:
            # Fail-open on telemetry errors; the executor remains bounded by
            # exchange-side validation and account risk settings.
            return

    def _validate_exit_contract(
        self,
        *,
        position_side: str | None,
        reduce_only: bool,
        close_position: bool,
        quantity: float | None,
        order_type: str,
        working_type: str | None,
        is_algo: bool,
    ) -> None:
        result = validate_exit_intent(
            position_mode=self.position_mode,
            position_side=position_side,
            exit_intent="close",
            reduce_only=reduce_only,
            close_position=close_position,
            quantity=quantity,
            order_type=order_type,
            working_type=working_type,
            is_algo=is_algo,
        )
        if not result.is_valid_exit_contract:
            raise ValueError(f"invalid_exit_contract:{result.reason}")

    def _protection_confirmed(self, prot: dict[str, Any], tps: list[float], trail_enabled: bool) -> bool:
        if prot.get("sl_algo_id") in (None, "", 0):
            return False
        for idx, _ in enumerate(tps, start=1):
            if prot.get(f"tp{idx}_algo_id") in (None, "", 0):
                return False
        if trail_enabled and not (prot.get("trail_client_id") or prot.get("trail_algo_id") or prot.get("trail_pending")):
            return False
        return True

    def _emit_protection_incident(self, sid: str, symbol: str, reason: str) -> None:
        self._exec_event({
            "sid": sid,
            "symbol": symbol,
            "action": "protection_invariant",
            "status": "failed",
            "reason": reason,
            "severity": "critical",
            "incident_tag": "capital_protection",
        })
        self._save_order_state(sid, {
            "action": "protection_invariant",
            "status": "failed",
            "symbol": symbol,
            "incident_flag": "protection_missing",
            "incident_reason": reason,
            "incident_tag": "capital_protection",
        })
        # Notify operator via Telegram (fail-open)
        if self.tg is not None:
            pass
            # try:
            #     self.tg.send_text(
            #         f"🛑 BINANCE protection invariant failed\n"
            #         f"symbol={symbol} sid={sid[:24]}...\n"
            #         f"reason={reason}"
            #     )
            # except Exception:
            #     pass

    def _structured_order_contract(
        self, *, sid: str,
        entry_ref: PlainOrderRef | None = None,
        sl_ref: AlgoOrderRef | None = None,
        tp_refs: list[AlgoOrderRef] | None = None,
        trail_ref: AlgoOrderRef | None = None,
    ) -> dict[str, Any]:
        """Build a nested materialized order-contract dict for orders:state:{sid}.

        Stores entry/protective/trailing order refs in a structured sub-document
        alongside (not replacing) the existing flat fields for backward compatibility.
        """
        contract: dict[str, Any] = {"sid": sid}
        if entry_ref is not None:
            contract["entry"] = {
                "order_id": entry_ref.order_id,
                "client_order_id": entry_ref.client_order_id,
                "type": entry_ref.type,
                "side": entry_ref.side,
                "position_side": entry_ref.position_side,
            }
        protective: dict[str, Any] = {}
        if sl_ref is not None:
            protective.update({
                "sl_algo_id": sl_ref.algo_id,
                "sl_client_algo_id": sl_ref.client_algo_id,
                "sl_working_type": sl_ref.working_type,
            })
        if tp_refs:
            protective["tp_algo_ids"] = [ref.algo_id for ref in tp_refs if ref.algo_id not in (None, 0)]
            protective["tp_client_algo_ids"] = [ref.client_algo_id for ref in tp_refs if ref.client_algo_id]
        if protective:
            contract["protective"] = protective
        if trail_ref is not None:
            contract["trailing"] = {
                "trail_algo_id": trail_ref.algo_id,
                "trail_client_algo_id": trail_ref.client_algo_id,
                "trail_working_type": trail_ref.working_type,
            }
        return contract

    def _causal_timestamps(self, payload: dict[str, Any] | None = None) -> dict[str, int]:
        """Extract causal timestamps from payload, falling back to now_ms.

        Returns ts_event_ms/ts_queue_ms/ts_exec_start_ms as a dict for merging
        into exec events and state documents.
        """
        src = payload or {}
        now_ms = _ms_now()
        ts_event_ms = int(src.get("ts_event_ms") or src.get("ts_queue_ms") or now_ms)
        ts_queue_ms = int(src.get("ts_queue_ms") or ts_event_ms)
        ts_exec_start_ms = int(src.get("ts_exec_start_ms") or now_ms)
        return {
            "ts_event_ms": ts_event_ms,
            "ts_queue_ms": ts_queue_ms,
            "ts_exec_start_ms": ts_exec_start_ms,
        }

    def _reconcile_entry_by_client_id(
        self, *, sid: str, symbol: str, client_order_id: str | None, client: BinanceFuturesClient
    ) -> dict[str, Any]:
        """Reconcile an entry order fill state via user-stream cache or REST."""
        cid = (client_order_id or '').strip()
        if not cid:
            return {}
        event_doc = self._lookup_user_stream_event(plain_client_id=cid)
        if event_doc:
            return dict(event_doc.get("order") or event_doc)
        try:
            return client.reconcile_entry_by_client_id(symbol, cid)
        except Exception:
            return {}

    def _reconcile_protection_by_sid(
        self, *, sid: str, symbol: str, client: BinanceFuturesClient
    ) -> dict[str, Any]:
        """Scan open algo orders to reconstruct protection refs for a sid."""
        try:
            return client.reconcile_protection_by_sid(symbol, sid)
        except Exception:
            return {}

    def _attempt_reconcile_after_exception(
        self, *, payload: dict[str, Any], action: str, symbol: str, client: BinanceFuturesClient
    ) -> dict[str, Any]:
        """Best-effort reconcile after an ambiguous 503/timeout error.

        Checks user-stream cache first (prefer_user_stream), then falls back to
        REST. If any order info is found, updates state and returns resolved dict.
        Returns empty dict if nothing can be reconciled.
        """
        sid = (payload.get("sid") or "").strip()
        state = self._load_order_state(sid)
        entry_cid = str(
            state.get("entry_client_order_id")
            or payload.get("entry_client_order_id")
            or _make_cid(sid, "entry", getattr(self, "r", None))
        )
        entry = self._reconcile_entry_by_client_id(
            sid=sid, symbol=symbol, client_order_id=entry_cid, client=client
        )
        protection = self._reconcile_protection_by_sid(sid=sid, symbol=symbol, client=client)
        if not entry and not protection:
            return {}
        resolved = {
            "sid": sid,
            "symbol": symbol,
            "action": action,
            "event_type": "reconcile_resolved",
            "reconcile_source": "user_stream_or_query",
        }
        if entry:
            resolved["reconciled_entry_order_id"] = entry.get("orderId") or entry.get("i")
            resolved["reconciled_entry_status"] = entry.get("status") or entry.get("X")
            self._save_order_state(sid, {
                "symbol": symbol,
                "entry_client_order_id": entry_cid,
                "binance_order_id": entry.get("orderId") or entry.get("i"),
            })
        if protection:
            resolved["reconciled_protection"] = json.dumps(protection, ensure_ascii=False, default=str)
        return resolved

    def _symbol_tier(self, symbol: str) -> str:
        """Return symbol risk tier: A (BTC/ETH), B (mid-cap), C (all others)."""
        s = (symbol or '').upper()
        if s in {"BTCUSDT", "ETHUSDT"}:
            return "A"
        if s in {"SOLUSDT", "XRPUSDT", "BNBUSDT"}:
            return "B"
        return "C"

    def _resolve_symbol_leverage(self, symbol: str) -> int:
        """Return the configured leverage for symbol, using tier fallback.

        Resolution order:
        1. BINANCE_LEVERAGE_{SYMBOL}  (e.g. BINANCE_LEVERAGE_BTCUSDT=15)
        2. BINANCE_LEVERAGE_TIER_{A|B|C}
        3. self.default_leverage
        """
        s = (symbol or '').upper()
        tier = self._symbol_tier(s)
        specific = os.getenv(f"BINANCE_LEVERAGE_{s}")
        if specific not in (None, ""):
            try:
                return max(1, int(float(specific)))
            except Exception:
                pass
        tier_key = f"BINANCE_LEVERAGE_TIER_{tier}"
        tier_default = {"A": 20, "B": 10, "C": 5}.get(tier, 5)
        try:
            return max(1, int(float(os.getenv(tier_key, str(tier_default)))))
        except Exception:
            return max(1, int(self.default_leverage))

    def _emergency_flatten_position(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        client: BinanceFuturesClient,
        filters: FiltersCache,
        reason: str = "protection_invariant",
    ) -> dict[str, Any]:
        """Cancel all orders then market-close the position using exact live qty from exchange.

        Uses _force_flatten_symbol_exact so the close qty is the live positionAmt,
        not the caller-supplied qty (which may be stale). Emits flatten_emergency event
        and surfaces residual fields for post-close observability.
        """
        close = self._force_flatten_symbol_exact(
            sid=sid,
            symbol=symbol,
            client=client,
            filters=filters,
            logical_side=logical_side,
            reason_tag="emerg",
        )
        if EXECUTION_EMERGENCY_FLATTEN_TOTAL:
            EXECUTION_EMERGENCY_FLATTEN_TOTAL.labels(symbol=symbol, reason=reason).inc()
        event = {
            "sid": sid,
            "symbol": symbol,
            "action": "emergency_flatten",
            "event_type": "flatten_emergency",
            "reason": reason,
            "incident_tag": "capital_protection",
            "emergency_order_id": close.get("close_order_id"),
            "emergency_client_id": close.get("close_client_id"),
            "emergency_reason": close.get("close_reason_tag") or close.get("status"),
            "residual_qty": close.get("residual_qty"),
            "residual_notional_usdt": close.get("residual_notional_usdt"),
            "residual_margin_usdt": close.get("residual_margin_usdt"),
        }
        self._exec_event(event)
        return {
            "emergency_order_id": close.get("close_order_id"),
            "emergency_client_id": close.get("close_client_id"),
            "emergency_reason": close.get("close_reason_tag") or close.get("status"),
            "residual_qty": close.get("residual_qty"),
            "residual_notional_usdt": close.get("residual_notional_usdt"),
            "residual_margin_usdt": close.get("residual_margin_usdt"),
        }

    # --- Protective orders (SL + TPs) ---

    # Prices crossed by ≤ this fraction of mark are nudged instead of dropped.
    # Handles stale-signal case where mark moved a few bps during placement.
    _PROTECTIVE_NUDGE_THRESHOLD: float = 0.001   # 0.1 %
    _PROTECTIVE_NUDGE_OFFSET: float    = 0.0005  # 0.05 % cushion away from mark

    def _validate_protective_prices(
        self,
        symbol: str,
        logical_side: str,
        sl: float | None,
        tps: list[float],
        *,
        client: BinanceFuturesClient,
        ref_price: float | None = None,
    ) -> tuple[float | None, list[float]]:
        """Validate SL and TP prices against the current mark price.

        Returns (validated_sl, validated_tps) with invalid prices removed or nudged.
        A price is "invalid" if it would immediately trigger the stop order
        (Binance error -2021).

        Prices barely crossed (within NUDGE_THRESHOLD) are nudged by NUDGE_OFFSET
        instead of dropped — handles stale-signal case where mark moved a few bps
        between signal generation and order placement.  Wildly-crossed prices are
        still dropped entirely.

          LONG position:
            SL  (STOP_MARKET  BUY-side exit)  : stopPrice must be < markPrice
            TP  (TAKE_PROFIT  SELL-side exit) : stopPrice must be > markPrice

          SHORT position:
            SL  (STOP_MARKET  BUY-side exit)  : stopPrice must be > markPrice
            TP  (TAKE_PROFIT  BUY-side exit)  : stopPrice must be < markPrice
        """
        # Fetch current mark price; fall back to ref_price if unavailable.
        mark: float | None = ref_price
        try:
            mp = float(client.get_mark_price(symbol) or 0.0)
            if mp > 0:
                mark = mp
        except Exception:
            pass

        if mark is None or mark <= 0:
            # No reference price — cannot validate, return as-is and let Binance decide.
            return sl, tps

        is_long = logical_side == "LONG"
        is_demo = getattr(self, "demo_client", None) is not None and client is self.demo_client
        # Demo networks have wild mark/contract spreads; use 0.5% threshold / 0.25% offset
        nudge_thresh = 0.005 if is_demo else self._PROTECTIVE_NUDGE_THRESHOLD
        nudge_off    = 0.0025 if is_demo else self._PROTECTIVE_NUDGE_OFFSET

        valid_sl: float | None = None
        if sl is not None and sl > 0:
            if is_long:
                # SL for LONG must be below mark (fires on drop)
                if sl < mark:
                    valid_sl = sl
                elif sl <= mark * (1.0 + nudge_thresh):
                    # Barely crossed — nudge down to sit just below mark
                    valid_sl = mark * (1.0 - nudge_off)
                # else: wildly above mark — drop it
            else:
                # SL for SHORT must be above mark (fires on rise)
                if sl > mark:
                    valid_sl = sl
                elif sl >= mark * (1.0 - nudge_thresh):
                    # Barely crossed — nudge up to sit just above mark
                    valid_sl = mark * (1.0 + nudge_off)
                # else: wildly below mark — drop it

        valid_tps: list[float] = []
        for tp in tps:
            if is_long:
                # TP for LONG must be above mark (fires on rise)
                if tp > mark:
                    valid_tps.append(tp)
                elif tp >= mark * (1.0 - nudge_thresh):
                    valid_tps.append(mark * (1.0 + nudge_off))
                # else: wildly below mark — drop it
            else:
                # TP for SHORT must be below mark (fires on fall)
                if tp < mark:
                    valid_tps.append(tp)
                elif tp <= mark * (1.0 + nudge_thresh):
                    valid_tps.append(mark * (1.0 - nudge_off))
                # else: wildly above mark — drop it

        return valid_sl, valid_tps


    def _resolve_execution_policy(self, payload: dict, symbol: str) -> ExecutionPolicyDecision:
        infra_degraded = _truthy(payload.get("infra_degraded")) or _truthy(payload.get("degraded_mode"))
        default_policy = self.exec_policy_default
        maker_allowed_symbols = set(self.exec_policy_maker_allowed_symbols)
        if self.rollout_flags.safety_forced(infra_degraded=infra_degraded):
            default_policy = SAFETY_FIRST
        if not self.rollout_flags.maker_allowed(infra_degraded=infra_degraded):
            maker_allowed_symbols = set()
        return resolve_execution_policy(
            payload=payload,
            symbol=symbol,
            default_policy=default_policy,
            maker_allowed_symbols=maker_allowed_symbols,
            tp_market_working_type=self.tp_market_working_type,
            tp_limit_trigger_working_type=self.tp_limit_trigger_working_type,
            tp_limit_time_in_force=self.tp_limit_time_in_force,
            watchdog_enabled=self.tp_limit_watchdog_enable,
            watchdog_timeout_ms=self.tp_limit_watchdog_timeout_ms,
        )

    def _position_qty_tolerance(self, symbol: str, *, filters: FiltersCache) -> float:
        try:
            # Use 1% of step_size as tolerance to ensure even one step is treated as non-flat
            return max(float(filters.get(symbol).step_size or 0.0) * 0.01, 1e-12)
        except Exception:
            return 1e-12

    def _observe_mark_contract_spread(self, symbol: str, *, client: BinanceFuturesClient) -> float | None:
        try:
            mark = float(client.get_mark_price(symbol) or 0.0)
            contract = float(client.get_ticker_price(symbol) or 0.0)
            if mark <= 0 or contract <= 0:
                return None
            spread_bps = (mark - contract) / contract * 10000.0
            if MARK_CONTRACT_SPREAD_BPS is not None:
                MARK_CONTRACT_SPREAD_BPS.labels(symbol=symbol).set(spread_bps)
            return spread_bps
        except Exception:
            return None

    def _observe_sl_trigger_semantics(self, symbol: str, *, client: BinanceFuturesClient) -> None:
        spread_bps = self._observe_mark_contract_spread(symbol, client=client)
        if spread_bps is None:
            return
        try:
            if SL_TRIGGER_MARK_MINUS_CONTRACT_BPS is not None:
                SL_TRIGGER_MARK_MINUS_CONTRACT_BPS.labels(symbol=symbol).set(spread_bps)
        except Exception:
            pass

    def _observe_tp_trigger_semantics(self, symbol: str, *, level: int, client: BinanceFuturesClient) -> None:
        spread_bps = self._observe_mark_contract_spread(symbol, client=client)
        if spread_bps is None:
            return
        try:
            if TP_TRIGGER_MARK_MINUS_CONTRACT_BPS is not None:
                TP_TRIGGER_MARK_MINUS_CONTRACT_BPS.labels(symbol=symbol, level=str(int(level))).set(spread_bps)
        except Exception:
            pass

    def _note_maker_tp_state(self, symbol: str, *, level: int, state: str) -> None:
        key = (symbol, int(level))
        stats = self._maker_tp_stats.setdefault(key, {"triggered": 0.0, "filled": 0.0, "fallback": 0.0})
        st = (state or "").upper()
        try:
            if st == "TRIGGERED":
                stats["triggered"] += 1.0
                if TP_LIMIT_TRIGGERED_TOTAL is not None:
                    TP_LIMIT_TRIGGERED_TOTAL.labels(symbol=symbol, level=str(int(level))).inc()
            elif st == "FILLED":
                stats["filled"] += 1.0
                if TP_LIMIT_FILLED_TOTAL is not None:
                    TP_LIMIT_FILLED_TOTAL.labels(symbol=symbol, level=str(int(level))).inc()
                if FEE_BPS_SAVED_ESTIMATE is not None:
                    FEE_BPS_SAVED_ESTIMATE.labels(symbol=symbol, level=str(int(level))).set(max(0.0, self.exec_fee_taker_bps - self.exec_fee_maker_bps))
            elif st == "WATCHDOG_MARKET_FALLBACK":
                stats["fallback"] += 1.0
                if TP_WATCHDOG_FALLBACK_TOTAL is not None:
                    TP_WATCHDOG_FALLBACK_TOTAL.labels(symbol=symbol, level=str(int(level))).inc()
            triggered = max(0.0, float(stats.get("triggered", 0.0)))
            filled = max(0.0, float(stats.get("filled", 0.0)))
            ratio = (filled / triggered) if triggered > 0 else 0.0
            if MAKER_FILL_RATIO is not None:
                MAKER_FILL_RATIO.labels(symbol=symbol, level=str(int(level))).set(ratio)
        except Exception:
            pass

    def _emit_tp_state(self, sid: str, symbol: str, level: int, state: str, **extra) -> None:
        tp_state = _tp_state_name(level, state)
        ev = {
            "sid": sid,
            "symbol": symbol,
            "action": "tp_state",
            "event_type": "tp_watchdog",
            "tp_level": int(level),
            "tp_state": tp_state,
            **extra,
        }
        self._exec_event(ev)
        self._note_maker_tp_state(symbol, level=int(level), state=str(state))
        # P5: mirror TP watchdog events to SQL for durable forensic audit
        try:
            sink = getattr(self, "execution_journal", None)
            if sink is not None:
                sink.record_watchdog_event(ev)
        except Exception:
            pass
        state_doc = {f"tp{int(level)}_state": tp_state}
        for k, v in extra.items():
            state_doc[f"tp{int(level)}_{k}"] = v
        self._save_order_state(sid, state_doc)
        if str(state).upper() in {"TRIGGERED", "PARTIAL", "FILLED", "WATCHDOG_MARKET_FALLBACK", "MONITOR_TIMEOUT", "ERROR"}:
            self._transition_state(
                sid,
                symbol=symbol,
                action="tp_state",
                next_state=tp_state,
                details={"tp_level": int(level)},
            )

    def _submit_reduce_only_market_exit(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        reason_tag: str,
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> dict:
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        q_close, _ = self._quantize(symbol, qty, None, filters=filters)
        params = {
            "symbol": symbol,
            "side": exit_side,
            "type": "MARKET",
            "quantity": q_close,
            "newClientOrderId": _make_cid(sid, reason_tag, getattr(self, "r", None)),
            "newOrderRespType": "RESULT",
        }
        if self.position_mode == "oneway":
            params["reduceOnly"] = True
            self._validate_exit_contract(
                position_side=pos_side,
                reduce_only=True,
                close_position=False,
                quantity=float(q_close),
                order_type="MARKET",
                working_type=None,
                is_algo=False,
            )
        elif pos_side:
            params["positionSide"] = pos_side
            self._validate_exit_contract(
                position_side=pos_side,
                reduce_only=False,
                close_position=False,
                quantity=float(q_close),
                order_type="MARKET",
                working_type=None,
                is_algo=False,
            )
        j = self._submit_plain_order_with_reconcile(
            sid=sid, symbol=symbol, action="emergency_flatten", params=params, client=client
        )
        # P5: return exit_order_ref for chain linkage
        exit_order_ref = self._format_order_ref(
            venue="binance", kind="close_market",
            order_id=j.get("orderId"), client_id=params["newClientOrderId"]
        )
        return {
            "close_order_id": j.get("orderId"),
            "close_client_id": params["newClientOrderId"],
            "close_order_status": j.get("status"),
            "close_reason_tag": reason_tag,
            "exit_order_ref": exit_order_ref,
        }

    def _legacy__emergency_flatten_position__dedupe_2(self, *, sid: str, symbol: str, logical_side: str, qty: float, client: BinanceFuturesClient, filters: FiltersCache) -> dict:
        close = self._submit_reduce_only_market_exit(sid=sid, symbol=symbol, logical_side=logical_side, qty=qty, reason_tag="emerg", client=client, filters=filters)
        if EXECUTION_EMERGENCY_FLATTEN_TOTAL:
            EXECUTION_EMERGENCY_FLATTEN_TOTAL.labels(symbol=symbol, reason="emerg").inc()
        self._transition_state(sid, symbol=symbol, action="emergency_flatten", next_state=FSM_EMERGENCY_FLATTENED, details=close)
        # P5: propagate exit_order_ref and derive closed_trade_id
        exit_order_ref = (close.get('exit_order_ref') or '')
        return {
            "emergency_order_id": close.get("close_order_id"),
            "emergency_client_id": close.get("close_client_id"),
            "exit_order_ref": exit_order_ref,
            "closed_trade_id": self._new_closed_trade_id(sid, exit_order_ref=exit_order_ref) if exit_order_ref else '',
        }


    def _place_protective(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, sl: float | None, tps: list[float],
        policy: ExecutionPolicyDecision,
        client: BinanceFuturesClient,
        filters: FiltersCache,
        ref_price: float | None = None,
        tp_qtys: list[float] | None = None,
        tp_ratio: list[float] | None = None,
        tier: str = "C",
    ) -> dict[str, Any]:
        out: dict[str, Any] = {
            "execution_policy": policy.name,
            "execution_policy_reason": policy.reason,
            "tp_watchdog_enabled": bool(policy.tp_watchdog_enabled),
            "tp_algo_ids": [],
            "tp_client_algo_ids": [],
        }
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        reduce_only_allowed = self.position_mode == "oneway"

        # Force CONTRACT_PRICE for demo/testnet to avoid instant-trigger anomalies
        # due to wild testnet mark prices vs last prices.
        is_demo = getattr(self, "demo_client", None) is not None and client is self.demo_client
        def _wt(base_wt: str) -> str:
            return "CONTRACT_PRICE" if is_demo else base_wt

        check_ref = None
        if sl and sl > 0:
            check_ref = sl
        elif tps:
            check_ref = float(tps[0])
        self._local_headroom_check(client=client, symbol=symbol, qty=qty, reference_price=check_ref, tier=tier)

        valid_sl, valid_tps = self._validate_protective_prices(
            symbol, logical_side, sl, tps,
            client=client, ref_price=ref_price,
        )

        if sl is not None and sl > 0 and valid_sl is None:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "sl_skip",
                "status": "warning",
                "msg": f"SL price {sl} already crossed mark price — skipped to avoid -2021",
                "sl_skipped": sl,
            })
        dropped_tps = [tp for tp in tps if tp not in valid_tps]
        if dropped_tps:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "tp_skip",
                "status": "warning",
                "msg": f"TP price(s) {dropped_tps} already crossed mark price — skipped to avoid -2021",
                "tp_skipped": str(dropped_tps),
            })

        if valid_sl is not None and valid_sl > 0:
            q_sl, sl_q = self._quantize(symbol, qty, valid_sl, filters=filters)
            p: dict[str, Any] = {
                "symbol": symbol,
                "side": exit_side,
                "type": "STOP_MARKET",
                "triggerPrice": sl_q,
                "workingType": _wt(self.sl_working_type),
                "clientAlgoId": _make_cid(sid, "sl", getattr(self, "r", None)),
            }
            if reduce_only_allowed:
                p["reduceOnly"] = True
                self._validate_exit_contract(
                    position_side=pos_side, reduce_only=True, close_position=False,
                    quantity=float(q_sl), order_type="STOP_MARKET",
                    working_type=_wt(self.sl_working_type), is_algo=True,
                )
                p["quantity"] = q_sl
            elif pos_side:
                p["positionSide"] = pos_side
                p["closePosition"] = True
                self._validate_exit_contract(
                    position_side=pos_side, reduce_only=False, close_position=True,
                    quantity=None, order_type="STOP_MARKET",
                    working_type=_wt(self.sl_working_type), is_algo=True,
                )
            try:
                j = self._submit_algo_order_with_reconcile(
                    sid=sid, symbol=symbol, action="place_sl", params=p, client=client
                )
                out["sl_algo_id"] = j.get("algoId")
                out["sl_client_algo_id"] = p["clientAlgoId"]
                out["sl_working_type"] = p["workingType"]
                out["sl_order_type"] = "STOP_MARKET"
            except Exception as e:
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "place_sl_failed",
                    "status": "error", "error": str(e)
                })

        if valid_tps:
            # Scale-in: use explicit TP qty allocation when provided by router
            if tp_qtys and len(tp_qtys) == len(valid_tps):
                parts = [float(q) for q in tp_qtys]
            elif tp_ratio:
                if len(tp_ratio) == len(valid_tps):
                    f = filters.get(symbol)
                    step = f.step_size or 0.0
                    ratios = [float(r) for r in tp_ratio]
                    sum_r = sum(ratios)
                    if sum_r > 0: ratios = [r / sum_r for r in ratios]
                    parts = []
                    remaining = qty
                    for i in range(len(valid_tps)):
                        if i == len(valid_tps) - 1:
                            q = remaining
                        else:
                            q = qty * ratios[i]
                            if step > 0: q = _round_down(q, step)
                            remaining -= q
                        if q > 0:
                            parts.append(q)
                else:
                    print(f"⚠️ payload tp_ratio length ({len(tp_ratio)}) != tp_levels length ({len(valid_tps)}) for {sid} — fallback to default split")
                    parts = self._split_tp_qtys(symbol, qty, len(valid_tps), filters=filters)
            else:
                parts = self._split_tp_qtys(symbol, qty, len(valid_tps), filters=filters)
            filters_obj = filters.get(symbol)
            cumulative = 0.0
            for idx, (tp, q_tp) in enumerate(zip(valid_tps, parts), start=1):
                cumulative += float(q_tp)
                expected_remaining = max(0.0, float(qty) - cumulative)
                q_tp2, tp_q = self._quantize(symbol, q_tp, tp, filters=filters)
                common: dict[str, Any] = {
                    "symbol": symbol,
                    "side": exit_side,
                    "workingType": _wt(policy.tp_working_type),
                    "clientAlgoId": _make_cid(sid, f"tp{idx}", getattr(self, "r", None)),
                }
                if policy is not None and policy.name == MAKER_FIRST:
                    limit_px = compute_limit_tp_price(
                        float(tp_q), logical_side,
                        offset_bps=self.tp_limit_price_offset_bps,
                        tick_size=float(filters_obj.tick_size or 0.0),
                    )
                    limit_px_s = _format_float(limit_px, float(filters_obj.tick_size or 0.0))
                    p = {
                        **common,
                        "type": "TAKE_PROFIT",
                        "triggerPrice": tp_q,
                        "price": limit_px_s,
                        "timeInForce": policy.tp_limit_time_in_force,
                        "quantity": q_tp2,
                    }
                    if reduce_only_allowed:
                        p["reduceOnly"] = True
                        self._validate_exit_contract(
                            position_side=pos_side, reduce_only=True, close_position=False,
                            quantity=float(q_tp2), order_type="TAKE_PROFIT",
                            working_type=_wt(policy.tp_working_type), is_algo=True,
                        )
                    elif pos_side:
                        p["positionSide"] = pos_side
                        self._validate_exit_contract(
                            position_side=pos_side, reduce_only=False, close_position=False,
                            quantity=float(q_tp2), order_type="TAKE_PROFIT",
                            working_type=_wt(policy.tp_working_type), is_algo=True,
                        )
                    try:
                        j = self._submit_algo_order_with_reconcile(
                            sid=sid, symbol=symbol, action=f"place_tp{idx}", params=p, client=client
                        )
                        out[f"tp{idx}_algo_id"] = j.get("algoId")
                        out[f"tp{idx}_client_algo_id"] = p["clientAlgoId"]
                        out[f"tp{idx}_working_type"] = p["workingType"]
                        out[f"tp{idx}_order_type"] = "TAKE_PROFIT"
                        out[f"tp{idx}_time_in_force"] = p["timeInForce"]
                        out[f"tp{idx}_qty"] = q_tp2
                        out[f"tp{idx}_trigger_price"] = tp_q
                        out[f"tp{idx}_limit_price"] = limit_px_s
                        out[f"tp{idx}_expected_remaining_qty"] = expected_remaining
                        out[f"tp{idx}_state"] = _tp_state_name(idx, "ARMED")
                        out["tp_algo_ids"].append(j.get("algoId"))
                        out["tp_client_algo_ids"].append(p["clientAlgoId"])
                        self._emit_tp_state(
                            sid, symbol, idx, "ARMED",
                            order_type="TAKE_PROFIT", policy=policy.name,
                            qty=q_tp2, trigger_price=tp_q, limit_price=limit_px_s,
                        )
                    except Exception as e:
                        self._exec_event({
                            "sid": sid, "symbol": symbol, "action": f"place_tp{idx}_failed",
                            "status": "error", "error": str(e)
                        })
                else:
                    p = {
                        **common,
                        "type": "TAKE_PROFIT_MARKET",
                        "triggerPrice": tp_q,
                    }
                    if reduce_only_allowed:
                        p["reduceOnly"] = True
                        p["quantity"] = q_tp2
                        self._validate_exit_contract(
                            position_side=pos_side, reduce_only=True, close_position=False,
                            quantity=float(q_tp2), order_type="TAKE_PROFIT_MARKET",
                            working_type=_wt(policy.tp_working_type), is_algo=True,
                        )
                    elif pos_side:
                        p["positionSide"] = pos_side
                        p["closePosition"] = True if idx == len(valid_tps) and len(valid_tps) == 1 else False
                        if p["closePosition"]:
                            self._validate_exit_contract(
                                position_side=pos_side, reduce_only=False, close_position=True,
                                quantity=None, order_type="TAKE_PROFIT_MARKET",
                                working_type=_wt(policy.tp_working_type), is_algo=True,
                            )
                        else:
                            p["quantity"] = q_tp2
                            self._validate_exit_contract(
                                position_side=pos_side, reduce_only=False, close_position=False,
                                quantity=float(q_tp2), order_type="TAKE_PROFIT_MARKET",
                                working_type=_wt(policy.tp_working_type), is_algo=True,
                            )
                    try:
                        j = self._submit_algo_order_with_reconcile(
                            sid=sid, symbol=symbol, action=f"place_tp{idx}", params=p, client=client
                        )
                        out[f"tp{idx}_algo_id"] = j.get("algoId")
                        out[f"tp{idx}_client_algo_id"] = p["clientAlgoId"]
                        out[f"tp{idx}_working_type"] = p["workingType"]
                        out[f"tp{idx}_order_type"] = "TAKE_PROFIT_MARKET"
                        out[f"tp{idx}_qty"] = q_tp2
                        out[f"tp{idx}_trigger_price"] = tp_q
                        out[f"tp{idx}_expected_remaining_qty"] = expected_remaining
                        out[f"tp{idx}_state"] = _tp_state_name(idx, "ARMED")
                        out["tp_algo_ids"].append(j.get("algoId"))
                        out["tp_client_algo_ids"].append(p["clientAlgoId"])
                    except Exception as e:
                        self._exec_event({
                            "sid": sid, "symbol": symbol, "action": f"place_tp{idx}_failed",
                            "status": "error", "error": str(e)
                        })
        return out


    # --- Order cancellation by token ---

    def _cancel_by_token(
        self, symbol: str, sid: str, *, client: BinanceFuturesClient
    ) -> int:
        """Cancel all open plain/algo orders for symbol whose client IDs match sid token."""
        token = _sha1_8(sid) if sid else ""
        canceled = 0

        orders = client.get_open_orders(symbol) or []
        for o in orders:
            cid = (o.get("clientOrderId") or "")
            if token and token not in cid:
                continue
            try:
                oid = _i(o.get("orderId"), 0)
                if oid:
                    client.cancel_plain_order(symbol, order_id=oid)
                    canceled += 1
            except Exception:
                continue

        try:
            algo_orders = client.get_open_algo_orders(symbol) or []
            for o in algo_orders:
                cid = (o.get("clientAlgoId") or "")
                if token and token not in cid:
                    continue
                try:
                    oid = _i(o.get("algoId"), 0)
                    if oid:
                        client.cancel_algo_order(symbol, algo_id=oid)
                        canceled += 1
                except Exception:
                    continue
        except BinanceAPIError as e:
            if e.status not in (404, 400):
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "_cancel_by_token: algo cancel error for symbol=%s: %s", symbol, e
                )

        return canceled

    # --- Position quantity / margin query ---

    def _get_position_info(
        self, symbol: str, logical_side: str | None = None,
        *, client: BinanceFuturesClient,
    ) -> tuple[float, float, int]:
        """Return (abs_qty, margin_usdt, leverage) for the symbol position.

        margin_usdt is ``isolatedMargin`` for ISOLATED mode and
        ``initialMargin`` for CROSSED (best proxy available without
        account-level breakdown).  Returns (0.0, 0.0, 0) if no position found.
        """
        risks = client.get_position_risk() or []
        for p in risks:
            if (p.get("symbol") or "").upper() != symbol.upper():
                continue
            amt = _f(p.get("positionAmt"))
            # margin: isolatedMargin > 0 for ISOLATED; fall back to initialMargin
            margin = _f(p.get("isolatedMargin")) or _f(p.get("initialMargin"))
            leverage = _i(p.get("leverage"), 0)
            ps = (p.get("positionSide") or "").upper().strip()

            if self.position_mode == "oneway":
                # If account is actually in Hedge mode but we are configured to Oneway,
                # skip empty LONG/SHORT records, otherwise we falsely return 0 and cancel stops.
                if ps in {"LONG", "SHORT"} and abs(amt) < 1e-9:
                    continue
                return abs(amt), margin, leverage
            # Hedge mode: match positionSide
            if logical_side and ps and ps in {"LONG", "SHORT"}:
                if logical_side.upper() == ps and abs(amt) > 0:
                    return abs(amt), margin, leverage
                continue
            return abs(amt), margin, leverage
        return 0.0, 0.0, 0

    def _get_position_qty(
        self, symbol: str, logical_side: str | None = None,
        *, client: BinanceFuturesClient,
    ) -> float:
        """Return absolute position quantity. Returns 0.0 if no position."""
        qty, _, _lev = self._get_position_info(symbol, logical_side, client=client)
        return qty

    # ---------------------------------------------------------------------------
    # Exact flatten / dust cleanup — live exchange truth source of truth
    # ---------------------------------------------------------------------------

    def _get_live_symbol_exposure(
        self,
        symbol: str,
        *,
        client: BinanceFuturesClient,
        filters: FiltersCache,
        logical_side: str | None = None,
    ) -> dict[str, Any]:
        """Read live exchange truth for a symbol.

        Returns the current signed/absolute qty, logical side, notional, margin,
        plus open plain/algo order counts. This is the source of truth for exact
        flatten / dust cleanup; callers must not infer close qty from local state.
        """
        qty = 0.0
        signed_qty = 0.0
        side = logical_side.upper() if logical_side else None
        margin = 0.0
        notional = 0.0
        leverage = 0
        risks = client.get_position_risk() or []
        for pos in risks:
            if (pos.get("symbol") or "").upper() != symbol.upper():
                continue
            signed_qty = _f(pos.get("positionAmt"), 0.0)
            qty = abs(float(signed_qty))
            if signed_qty > 0:
                side = "LONG"
            elif signed_qty < 0:
                side = "SHORT"
            margin = _f(pos.get("isolatedMargin"), 0.0) or _f(pos.get("initialMargin"), 0.0)
            notional = abs(_f(pos.get("notional"), 0.0))
            leverage = _i(pos.get("leverage"), 0)
            break
        plain_orders: list[dict[str, Any]] = []
        algo_orders: list[dict[str, Any]] = []
        plain_err = ""
        algo_err = ""
        try:
            plain_orders = list(client.get_open_orders(symbol) or [])
        except Exception as exc:
            plain_err = str(exc)
        try:
            algo_orders = list(client.get_open_algo_orders(symbol) or [])
        except Exception as exc:
            algo_err = str(exc)
        tol = self._position_qty_tolerance(symbol, filters=filters)
        is_flat_qty = math.isclose(qty, 0.0, abs_tol=tol)
        return {
            "symbol": symbol,
            "signed_qty": signed_qty,
            "abs_qty": qty,
            "logical_side": side,
            "notional_usdt": notional,
            "margin_usdt": margin,
            "leverage": leverage,
            "open_plain_orders": len(plain_orders),
            "open_algo_orders": len(algo_orders),
            "plain_order_refs": plain_orders,
            "algo_order_refs": algo_orders,
            "plain_orders_error": plain_err,
            "algo_orders_error": algo_err,
            "qty_tolerance": tol,
            "is_flat_qty": bool(is_flat_qty),
            # is_flat requires qty==0 AND no outstanding orders that could create fills
            "is_flat": bool(is_flat_qty and len(plain_orders) == 0 and len(algo_orders) == 0),
        }

    def _is_dust_position_snapshot(self, snapshot: dict[str, Any]) -> bool:
        """Return True if the position snapshot looks like a sub-threshold dust remnant.

        A position is considered dust if qty > 0 but its margin or notional is
        at or below the configured thresholds. These are intentionally conservative
        so we only label positions as dust when we are certain they are economically
        negligible.
        """
        qty = abs(_f(snapshot.get("abs_qty"), 0.0))
        if qty <= 0.0:
            return False
        margin = abs(_f(snapshot.get("margin_usdt"), 0.0))
        notional = abs(_f(snapshot.get("notional_usdt"), 0.0))
        return bool(
            (margin > 0.0 and margin <= float(self.dust_margin_usdt))
            or (notional > 0.0 and notional <= float(self.dust_notional_usdt))
        )

    def _cancel_all_symbol_orders_best_effort(
        self,
        *,
        symbol: str,
        client: BinanceFuturesClient,
    ) -> dict[str, Any]:
        """Cancel plain + algo orders for the symbol and report observed counts.

        First issues the bulk cancel, then individually retries each known order
        so transient bulk-cancel failures don't leave orphaned orders that would
        block a subsequent reduce-only close.
        """
        plain_orders: list[dict[str, Any]] = []
        algo_orders: list[dict[str, Any]] = []
        try:
            plain_orders = list(client.get_open_orders(symbol) or [])
        except Exception:
            plain_orders = []
        try:
            algo_orders = list(client.get_open_algo_orders(symbol) or [])
        except Exception:
            algo_orders = []
        canceled_plain = 0
        canceled_algo = 0
        with contextlib.suppress(Exception):
            client.cancel_all_orders(symbol)
        # Per-order fallback cancels in case bulk endpoint missed anything
        for row in plain_orders:
            try:
                oid = _i(row.get("orderId"), 0)
                cid = str(row.get("clientOrderId") or row.get("origClientOrderId") or "").strip() or None
                if oid:
                    client.cancel_plain_order(symbol, order_id=oid)
                    canceled_plain += 1
                elif cid:
                    client.cancel_plain_order(symbol, client_order_id=cid)
                    canceled_plain += 1
            except Exception:
                continue
        for row in algo_orders:
            try:
                oid = _i(row.get("algoId"), 0)
                cid = (row.get("clientAlgoId") or "").strip() or None
                if oid:
                    client.cancel_algo_order(symbol, algo_id=oid)
                    canceled_algo += 1
                elif cid:
                    client.cancel_algo_order(symbol, client_algo_id=cid)
                    canceled_algo += 1
            except Exception:
                continue
        return {
            "plain_seen": len(plain_orders),
            "algo_seen": len(algo_orders),
            "plain_canceled": canceled_plain,
            "algo_canceled": canceled_algo,
        }

    def _verify_symbol_flat(
        self,
        *,
        symbol: str,
        client: BinanceFuturesClient,
        filters: FiltersCache,
        timeout_ms: int | None = None,
        logical_side: str | None = None,
    ) -> dict[str, Any]:
        """Poll exchange truth until symbol is flat and has no open orders.

        Emits EXECUTION_DUST_RESIDUAL_QTY on each poll and
        EXECUTION_FORCE_FLAT_VERIFY_TOTAL on exit with result label:
          'flat'     — position and open orders both gone
          'dust'     — position remains but is below dust thresholds
          'residual' — non-trivial position still open after timeout
        """
        timeout_ms = int(timeout_ms or self.dust_verify_timeout_ms)
        deadline = time.time() + max(timeout_ms, 0) / 1000.0
        last = self._get_live_symbol_exposure(symbol, client=client, filters=filters, logical_side=logical_side)
        while time.time() <= deadline:
            last = self._get_live_symbol_exposure(symbol, client=client, filters=filters, logical_side=logical_side)
            if EXECUTION_DUST_RESIDUAL_QTY is not None:
                with contextlib.suppress(Exception):
                    EXECUTION_DUST_RESIDUAL_QTY.labels(symbol=symbol).set(float(last.get("abs_qty") or 0.0))
            if last.get("is_flat"):
                if EXECUTION_FORCE_FLAT_VERIFY_TOTAL is not None:
                    with contextlib.suppress(Exception):
                        EXECUTION_FORCE_FLAT_VERIFY_TOTAL.labels(symbol=symbol, result="flat").inc()
                return last
            time.sleep(max(float(self.dust_verify_poll_ms), 100.0) / 1000.0)
        if EXECUTION_FORCE_FLAT_VERIFY_TOTAL is not None:
            try:
                result = "dust" if self._is_dust_position_snapshot(last) else "residual"
                EXECUTION_FORCE_FLAT_VERIFY_TOTAL.labels(symbol=symbol, result=result).inc()
            except Exception:
                pass
        return last

    def _force_flatten_symbol_exact(
        self,
        *,
        sid: str,
        symbol: str,
        client: BinanceFuturesClient,
        filters: FiltersCache,
        logical_side: str | None = None,
        reason_tag: str,
        max_attempts: int | None = None,
    ) -> dict[str, Any]:
        """Exact close path using live positionRisk quantity and post-close verify loop.

        Used for cancel / emergency flatten / dirty reversal / dust cleanup.
        Every attempt reads the current exchange qty, cancels all plain+algo orders,
        submits reduce-only on the exact live exposure, then verifies flatness.

        Returns a result dict with:
          status          — 'already_flat' | 'closed' | 'dust_remaining' | 'residual_position'
          residual_qty    — abs qty remaining after all attempts
          residual_notional_usdt / residual_margin_usdt
          attempts        — list of per-attempt detail dicts
          verify          — final exposure snapshot
          (+ fields from last _submit_reduce_only_market_exit call, e.g. close_order_id)
        """
        attempts: list[dict[str, Any]] = []
        max_attempts = max(1, int(max_attempts or self.dust_close_retries))
        initial = self._get_live_symbol_exposure(symbol, client=client, filters=filters, logical_side=logical_side)
        if initial.get("is_flat"):
            return {
                "status": "already_flat",
                "attempts": attempts,
                "residual_qty": 0.0,
                "residual_notional_usdt": 0.0,
                "residual_margin_usdt": 0.0,
                "verify": initial,
            }
        verify = initial
        last_close: dict[str, Any] = {}
        for attempt in range(1, max_attempts + 1):
            live = self._get_live_symbol_exposure(symbol, client=client, filters=filters, logical_side=logical_side)
            verify = live
            if live.get("is_flat"):
                break
            live_qty = float(live.get("abs_qty") or 0.0)
            live_side = str(live.get("logical_side") or logical_side or "").upper().strip() or None
            if live_qty <= 0.0 or live_side not in {"LONG", "SHORT"}:
                break
            # Cancel all standing orders first so reduce-only cannot be blocked
            canceled = self._cancel_all_symbol_orders_best_effort(symbol=symbol, client=client)
            close = self._submit_reduce_only_market_exit(
                sid=sid,
                symbol=symbol,
                logical_side=live_side,
                qty=live_qty,
                reason_tag=reason_tag if attempt == 1 else f"{reason_tag}{attempt}",
                client=client,
                filters=filters,
            )
            verify = self._verify_symbol_flat(
                symbol=symbol,
                client=client,
                filters=filters,
                logical_side=live_side,
            )
            attempt_doc = {
                "attempt": attempt,
                "live_qty": live_qty,
                "live_side": live_side,
                "is_dust_before": self._is_dust_position_snapshot(live),
                "canceled": canceled,
                "close": close,
                "verify": {
                    "abs_qty": float(verify.get("abs_qty") or 0.0),
                    "notional_usdt": float(verify.get("notional_usdt") or 0.0),
                    "margin_usdt": float(verify.get("margin_usdt") or 0.0),
                    "open_plain_orders": int(verify.get("open_plain_orders") or 0),
                    "open_algo_orders": int(verify.get("open_algo_orders") or 0),
                    "is_flat": bool(verify.get("is_flat")),
                    "is_dust": self._is_dust_position_snapshot(verify),
                }
            }
            attempts.append(attempt_doc)
            last_close = close
            if verify.get("is_flat"):
                break
        status = (
            "closed" if verify.get("is_flat")
            else ("dust_remaining" if self._is_dust_position_snapshot(verify) else "residual_position")
        )
        if EXECUTION_DUST_CLEANUP_TOTAL is not None:
            with contextlib.suppress(Exception):
                EXECUTION_DUST_CLEANUP_TOTAL.labels(symbol=symbol, result=status).inc()
        return {
            "status": status,
            "attempts": attempts,
            "residual_qty": float(verify.get("abs_qty") or 0.0),
            "residual_notional_usdt": float(verify.get("notional_usdt") or 0.0),
            "residual_margin_usdt": float(verify.get("margin_usdt") or 0.0),
            "verify": verify,
            **last_close,
        }

    # --- Trailing stop placement / maker TP watchdog ---

    def _cancel_algo_order_best_effort(
        self,
        *,
        symbol: str,
        client: BinanceFuturesClient,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> None:
        try:
            if algo_id:
                client.cancel_algo_order(symbol, algo_id=int(algo_id))
                return
            if client_algo_id:
                client.cancel_algo_order(symbol, client_algo_id=str(client_algo_id))
        except Exception:
            return

    def _cancel_plain_order_best_effort(
        self,
        *,
        symbol: str,
        client: BinanceFuturesClient,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> None:
        try:
            if order_id:
                client.cancel_plain_order(symbol, order_id=int(order_id))
                return
            if client_order_id:
                client.cancel_plain_order(symbol, client_order_id=str(client_order_id))
        except Exception:
            return

    def _legacy__cancel_plain_order_best_effort__dedupe_2(
        self,
        *,
        symbol: str,
        client: BinanceFuturesClient,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> None:
        try:
            if order_id:
                client.cancel_plain_order(symbol, order_id=int(order_id))
                return
            if client_order_id:
                client.cancel_plain_order(symbol, client_order_id=str(client_order_id))
        except Exception:
            return

    def _legacy__cancel_plain_order_best_effort__dedupe_3(
        self,
        *,
        symbol: str,
        client: BinanceFuturesClient,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> None:
        try:
            if order_id:
                client.cancel_plain_order(symbol, order_id=int(order_id))
                return
            if client_order_id:
                client.cancel_plain_order(symbol, client_order_id=str(client_order_id))
        except Exception:
            return

    def _legacy__cancel_plain_order_best_effort__dedupe_4(
        self,
        *,
        symbol: str,
        client: BinanceFuturesClient,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> None:
        try:
            if order_id:
                client.cancel_plain_order(symbol, order_id=int(order_id))
                return
            if client_order_id:
                client.cancel_plain_order(symbol, client_order_id=str(client_order_id))
        except Exception:
            return

    def _try_arm_trailing_after_confirmed_tp(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        sl_algo_id: int | None,
        callback_rate_pct: float,
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> dict[str, Any]:
        qty = self._get_position_qty(symbol, logical_side=logical_side, client=client)
        if qty <= 0:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_arm", "status": "no_position_after_tp"
            })
            return {}
        if sl_algo_id:
            self._cancel_algo_order_best_effort(symbol=symbol, client=client, algo_id=sl_algo_id)
        trail = self._place_trailing_stop(
            sid=sid, symbol=symbol, logical_side=logical_side, qty=qty,
            callback_rate_pct=callback_rate_pct, client=client, filters=filters,
        )
        self._exec_event({
            "sid": sid, "symbol": symbol, "action": "trail_arm", "status": "armed",
            "side": logical_side, "qty": qty, "trail_callback_rate_pct": callback_rate_pct, **trail,
        })
        self._transition_state(sid, symbol=symbol, action="trail_arm", next_state=FSM_TRAIL_ARMED, details=trail)
        self._save_order_state(sid, {
            "action": "trail_arm", "status": "armed", "symbol": symbol, "side": logical_side,
            "trail_pending": False, **trail,
        })
        # if self.tg is not None and self.trail_notify:
        #     self.tg.send_text(
        #         f"🧷 BINANCE trailing armed\n"
        #         f"symbol={symbol} side={logical_side}\n"
        #         f"sid={sid[:24]}...\n"
        #         f"cb={callback_rate_pct:.1f}%"
        #     )
        return trail

    def _track_maker_tp_level_thread(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        level: int,
        trigger_price: float,
        planned_qty: float,
        target_remaining_qty: float,
        initial_qty: float,
        working_type: str,
        algo_id: int | None,
        client_algo_id: str | None,
        sl_algo_id: int | None,
        trail_after_tp1: bool,
        callback_rate_pct: float | None,
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> None:
        try:
            deadline = time.time() + float(self.tp_trigger_monitor_timeout_s)
            poll_s = max(0.2, float(self.trail_arm_poll_s))
            touched = False
            partial_emitted = False
            trail_armed = False
            tol = self._position_qty_tolerance(symbol, filters=filters)
            touch_ts_ms: int | None = None

            while time.time() < deadline:
                px = float(client.get_working_price(symbol, working_type) or 0.0)
                if px <= 0:
                    time.sleep(poll_s)
                    continue

                if not touched:
                    touched = (px >= float(trigger_price)) if logical_side == "LONG" else (px <= float(trigger_price))
                    if touched:
                        touch_ts_ms = _ms_now()
                        self._emit_tp_state(
                            sid, symbol, level, "TRIGGERED",
                            working_price=px, trigger_price=trigger_price, working_type=working_type,
                        )

                if touched:
                    current_qty = self._get_position_qty(symbol, logical_side=logical_side, client=client)
                    if current_qty <= target_remaining_qty + tol:
                        self._emit_tp_state(
                            sid, symbol, level, "FILLED",
                            current_qty=current_qty, target_remaining_qty=target_remaining_qty, planned_qty=planned_qty,
                        )
                        if level == self.trail_activate_tp_level and trail_after_tp1 and callback_rate_pct is not None and not trail_armed:
                            self._try_arm_trailing_after_confirmed_tp(
                                sid=sid, symbol=symbol, logical_side=logical_side, sl_algo_id=sl_algo_id,
                                callback_rate_pct=callback_rate_pct, client=client, filters=filters,
                            )
                        return
                    if current_qty < initial_qty - tol and not partial_emitted:
                        partial_emitted = True
                        self._emit_tp_state(
                            sid, symbol, level, "PARTIAL",
                            current_qty=current_qty, target_remaining_qty=target_remaining_qty, planned_qty=planned_qty,
                        )
                        if level == self.trail_activate_tp_level and trail_after_tp1 and callback_rate_pct is not None and not trail_armed:
                            self._try_arm_trailing_after_confirmed_tp(
                                sid=sid, symbol=symbol, logical_side=logical_side, sl_algo_id=sl_algo_id,
                                callback_rate_pct=callback_rate_pct, client=client, filters=filters,
                            )
                            trail_armed = True

                    if touch_ts_ms is not None and (_ms_now() - touch_ts_ms) >= int(self.tp_limit_watchdog_timeout_ms):
                        self._cancel_algo_order_best_effort(
                            symbol=symbol, client=client, algo_id=algo_id, client_algo_id=client_algo_id,
                        )
                        current_qty = self._get_position_qty(symbol, logical_side=logical_side, client=client)
                        missing_qty = max(0.0, current_qty - float(target_remaining_qty))
                        if missing_qty > tol:
                            close = self._submit_reduce_only_market_exit(
                                sid=sid, symbol=symbol, logical_side=logical_side, qty=missing_qty,
                                reason_tag=f"tp{int(level)}wd", client=client, filters=filters,
                            )
                            self._emit_tp_state(
                                sid, symbol, level, "WATCHDOG_MARKET_FALLBACK",
                                current_qty=current_qty, target_remaining_qty=target_remaining_qty,
                                missing_qty=missing_qty, close_order_id=close.get("close_order_id"),
                            )
                            if level == self.trail_activate_tp_level and trail_after_tp1 and callback_rate_pct is not None and not trail_armed:
                                time.sleep(0.2)
                                self._try_arm_trailing_after_confirmed_tp(
                                    sid=sid, symbol=symbol, logical_side=logical_side, sl_algo_id=sl_algo_id,
                                    callback_rate_pct=callback_rate_pct, client=client, filters=filters,
                                )
                            return
                        self._emit_tp_state(
                            sid, symbol, level, "FILLED",
                            current_qty=current_qty, target_remaining_qty=target_remaining_qty, planned_qty=planned_qty,
                        )
                        return

                time.sleep(poll_s)

            if not touched:
                self._emit_tp_state(sid, symbol, level, "MONITOR_TIMEOUT", trigger_price=trigger_price)
        except Exception as e:
            self._emit_tp_state(sid, symbol, level, "ERROR", msg=str(e)[:400])

    def _start_maker_tp_watchdogs(
        self,
        *,
        payload: dict[str, Any],
        sid: str,
        symbol: str,
        logical_side: str,
        filled_qty: float,
        prot: dict[str, Any],
        tps: list[float],
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> dict[str, Any]:
        if prot.get("execution_policy") != MAKER_FIRST or not tps or not _truthy(prot.get("tp_watchdog_enabled")):
            return {}

        trail_enabled = _truthy(payload.get("trail_after_tp1")) and bool(tps)
        callback_rate_pct: float | None = None
        if trail_enabled:
            callback_rate_pct = compute_trailing_callback_rate_pct(
                payload,
                min_pct=float(self.trail_cb_min),
                max_pct=float(self.trail_cb_max),
                default_pct=float(self.trail_cb_default),
            )

        summary = {"tp_watchdog_status": "started", "tp_watchdog_timeout_ms": self.tp_limit_watchdog_timeout_ms}
        if callback_rate_pct is not None:
            summary["trail_callback_rate_pct"] = callback_rate_pct

        for idx, _ in enumerate(tps, start=1):
            algo_id = _i(prot.get(f"tp{idx}_algo_id"), 0) or None
            if algo_id is None and not prot.get(f"tp{idx}_client_algo_id"):
                continue
            planned_qty = _f(prot.get(f"tp{idx}_qty"), 0.0)
            target_remaining_qty = _f(prot.get(f"tp{idx}_expected_remaining_qty"), 0.0)
            trigger_price = _f(prot.get(f"tp{idx}_trigger_price"), 0.0)
            working_type = str(prot.get(f"tp{idx}_working_type") or self.tp_limit_trigger_working_type).upper()
            t = threading.Thread(
                target=self._track_maker_tp_level_thread,
                kwargs={
                    "sid": sid,
                    "symbol": symbol,
                    "logical_side": logical_side,
                    "level": idx,
                    "trigger_price": trigger_price,
                    "planned_qty": planned_qty,
                    "target_remaining_qty": target_remaining_qty,
                    "initial_qty": float(filled_qty),
                    "working_type": working_type,
                    "algo_id": algo_id,
                    "client_algo_id": prot.get(f"tp{idx}_client_algo_id"),
                    "sl_algo_id": _i(prot.get("sl_algo_id"), 0) or None,
                    "trail_after_tp1": bool(trail_enabled and idx == self.trail_activate_tp_level),
                    "callback_rate_pct": callback_rate_pct if idx == self.trail_activate_tp_level else None,
                    "client": client,
                    "filters": filters,
                },
                daemon=True,
            )
            t.start()
        return summary

    def _monitor_trade_lifecycle_thread(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        client: BinanceFuturesClient,
    ) -> None:
        """Polls position for symbol/sid and cleans up all residual orders on close.
        
        This is the ultimate 'anti-tail' guard. It runs in the background and ensures
        that once the position is gone (closed by TP, SL, or manual intervention),
        any lingering protective orders for this SID are cancelled.
        """
        try:
            poll_s = float(os.getenv("TRADE_LIFECYCLE_POLL_S", "5.0"))
            # Safety timeout: 2x of BINANCE_TRAIL_ARM_TIMEOUT_S or default 4h
            deadline_s = float(os.getenv("TRADE_LIFECYCLE_TIMEOUT_S", "14400"))
            deadline = time.time() + deadline_s

            # Cache filters once
            filters = FiltersCache(client)

            while time.time() < deadline:
                # 1. Get position info
                risks = client.get_position_risk() or []
                qty = 0.0
                current_logical = None
                for p in risks:
                    if (p.get("symbol") or "").upper() == symbol:
                        amt = _f(p.get("positionAmt"))
                        qty = abs(amt)
                        if qty > 0:
                            current_logical = "LONG" if amt > 0 else "SHORT"
                        break

                # 2. Check for closure or reversal
                # We use a small tolerance for qty (defined by symbol step size)
                tol = self._position_qty_tolerance(symbol, filters=filters)

                is_closed = (qty <= tol)
                is_reversed = (current_logical is not None and current_logical != logical_side)

                if is_closed or is_reversed:
                    # Position is gone. Clean up all orders for this symbol/SID.
                    #
                    # When position is fully closed (is_closed), use cancel_all to
                    # catch Binance-converted algo→plain TP orders whose
                    # clientOrderId no longer contains the sid token.  This is safe
                    # because there is no live position on this symbol.
                    #
                    # When position is reversed (is_reversed), keep token-based
                    # cancel to avoid touching the new direction's protective orders.
                    is_startup_watchdog = str(sid).startswith("_startup_")
                    canceled = 0
                    cancel_detail: dict[str, Any] = {}
                    if is_closed:
                        # Bulk cancel: position is flat — no risk of collateral damage
                        cancel_detail = self._cancel_all_symbol_orders_best_effort(
                            symbol=symbol, client=client,
                        )
                        canceled = int(cancel_detail.get("plain_canceled", 0) or 0) + int(cancel_detail.get("algo_canceled", 0) or 0)
                    else:
                        # Reversed: only cancel orders belonging to the old SID
                        canceled = self._cancel_by_token(symbol, sid, client=client)
                    self._exec_event({
                        "sid": sid,
                        "symbol": symbol,
                        "action": "lifecycle_cleanup",
                        "event_type": "position_closed_cleanup",
                        "final_qty": qty,
                        "reason": "reversed" if is_reversed else "closed",
                        "canceled_orders_count": canceled,
                        "startup_watchdog": is_startup_watchdog,
                        **cancel_detail,
                    })
                    return

                time.sleep(poll_s)

            # If we hit deadline, emit a warning
            self._exec_event({
                "sid": sid,
                "symbol": symbol,
                "action": "lifecycle_monitor_timeout",
                "status": "warning",
                "msg": f"Lifecycle monitor timed out after {deadline_s}s without observing position closure."
            })
        except Exception as e:
            # Lifecycle monitor is a sidecar; ensure it never crashes the main loop
            with contextlib.suppress(Exception):
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "lifecycle_monitor_error",
                    "error": str(e)
                })

    def _place_trailing_stop(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, callback_rate_pct: float,
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> dict[str, Any]:
        """Place TRAILING_STOP_MARKET through the Algo API."""
        is_demo = getattr(self, "demo_client", None) is not None and client is self.demo_client
        actual_wt = "CONTRACT_PRICE" if is_demo else self.trail_working_type

        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        q, _ = self._quantize(symbol, qty, None, filters=filters)
        if float(q) <= 0:
            raise ValueError("trail qty <= 0")

        p: dict[str, Any] = {
            "symbol": symbol,
            "side": exit_side,
            "type": "TRAILING_STOP_MARKET",
            "quantity": q,
            "callbackRate": float(callback_rate_pct),
            "workingType": actual_wt,
            "clientAlgoId": _make_cid(sid, "trail", getattr(self, "r", None)),
        }
        if self.position_mode == "oneway":
            p["reduceOnly"] = True
            self._validate_exit_contract(
                position_side=pos_side,
                reduce_only=True,
                close_position=False,
                quantity=float(q),
                order_type="TRAILING_STOP_MARKET",
                working_type=actual_wt,
                is_algo=True,
            )
        if pos_side:
            p["positionSide"] = pos_side

        j = self._submit_algo_order_with_reconcile(
            sid=sid, symbol=symbol, action="place_trailing", params=p, client=client
        )
        return {
            "trail_algo_id": j.get("algoId"),
            "trail_client_id": p["clientAlgoId"],
            "trail_working_type": p["workingType"],
        }

    # --- Trailing helpers (orchestrator mode) ---

    @staticmethod
    def _compute_profile_sl(
        side: str, current_price: float, trail_distance: float,
        original_sl: float, point: float,
    ) -> float | None:
        """Compute trailing SL from profile distance. Pure math, no I/O.

        Mirrors TP1TrailingOrchestrator._compute_trailing_sl logic:
          LONG:  new_sl = current_price - trail_distance  (but never worse than original_sl)
          SHORT: new_sl = current_price + trail_distance  (but never worse than original_sl)

        Returns None if the computed SL is not an improvement or is invalid.
        """
        if trail_distance <= 0 or current_price <= 0:
            return None
        if point <= 0:
            point = 0.0001

        side = side.upper()
        if side == "SHORT":
            candidate = current_price + trail_distance
            # For SHORT: lower SL is better (closer to price)
            if original_sl > 0:
                candidate = min(candidate, original_sl)
            candidate = max(candidate, current_price + point)
            # Round up for SHORT
            candidate = math.ceil(candidate / point) * point
            if candidate <= current_price:
                candidate = current_price + point
        else:  # LONG
            candidate = current_price - trail_distance
            # For LONG: higher SL is better (closer to price)
            if original_sl > 0:
                candidate = max(candidate, original_sl)
            candidate = min(candidate, current_price - point)
            # Round down for LONG
            candidate = math.floor(candidate / point) * point
            if candidate >= current_price:
                candidate = current_price - point

        if candidate <= 0:
            return None
        return candidate

    def _replace_sl_order_on_exchange(
        self, *, sid: str, symbol: str, logical_side: str,
        new_sl: float, old_sl_algo_id: int | None,
        old_sl_client_id: str | None,
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> dict[str, Any]:
        """Cancel existing SL algo order and place new STOP_MARKET at new_sl.

        Returns dict with new sl_algo_id and sl_client_algo_id.
        Raises on failure (caller decides retry policy).
        """
        # Cancel old SL (best-effort)
        if old_sl_algo_id or old_sl_client_id:
            try:
                client.cancel_algo_order(
                    symbol,
                    algo_id=int(old_sl_algo_id) if old_sl_algo_id else None,
                    client_algo_id=old_sl_client_id,
                )
            except Exception:
                pass  # might already be cancelled

        is_demo = getattr(self, "demo_client", None) is not None and client is self.demo_client
        actual_wt = "CONTRACT_PRICE" if is_demo else self.sl_working_type

        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        reduce_only_allowed = (self.position_mode == "oneway")

        # Quantize SL price
        _, sl_q = self._quantize(symbol, 0.001, new_sl, filters=filters)
        new_cid = _make_cid(sid, "tsl", getattr(self, "r", None))

        p: dict[str, Any] = {
            "symbol": symbol,
            "side": exit_side,
            "type": "STOP_MARKET",
            "triggerPrice": sl_q,
            "workingType": actual_wt,
            "clientAlgoId": new_cid,
        }
        if reduce_only_allowed:
            p["reduceOnly"] = True
            # Use closePosition for the trailing SL to close the full remaining position
            p.pop("reduceOnly", None)
            if pos_side:
                p["positionSide"] = pos_side
            p["closePosition"] = True
        elif pos_side:
            p["positionSide"] = pos_side
            p["closePosition"] = True

        j = self._submit_algo_order_with_reconcile(
            sid=sid, symbol=symbol, action="trail_sl_move", params=p, client=client
        )
        return {
            "sl_algo_id": j.get("algoId"),
            "sl_client_algo_id": new_cid,
        }

    # --- Trailing arming background thread ---

    def _arm_trailing_after_tp1_thread(
        self, *, sid: str, symbol: str, logical_side: str,
        tp1: float, callback_rate_pct: float, sl_order_id: int | None,
        tp_client_algo_id: str | None = None,
        client: BinanceFuturesClient,
        filters: FiltersCache,
        trail_profile_name: str = "",
        signal_atr: float = 0.0,
        original_sl: float = 0.0,
        trail_atr_mult_calibrated: float | None = None,
    ) -> None:
        """Daemon thread: wait for TP order to FILL, then manage trailing stop.

        Option A implementation: Instead of polling mark_price (which causes race conditions
        with LIMIT orders), polling the user stream cache and position size checks for a confirmed FILL.
        """
        try:
            deadline = time.time() + float(self.trail_arm_timeout_s)
            poll_s = max(0.2, float(self.trail_arm_poll_s))
            touched = False
            mp = 0.0

            # For position size drop fallback
            last_risk_poll = 0.0
            risk_poll_interval = 2.0
            try:
                initial_qty, _, _ = self._get_position_info(symbol, logical_side=logical_side, client=client)
            except Exception:
                initial_qty = 0.0

            while time.time() < deadline:
                # 1. Option A: Native User Stream WebSocket logic (event-driven fill confirmation)
                if tp_client_algo_id:
                    event_doc = self._lookup_user_stream_event(algo_client_id=tp_client_algo_id) or {}
                    status = (event_doc.get("status") or "").upper()
                    if status in {"FILLED", "PARTIALLY_FILLED"}:
                        touched = True
                        break
                    elif status in {"CANCELED", "REJECTED", "EXPIRED", "NEW_REJECTED"}:
                        break

                # 2. Resilient backup: Poll actual position size drops (indicates a TP filled or closed)
                now = time.time()
                if now - last_risk_poll >= risk_poll_interval:
                    last_risk_poll = now
                    try:
                        qty, _, _ = self._get_position_info(symbol, logical_side=logical_side, client=client)
                        if getattr(filters.get(symbol), "step_size", None):
                            tol = filters.get(symbol).step_size / 2.0
                        else:
                            tol = 0.0001

                        # If position size shrank significantly, we consider it "touched"
                        # meaning a protective order (likely TP) executed.
                        if qty < initial_qty - tol and qty > 0:
                            touched = True
                            break
                        if qty <= 0:
                            # Position fully closed, nothing left to trail
                            break
                    except Exception:
                        pass

                time.sleep(poll_s)

            if not touched:
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "trail_arm",
                    "status": "timeout", "trail_tp1": tp1,
                    "trail_callback_rate_pct": callback_rate_pct,
                })
                return

            # Get the exact mark price upon confirmation so the trailing orchestrator has a valid baseline
            try:
                mp = float(client.get_mark_price(symbol) or 0.0)
            except Exception:
                mp = tp1 if tp1 > 0 else 0.0

            qty, margin_usdt, leverage = self._get_position_info(symbol, logical_side=logical_side, client=client)
            if qty <= 0:
                self._exec_event({
                    "sid": sid, "symbol": symbol,
                    "action": "trail_arm", "status": "no_position",
                })
                return

            # ── Branch: orchestrator vs native ──
            if self.trail_mode == "orchestrator":
                self._arm_trailing_orchestrator(
                    sid=sid, symbol=symbol, logical_side=logical_side,
                    tp1=tp1, mark_price_at_tp1=max(mp, 0.0001), qty=qty,
                    sl_order_id=sl_order_id,
                    trail_profile_name=trail_profile_name,
                    signal_atr=signal_atr, original_sl=original_sl,
                    margin_usdt=margin_usdt, leverage=leverage,
                    trail_atr_mult_calibrated=trail_atr_mult_calibrated,
                    client=client, filters=filters,
                )
            else:
                self._arm_trailing_native(
                    sid=sid, symbol=symbol, logical_side=logical_side,
                    tp1=tp1, mp=mp, qty=qty,
                    callback_rate_pct=callback_rate_pct,
                    sl_order_id=sl_order_id,
                    margin_usdt=margin_usdt, leverage=leverage,
                    client=client, filters=filters,
                )
        except Exception as e:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "error", "msg": str(e)[:900],
            })

    def _arm_trailing_native(
        self, *, sid: str, symbol: str, logical_side: str,
        tp1: float, mp: float, qty: float,
        callback_rate_pct: float, sl_order_id: int | None,
        margin_usdt: float, leverage: int,
        client: BinanceFuturesClient, filters: FiltersCache,
    ) -> None:
        """Old behavior: cancel hard SL, place TRAILING_STOP_MARKET."""
        if sl_order_id:
            with contextlib.suppress(Exception):
                client.cancel_algo_order(symbol, algo_id=int(sl_order_id))

        trail = self._place_trailing_stop(
            sid=sid, symbol=symbol, logical_side=logical_side,
            qty=qty, callback_rate_pct=callback_rate_pct,
            client=client, filters=filters,
        )

        ev = {
            "sid": sid, "symbol": symbol, "action": "trail_arm",
            "status": "armed", "side": logical_side, "qty": qty,
            "trail_tp1": tp1, "trail_callback_rate_pct": callback_rate_pct,
            "trail_mode": "native",
            **trail,
        }
        self._exec_event(ev)

        self._save_order_state(sid, {
            "action": "trail_arm", "status": "armed",
            "symbol": symbol, "side": logical_side,
            "trail_algo_id": trail.get("trail_algo_id"),
            "trail_client_id": trail.get("trail_client_id"),
            "trail_tp1": tp1, "trail_callback_rate_pct": callback_rate_pct,
            "trail_mode": "native",
        })

        self._notify_trail_armed(
            sid=sid, symbol=symbol, logical_side=logical_side,
            tp1=tp1, qty=qty, mp=mp,
            margin_usdt=margin_usdt, leverage=leverage,
            mode_label="native", extra_line=f"cb={callback_rate_pct:.1f}%",
        )

    def _arm_trailing_orchestrator(
        self, *, sid: str, symbol: str, logical_side: str,
        tp1: float, mark_price_at_tp1: float, qty: float,
        sl_order_id: int | None,
        trail_profile_name: str, signal_atr: float, original_sl: float,
        margin_usdt: float, leverage: int,
        trail_atr_mult_calibrated: float | None,
        client: BinanceFuturesClient, filters: FiltersCache,
    ) -> None:
        """Orchestrator mode: compute SL from profile, then continuously move STOP_MARKET."""

        # Resolve profile
        profile = None
        profile_name = trail_profile_name or self.trail_profile_name
        if self._trailing_profiles is not None:
            profile = self._trailing_profiles.get(profile_name)
        if profile is None and self._trailing_profiles is not None:
            profile = self._trailing_profiles.get("rocket_v1")

        if profile is None:
            # Fail-open: fall back to native mode
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "profile_not_found", "trail_profile": profile_name,
                "msg": "Falling back to native trailing",
            })
            self._arm_trailing_native(
                sid=sid, symbol=symbol, logical_side=logical_side,
                tp1=tp1, mp=mark_price_at_tp1, qty=qty,
                callback_rate_pct=self.trail_cb_default,
                sl_order_id=sl_order_id,
                margin_usdt=margin_usdt, leverage=leverage,
                client=client, filters=filters,
            )
            return

        # Compute trail distance (price units)
        atr_value = signal_atr if signal_atr and signal_atr > 0 else 0.0

        # Use dynamically calibrated multi from payload if available, else static profile
        active_atr_mult = trail_atr_mult_calibrated if trail_atr_mult_calibrated and trail_atr_mult_calibrated > 0 else profile.atr_mult

        if atr_value > 0:
            trail_distance = atr_value * active_atr_mult
        elif profile.mode == "POINTS" and getattr(profile, "points", 0) > 0:
            # Approximate point from filters or use 0.01 fallback
            point = self._get_point_size(symbol, filters)
            trail_distance = profile.points * point
        else:
            # No ATR and no POINTS — use default 0.6% of tp1 as distance
            trail_distance = tp1 * 0.006
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "atr_missing", "trail_profile": profile_name,
                "msg": f"ATR not in signal, using fallback {trail_distance:.4f}",
            })

        if trail_distance <= 0:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "distance_zero", "trail_profile": profile_name,
            })
            return

        # Optional: evaluate trailing condition
        if self._trailing_condition is not None:
            try:
                # Build a minimal ctx object for the evaluator
                class _Ctx:
                    pass
                ctx = _Ctx()
                decision = self._trailing_condition.evaluate(
                    ctx,
                    side=logical_side,
                    symbol=symbol,
                    kind="breakout",  # default kind for execution-triggered trailing
                    tf="1m",
                    regime="na",
                )
                if not decision.enabled:
                    self._exec_event({
                        "sid": sid, "symbol": symbol, "action": "trail_arm",
                        "status": "condition_blocked",
                        "reason": decision.reason,
                    })
                    return
            except Exception as cond_err:
                # Fail-open: proceed with trailing even if condition gate fails
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "trail_arm",
                    "status": "condition_error", "msg": str(cond_err)[:300],
                })

        point = self._get_point_size(symbol, filters)

        # Compute initial trailing SL
        new_sl = self._compute_profile_sl(
            side=logical_side,
            current_price=mark_price_at_tp1,
            trail_distance=trail_distance,
            original_sl=original_sl,
            point=point,
        )
        if new_sl is None:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "sl_compute_failed", "trail_distance": trail_distance,
            })
            return

        # Cancel old hard SL and place initial trailing SL
        sl_refs = self._replace_sl_order_on_exchange(
            sid=sid, symbol=symbol, logical_side=logical_side,
            new_sl=new_sl, old_sl_algo_id=sl_order_id,
            old_sl_client_id=None,
            client=client, filters=filters,
        )

        self._exec_event({
            "sid": sid, "symbol": symbol, "action": "trail_arm",
            "status": "armed", "side": logical_side, "qty": qty,
            "trail_tp1": tp1, "trail_mode": "orchestrator",
            "trail_profile": profile_name,
            "trail_atr_mult": active_atr_mult,
            "trail_distance": trail_distance,
            "trail_new_sl": new_sl,
            **sl_refs,
        })

        self._save_order_state(sid, {
            "action": "trail_arm", "status": "armed",
            "symbol": symbol, "side": logical_side,
            "trail_mode": "orchestrator",
            "trail_profile": profile_name,
            "trail_sl": new_sl,
            "trail_distance": trail_distance,
            **sl_refs,
        })

        self._notify_trail_armed(
            sid=sid, symbol=symbol, logical_side=logical_side,
            tp1=tp1, qty=qty, mp=mark_price_at_tp1,
            margin_usdt=margin_usdt, leverage=leverage,
            mode_label="orchestrator",
            extra_line=f"profile={profile_name} atr_mult={active_atr_mult:.2f} sl={new_sl:.4f}",
        )

        # ── Continuous SL-move loop ──
        current_sl = new_sl
        current_sl_algo_id = sl_refs.get("sl_algo_id")
        current_sl_client_id = sl_refs.get("sl_client_algo_id")
        sl_moves = 0
        loop_deadline = time.time() + float(self.trail_loop_timeout_s)
        loop_poll_s = max(0.5, float(self.trail_loop_poll_s))
        min_delta_pct = max(0.001, float(self.trail_sl_move_min_delta_pct))

        while time.time() < loop_deadline:
            time.sleep(loop_poll_s)

            # Check position still exists
            pos_qty, _, _ = self._get_position_info(symbol, logical_side=logical_side, client=client)
            if pos_qty <= 0:
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "trail_sl_loop",
                    "status": "position_closed", "sl_moves": sl_moves,
                })
                break

            try:
                mp = float(client.get_mark_price(symbol) or 0.0)
            except BinanceAPIError as _rate_err:
                if _rate_err.status == 429:
                    time.sleep(loop_poll_s * 5)  # 429 penalty backoff
                    continue
                mp = 0.0
            if mp <= 0:
                continue

            candidate_sl = self._compute_profile_sl(
                side=logical_side,
                current_price=mp,
                trail_distance=trail_distance,
                original_sl=current_sl,  # use current SL as floor
                point=point,
            )
            if candidate_sl is None:
                continue

            # Only move SL if it improves by at least min_delta_pct
            if logical_side == "LONG":
                improved = candidate_sl > current_sl
                delta_pct = abs(candidate_sl - current_sl) / current_sl * 100 if current_sl > 0 else 0
            else:
                improved = candidate_sl < current_sl
                delta_pct = abs(candidate_sl - current_sl) / current_sl * 100 if current_sl > 0 else 0

            if not improved or delta_pct < min_delta_pct:
                continue

            # Move the SL
            try:
                new_refs = self._replace_sl_order_on_exchange(
                    sid=sid, symbol=symbol, logical_side=logical_side,
                    new_sl=candidate_sl,
                    old_sl_algo_id=current_sl_algo_id,
                    old_sl_client_id=current_sl_client_id,
                    client=client, filters=filters,
                )
                old_sl = current_sl
                current_sl = candidate_sl
                current_sl_algo_id = new_refs.get("sl_algo_id")
                current_sl_client_id = new_refs.get("sl_client_algo_id")
                sl_moves += 1

                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "trail_sl_move",
                    "status": "moved", "old_sl": old_sl, "new_sl": current_sl,
                    "mark_price": mp, "sl_moves": sl_moves,
                })
            except Exception as move_err:
                # Non-fatal: log and continue the loop
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "trail_sl_move",
                    "status": "error", "msg": str(move_err)[:500],
                    "attempted_sl": candidate_sl, "current_sl": current_sl,
                })

        # Loop finished (timeout or position closed)
        self._exec_event({
            "sid": sid, "symbol": symbol, "action": "trail_sl_loop",
            "status": "finished", "total_sl_moves": sl_moves,
            "final_sl": current_sl,
        })

    def _get_point_size(self, symbol: str, filters: FiltersCache) -> float:
        """Best-effort point (tick) size from filters or hardcoded defaults."""
        try:
            f = filters.get(symbol)
            if f and hasattr(f, "tick_size") and f.tick_size > 0:
                return float(f.tick_size)
            if f and hasattr(f, "price_filter"):
                pf = f.price_filter
                if isinstance(pf, dict) and float(pf.get("tickSize", 0)) > 0:
                    return float(pf["tickSize"])
        except Exception:
            pass
        defaults = {
            "BTCUSDT": 0.10, "ETHUSDT": 0.01, "SOLUSDT": 0.0010,
            "BNBUSDT": 0.010, "XRPUSDT": 0.0001, "DOGEUSDT": 0.00001,
        }
        return defaults.get(symbol, 0.01)

    def _notify_trail_armed(
        self, *, sid: str, symbol: str, logical_side: str,
        tp1: float, qty: float, mp: float,
        margin_usdt: float, leverage: int,
        mode_label: str, extra_line: str,
    ) -> None:
        """Send Telegram notification about trailing activation (rate-limited)."""
        return

    def _maybe_start_trailing_after_tp1(
        self, *, payload: dict[str, Any], sid: str, symbol: str,
        logical_side: str, entry_price: float | None, tp_levels: list[float],
        client: BinanceFuturesClient,
        filters: FiltersCache,
        initial_qty: float | None = None,
        sl_order_id: int | None = None,
        tp1_working_type: str = "MARK_PRICE",
        policy: ExecutionPolicyDecision | None = None,
        prot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Start trailing arming daemon thread if trail_after_tp1=true in payload."""
        if not _truthy(payload.get("trail_after_tp1")):
            return {}
        if not tp_levels:
            return {"trail_after_tp1": True, "trail_status": "no_tp_levels"}

        cb = compute_trailing_callback_rate_pct(
            payload,
            min_pct=float(self.trail_cb_min),
            max_pct=float(self.trail_cb_max),
            default_pct=float(self.trail_cb_default),
        )
        if policy is not None and policy.name == MAKER_FIRST:
            return {
                "trail_after_tp1": True,
                "trail_callback_rate_pct": cb,
                "trail_status": "managed_by_tp_watchdog",
                "trail_pending": True,
            }

        # Use configured TP level for trailing activation (default=TP2)
        # Per-payload override from scale-in router (TP1 for scale-in schemas)
        requested_level = payload.get("trail_activate_tp_level_requested")
        if requested_level is not None:
            try:
                tp_idx = int(requested_level) - 1
            except (ValueError, TypeError):
                tp_idx = self.trail_activate_tp_level - 1
        else:
            tp_idx = self.trail_activate_tp_level - 1
        if tp_idx >= len(tp_levels):
            tp_idx = len(tp_levels) - 1  # fallback to last available TP level
        tp1 = float(tp_levels[tp_idx])

        # Extract orchestrator-specific fields from payload
        trail_profile_name = (payload.get("trail_profile") or "").strip()
        signal_atr = 0.0
        for atr_key in ("atr", "atr_value", "atr_14", "atr_m5"):
            try:
                v = float(payload.get(atr_key) or 0)
                if v > 0:
                    signal_atr = v
                    break
            except (ValueError, TypeError):
                continue
        original_sl = 0.0
        with contextlib.suppress(ValueError, TypeError):
            original_sl = float(payload.get("sl") or 0)

        trail_atr_mult_calibrated = None
        try:
            v = float(payload.get("trail_atr_mult_calibrated") or 0)
            if v > 0:
                trail_atr_mult_calibrated = v
        except (ValueError, TypeError):
            pass

        prot = prot or {}
        tp_client_algo_id = prot.get(f"tp{tp_idx + 1}_client_algo_id")

        t = threading.Thread(
            target=self._arm_trailing_after_tp1_thread,
            kwargs={
                "sid": sid, "symbol": symbol, "logical_side": logical_side,
                "tp1": tp1, "callback_rate_pct": cb, "sl_order_id": sl_order_id,
                "tp_client_algo_id": tp_client_algo_id,
                "client": client, "filters": filters,
                "trail_profile_name": trail_profile_name,
                "signal_atr": signal_atr,
                "original_sl": original_sl,
                "trail_atr_mult_calibrated": trail_atr_mult_calibrated,
            },
            daemon=True,
        )
        t.start()
        return {
            "trail_after_tp1": True,
            "trail_tp1": tp1,
            "trail_callback_rate_pct": cb,
            "trail_mode": self.trail_mode,
            "trail_profile": trail_profile_name or self.trail_profile_name,
            "trail_status": "arming",
            "trail_pending": True,
        }

    def _start_lifecycle_watchdog(
        self, sid: str, symbol: str, logical_side: str, client: BinanceFuturesClient
    ) -> None:
        """Start a background thread to monitor position closure and clean up orders."""
        t = threading.Thread(
            target=self._monitor_trade_lifecycle_thread,
            kwargs={
                "sid": sid,
                "symbol": symbol,
                "logical_side": logical_side,
                "client": client,
            },
            daemon=True,
        )
        t.start()

    def _reconcile_open_positions_on_startup(self) -> None:
        """On startup: scan all open positions and restart lifecycle watchdogs.

        This is the primary defense against leaked stop orders after executor
        restart. When the container is stopped/restarted, all in-memory daemon
        threads (including lifecycle watchdogs) are lost. On the next startup
        this method re-arms a watchdog for every open position found on Binance,
        ensuring orphaned SL/TP orders will be cancelled once the position closes.

        Additionally:
          - Checks open positions for missing protection (no SL/TP orders) and
            emits a Telegram alert + exec stream event.
          - Cancels orphaned orders on symbols with zero position (orders that
            survived a position close without cleanup, e.g. due to algo→plain
            conversion losing the sid token).

        ENV: EXEC_STARTUP_RECONCILE=0 to disable (default: 1/enabled).
        """
        if not _bool_env("EXEC_STARTUP_RECONCILE", True):
            return
        clients = []
        if self.client is not None:
            clients.append((self.client, "prod"))
        if self.demo_client is not None:
            clients.append((self.demo_client, "demo"))
        for client, label in clients:
            try:
                risks = client.get_position_risk() or []
                open_symbols: set[str] = set()  # symbols with live positions
                for pos in risks:
                    amt = 0.0
                    try:
                        amt = float(pos.get("positionAmt") or 0.0)
                    except Exception:
                        continue
                    if abs(amt) < 1e-9:
                        continue
                    symbol = (pos.get("symbol") or "").upper()
                    if not symbol:
                        continue
                    if self.allowlist and symbol not in self.allowlist:
                        continue
                    open_symbols.add(symbol)
                    logical = "LONG" if amt > 0 else "SHORT"
                    pseudo_sid = f"_startup_{symbol}_{logical}"
                    print(
                        f"[startup_reconcile] Restoring lifecycle watchdog "
                        f"{label} {symbol} {logical} amt={amt:.4f}"
                    )
                    self._start_lifecycle_watchdog(pseudo_sid, symbol, logical, client)

                    # --- Protection health check on startup ---
                    # Verify at least one protective order exists for this position.
                    try:
                        api_error = False
                        plain_orders = []
                        try:
                            plain_orders = client.get_open_orders(symbol) or []
                        except Exception as e:
                            api_error = True
                            print(f"[startup_reconcile] get_open_orders error for {symbol}: {e}")
                        algo_orders = []
                        try:
                            algo_orders = client.get_open_algo_orders(symbol) or []
                        except Exception as e:
                            api_error = True
                            print(f"[startup_reconcile] get_open_algo_orders error for {symbol}: {e}")
                        has_sl = False
                        has_tp = False
                        for o in plain_orders:
                            otype = str(o.get("origType") or o.get("type") or "").upper()
                            if "STOP" in otype and "TAKE_PROFIT" not in otype:
                                has_sl = True
                            if "TAKE_PROFIT" in otype:
                                has_tp = True
                        for o in algo_orders:
                            otype = (o.get("type") or "").upper()
                            if "STOP" in otype and "TAKE_PROFIT" not in otype:
                                has_sl = True
                            if "TAKE_PROFIT" in otype:
                                has_tp = True
                        if not has_sl and not has_tp and not api_error:
                            notional = abs(_f(pos.get("notional"), 0.0))
                            margin = abs(_f(pos.get("isolatedMargin"), 0.0)
                                         or _f(pos.get("initialMargin"), 0.0))
                            msg = (
                                f"⚠️ STARTUP RECONCILE: UNPROTECTED POSITION [{label}]\n"
                                f"Symbol: {symbol} {logical}\n"
                                f"Qty: {abs(amt):.4f}\n"
                                f"Notional: ${notional:.2f} | Margin: ${margin:.2f}\n"
                                f"Plain orders: {len(plain_orders)} | Algo: {len(algo_orders)}\n"
                                f"⚠️ No SL/TP protection found!"
                            )
                            print(f"[startup_reconcile] {msg}")
                            with contextlib.suppress(Exception):
                                self.tg.send_message(msg)
                            if _bool_env("EXEC_FLATTEN_UNPROTECTED_ON_STARTUP", False):
                                print(f"[startup_reconcile] AUTO-FLATTENING unprotected position {symbol} {logical} qty={amt}")
                                try:
                                    client_filters = getattr(self, "demo_filters" if label == "demo" else "filters", None)
                                    emerg = self._emergency_flatten_position(
                                        sid=pseudo_sid, symbol=symbol, logical_side=logical, qty=abs(amt),
                                        client=client, filters=client_filters
                                    )
                                    self._exec_event({
                                        "sid": pseudo_sid, "symbol": symbol,
                                        "action": "startup_reconcile",
                                        "event_type": "unprotected_position_flattened",
                                        "logical_side": logical, "qty": abs(amt),
                                        "flatten_details": emerg,
                                    })
                                except Exception as flat_exc:
                                    print(f"[startup_reconcile] failed to auto-flatten {symbol}: {flat_exc}")
                            else:
                                self._exec_event({
                                    "sid": pseudo_sid, "symbol": symbol,
                                    "action": "startup_reconcile",
                                    "event_type": "unprotected_position_detected",
                                    "logical_side": logical, "qty": abs(amt),
                                    "notional": notional, "margin": margin,
                                    "plain_orders": len(plain_orders),
                                    "algo_orders": len(algo_orders),
                                    "client_label": label,
                                })
                    except Exception as prot_exc:
                        print(f"[startup_reconcile] protection check error {symbol}: {prot_exc}")

                # --- Orphan order cleanup on startup ---
                # Cancel orders on allowlisted symbols that have no open position.
                if _bool_env("EXEC_STARTUP_ORPHAN_CLEANUP", True):
                    try:
                        for symbol in (self.allowlist or set()):
                            if symbol in open_symbols:
                                continue
                            plain_orders = client.get_open_orders(symbol) or []
                            algo_orders = []
                            with contextlib.suppress(Exception):
                                algo_orders = client.get_open_algo_orders(symbol) or []
                            total = len(plain_orders) + len(algo_orders)
                            if total == 0:
                                continue
                            print(
                                f"[startup_reconcile] Orphan cleanup {label} {symbol}: "
                                f"{len(plain_orders)} plain + {len(algo_orders)} algo orders, no position"
                            )
                            cancel_result = self._cancel_all_symbol_orders_best_effort(
                                symbol=symbol, client=client,
                            )
                            self._exec_event({
                                "sid": f"_startup_orphan_{symbol}",
                                "symbol": symbol,
                                "action": "startup_reconcile",
                                "event_type": "orphan_orders_cleaned",
                                "plain_seen": cancel_result.get("plain_seen", 0),
                                "algo_seen": cancel_result.get("algo_seen", 0),
                                "plain_canceled": cancel_result.get("plain_canceled", 0),
                                "algo_canceled": cancel_result.get("algo_canceled", 0),
                                "client_label": label,
                            })
                            msg = (
                                f"🧹 STARTUP ORPHAN CLEANUP [{label}]\n"
                                f"Symbol: {symbol}\n"
                                f"Canceled: {cancel_result.get('plain_canceled', 0)} plain, "
                                f"{cancel_result.get('algo_canceled', 0)} algo\n"
                                f"(orders with no active position)"
                            )
                            with contextlib.suppress(Exception):
                                self.tg.send_message(msg)
                    except Exception as orphan_exc:
                        print(f"[startup_reconcile] orphan cleanup error {label}: {orphan_exc}")

            except Exception as exc:
                print(f"[startup_reconcile] {label} error: {exc}")

    # --- Action handlers ---

    def handle_open(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Open a new position: entry order → wait fill → SL/TP → trailing arming.

        Routing: payload[is_virtual]=true → demo/testnet client; else → prod client.
        """
        client, filters = self._resolve_client(payload)
        self._sync_client_clock(client)
        is_virtual = _truthy(payload.get("is_virtual")) or _truthy(payload.get("virtual"))

        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        self._guard_sid_not_quarantined(sid, symbol=symbol, action='open')
        # P12: block open if symbol has an active manual hold from the runbook
        self._guard_symbol_not_manually_held(symbol=symbol, action='open')
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")
        # TradFi-Perps guard: if we already got -4411 for this symbol in this
        # session, reject immediately without hitting Binance (no spam retries).
        if symbol in self._tradfi_blocked:
            raise ValueError(
                f"TradFi-Perps agreement not signed for {symbol} (Binance -4411). "
                "Sign the contract at Binance Futures UI to re-enable this symbol."
            )
        # P5: pass client so guard can check exchange truth before releasing
        self._guard_single_active_symbol_open(sid=sid, symbol=symbol, client=client)

        recovered = self._resume_open_from_state(sid, symbol=symbol, client=client)
        if recovered is not None:
            try:
                if EXECUTION_DUPLICATE_PREVENTED_TOTAL is not None:
                    EXECUTION_DUPLICATE_PREVENTED_TOTAL.labels(symbol=symbol, reason="state_resume").inc()
            except Exception:
                pass
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "open",
                "event_type": "duplicate_prevented", "duplicate_reason": "state_resume",
            })
            return dict(recovered, duplicate_prevented=True)

        self._ensure_symbol_settings(symbol, client=client)

        side, logical, side_int = _normalize_side(payload)
        policy = self._resolve_execution_policy(payload, symbol)

        # --- Dirty Reversal & Orphan Cleanup ---
        # If opening a position we must clean up the opposing side before entry.
        # In One-Way mode an opposing positionAmt must be exactly closed first;
        # in Hedge mode simple reversal doesn't apply (both sides can coexist).
        # We use _force_flatten_symbol_exact so the close qty comes from live
        # positionRisk (exchange truth), all orders are cancelled first, and
        # flatness is verified before proceeding with the new entry.
        try:
            live = self._get_live_symbol_exposure(
                symbol, client=client, filters=filters, logical_side=logical,
            )
            is_opposing = (
                self.position_mode == "oneway"
                and live.get("abs_qty", 0.0) > 0.0
                and live.get("logical_side") not in (None, "", logical)
            )
            if is_opposing:
                opposing_side = (live.get("logical_side") or "").upper().strip()
                self._force_flatten_symbol_exact(
                    sid=sid,
                    symbol=symbol,
                    client=client,
                    filters=filters,
                    logical_side=opposing_side,
                    reason_tag="rev_close",
                )
            elif live.get("abs_qty", 0.0) == 0.0 or is_opposing:
                # No opposing position but might have orphaned algo orders for this side
                with contextlib.suppress(Exception):
                    self._cancel_all_symbol_orders_best_effort(symbol=symbol, client=client)
        except Exception:
            pass
        # ---------------------------------------


        qty = _normalize_qty(payload, self.assume_lot_is_qty, symbol=symbol)
        entry = payload.get("entry")
        price: float | None = None
        if entry not in (None, 0, "", "0"):
            price = _f(entry)

        order_type = str(payload.get("type") or ("limit" if price else "market")).upper()
        order_type = "MARKET" if order_type in {"MARKET", "MKT"} else "LIMIT"

        # --- Pre-flight: validate LIMIT entry price against mark price (-2021 guard) ---
        # A LIMIT BUY above mark price or LIMIT SELL below mark price would
        # execute immediately as a market order on Binance Futures — but some
        # order types (e.g. protective stops placed as LIMIT) raise -2021 instead.
        # For a plain entry LIMIT, Binance Futures actually sends it to the
        # book, but if the entry price has significantly crossed (i.e. trade
        # signal is stale), we fall back to MARKET to ensure fill.
        if order_type == "LIMIT" and price is not None and price > 0:
            try:
                mark_price_entry = float(client.get_mark_price(symbol) or 0.0)
                if mark_price_entry > 0:
                    # LONG limit BUY: if mark has already risen above entry → stale
                    # SHORT limit SELL: if mark has already fallen below entry → stale
                    limit_is_stale = (
                        (logical == "LONG" and mark_price_entry > price)
                        or (logical == "SHORT" and mark_price_entry < price)
                    )
                    if limit_is_stale:
                        self._exec_event({
                            "sid": sid, "symbol": symbol, "action": "entry_fallback",
                            "status": "warning",
                            "msg": (
                                f"LIMIT entry {price} crossed by mark {mark_price_entry:.4f} "
                                f"({logical}) — falling back to MARKET to avoid -2021"
                            ),
                            "entry_requested": price,
                            "mark_price": mark_price_entry,
                        })
                        order_type = "MARKET"
                        price = None
            except Exception:
                pass  # fail-open: proceed with original LIMIT if mark price unavailable
        # -----------------------------------------------------------------------

        q, p = self._quantize(symbol, qty, price, filters=filters)
        self._transition_state(
            sid, symbol=symbol, action="open", next_state=FSM_VALIDATED,
            details={"execution_policy": policy.name, "logical_side": logical, "side_int": side_int}
        )
        self._exec_event({
            "sid": sid, "symbol": symbol, "action": "open", "qty": float(q),
            "event_type": "INTENT_ACCEPTED", "fsm_state": "VALIDATED", "ts_ms": _ms_now()
        })
        if float(q) <= 0:
            raise ValueError("qty <= 0 after quantisation")

        # --- Margin Guard (pre-order safety check) ---
        try:
            # 1. Fetch available balance (with TTL caching to avoid REST latency)
            available_balance = self._get_available_balance(client)

            # 2. Determine margin required
            # Priority: payload explicitly providing 'margin' or 'margin_usdt'
            margin_required = float(payload.get("margin") or payload.get("margin_usdt") or 0.0)
            if margin_required <= 0:
                # Fallback calculation: (qty * price) / leverage
                lev = self._resolve_symbol_leverage(symbol)
                # Use mark price if limit price is not set
                calc_p = float(p) if (p is not None and float(p) > 0) else float(client.get_mark_price(symbol) or 0)
                if lev > 0 and calc_p > 0:
                    margin_required = (float(q) * calc_p) / lev

            # 3. Ratio check (available_balance / margin_required >= 4.0)
            if margin_required > 0:
                margin_ratio = available_balance / margin_required
                if margin_ratio < 4.0:
                    # print(
                    #     f"⚠️ MARGIN GUARD: skipping {symbol} (sid={sid}). "
                    #     f"Ratio {margin_ratio:.2f} < 4.0 (Balance: {available_balance:.2f}, Margin: {margin_required:.2f})"
                    # )
                    if EXECUTION_MARGIN_GUARD_SKIPPED_TOTAL is not None:
                        EXECUTION_MARGIN_GUARD_SKIPPED_TOTAL.labels(
                            symbol=symbol, venue=("demo" if is_virtual else "prod")
                        ).inc()

                    self._exec_event({
                        "sid": sid, "symbol": symbol, "action": "open",
                        "event_type": "skipped", "reason": "margin_guard",
                        "ratio": round(margin_ratio, 4),
                        "balance": round(available_balance, 2),
                        "margin": round(margin_required, 2)
                    })
                    return {"status": "skipped", "reason": "margin_guard", "ratio": margin_ratio}
        except Exception as guard_exc:
            # Fail-closed: if we cannot verify balance, we do not trade.
            print(f"❌ MARGIN GUARD ERROR for {symbol} (sid={sid}): {guard_exc}")
            raise
        # ---------------------------------------------

        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "side_int": side_int,
            "type": order_type,
            "quantity": q,
            "newClientOrderId": _make_cid(sid, "entry", getattr(self, "r", None)),
        }

        pos_side = _position_side_for_mode(self.position_mode, logical)
        if pos_side:
            params["positionSide"] = pos_side

        if order_type == "LIMIT":
            if p is None or float(p) <= 0:
                raise ValueError("LIMIT order requires a valid entry price")
            params["price"] = p
            params["timeInForce"] = self.safety_entry_time_in_force if policy.name == SAFETY_FIRST else "GTC"

        self._transition_state(
            sid, symbol=symbol, action="open", next_state=FSM_ENTRY_SUBMITTED,
            details={"entry_client_order_id": params["newClientOrderId"], "entry_type": order_type}
        )
        try:
            if EXECUTION_ENTRY_SUBMITTED_TOTAL is not None:
                EXECUTION_ENTRY_SUBMITTED_TOTAL.labels(symbol=symbol, venue=("demo" if is_virtual else "prod"), order_type=order_type).inc()
        except Exception:
            pass
        j_entry = self._submit_plain_order_with_reconcile(
            sid=sid, symbol=symbol, action="open_entry", params=params, client=client
        )
        order_id = _i(j_entry.get("orderId"), 0)
        if not order_id:
            raise RuntimeError(f"no orderId in response: {j_entry}")
        self._transition_state(
            sid, symbol=symbol, action="open", next_state=FSM_ENTRY_ACKED,
            details={"binance_order_id": order_id, "entry_client_order_id": params["newClientOrderId"]}
        )

        j_final = self._wait_fill(symbol, order_id, timeout_s=self.fill_timeout_s, client=client)
        status = (j_final.get("status") or "").upper()
        filled_qty = _f(j_final.get("executedQty"), 0.0)
        avg_price = _f(j_final.get("avgPrice"), 0.0)

        # P2: MAKER_FIRST FALLBACK
        if status != "FILLED" and policy.name == MAKER_FIRST and order_type == "LIMIT":
            try:
                j_cancel = client.cancel_order(symbol, order_id=order_id)
                status = (j_cancel.get("status") or "").upper()
                filled_qty = _f(j_cancel.get("executedQty"), filled_qty)
                avg_price = _f(j_cancel.get("avgPrice"), avg_price)
            except Exception:
                pass  # If cancel fails, order might have filled or already been canceled

            if float(q) > filled_qty:
                rem_qty = float(q) - filled_qty
                rem_q_str, _ = self._quantize(symbol, rem_qty, None, filters=filters)
                if float(rem_q_str) > 0:
                    params_mkt = {
                        "symbol": symbol,
                        "side": side,
                        "type": "MARKET",
                        "quantity": rem_q_str,
                        "newClientOrderId": _make_cid(sid, "entry_fb", getattr(self, "r", None)),
                    }
                    if pos_side:
                        params_mkt["positionSide"] = pos_side

                    j_fallback = self._submit_plain_order_with_reconcile(
                        sid=sid, symbol=symbol, action="open_fb", params=params_mkt, client=client
                    )
                    fb_id = _i(j_fallback.get("orderId"), 0)
                    fb_final = self._wait_fill(symbol, fb_id, timeout_s=self.fill_timeout_s, client=client)

                    fb_status = (fb_final.get("status") or "").upper()
                    fb_filled = _f(fb_final.get("executedQty"), 0.0)
                    fb_avg = _f(fb_final.get("avgPrice"), 0.0)

                    if fb_status in {"FILLED", "PARTIALLY_FILLED"}:
                        total_filled = filled_qty + fb_filled
                        if total_filled > 0:
                            if filled_qty > 0 and fb_filled > 0:
                                avg_price = ((filled_qty * avg_price) + (fb_filled * fb_avg)) / total_filled
                            else:
                                avg_price = fb_avg if fb_filled > 0 else avg_price
                        filled_qty = total_filled
                        status = "FILLED" if fb_status == "FILLED" else "PARTIALLY_FILLED"
                        j_final = fb_final

        if status not in {"FILLED", "PARTIALLY_FILLED"}:
            raise RuntimeError(f"entry not filled: status={status} order={j_final}")
        if filled_qty <= 0:
            raise RuntimeError(f"filled_qty=0: order={j_final}")
        entry_fsm = FSM_ENTRY_PARTIAL if status == "PARTIALLY_FILLED" else FSM_ENTRY_FILLED
        self._transition_state(
            sid, symbol=symbol, action="open", next_state=entry_fsm,
            details={"filled_qty": filled_qty, "avg_price": avg_price, "entry_status": status}
        )
        try:
            if EXECUTION_ENTRY_FILLED_TOTAL is not None:
                EXECUTION_ENTRY_FILLED_TOTAL.labels(symbol=symbol, venue=("demo" if is_virtual else "prod"), fill_status=status).inc()
        except Exception:
            pass

        sl = _f(payload.get("sl"), 0.0) if payload.get("sl") is not None else None
        sl = sl if sl and sl > 0 else None
        tps_raw = payload.get("tp_levels") or []
        tps = [float(x) for x in tps_raw if x not in (None, "")]
        tps = [tp for tp in tps if tp > 0]

        self._start_lifecycle_watchdog(sid, symbol, logical, client)
        self._transition_state(sid, symbol=symbol, action="open", next_state=FSM_PROTECTION_ARMING)
        # P0-5: mark arming timestamp so KillSwitchTimeoutExceeded alert can page if
        # protection is never confirmed within PROTECTION_ARM_TIMEOUT_MS.
        try:
            if KILL_SWITCH_ARMED_TIMESTAMP is not None:
                KILL_SWITCH_ARMED_TIMESTAMP.labels(symbol=symbol).set(time.time())
            if KILL_SWITCH_ACTIVE is not None:
                KILL_SWITCH_ACTIVE.labels(scope="symbol", reason="protection_arming").set(1)
        except Exception:
            pass
        prot = self._place_protective(
            sid=sid, symbol=symbol, logical_side=logical,
            qty=filled_qty, sl=sl, tps=tps, policy=policy,
            client=client, filters=filters,
            ref_price=avg_price if avg_price > 0 else None,
            tp_ratio=payload.get("tp_ratio"),
            tier=payload.get("tier", "C"),
        )

        # Extra guard: warn if caller supplied no SL/TP at all (payload omitted them),
        # distinct from the validator-skip case handled inside _place_protective.
        if not sl and not tps and self.tg is not None:
            self.tg.send_text(
                f"⚠️ BINANCE opened without SL/TP\n"
                f"symbol={symbol} side={logical} qty={filled_qty}\n"
                f"exec_price={avg_price:.4f}\n"
                f"Payload contained no sl/tp_levels — position is unprotected!"
            )

        trail = self._maybe_start_trailing_after_tp1(
            payload=payload, sid=sid, symbol=symbol, logical_side=logical,
            entry_price=avg_price if avg_price > 0 else (p or None),
            initial_qty=filled_qty,
            sl_order_id=_i(prot.get("sl_algo_id"), 0) or None,
            tp_levels=tps,
            tp1_working_type=str(prot.get("tp1_working_type") or policy.tp_working_type),
            policy=policy,
            client=client, filters=filters,
            prot=prot,
        )
        # Pre-initialise: populated below only when protection is confirmed
        maker_watchdogs: dict[str, Any] = {}


        # Anti-blowup invariant: a filled entry must not remain naked. We treat
        # missing protection refs as a critical incident and immediately flatten.
        trail_enabled = _truthy(payload.get("trail_after_tp1")) and bool(tps)
        is_protection_ok = self._protection_confirmed({**prot, **trail}, tps, trail_enabled)
        if not is_protection_ok and not is_virtual:
            self._emit_protection_incident(sid, symbol, "entry_filled_without_confirmed_protection")
            try:
                if EXECUTION_PROTECTION_ARM_TIMEOUT_TOTAL is not None:
                    EXECUTION_PROTECTION_ARM_TIMEOUT_TOTAL.labels(symbol=symbol, execution_policy=policy.name).inc()
                if EXECUTION_POSITION_UNPROTECTED_SECONDS is not None:
                    EXECUTION_POSITION_UNPROTECTED_SECONDS.labels(symbol=symbol).set(float(self.protection_arm_timeout_ms) / 1000.0)
            except Exception:
                pass
            emerg = self._emergency_flatten_position(
                sid=sid, symbol=symbol, logical_side=logical, qty=filled_qty,
                client=client, filters=filters,
            )

            if self.tg is not None:
                self.tg.send_text(
                    f"🚨 [P0] PROTECTION ARM TIMEOUT - EMERGENCY FLATTEN\n"
                    f"symbol={symbol} side={logical} qty={filled_qty}\n"
                    f"Executor failed to confirm protection within {self.protection_arm_timeout_ms}ms.\n"
                    f"Position has been forcefully flattened to prevent naked exposure!"
                )

            self._transition_state(sid, symbol=symbol, action="open", next_state=FSM_EMERGENCY_FLATTENED, details=emerg)
            # P0-5: position flattened — clear armed timestamp (no longer naked)
            try:
                if KILL_SWITCH_ARMED_TIMESTAMP is not None:
                    KILL_SWITCH_ARMED_TIMESTAMP.labels(symbol=symbol).set(0)
                if KILL_SWITCH_ACTIVE is not None:
                    KILL_SWITCH_ACTIVE.labels(scope="symbol", reason="protection_arming").set(0)
            except Exception:
                pass
            prot = {**prot, **emerg, "protection_invariant_failed": True}
        else:
            if not is_protection_ok and is_virtual:
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "protection_bypass",
                    "status": "warning", "msg": "Demo: No SL/TP found, skipping emergency flatten."
                })
            # Protection confirmed (or bypassed for demo) — start maker TP watchdog threads if policy requires it
            maker_watchdogs = self._start_maker_tp_watchdogs(
                payload=payload,
                sid=sid,
                symbol=symbol,
                logical_side=logical,
                filled_qty=filled_qty,
                prot=prot,
                tps=tps,
                client=client,
                filters=filters,
            )

        # P5: compute audit chain + order refs before building result
        audit_chain = self._derive_audit_chain_fields(payload, sid)
        audit_policies = self._derive_entry_exit_policies(execution_policy=policy.name)
        order_refs = {
            "entry_order_ref": self._format_order_ref(
                venue="binance", kind="entry",
                order_id=order_id, client_id=params["newClientOrderId"]
            )
        }

        self._transition_state(sid, symbol=symbol, action="open", next_state=FSM_PROTECTED, details={**prot, **trail})
        # P0-5: protection confirmed — clear armed timestamp
        try:
            if KILL_SWITCH_ARMED_TIMESTAMP is not None:
                KILL_SWITCH_ARMED_TIMESTAMP.labels(symbol=symbol).set(0)
            if KILL_SWITCH_ACTIVE is not None:
                KILL_SWITCH_ACTIVE.labels(scope="symbol", reason="protection_arming").set(0)
        except Exception:
            pass
        if tps:
            self._transition_state(sid, symbol=symbol, action="open", next_state=FSM_TP_POLICY_ARMED, details={"tp_levels_count": len(tps)})

        result = {
            "sid": sid, "symbol": symbol, "action": "open",
            "status": status.lower(), "side": logical,
            "qty": filled_qty, "exec_price": avg_price,
            "binance_order_id": order_id,
            "is_virtual": str(is_virtual).lower(),
            "venue": f"binance_{'demo' if is_virtual else 'prod'}",
            "execution_policy": policy.name,
            **audit_chain, **audit_policies, **order_refs,
            **prot, **trail, **maker_watchdogs,
            "json": json.dumps(
                {"entry": j_final, "protective": prot, "trailing": trail, "tp_watchdogs": maker_watchdogs},
                ensure_ascii=False, default=str,
            )
        }

        # --- Calibration / shadow metadata passthrough ---
        # Ensure calib fields from signal payload survive into result and orders:state
        _calib_extra: dict[str, Any] = {}
        try:
            from services.shadow_calib_meta import extract_calib_fields
            _calib_extra = extract_calib_fields(payload)
        except Exception:
            pass
        if _calib_extra:
            result.update(_calib_extra)

        # Save orders:state:{sid} for fast SID→Binance ID lookup by downstream services
        self._save_order_state(sid, {
            "action": "open",
            "status": status.lower(),
            "symbol": symbol,
            "side": logical,
            "qty": filled_qty,
            "exec_price": avg_price,
            "binance_order_id": order_id,
            "is_virtual": is_virtual,
            "venue": f"binance_{'demo' if is_virtual else 'prod'}",
            "execution_policy": policy.name,
            **audit_chain, **audit_policies, **order_refs,
            **prot, **trail, **maker_watchdogs,
            **_calib_extra,
        })
        return result

    # ─────────────────────────────────────────────────────────────────────
    # P12: protection contract helpers — resolve expected SL/TP/trail from
    # the layered payload-then-state fallback chain.
    # ─────────────────────────────────────────────────────────────────────

    def _expected_requested_sl(self, payload: dict[str, Any], state: dict[str, Any]) -> float | None:
        """Return the effective SL price: payload wins; falls back to state-saved value."""
        for src in (payload, state):
            v = src.get("sl") if src.get("sl") is not None else src.get("sl_requested")
            if v is not None:
                f = _f(v, 0.0)
                if f > 0:
                    return f
        return None

    def _expected_requested_tps(self, payload: dict[str, Any], state: dict[str, Any]) -> list[float]:
        """Return effective TP levels list: payload wins; falls back to state-saved values."""
        for src in (payload, state):
            raw = src.get("tp_levels") or src.get("tp_levels_requested")
            if raw:
                parsed = [float(x) for x in raw if x not in (None, "")]
                result = [tp for tp in parsed if tp > 0]
                if result:
                    return result
        return []

    def _expected_requested_tp_qtys(self, payload: dict[str, Any], state: dict[str, Any]) -> list[float] | None:
        """Return explicit TP qty overrides from scale-in router, or None.

        Scale-in payloads carry tp_qtys_requested_json (JSON-encoded list of floats)
        that override the default even-split from _split_tp_qtys.
        """
        for src in (payload, state):
            raw = src.get("tp_qtys_requested_json") or src.get("tp_qtys_requested")
            if raw:
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except Exception:
                        continue
                if isinstance(raw, list) and raw:
                    return [float(x) for x in raw if x not in (None, "")]
        return None

    def _trail_requested(self, payload: dict[str, Any], state: dict[str, Any]) -> bool:
        """Return whether trail-after-TP1 was requested: payload wins; falls back to state."""
        for src in (payload, state):
            v = src.get("trail_after_tp1_requested")
            if v is not None:
                return _truthy(v)
            v = src.get("trail_after_tp1")
            if v is not None:
                return _truthy(v)
        return False

    # ─────────────────────────────────────────────────────────────────────
    # P0: operator kill-switch for modify/resize paths
    # ─────────────────────────────────────────────────────────────────────

    def _guard_binance_action_enabled(self, *, action: str, sid: str, symbol: str) -> None:
        """Hard block unsafe mutation paths during incident containment.

        P0 risk containment: operators can disable `modify` and/or `resize` while
        the protection-placement / reconcile path is being hardened.  The block
        is explicit, observable, and ack-safe (handled in process_one without
        retry/DLQ noise).
        """
        act = (action or "").strip().lower()
        blocked = (act == "modify" and bool(getattr(self, "exec_disable_modify_on_binance", False))) or (
            act == "resize" and bool(getattr(self, "exec_disable_resize_on_binance", False))
        )
        if not blocked:
            return
        reason = str(getattr(self, "exec_blocked_action_reason", "operator_risk_hold") or "operator_risk_hold")
        details = {
            "sid": sid,
            "symbol": symbol,
            "action": act,
            "status": "blocked",
            "severity": "warning",
            "event_type": "execution_action_blocked",
            "reason": reason,
            "blocked_by_feature_flag": True,
        }
        try:
            if EXECUTION_OPERATION_BLOCKED_TOTAL is not None:
                EXECUTION_OPERATION_BLOCKED_TOTAL.labels(action=act, reason=reason).inc()
        except Exception:
            pass
        if sid and bool(getattr(self, "exec_blocked_action_state_write", True)):
            self._save_order_state(sid, {
                **details,
                "symbol": symbol,
                "action": act,
            })
        self._exec_event(details)
        raise RuntimeError(f"action_blocked:{act}:{reason}")

    # ─────────────────────────────────────────────────────────────────────
    # P12: strict protection verify + repair
    # ─────────────────────────────────────────────────────────────────────

    def _verify_protection_on_exchange(
        self, *, sid: str, symbol: str, payload: dict[str, Any],
        state: dict[str, Any], client: BinanceFuturesClient,
        # P3: explicit expected prices for strict price-mismatch detection
        sl: float | None = None,
        tps: list[float] | None = None,
    ) -> dict[str, Any]:
        """Strict verification of protection orders on-exchange using inspect_protection_set.

        Returns the inspection result dict with is_complete, missing, mismatched, etc.
        Emits EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL metric when incomplete.
        P3: passes expected_sl_price and expected_tp_prices for price-mismatch detection.
        """
        # Resolve expected protection from payload/state (payload wins)
        expected_sl = sl if sl is not None else self._expected_requested_sl(payload, state)
        expected_tps = list(tps) if tps is not None else self._expected_requested_tps(payload, state)
        trail = self._trail_requested(payload, state)
        try:
            result = client.inspect_protection_set(
                symbol=symbol, sid=sid,
                expected_sl=(expected_sl is not None and expected_sl > 0),
                expected_tps=expected_tps,
                trail_expected=trail and bool(expected_tps),
                # P3/P4: pass expected prices for strict trigger-price comparison
                expected_sl_price=expected_sl,
                expected_tp_prices=expected_tps if expected_tps else None,
            )
        except TypeError:
            # Older client stub that doesn't accept expected_sl_price / expected_tp_prices
            try:
                result = client.inspect_protection_set(
                    symbol=symbol, sid=sid,
                    expected_sl=(expected_sl is not None and expected_sl > 0),
                    expected_tps=expected_tps,
                    trail_expected=trail and bool(expected_tps),
                )
            except Exception as e:
                result = {
                    "is_complete": False,
                    "missing": ["exchange_query_failed"],
                    "mismatched": [],
                    "error": str(e)[:500],
                }
        except Exception as e:
            result = {
                "is_complete": False,
                "missing": ["exchange_query_failed"],
                "mismatched": [],
                "error": str(e)[:500],
            }
        if not result.get("is_complete"):
            missing = result.get("missing", [])
            mismatched = result.get("mismatched", [])
            reason = ",".join(missing + [f"mismatched:{m}" for m in mismatched]) or "unknown"
            try:
                if EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL is not None:
                    EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL.labels(
                        phase="verify", reason=reason[:64],
                    ).inc()
            except Exception:
                pass
        return result

    def _repair_open_protection(
        self, *, sid: str, symbol: str, payload: dict[str, Any],
        state: dict[str, Any], client: BinanceFuturesClient,
        filters: FiltersCache, policy: Any,
    ) -> tuple:
        """Attempt to repair missing protection orders (SL/TP).

        Returns (repair_state_str, is_now_complete: bool).
        Emits EXECUTION_PROTECTION_REPAIR_TOTAL for each component repaired.
        """
        sl = self._expected_requested_sl(payload, state)
        tps = self._expected_requested_tps(payload, state)
        logical = str(state.get("side") or payload.get("side") or "LONG").upper()
        if logical in {"BUY", "LONG"}:
            logical = "LONG"
        else:
            logical = "SHORT"
        qty = _f(state.get("qty") or payload.get("qty"), 0.0)
        avg_price = _f(state.get("exec_price") or state.get("avg_price"), 0.0)

        # Re-place missing protective orders
        prot = self._place_protective(
            sid=sid, symbol=symbol, logical_side=logical,
            qty=qty, sl=sl, tps=tps, policy=policy,
            client=client, filters=filters,
            ref_price=avg_price if avg_price > 0 else None,
            tp_ratio=payload.get("tp_ratio") if payload else None,
            tier=state.get("tier", payload.get("tier", "C")) if payload else state.get("tier", "C"),
        )

        # Log repair metrics
        for component in ["sl", "tp"]:
            try:
                if EXECUTION_PROTECTION_REPAIR_TOTAL is not None:
                    EXECUTION_PROTECTION_REPAIR_TOTAL.labels(
                        symbol=symbol, component=component
                    ).inc()
            except Exception:
                pass

        # Verify after repair
        verify = self._verify_protection_on_exchange(
            sid=sid, symbol=symbol, payload=payload, state=state, client=client,
        )
        is_complete = verify.get("is_complete", False)

        self._exec_event({
            "sid": sid, "symbol": symbol, "action": "repair",
            "event_type": "protection_repair_result",
            "is_complete": is_complete,
            "missing_after_repair": (verify.get("missing", [])),
        })

        return ("repaired" if is_complete else "repair_incomplete", is_complete)

    def _reconcile_entry_by_client_id(
        self, *, sid: str, symbol: str, client: BinanceFuturesClient, payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Query exchange for entry order status using client orderID pattern.

        P12: Used during reconcile after exception to determine if open succeeded.
        Returns raw order dict or None if not found.
        """
        cid = getattr(client, "_build_client_order_id", lambda sid, tag: None)(sid, "open")
        if not cid:
            return None
        try:
            result = client.query_plain_order(symbol, client_order_id=cid)
            return result if isinstance(result, dict) else None
        except Exception:
            return None

    def _reconcile_protection_by_sid(
        self, *, sid: str, symbol: str, client: BinanceFuturesClient, payload: dict[str, Any],
        state: dict[str, Any],
    ) -> dict[str, Any]:
        """Query exchange for current protection (SL/TP) status for a given sid.

        P12: Used in reconcile and resume paths.
        Delegates to inspect_protection_set (or reconcile_protection_by_sid if client supports it).
        """
        try:
            if hasattr(client, "reconcile_protection_by_sid"):
                return client.reconcile_protection_by_sid(symbol=symbol, sid=sid)
            return self._verify_protection_on_exchange(
                sid=sid, symbol=symbol, payload=payload, state=state, client=client,
            )
        except Exception as e:
            return {"is_complete": False, "missing": ["exchange_query_failed"], "error": str(e)[:500]}

    def _legacy__resume_open_from_state__dedupe_1(
        self, sid: str, *, symbol: str, client: BinanceFuturesClient,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """P12: Resume an open trade from persisted state after executor restart.

        Called when exec_resume_open_repair=1 and the FSM state is ENTRY_FILLED or ENTRY_PARTIAL
        (protection placement may not have completed during prior run).

        Verifies protection on exchange: if complete, transitions to PROTECTED.
        If incomplete: attempts repair → transitions PROTECTED or EMERGENCY_FLATTENED.

        Returns a resume-status dict or None if no resume action was necessary.
        """
        if not bool(getattr(self, "exec_resume_open_repair", True)):
            return None

        state = self._load_order_state(sid)
        fsm = (state.get("fsm_state") or "")
        if fsm not in {FSM_ENTRY_FILLED, FSM_ENTRY_PARTIAL}:
            return None

        payload = payload or {}
        logical = (state.get("side") or "LONG").upper()
        if logical in {"BUY", "LONG"}:
            logical = "LONG"
        else:
            logical = "SHORT"

        verify = self._verify_protection_on_exchange(
            sid=sid, symbol=symbol, payload=payload, state=state, client=client,
        )
        if verify.get("is_complete"):
            self._transition_state(
                sid, symbol=symbol, action="resume",
                next_state=FSM_PROTECTED,
                details={"recovered_from_state": True, "resume_repair": "already_complete"},
            )
            tps = self._expected_requested_tps(payload, state)
            if tps:
                self._transition_state(
                    sid, symbol=symbol, action="resume",
                    next_state=FSM_TP_POLICY_ARMED,
                    details={"tp_levels_count": len(tps)},
                )
            return {"recovered_from_state": True, "resume_repair": "already_complete"}

        # Protection incomplete — attempt repair
        policy = self._resolve_execution_policy({**state, **payload}, symbol)
        try:
            filters = None  # not available in resume path
            repair_state, is_complete = self._repair_open_protection(
                sid=sid, symbol=symbol, payload=payload, state=state,
                client=client, filters=filters, policy=policy,
            )
        except Exception:
            repair_state = "repair_failed"
            is_complete = False

        if is_complete:
            self._transition_state(
                sid, symbol=symbol, action="resume",
                next_state=FSM_PROTECTED,
                details={"recovered_from_state": True, "resume_repair": repair_state},
            )
            tps = self._expected_requested_tps(payload, state)
            if tps:
                self._transition_state(
                    sid, symbol=symbol, action="resume",
                    next_state=FSM_TP_POLICY_ARMED,
                    details={"tp_levels_count": len(tps)},
                )
            return {"recovered_from_state": True, "resume_repair": repair_state}
        else:
            # Repair failed — emergency flatten
            qty = _f(state.get("qty"), 0.0)
            emerg = self._emergency_flatten_position(
                sid=sid, symbol=symbol, logical_side=logical, qty=qty,
                client=client, filters=None,
            )
            self._transition_state(sid, symbol=symbol, action="resume",
                                   next_state=FSM_EMERGENCY_FLATTENED, details=emerg)
            return {"recovered_from_state": True, "resume_repair": "emergency_flattened"}

    def _legacy__attempt_reconcile_after_exception__dedupe_1(
        self, *, payload: dict[str, Any], action: str, symbol: str,
        client: BinanceFuturesClient,
    ) -> dict[str, Any]:
        """P12: Attempt reconcile after action raised an exception.

        For action='open': queries entry order status + protection status.
        If entry is FILLED but protection is incomplete (and
        exec_reconcile_require_protection_complete=1), returns empty dict
        so caller treats this as unresolved.

        Returns an event dict with reconcile details, or {} if unresolved.
        """
        sid = (payload.get("sid") or "").strip()
        state = self._load_order_state(sid)

        # Query entry order
        entry_result = self._reconcile_entry_by_client_id(
            sid=sid, symbol=symbol, client=client, payload=payload,
        )
        if not entry_result:
            # Could not determine entry status — unresolved
            return {}

        entry_order_id = _i(entry_result.get("orderId"), 0) or None
        entry_status = (entry_result.get("status") or "")

        if action == "open" and entry_status == "FILLED":
            # Check protection completeness
            protection = self._reconcile_protection_by_sid(
                sid=sid, symbol=symbol, client=client, payload=payload, state=state,
            )
            is_complete = bool(protection.get("is_complete"))
            require_complete = bool(getattr(self, "exec_reconcile_require_protection_complete", True))
            if require_complete and not is_complete:
                # Try to repair
                policy = self._resolve_execution_policy({**state, **payload}, symbol)
                try:
                    _repair_state, is_now_complete = self._repair_open_protection(
                        sid=sid, symbol=symbol, payload=payload, state=state,
                        client=client, filters=None, policy=policy,
                    )
                    is_complete = is_now_complete
                except Exception:
                    is_complete = False
                if not is_complete:
                    # Still incomplete — return unresolved so caller can DLQ/retry
                    try:
                        if EXECUTION_RECONCILE_PARTIAL_PROTECTION_TOTAL is not None:
                            EXECUTION_RECONCILE_PARTIAL_PROTECTION_TOTAL.labels(
                                action=action, symbol=symbol,
                            ).inc()
                    except Exception:
                        pass
                    return {}

            event = {
                "sid": sid,
                "symbol": symbol,
                "action": action,
                "event_type": "reconcile_resolved",
                "reconcile_source": "user_stream_or_query",
                "reconciled_entry_order_id": entry_order_id,
                "reconciled_entry_status": entry_status,
                "protection_complete": is_complete,
            }
            self._exec_event(event)
            return event

        # Other actions or non-FILLED status — return unresolved
        return {}

    # ─────────────────────────────────────────────────────────────────────
    # P3: strict modify/resize protect-replace helpers
    # ─────────────────────────────────────────────────────────────────────

    def _read_live_position(
        self,
        *,
        symbol: str,
        client: BinanceFuturesClient,
    ) -> dict[str, Any]:
        """Read current live position from exchange.

        Returns a normalised dict so callers don't parse positionAmt directly:
            {is_open, qty, logical_side, position_amt}
        Fail-open: returns is_open=False on network errors.
        """
        try:
            risks = client.get_position_risk() or []
        except Exception:
            risks = []
        for pos in risks:
            if (pos.get("symbol") or "").upper() != symbol.upper():
                continue
            amt = _f(pos.get("positionAmt"), 0.0)
            qty = abs(amt)
            is_open = qty > 0.0
            logical_side = ("LONG" if amt > 0 else "SHORT") if is_open else None
            return {
                "is_open": is_open,
                "qty": qty,
                "logical_side": logical_side,
                "position_amt": amt,
            }
        return {"is_open": False, "qty": 0.0, "logical_side": None, "position_amt": 0.0}

    def _cancel_expected_protection_refs(
        self,
        *,
        sid: str,
        symbol: str,
        client: BinanceFuturesClient,
    ) -> list[str]:
        """Explicitly cancel SL/TP/trail algo orders by deterministic clientAlgoId.

        P3: before re-arming we cancel by known ID rather than cancel_all_orders
        so we don't accidentally cancel unrelated orders in multi-symbol or shared
        account scenarios.  Falls back to cancel_all_orders if client does not
        expose cancel_algo_order.

        Returns list of cancelled clientAlgoIds.
        """
        tags = ["sl", "tp1", "tp2", "tp3", "tp4", "trail"]
        cancelled_cids: list[str] = []
        has_cancel = hasattr(client, "cancel_algo_order")
        has_build = hasattr(client, "_build_client_algo_id")
        if has_cancel and has_build:
            for tag in tags:
                cid = client._build_client_algo_id(sid, tag)
                try:
                    client.cancel_algo_order(symbol, client_algo_id=cid)
                    cancelled_cids.append(cid)
                except Exception:
                    pass  # not found = already gone / never placed, that's fine
        else:
            # Fallback: cancel all orders for symbol
            with contextlib.suppress(Exception):
                client.cancel_all_orders(symbol)
        return cancelled_cids

    def _replace_position_protection(
        self,
        *,
        sid: str,
        symbol: str,
        action: str,
        logical_side: str,
        live_qty: float,
        sl: float | None,
        tps: list[float],
        payload: dict[str, Any],
        policy: Any,
        client: BinanceFuturesClient,
        filters: FiltersCache,
        ref_price: float | None = None,
    ) -> dict[str, Any]:
        """Strict protection replacement invariant (P3).

        Phase 1 — cancel:  explicitly cancel all known SL/TP/trail refs by clientAlgoId.
        Phase 2 — replace: transition to PROTECTION_REPLACING; re-arm SL/TP/trail.
        Phase 3 — verify:  inspect_protection_set confirms prices + presence.
        Phase 4 — settle:  transition to PROTECTED or, on timeout / verify fail, to
                            EMERGENCY_FLATTENED.

        Emits EXECUTION_PROTECTION_REPLACE_TOTAL and
        EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS metrics.
        """
        max_naked_ms = int(getattr(self, "protection_replace_max_naked_ms", 3000))
        strict = bool(getattr(self, "exec_modify_resize_strict_replace", True))

        # --- Phase 1: cancel old protection refs ---
        self._cancel_expected_protection_refs(sid=sid, symbol=symbol, client=client)

        # --- Phase 2: transition to PROTECTION_REPLACING + re-arm ---
        self._transition_state(
            sid, symbol=symbol, action=action,
            next_state=FSM_PROTECTION_REPLACING,
            details={"live_qty": live_qty, "sl": sl, "tps": list(tps)},
        )
        ts_naked_start = _ms_now()
        prot: dict[str, Any] = {}
        trail: dict[str, Any] = {}
        try:
            if live_qty > 0 and (sl or tps):
                # Scale-in: extract explicit TP qty allocation from payload
                tp_qtys_override = None
                for _src in (payload,):
                    _raw_tpq = _src.get("tp_qtys_requested_json") or _src.get("tp_qtys_requested")
                    if _raw_tpq:
                        if isinstance(_raw_tpq, str):
                            with contextlib.suppress(Exception):
                                tp_qtys_override = json.loads(_raw_tpq)
                        elif isinstance(_raw_tpq, list):
                            tp_qtys_override = _raw_tpq
                        break
                prot = self._place_protective(
                    sid=sid, symbol=symbol, logical_side=logical_side,
                    qty=live_qty, sl=sl, tps=tps, policy=policy,
                    client=client, filters=filters,
                    ref_price=ref_price,
                    tp_qtys=tp_qtys_override,
                    tier=payload.get("tier", "C"),
                )
            trail_requested = _truthy(payload.get("trail_after_tp1_requested"))
            if trail_requested:
                trail = self._maybe_start_trailing_after_tp1(
                    payload=payload, sid=sid, symbol=symbol, logical_side=logical_side,
                    initial_qty=live_qty,
                    sl_order_id=_i(prot.get("sl_algo_id"), 0) or None,
                    tp_levels=tps,
                    tp1_working_type=str(prot.get("tp1_working_type") or getattr(policy, "tp_working_type", "MARK_PRICE")),
                    policy=policy,
                    client=client, filters=filters,
                )
        except Exception as e:
            # Could not even place protection — immediate emergency flatten
            naked_ms = _ms_now() - ts_naked_start
            try:
                if EXECUTION_PROTECTION_REPLACE_TOTAL is not None:
                    EXECUTION_PROTECTION_REPLACE_TOTAL.labels(
                        symbol=symbol, action=action, result="place_failed",
                    ).inc()
                if EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS is not None:
                    EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS.labels(
                        symbol=symbol, action=action,
                    ).set(float(naked_ms))
            except Exception:
                pass
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": action,
                "event_type": "protection_replace_place_failed",
                "error": str(e)[:500],
                "naked_ms": naked_ms,
            })
            emerg = self._emergency_flatten_position(
                sid=sid, symbol=symbol, logical_side=logical_side, qty=live_qty,
                client=client, filters=filters,
            )
            self._transition_state(sid, symbol=symbol, action=action,
                                   next_state=FSM_EMERGENCY_FLATTENED, details=emerg)
            return {**emerg, "status": "emergency_flattened", "side": logical_side, "qty": live_qty}

        naked_ms = _ms_now() - ts_naked_start
        try:
            if EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS is not None:
                EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS.labels(
                    symbol=symbol, action=action,
                ).set(float(naked_ms))
        except Exception:
            pass

        # Naked-window time-budget exceeded — immediate emergency flatten
        if naked_ms > max_naked_ms:
            try:
                if EXECUTION_PROTECTION_REPLACE_TOTAL is not None:
                    EXECUTION_PROTECTION_REPLACE_TOTAL.labels(
                        symbol=symbol, action=action, result="naked_timeout",
                    ).inc()
            except Exception:
                pass
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": action,
                "event_type": "protection_replace_naked_timeout",
                "naked_ms": naked_ms,
                "max_naked_ms": max_naked_ms,
            })
            emerg = self._emergency_flatten_position(
                sid=sid, symbol=symbol, logical_side=logical_side, qty=live_qty,
                client=client, filters=filters,
            )
            self._transition_state(sid, symbol=symbol, action=action,
                                   next_state=FSM_EMERGENCY_FLATTENED, details=emerg)
            return {**emerg, "status": "emergency_flattened", "side": logical_side, "qty": live_qty}

        # --- Phase 3: verify on-exchange (strict mode only) ---
        is_verified = True
        if strict and bool(getattr(self, "exec_strict_protection_verify", True)):
            verify = self._verify_protection_on_exchange(
                sid=sid, symbol=symbol, payload=payload, state={}, client=client,
                sl=sl, tps=tps,
            )
            is_verified = verify.get("is_complete", False)
            if not is_verified:
                try:
                    if EXECUTION_PROTECTION_REPLACE_TOTAL is not None:
                        EXECUTION_PROTECTION_REPLACE_TOTAL.labels(
                            symbol=symbol, action=action, result="verify_failed",
                        ).inc()
                except Exception:
                    pass
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": action,
                    "event_type": "protection_replace_verify_failed",
                    "missing": (verify.get("missing", [])),
                    "mismatched": (verify.get("mismatched", [])),
                    "naked_ms": naked_ms,
                })
                emerg = self._emergency_flatten_position(
                    sid=sid, symbol=symbol, logical_side=logical_side, qty=live_qty,
                    client=client, filters=filters,
                )
                self._transition_state(sid, symbol=symbol, action=action,
                                       next_state=FSM_EMERGENCY_FLATTENED, details=emerg)
                return {
                    **emerg,
                    "status": "emergency_flattened",
                    "side": logical_side, "qty": live_qty,
                    **verify,
                }

        # --- Phase 4: settle (PROTECTED) ---
        try:
            if EXECUTION_PROTECTION_REPLACE_TOTAL is not None:
                EXECUTION_PROTECTION_REPLACE_TOTAL.labels(
                    symbol=symbol, action=action, result="ok",
                ).inc()
        except Exception:
            pass
        self._transition_state(sid, symbol=symbol, action=action,
                               next_state=FSM_PROTECTED,
                               details={**prot, **trail, "naked_ms": naked_ms})
        if tps:
            self._transition_state(sid, symbol=symbol, action=action,
                                   next_state=FSM_TP_POLICY_ARMED,
                                   details={"tp_levels_count": len(tps)})
        self._exec_event({
            "sid": sid, "symbol": symbol, "action": action,
            "event_type": "protection_replaced",
            "naked_ms": naked_ms,
            "is_verified": is_verified,
        })
        return {
            "status": "ok",
            "side": logical_side,
            "qty": live_qty,
            "naked_ms": naked_ms,
            **prot,
            **trail,
        }

    def handle_modify(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Modify protection for an existing position under a strict replace invariant.

        P3: cancel old protection → PROTECTION_REPLACING → verify new protection
        matches requested contract (prices + presence) → PROTECTED or emergency flatten.
        """
        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        self._guard_binance_action_enabled(action="modify", sid=sid, symbol=symbol)
        client, filters = self._resolve_client(payload)
        self._sync_client_clock(client)
        self._guard_sid_not_quarantined(sid, symbol=symbol, action='modify')
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")
        # P3: only allow modify in stable states (prevents double-arming on PROTECTION_ARMING etc.)
        state = self._load_order_state(sid)
        fsm_state = (state.get("fsm_state") or "")
        if fsm_state and fsm_state not in {
            FSM_ENTRY_FILLED, FSM_ENTRY_PARTIAL, FSM_PROTECTED,
            FSM_TP_POLICY_ARMED, FSM_TRAIL_ARMED,
        }:
            raise RuntimeError(f"modify_forbidden_in_state:{fsm_state}")
        canceled = self._cancel_by_token(symbol, sid, client=client)
        # P3: use live position from exchange (avoid stale local state)
        live = self._read_live_position(symbol=symbol, client=client)
        if not live.get("is_open"):
            return {
                "sid": sid,
                "symbol": symbol,
                "action": "modify",
                "status": "no_position",
                "canceled_orders": canceled,
            }
        logical = (live.get("logical_side") or "")
        qty = float(live.get("qty") or 0.0)
        # P3: merge state into payload so missing sl/tp fall back to last saved contract
        policy = self._resolve_execution_policy({**state, **payload}, symbol)
        sl = self._expected_requested_sl(payload, state)
        tps = self._expected_requested_tps(payload, state)
        trail_requested = self._trail_requested(payload, state)
        mark_price: float | None = None
        try:
            mp = float(client.get_mark_price(symbol) or 0.0)
            mark_price = mp if mp > 0 else None
        except Exception:
            pass
        # P3: strict replace — cancel old refs, re-arm, verify prices match
        replaced = self._replace_position_protection(
            sid=sid,
            symbol=symbol,
            action="modify",
            logical_side=logical,
            live_qty=qty,
            sl=sl,
            tps=tps,
            payload={**state, **payload, "trail_after_tp1_requested": trail_requested},
            policy=policy,
            client=client,
            filters=filters,
            ref_price=mark_price,
        )
        # P5: derive audit chain from payload and include in result
        audit_chain = self._derive_audit_chain_fields(payload, sid)
        audit_policies = self._derive_entry_exit_policies(execution_policy=policy.name)
        final_status = (replaced.get("status") or "ok")
        self._save_order_state(sid, {
            "action": "modify",
            "status": final_status,
            "symbol": symbol,
            "side": replaced.get("side") or logical,
            "qty": replaced.get("qty") or qty,
            "execution_policy": policy.name,
            "sl_requested": sl,
            "tp_levels_requested": list(tps),
            "trail_after_tp1_requested": trail_requested,
            **audit_chain,
            **audit_policies,
            **replaced,
        })
        return {
            "sid": sid,
            "symbol": symbol,
            "action": "modify",
            "status": final_status,
            "side": replaced.get("side") or logical,
            "qty": replaced.get("qty") or qty,
            "canceled_orders": canceled,
            "execution_policy": policy.name,
            "sl_requested": sl,
            "tp_levels_requested": list(tps),
            "trail_after_tp1_requested": trail_requested,
            **audit_chain,
            **audit_policies,
            **replaced,
            "json": json.dumps(
                {"canceled": canceled, "replaced": replaced},
                ensure_ascii=False,
                default=str,
            )
        }


    def handle_cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Cancel: remove our orders + market-close any open position using exact live qty.

        The close qty is read from live positionRisk (exchange truth) instead of
        local intent. All plain and algo orders are cancelled before the reduce-only
        close, and flatness is verified afterwards.
        """
        client, filters = self._resolve_client(payload)
        self._sync_client_clock(client)

        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")

        # P5: derive audit chain before cancel so exit refs are correctly attributed
        audit_chain = self._derive_audit_chain_fields(payload, sid)
        canceled = self._cancel_by_token(symbol, sid, client=client)

        # Close any open position — use exact live qty from exchange truth
        close = self._force_flatten_symbol_exact(
            sid=sid,
            symbol=symbol,
            client=client,
            filters=filters,
            logical_side=None,  # inferred from live positionRisk
            reason_tag="close",
        )
        closed = close.get("status") in {"closed", "already_flat"}
        close_order_id: int | None = _i(close.get("close_order_id"), 0) or None
        verify = close.get("verify") or {}
        logical = (verify.get("logical_side") or "").upper().strip() or None
        qty = float(close.get("residual_qty") or 0.0) if not closed else 0.0

        # P5: build exit_order_ref and closed_trade_id for chain linkage
        exit_order_ref = ''
        closed_trade_id = ''
        if close_order_id:
            exit_order_ref = self._format_order_ref(venue='binance', kind='exit', order_id=close_order_id)
            closed_trade_id = (
                (payload.get('closed_trade_id') or '')
                or self._new_closed_trade_id(sid, exit_order_ref=exit_order_ref)
            )
        result = {
            "sid": sid, "symbol": symbol, "action": "cancel",
            "status": "ok", "side": logical or "",
            "qty": qty, "canceled_orders": canceled,
            "close_order_id": close_order_id or "",
            "exit_order_ref": exit_order_ref,
            "closed_trade_id": closed_trade_id,
            "closed": str(closed).lower(),
            "residual_qty": float(close.get("residual_qty") or 0.0),
            "residual_notional_usdt": float(close.get("residual_notional_usdt") or 0.0),
            "residual_margin_usdt": float(close.get("residual_margin_usdt") or 0.0),
            **audit_chain,
        }
        # Update orders:state:{sid}: mark position as closed
        if closed:
            self._save_order_state(sid, {
                "action": "cancel",
                "status": "closed",
                "symbol": symbol,
                "side": logical or "",
                "close_order_id": close_order_id or "",
                "exit_order_ref": exit_order_ref,
                "closed_trade_id": closed_trade_id,
                "closed": True,
                "residual_qty": float(close.get("residual_qty") or 0.0),
                "residual_notional_usdt": float(close.get("residual_notional_usdt") or 0.0),
                "residual_margin_usdt": float(close.get("residual_margin_usdt") or 0.0),
                **audit_chain,
            })
        return result


    def _normalize_resize_target(
        self, current_qty: float, payload: dict[str, Any]
    ) -> tuple[str, float, float]:
        """Return (resize_mode, delta_qty, target_qty) from payload.

        Supports two modes:
        - resize_mode=delta_qty: adjust current by delta_qty (signed)
        - resize_mode=target_qty: set absolute target (inferred also when target_qty present)
        """
        resize_mode = str(
            payload.get("resize_mode")
            or ("target_qty" if payload.get("target_qty") not in (None, "") else "delta_qty")
        ).strip().lower()
        if resize_mode not in {"delta_qty", "target_qty"}:
            raise ValueError(f"unsupported_resize_mode:{resize_mode}")
        if resize_mode == "delta_qty":
            delta_qty = _f(payload.get("delta_qty"), 0.0)
            target_qty = max(0.0, float(current_qty) + float(delta_qty))
        else:
            target_qty = _f(
                payload.get("target_qty")
                if payload.get("target_qty") not in (None, "")
                else payload.get("qty"),
                0.0,
            )
            delta_qty = float(target_qty) - float(current_qty)
        return resize_mode, float(delta_qty), float(target_qty)

    def handle_resize(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Resize an existing position and re-arm protection using strict replace invariant.

        Supports:
        - resize_mode=delta_qty  → add/reduce by delta_qty
        - resize_mode=target_qty → set absolute position size

        P3: after resize, reads live position from exchange (not target_qty) so we handle
        partial fills or exchange rounding correctly.  If position resolves to zero
        (reduce-to-zero), transitions to FSM_EXIT_FILLED. Otherwise uses
        _replace_position_protection for strict re-arm.
        """
        self._guard_binance_action_enabled(action="resize", sid=(payload.get("sid") or "").strip(),
                                          symbol=(payload.get("symbol") or "").strip().upper())
        client, filters = self._resolve_client(payload)
        self._sync_client_clock(client)
        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        self._guard_sid_not_quarantined(sid, symbol=symbol, action='resize')
        state = self._load_order_state(sid)
        fsm_state = (state.get("fsm_state") or "")
        # Only allow resize in stable states (not mid-arm or terminals)
        if fsm_state and fsm_state not in {
            FSM_ENTRY_FILLED, FSM_ENTRY_PARTIAL, FSM_PROTECTED,
            FSM_TP_POLICY_ARMED, FSM_TRAIL_ARMED,
        }:
            raise RuntimeError(f"resize_forbidden_in_state:{fsm_state}")
        # P3: read live position before resize (source of truth for current size)
        live_pre = self._read_live_position(symbol=symbol, client=client)
        if not live_pre.get("is_open"):
            return {
                "sid": sid,
                "symbol": symbol,
                "action": "resize",
                "status": "no_position",
            }
        logical = (live_pre.get("logical_side") or "")
        current_qty = float(live_pre.get("qty") or 0.0)
        resize_mode, delta_qty, target_qty = self._normalize_resize_target(current_qty, payload)
        if math.isclose(delta_qty, 0.0, abs_tol=1e-12):
            return {
                "sid": sid, "symbol": symbol, "action": "resize", "status": "noop",
                "current_qty": current_qty, "target_qty": target_qty,
            }
        self._transition_state(
            sid, symbol=symbol, action="resize", next_state=FSM_VALIDATED,
            details={"resize_mode": resize_mode, "current_qty": current_qty, "target_qty": target_qty},
        )
        resize_side = "BUY" if logical == "LONG" else "SELL"
        pos_side = _position_side_for_mode(self.position_mode, logical)
        if delta_qty > 0:
            # Increasing position — submit a market add order
            q_resize, _ = self._quantize(symbol, delta_qty, None, filters=filters)
            params: dict[str, Any] = {
                "symbol": symbol,
                "side": resize_side,
                "type": "MARKET",
                "quantity": q_resize,
                "newClientOrderId": _make_cid(sid, "resize", getattr(self, "r", None)),
            }
            if pos_side:
                params["positionSide"] = pos_side
            j_resize = self._submit_plain_order_with_reconcile(
                sid=sid, symbol=symbol, action="resize_add", params=params, client=client
            )
        else:
            # Decreasing position — submit a reduce-only market exit
            reduce_qty = abs(delta_qty)
            j_resize = self._submit_reduce_only_market_exit(
                sid=sid, symbol=symbol, logical_side=logical, qty=reduce_qty,
                reason_tag="resize", client=client, filters=filters,
            )
        # P3: read live position AFTER the resize to handle partial fills and exchange rounding
        live_post = self._read_live_position(symbol=symbol, client=client)
        final_qty = float(live_post.get("qty") or 0.0)
        tol = self._position_qty_tolerance(symbol, filters=filters)
        dust_result: dict[str, Any] = {}
        if not live_post.get("is_open"):
            # Position resolved to zero (reduce-to-zero / full close)
            self._transition_state(
                sid, symbol=symbol, action="resize",
                next_state=FSM_EXIT_FILLED,
                details={"resize_mode": resize_mode, "target_qty": 0.0},
            )
            return {
                "sid": sid, "symbol": symbol, "action": "resize",
                "status": "flat",
                "current_qty": current_qty,
                "target_qty": 0.0,
                "live_qty": 0.0,
                "resize_order": j_resize,
            }

        # Dust tail cleanup: if target is zero but tiny residual remains, exact-flatten it
        if target_qty <= tol < final_qty:
            dust_result = self._force_flatten_symbol_exact(
                sid=sid,
                symbol=symbol,
                client=client,
                filters=filters,
                logical_side=logical,
                reason_tag="resize_flat",
            )
            live_post2 = self._read_live_position(symbol=symbol, client=client)
            final_qty = float(live_post2.get("qty") or 0.0)
            if not live_post2.get("is_open"):
                self._transition_state(
                    sid, symbol=symbol, action="resize",
                    next_state=FSM_EXIT_FILLED,
                    details={"resize_mode": resize_mode, "target_qty": 0.0},
                )
                return {
                    "sid": sid, "symbol": symbol, "action": "resize",
                    "status": "flat",
                    "current_qty": current_qty,
                    "target_qty": 0.0,
                    "live_qty": 0.0,
                    "resize_order": j_resize,
                    "dust_cleanup": dust_result,
                }
        # Position still open — use strict replace invariant for protection re-arm
        policy = self._resolve_execution_policy({**state, **payload}, symbol)
        sl = self._expected_requested_sl(payload, state)
        tps = self._expected_requested_tps(payload, state)
        trail_requested = self._trail_requested(payload, state)
        mark_price: float | None = None
        try:
            mp = float(client.get_mark_price(symbol) or 0.0)
            mark_price = mp if mp > 0 else None
        except Exception:
            pass
        replaced = self._replace_position_protection(
            sid=sid,
            symbol=symbol,
            action="resize",
            logical_side=logical,
            live_qty=final_qty,
            sl=sl,
            tps=tps,
            payload={**state, **payload, "trail_after_tp1_requested": trail_requested},
            policy=policy,
            client=client,
            filters=filters,
            ref_price=mark_price,
        )
        self._save_order_state(sid, {
            "action": "resize",
            "status": (replaced.get("status") or "ok"),
            "symbol": symbol,
            "side": logical,
            "qty": final_qty,
            "resize_mode": resize_mode,
            "resize_delta_qty": delta_qty,
            "resize_target_qty": target_qty,
            "live_qty_post_resize": final_qty,
            "sl_requested": sl,
            "tp_levels_requested": list(tps),
            # Scale-in metadata (set by execution_router when open→resize)
            "tp_qtys_requested_json": payload.get("tp_qtys_requested_json") or "",
            "trail_activate_tp_level_requested": payload.get("trail_activate_tp_level_requested") or "",
            "scale_in_seq": payload.get("scale_in_seq") or "",
            "source_signal_id": payload.get("source_signal_id") or "",
            "owner_sid": payload.get("owner_sid") or "",
            **replaced,
        })
        return {
            "sid": sid, "symbol": symbol, "action": "resize",
            "status": (replaced.get("status") or "ok"),
            "side": logical, "current_qty": current_qty, "target_qty": target_qty,
            "live_qty": final_qty,
            "delta_qty": delta_qty, "resize_order": j_resize,
            **replaced,
        }

    # --- Main processing loop ---

    def process_one(self, raw: str) -> None:
        """Dispatch one queue message. Handles all error cases with DLQ/retry."""
        try:
            payload = json.loads(raw)
        except Exception:
            self._dlq(raw, "bad_json")
            self._ack_processing(raw)
            return

        action = (payload.get("action") or "").strip().lower()
        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()

        if not action or not sid or not symbol:
            self._dlq(raw, "missing_required_fields")
            self._exec_event({
                "sid": sid or "", "symbol": symbol or "", "action": action or "",
                "severity": "error", "msg": "missing_required_fields",
            })
            self._ack_processing(raw)
            return

        # Stamp causal timestamps early so all downstream paths have them
        payload.setdefault("ts_exec_start_ms", _ms_now())
        payload.setdefault(
            "ts_queue_ms",
            int(payload.get("ts_queue_ms") or payload.get("ts_event_ms") or payload["ts_exec_start_ms"])
        )

        try:
            intent = ExecutionIntent.from_payload(payload)
            if EXECUTION_INTENT_AGE_MS is not None:
                EXECUTION_INTENT_AGE_MS.labels(symbol=intent.symbol).set(payload["ts_exec_start_ms"] - intent.ts_decision_ms)
            validate_execution_intent(intent, payload["ts_exec_start_ms"])
        except ValueError as e:
            if str(e) == "INTENT_EXPIRED":
                if EXECUTION_INTENT_REJECTED_TOTAL is not None:
                    EXECUTION_INTENT_REJECTED_TOTAL.labels(symbol=symbol, reason="ttd_expired").inc()
                self._transition_state(sid, symbol=symbol, action=action, next_state=FSM_FAILED,
                                       details={"failure_reason": "ttd_expired"})
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": action,
                    "event_type": "INTENT_EXPIRED", "fsm_state": "FAILED",
                    "severity": "error", "msg": "Execution intent expired (TTD budget exceeded)"
                })
                self._dlq(raw, "ttd_expired")
                self._ack_processing(raw)
                return
        except Exception:
            pass

        try:
            if action == "open":
                out = self.handle_open(payload)
            elif action == "modify":
                out = self.handle_modify(payload)
            elif action == "cancel":
                out = self.handle_cancel(payload)
            elif action == "resize":
                out = self.handle_resize(payload)
            else:
                raise ValueError(f"unknown action: {action}")

            self._exec_event(out)
            self._ack_processing(raw)

        except OpenBlockedByActiveSymbolError as e:
            self._exec_event(e.details)
            self._ack_processing(raw)
            return
        except Exception as e:
            cls = _classify_error(e)
            msg = str(e)

            # ── TradFi-Perps agreement guard (-4411) ─────────────────────────
            # Binance requires a manual account-level agreement for instruments
            # like XAUUSDT (Gold Perp). Retrying will always fail until the
            # operator signs the agreement at Binance Futures UI.
            # We: (1) log clearly, (2) block the symbol in-memory for this
            #     session, (3) send one Telegram alert, (4) DLQ immediately.
            if is_tradfi_perps_error(e):
                import logging as _logging
                _logging.getLogger(__name__).error(
                    "TradFi-Perps agreement not signed for symbol=%s. "
                    "Manual action required: sign TradFi-Perps contract at "
                    "Binance Futures UI (fapi). "
                    "Symbol suspended for this session. sid=%s action=%s",
                    symbol, sid, action,
                )
                self._tradfi_blocked.add(symbol)
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": action,
                    "severity": "error",
                    "msg": f"TradFi-Perps agreement required for {symbol} (Binance -4411). "
                           "Sign the TradFi-Perps contract at Binance Futures UI.",
                    "error_class": "fatal",
                    "event_type": "TRADFI_PERPS_NOT_SIGNED",
                })
                # User requested to disable Telegram notifications for TradFi-Perps
                # (Silenced to reduce noise)
                pass
                self._transition_state(sid, symbol=symbol, action=action, next_state=FSM_FAILED,
                                       details={"failure_reason": f"tradfi_perps_not_signed:{symbol}"})
                self._dlq(raw, f"fatal:tradfi_perps_not_signed:{symbol}")
                self._ack_processing(raw)
                return
            # ─────────────────────────────────────────────────────────────────

            self._exec_event({
                "sid": sid, "symbol": symbol, "action": action,
                "severity": "error", "msg": msg, "error_class": cls,
            })
            # Attempt reconcile for ambiguous 503/timeout outcomes before escalating
            try:
                client_for_reconcile, _ = self._resolve_client(payload)
            except Exception:
                client_for_reconcile = None
            if isinstance(e, BinanceAPIError) and client_for_reconcile is not None and \
                    client_for_reconcile.is_ambiguous_execution_error(e):
                resolved = self._attempt_reconcile_after_exception(
                    payload=payload, action=action, symbol=symbol, client=client_for_reconcile
                )
                if resolved:
                    self._exec_event(resolved)
                    self._ack_processing(raw)
                    return
            if self.tg is not None:
                send_tg = True
                # Allowlist mismatches are expected noise — suppress Telegram entirely
                if "symbol not in allowlist" in msg:
                    send_tg = False
                # FSM state conflicts & stale position refs are expected operational noise
                if "forbidden_in_state:" in msg or "no_open_position" in msg:
                    send_tg = False

                # TradFi-Perps agreement missing is expected noise for unsigned symbols
                if "TradFi-Perps agreement not signed" in msg:
                    send_tg = False

                # Notional too small (-4164) is expected noise for small signals/accounts
                if "-4164" in msg or "notional must be no smaller than" in msg:
                    send_tg = False

                if send_tg:
                    self.tg.send_text(
                        f"\u274c BINANCE EXEC error\n"
                        f"action={action} symbol={symbol}\n"
                        f"sid={sid[:24]}...\n"
                        f"{msg[:500]}"
                    )

            # Special case: -1021 timestamp drift → sync and treat as transient
            if isinstance(e, BinanceAPIError):
                payload_e = e.payload if isinstance(e.payload, dict) else {}
                if payload_e.get("code") == -1021:
                    try:
                        # Sync whichever clients are configured
                        for _c in (self.demo_client, self.client):
                            if _c is not None:
                                with contextlib.suppress(Exception):
                                    _c.sync_time()
                        cls = "transient"
                    except Exception:
                        pass


            if cls == "transient":
                retry_n = int(payload.get("retry_n") or 0)
                if retry_n < self.max_retry:
                    self._requeue(payload, raw, msg)
                    self._ack_processing(raw)
                    time.sleep(0.25)
                    return

            # Fatal OR max retries exceeded
            self._transition_state(sid, symbol=symbol, action=action, next_state=FSM_FAILED, details={"failure_reason": msg})
            self._dlq(raw, f"{cls}:{msg}")
            self._ack_processing(raw)

    def run_once(self, *, timeout: int = 5) -> bool:
        """Process at most one queue item.

        Returns True when a message was consumed, False on idle timeout. This is
        intentionally deterministic so integration/replay harnesses can drive
        the executor without an infinite background loop.

        Note: BRPOPLPUSH was removed in Redis 7.0; we use BLMOVE RIGHT LEFT instead
        which provides identical at-least-once delivery semantics.
        """  # noqa: E501
        # redis-py ≥ 5.x: blmove(first_list, second_list, timeout, src, dest)
        raw = self.r.blmove(self.queue, self.queue_processing, timeout, "RIGHT", "LEFT")
        if not raw:
            return False
        self.process_one(raw)
        return True

    def run_forever(self) -> None:
        """Blocking main loop. BLMOVE ensures at-least-once delivery.

        Note: BRPOPLPUSH was removed in Redis 7.0; we use BLMOVE RIGHT LEFT instead
        which provides identical at-least-once delivery semantics.
        """
        print("🚀 BinanceExecutor starting")
        print(f"   queue={self.queue}")
        print(f"   processing={self.queue_processing}")
        print(f"   dlq={self.queue_dlq}")
        print(f"   exec_stream={self.exec_stream}")
        print(f"   position_mode={self.position_mode}")
        print(f"   trail_cb=[{self.trail_cb_min}%..{self.trail_cb_max}%] default={self.trail_cb_default}%")
        if self.allowlist:
            print(f"   allowlist={sorted(self.allowlist)}")

        self._reconcile_open_positions_on_startup()
        while True:
            try:
                # redis-py ≥ 5.x: blmove(first_list, second_list, timeout, src, dest)
                raw = self.r.blmove(self.queue, self.queue_processing, 5, "RIGHT", "LEFT")
                if not raw:
                    continue
                self.process_one(raw)
            except getattr(redis.exceptions, "BusyLoadingError", type("DummyError", (Exception,), {})):
                print("⏳ Redis is loading dataset in memory, waiting 5s...")
                time.sleep(5.0)
            except Exception as e:
                exc_str = str(e).lower()
                if type(e).__name__ == "BusyLoadingError" or "loading the dataset in memory" in exc_str:
                    print("⏳ Redis is loading dataset in memory, waiting 5s...")
                    time.sleep(5.0)
                else:
                    # Global fail-open protection: never let a loop error stop the executor
                    print(f"❌ executor loop error: {e}")
                    time.sleep(1.0)


def main() -> None:
    # P1.2.3: Bootstrap gate — blocks executor startup until projection cluster
    # and user-stream contour are both healthy. Off by default (safe rollback:
    # set EXEC_BOOTSTRAP_REQUIRE_READY=0 to bypass without code change).
    if _bool_env("EXEC_BOOTSTRAP_REQUIRE_READY", False):
        try:
            from services.execution_bootstrap_supervisor import wait_until_env_ready
        except Exception:
            from execution_bootstrap_supervisor import wait_until_env_ready  # type: ignore
        snap = wait_until_env_ready(
            timeout_ms=int(os.getenv("EXEC_BOOTSTRAP_TIMEOUT_MS", "0")),
            poll_ms=int(os.getenv("EXEC_BOOTSTRAP_POLL_MS", "500")),
        )
        if not bool(getattr(snap, 'ready', False)):
            raise RuntimeError(
                f"execution bootstrap dependencies not ready: {getattr(snap, 'reason', 'unknown')}"
            )
    if start_http_server is not None:
        port = int(os.getenv("BINANCE_EXECUTOR_METRICS_PORT", "9876"))
        print(f"📊 metrics server starting at port={port}")
        start_http_server(port)

    BinanceExecutor().run_forever()


if __name__ == "__main__":
    main()
