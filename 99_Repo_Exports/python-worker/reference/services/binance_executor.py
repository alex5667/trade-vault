#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis

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
  BINANCE_DEFAULT_LEVERAGE=20
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

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from services.binance_futures_client import BinanceAPIError, BinanceFuturesClient
from services.execution_intent_validator import validate_exit_intent

try:
    from services.execution_journal import ExecutionJournalSink
    from services.execution_policy import (
        MAKER_FIRST,
        SAFETY_FIRST,
        ExecutionPolicyDecision,
        resolve_execution_policy,
    )
    from services.execution_state_replay import (
        persist_state_snapshot,
        rebuild_state_with_fallback,
    )
    from services.rollout_flags import RolloutFlags
except Exception:  # pragma: no cover - standalone bundle / local tests
    from binance_futures_client import BinanceAPIError, BinanceFuturesClient
    from execution_intent_validator import validate_exit_intent
    try:
        from execution_policy import (
            MAKER_FIRST,
            SAFETY_FIRST,
            ExecutionPolicyDecision,
            resolve_execution_policy,
        )
    except Exception:
        pass
    from execution_journal import ExecutionJournalSink
    from execution_state_replay import (
        persist_state_snapshot,
        rebuild_state_with_fallback,
    )
    from rollout_flags import RolloutFlags

from services.telegram.telegram_client import TelegramClient

try:
    from prometheus_client import REGISTRY, Counter
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    REGISTRY = None  # type: ignore


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
FSM_PROTECTED = "PROTECTED"
FSM_TP_POLICY_ARMED = "TP_POLICY_ARMED"
FSM_TRAIL_ARMED = "TRAIL_ARMED"
FSM_EXIT_FILLED = "EXIT_FILLED"
FSM_EMERGENCY_FLATTENED = "EMERGENCY_FLATTENED"
FSM_FAILED = "FAILED"
TERMINAL_FSM_STATES = {FSM_EXIT_FILLED, FSM_EMERGENCY_FLATTENED, FSM_FAILED}
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


def _make_cid(sid: str, tag: str) -> str:
    """Build a deterministic clientOrderId ≤36 chars: <base>-<sha1[:8]>-<tag>."""
    token = _sha1_8(sid)
    base = sid.replace(" ", "").replace(":", "-")
    base = base[: max(6, 36 - (len(tag) + len(token) + 2))]
    cid = f"{base}-{token}-{tag}"
    return cid[:36]


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

def _normalize_side(payload: dict[str, Any]) -> tuple[str, str]:
    """Return (binance_side, logical_side).

    Accepts: BUY/SELL (Binance native) or LONG/SHORT (strategy convention).
    """
    side = str(payload.get("side") or payload.get("direction") or "").upper().strip()
    if side in {"BUY", "SELL"}:
        return side, "LONG" if side == "BUY" else "SHORT"
    if side in {"LONG", "SHORT"}:
        return ("BUY" if side == "LONG" else "SELL"), side
    raise ValueError(f"bad side: {payload.get('side')!r}")


def _normalize_qty(payload: dict[str, Any], assume_lot_is_qty: bool) -> float:
    """Extract trade quantity from payload.

    Checks: qty → quantity → lot (only if assume_lot_is_qty=True).
    MT5 payloads use 'lot'; Binance executor targets native qty.
    """
    if payload.get("qty") is not None:
        return _f(payload.get("qty"))
    if payload.get("quantity") is not None:
        return _f(payload.get("quantity"))
    if assume_lot_is_qty and payload.get("lot") is not None:
        return _f(payload.get("lot"))
    raise ValueError("missing qty (provide qty/quantity; set BINANCE_ASSUME_LOT_IS_QTY=1 for 'lot')")


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
    All other BinanceAPIError codes, connection issues that look like
    nothing we can retry → fatal.
    """
    if isinstance(e, BinanceAPIError):
        payload = e.payload if isinstance(e.payload, dict) else {}
        code = payload.get("code")
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
    """

    def __init__(self) -> None:
        if redis is None:
            raise RuntimeError("redis-py is required (pip install redis)")
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

        # Default queue: orders:queue:binance (separate from MT5 orders:queue:mt5)
        self.queue = os.getenv("ORDERS_QUEUE_BINANCE") or os.getenv("ORDERS_QUEUE") or "orders:queue:binance"
        self.queue_processing = os.getenv("ORDERS_QUEUE_BINANCE_PROCESSING") or f"{self.queue}:processing"
        self.queue_dlq = os.getenv("ORDERS_QUEUE_BINANCE_DLQ") or f"{self.queue}:dlq"
        self.exec_stream = os.getenv("EXEC_STREAM", "orders:exec")

        # Optional symbol allowlist guard (prevents accidental symbol typos hitting Binance)
        allow = (os.getenv("BINANCE_SYMBOL_ALLOWLIST") or "").strip()
        self.allowlist = {s.strip().upper() for s in allow.split(",") if s.strip()} if allow else set()

        # Position mode: oneway (default) or hedge
        self.position_mode = (os.getenv("BINANCE_POSITION_MODE") or "oneway").strip().lower()
        if self.position_mode not in {"oneway", "hedge"}:
            self.position_mode = "oneway"

        self.assume_lot_is_qty = _bool_env("BINANCE_ASSUME_LOT_IS_QTY", False)
        self.max_retry = int(os.getenv("BINANCE_MAX_RETRY", "3"))
        self.fill_timeout_s = float(os.getenv("BINANCE_FILL_TIMEOUT_S", "8.0"))
        self.fill_poll_s = float(os.getenv("BINANCE_FILL_POLL_S", "0.25"))

        # Auto-init margin type and leverage on first open per symbol
        self.init_symbol_settings = _bool_env("BINANCE_INIT_SYMBOL_SETTINGS", False)
        self.margin_type = (os.getenv("BINANCE_MARGIN_TYPE") or "ISOLATED").strip().upper()
        self.default_leverage = int(os.getenv("BINANCE_DEFAULT_LEVERAGE", "100"))

        # Telegram: optional, used only for execution errors and trailing notifications
        self.tg = TelegramClient.from_env()

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

        # Demo client — built from BINANCE_DEMO_ prefix (testnet)
        _demo_key = (os.getenv("BINANCE_DEMO_API_KEY") or "").strip()
        if _demo_key:
            self.demo_client: BinanceFuturesClient | None = BinanceFuturesClient.from_env(prefix="BINANCE_DEMO_")
            self.demo_filters = FiltersCache(self.demo_client)
            print(f"   demo_client: base_url={self.demo_client.base_url}")
        else:
            self.demo_client = None
            self.demo_filters = None
            print("   demo_client: not configured (BINANCE_DEMO_API_KEY not set)")

        # Production client — built from BINANCE_ prefix
        _prod_key = (os.getenv("BINANCE_API_KEY") or "").strip()
        if _prod_key:
            self.client: BinanceFuturesClient | None = BinanceFuturesClient.from_env(prefix="BINANCE_")
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
        self.protection_slippage_buffer_bps = float(os.getenv("PROTECTION_SLIPPAGE_BUFFER_BPS", "15.0"))
        self.account_available_floor_usd = float(os.getenv("ACCOUNT_AVAILABLE_FLOOR_USD", "25.0"))

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
        self.tp_limit_watchdog_timeout_ms = int(os.getenv("TP_LIMIT_WATCHDOG_TIMEOUT_MS", "4000"))
        self.tp_trigger_monitor_timeout_s = float(os.getenv("TP_TRIGGER_MONITOR_TIMEOUT_S", "7200"))
        self.tp_limit_price_offset_bps = float(os.getenv("TP_LIMIT_PRICE_OFFSET_BPS", "0.0"))
        self.safety_entry_time_in_force = (os.getenv("SAFETY_ENTRY_TIME_IN_FORCE") or "IOC").strip().upper()

        # Trailing activation guard
        self.trail_activate_price_bps = float(os.getenv("TRAIL_ACTIVATE_PRICE_BPS", "5.0"))

        # Anti-blowup invariant: after entry fill, protection must be confirmed
        # within this window, otherwise the executor will emergency-flatten.
        self.protection_arm_timeout_ms = int(os.getenv("PROTECTION_ARM_TIMEOUT_MS", "2500"))

        # Local protection headroom reserve. Binance does not perform a margin
        # check before algo trigger; we keep a small local reserve to avoid
        # standing a protection order that becomes non-viable at trigger time.
        self.protection_fee_buffer_bps = float(os.getenv("PROTECTION_FEE_BUFFER_BPS", "8.0"))
        self.protection_slippage_buffer_bps = float(os.getenv("PROTECTION_SLIPPAGE_BUFFER_BPS", "15.0"))
        self.account_available_floor_usd = float(os.getenv("ACCOUNT_AVAILABLE_FLOOR_USD", "25.0"))

        self.r = redis.from_url(self.redis_url, decode_responses=True)

        # --- orders:state:{sid} — fast lookup of Binance IDs by signal ID ---
        self.state_key_prefix = (os.getenv("ORDERS_STATE_KEY_PREFIX") or "orders:state:").rstrip(":") + ":"
        self.state_ttl = int(os.getenv("ORDERS_STATE_TTL_SEC", "86400"))  # default 24h
        # P3.3: replay/rehydrate knobs. When orders:state:{sid} is absent the
        # executor replays orders:exec to rebuild the snapshot (EXEC_REHYDRATE_ON_STATE_MISS)
        # rather than treating a miss as a fresh signal.
        self.exec_replay_scan_count = int(os.getenv("EXEC_REPLAY_SCAN_COUNT", "20000"))
        self.exec_rehydrate_on_state_miss = _bool_env("EXEC_REHYDRATE_ON_STATE_MISS", True)

        # Counter to limit trailing arm notifications per symbol/side
        self._trail_arm_counts = {}
        self._trail_arm_lock = threading.Lock()

        # Reconcile + user-stream integration. The worker stores the latest
        # normalized ORDER_TRADE_UPDATE / ALGO_UPDATE payloads in Redis so the
        # executor can verify ambiguous submissions before attempting a retry.
        self.reconcile_enable = bool(self.rollout_flags.exec_reconcile_enable and _bool_env("EXEC_RECONCILE_ENABLE", True))
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

    def _is_sid_quarantined(self, sid: str) -> bool:
        if not self.exec_quarantine_resume_guard_enable or not sid or not self.orders_quarantine_sids_key:
            return False
        try:
            return bool(self.r.sismember(self.orders_quarantine_sids_key, sid))
        except Exception:
            return False

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
        """Write an execution fact to orders:exec stream (fail-open)."""
        fields = dict(fields)
        fields.setdefault("ts_ms", str(_ms_now()))
        fields.setdefault("mono_ms", str(_mono_ms()))
        fields.setdefault("venue", "binance")
        try:
            self.r.xadd(self.exec_stream, {k: str(v) for k, v in fields.items() if v is not None})
        except Exception:
            pass
        try:
            # Mirror to SQL journal (fail-open; Redis stream is primary)
            sink = getattr(self, "execution_journal", None)
            if sink is not None:
                sink.record_event(fields)
        except Exception:
            pass

    def _dlq(self, raw: str, reason: str) -> None:
        """Push unprocessable message to DLQ list (fail-open)."""
        try:
            self.r.lpush(
                self.queue_dlq,
                json.dumps({"reason": reason, "raw": raw, "ts_ms": _ms_now()}),
            )
        except Exception:
            pass

    def _ack_processing(self, raw: str) -> None:
        """Remove message from the processing list (BRPOPLPUSH safety net)."""
        try:
            self.r.lrem(self.queue_processing, 1, raw)
        except Exception:
            pass

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

    def _save_order_state(self, sid: str, state: dict[str, Any]) -> None:
        """Write orders:state:{sid} Redis key for fast SID→Binance ID lookup.

        P5 note: the Redis state key is a *materialized view*, not the primary
        event log.  We therefore merge new fields into the existing snapshot so
        durable chain fields (signal_id, execution_plan_id, entry/exit refs,
        closed_trade_id) survive partial updates such as TP watchdog or trail
        arming.

        Schema (fields written on open):
          ts_ms            — unix ms timestamp
          venue            — always 'binance'
          action           — open | cancel | modify
          status           — filled | closed | ...
          symbol, side, qty, exec_price
          binance_order_id — Binance entry order ID
          sl_algo_id      — SL STOP_MARKET algo ID
          tp1_algo_id, tp2_algo_id, ...  — TP algo IDs
          trail_algo_id   — TRAILING_STOP_MARKET algo ID (written when arming fires)
          signal_id, execution_plan_id   — P5 chain fields (never overwritten once set)
          entry_order_ref, exit_order_ref, closed_trade_id — P5 order chain refs

        Consumers: r.get(f"orders:state:{sid}") → json.loads()
        TTL: ORDERS_STATE_TTL_SEC (default 86400 = 24h).
        Fail-open: all Redis errors silently swallowed.
        """
        try:
            # P5: read-merge-write to preserve chain fields across partial updates
            existing: dict[str, Any] = {}
            try:
                raw_prev = self.r.get(f"{self.state_key_prefix}{sid}")
                if raw_prev:
                    parsed_prev = json.loads(raw_prev)
                    if isinstance(parsed_prev, dict):
                        existing = parsed_prev
            except Exception:
                existing = {}
            merged = dict(existing)
            merged.update(state or {})
            # Preserve original created_at_ms (set once on open)
            if 'created_at_ms' not in merged:
                merged['created_at_ms'] = int(existing.get('created_at_ms') or _ms_now())
            merged['updated_at_ms'] = _ms_now()
            doc = {"ts_ms": _ms_now(), "venue": "binance", **merged}
            self.r.set(
                f"{self.state_key_prefix}{sid}",
                json.dumps(doc, ensure_ascii=False, default=str),
                ex=self.state_ttl if self.state_ttl > 0 else None,
            )
            try:
                # Mirror state snapshot and protection refs to SQL journal (fail-open)
                sink = getattr(self, "execution_journal", None)
                if sink is not None:
                    sink.upsert_order_snapshot(doc)
                    sink.upsert_protection_refs(doc)
            except Exception:
                pass
        except Exception:
            pass  # fail-open: state is best-effort; exec stream is authoritative

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
        """Load persisted orders:state:{sid} document.

        The executor treats this key as a fast materialized view. It is used to
        recover a previously-submitted order path after a worker restart and to
        enforce idempotent state transitions for the same signal id. If the hot
        state key is missing, the worker replays ``orders:exec`` to rehydrate the
        snapshot before continuing.
        """
        try:
            raw = self.r.get(f"{self.state_key_prefix}{sid}")
            if raw:
                doc = json.loads(raw)
                if isinstance(doc, dict):
                    return doc
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
        """Persist one idempotent FSM transition and mirror it into orders:exec."""
        prev = self._load_order_state(sid)
        prev_state = (prev.get("fsm_state") or "")
        if prev_state == next_state:
            return prev
        doc = dict(prev)
        doc.update(details or {})
        doc["sid"] = sid
        doc["symbol"] = symbol
        doc["action"] = action
        doc["fsm_prev_state"] = prev_state
        doc["fsm_state"] = next_state
        doc["fsm_ts_ms"] = _ms_now()
        doc["fsm_mono_ms"] = _mono_ms()
        self._save_order_state(sid, doc)
        if EXECUTION_STATE_TRANSITION_TOTAL:
            EXECUTION_STATE_TRANSITION_TOTAL.labels(action=action, symbol=symbol, next_state=next_state).inc()
        self._exec_event({
            "sid": sid,
            "symbol": symbol,
            "action": action,
            "event_type": "state_transition",
            "prev_state": prev_state,
            "fsm_state": next_state,
            **(details or {}),
        })
        return doc

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
            if not self.reconcile_enable or not client.is_ambiguous_execution_error(exc):
                raise
            self._mark_pending_reconcile(sid, symbol=symbol, action=action, reason=str(exc))
            client_id = (params.get("newClientOrderId") or "").strip() or None
            event_doc = self._lookup_user_stream_event(plain_client_id=client_id)
            if event_doc:
                return dict(event_doc.get("order") or event_doc)
            if client_id:
                return client.query_plain_order(symbol, client_order_id=client_id)
            raise

    def _submit_algo_order_with_reconcile(self, *, sid: str, symbol: str, action: str, params: dict[str, Any], client: BinanceFuturesClient) -> dict[str, Any]:
        try:
            return client.post_algo_order(params)
        except Exception as exc:
            if not self.reconcile_enable or not client.is_ambiguous_execution_error(exc):
                raise
            self._mark_pending_reconcile(sid, symbol=symbol, action=action, reason=str(exc))
            client_algo_id = str(params.get("clientAlgoId") or params.get("newClientOrderId") or "").strip() or None
            event_doc = self._lookup_user_stream_event(algo_client_id=client_algo_id)
            if event_doc:
                return dict(event_doc.get("algo") or event_doc)
            if client_algo_id:
                return client.query_algo_order(symbol, client_algo_id=client_algo_id)
            raise

    def _resume_open_from_state(
        self, sid: str, *, symbol: str, client: BinanceFuturesClient
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
        try:
            client.post_leverage(symbol, self.default_leverage)
        except BinanceAPIError as e:
            # -4028: "Leverage 100 is not valid, maximum is N for SYMBOL"
            # payload may have {'maxLeverage': N} or the message contains it.
            max_lev = self._parse_max_leverage(e)
            if max_lev and max_lev > 0:
                capped = min(self.default_leverage, max_lev)
                try:
                    client.post_leverage(symbol, capped)
                    import logging as _logging
                    _logging.getLogger(__name__).info(
                        "leverage fallback: %s requested=%d max_allowed=%d → set=%d",
                        symbol, self.default_leverage, max_lev, capped,
                    )
                except Exception:
                    pass
        except Exception:
            pass

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
            reserve = notional * (self.protection_fee_buffer_bps + self.protection_slippage_buffer_bps) / 10000.0
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
        })
        self._save_order_state(sid, {
            "action": "protection_invariant",
            "status": "failed",
            "symbol": symbol,
            "incident_flag": "protection_missing",
            "incident_reason": reason,
        })

    def _emergency_flatten_position(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> dict[str, Any]:
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        q_close, _ = self._quantize(symbol, qty, None, filters=filters)
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": exit_side,
            "type": "MARKET",
            "quantity": q_close,
            "newClientOrderId": _make_cid(sid, "emerg"),
        }
        if self.position_mode == "oneway":
            params["reduceOnly"] = True
        elif pos_side:
            params["positionSide"] = pos_side
        self._validate_exit_contract(
            position_side=pos_side,
            reduce_only=bool(params.get("reduceOnly")),
            close_position=False,
            quantity=float(q_close),
            order_type="MARKET",
            working_type=None,
            is_algo=False,
        )
        j = client.post_plain_order(params)
        return {
            "emergency_order_id": j.get("orderId"),
            "emergency_client_id": params["newClientOrderId"],
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
        nudge_thresh = self._PROTECTIVE_NUDGE_THRESHOLD
        nudge_off    = self._PROTECTIVE_NUDGE_OFFSET

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
            return max(float(filters.get(symbol).step_size or 0.0), 1e-12)
        except Exception:
            return 1e-12

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
            "newClientOrderId": _make_cid(sid, reason_tag),
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

    def _emergency_flatten_position(self, *, sid: str, symbol: str, logical_side: str, qty: float, client: BinanceFuturesClient, filters: FiltersCache) -> dict:
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
    ) -> dict[str, Any]:
        out: dict[str, Any] = {
            "execution_policy": policy.name,
            "execution_policy_reason": policy.reason,
            "tp_watchdog_enabled": bool(policy.tp_watchdog_enabled),
        }
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        reduce_only_allowed = self.position_mode == "oneway"

        check_ref = None
        if sl and sl > 0:
            check_ref = sl
        elif tps:
            check_ref = float(tps[0])
        self._local_headroom_check(client=client, symbol=symbol, qty=qty, reference_price=check_ref)

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
                "workingType": self.sl_working_type,
                "clientAlgoId": _make_cid(sid, "sl"),
            }
            if reduce_only_allowed:
                p["reduceOnly"] = True
                self._validate_exit_contract(
                    position_side=pos_side, reduce_only=True, close_position=False,
                    quantity=float(q_sl), order_type="STOP_MARKET",
                    working_type=self.sl_working_type, is_algo=True,
                )
                p["quantity"] = q_sl
            elif pos_side:
                p["positionSide"] = pos_side
                p["closePosition"] = True
                self._validate_exit_contract(
                    position_side=pos_side, reduce_only=False, close_position=True,
                    quantity=None, order_type="STOP_MARKET",
                    working_type=self.sl_working_type, is_algo=True,
                )
            j = self._submit_algo_order_with_reconcile(
                sid=sid, symbol=symbol, action="place_sl", params=p, client=client
            )
            out["sl_algo_id"] = j.get("algoId")
            out["sl_client_algo_id"] = p["clientAlgoId"]
            out["sl_working_type"] = p["workingType"]
            out["sl_order_type"] = "STOP_MARKET"

        if valid_tps:
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
                    "workingType": policy.tp_working_type,
                    "clientAlgoId": _make_cid(sid, f"tp{idx}"),
                }
                if policy.name == MAKER_FIRST:
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
                            working_type=policy.tp_working_type, is_algo=True,
                        )
                    elif pos_side:
                        p["positionSide"] = pos_side
                        self._validate_exit_contract(
                            position_side=pos_side, reduce_only=False, close_position=False,
                            quantity=float(q_tp2), order_type="TAKE_PROFIT",
                            working_type=policy.tp_working_type, is_algo=True,
                        )
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
                    self._emit_tp_state(
                        sid, symbol, idx, "ARMED",
                        order_type="TAKE_PROFIT", policy=policy.name,
                        qty=q_tp2, trigger_price=tp_q, limit_price=limit_px_s,
                    )
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
                            working_type=policy.tp_working_type, is_algo=True,
                        )
                    elif pos_side:
                        p["positionSide"] = pos_side
                        p["closePosition"] = True if idx == len(valid_tps) and len(valid_tps) == 1 else False
                        if p["closePosition"]:
                            self._validate_exit_contract(
                                position_side=pos_side, reduce_only=False, close_position=True,
                                quantity=None, order_type="TAKE_PROFIT_MARKET",
                                working_type=policy.tp_working_type, is_algo=True,
                            )
                        else:
                            p["quantity"] = q_tp2
                            self._validate_exit_contract(
                                position_side=pos_side, reduce_only=False, close_position=False,
                                quantity=float(q_tp2), order_type="TAKE_PROFIT_MARKET",
                                working_type=policy.tp_working_type, is_algo=True,
                            )
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
                pass

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
            if self.position_mode == "oneway":
                return abs(amt), margin, leverage
            # Hedge mode: match positionSide
            ps = (p.get("positionSide") or "").upper().strip()
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
        if self.tg is not None and self.trail_notify:
            self.tg.send_text(
                f"🧷 BINANCE trailing armed\n"
                f"symbol={symbol} side={logical_side}\n"
                f"sid={sid[:24]}...\n"
                f"cb={callback_rate_pct:.1f}%"
            )
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
                        if level == 1 and trail_after_tp1 and callback_rate_pct is not None and not trail_armed:
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
                        if level == 1 and trail_after_tp1 and callback_rate_pct is not None and not trail_armed:
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
                            if level == 1 and trail_after_tp1 and callback_rate_pct is not None and not trail_armed:
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
            atr = None
            if payload.get("atr") is not None:
                try:
                    atr = float(payload.get("atr"))
                except Exception:
                    atr = None
            callback_rate_pct = compute_trailing_callback_rate_pct(
                payload,
                entry_price=float(payload.get("entry")) if payload.get("entry") not in (None, "", 0, "0") else None,
                atr=atr,
                min_pct=float(self.trail_cb_min),
                max_pct=float(self.trail_cb_max),
                default_pct=float(self.trail_cb_default),
                atr_mult_default=float(self.trail_atr_mult_default),
            ),

        summary = {"tp_watchdog_status": "started", "tp_watchdog_timeout_ms": self.tp_limit_watchdog_timeout_ms}
        if callback_rate_pct is not None:
            summary["trail_callback_rate_pct"] = callback_rate_pct,

        for idx, _ in enumerate(tps, start=1):
            algo_id = _i(prot.get(f"tp{idx}_algo_id"), 0) or None,
            if algo_id is None and not prot.get(f"tp{idx}_client_algo_id"):
                continue
            planned_qty = _f(prot.get(f"tp{idx}_qty"), 0.0),
            target_remaining_qty = _f(prot.get(f"tp{idx}_expected_remaining_qty"), 0.0),
            trigger_price = _f(prot.get(f"tp{idx}_trigger_price"), 0.0),
            working_type = str(prot.get(f"tp{idx}_working_type") or self.tp_limit_trigger_working_type).upper(),
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
                    "trail_after_tp1": bool(trail_enabled and idx == 1),
                    "callback_rate_pct": callback_rate_pct if idx == 1 else None,
                    "client": client,
                    "filters": filters,
                },
                daemon=True,
            )
            t.start()
        return summary

    def _place_trailing_stop(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, callback_rate_pct: float,
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> dict[str, Any]:
        """Place TRAILING_STOP_MARKET through the Algo API."""
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
            "workingType": self.trail_working_type,
            "clientAlgoId": _make_cid(sid, "trail"),
        }
        if self.position_mode == "oneway":
            p["reduceOnly"] = True
            self._validate_exit_contract(
                position_side=pos_side,
                reduce_only=True,
                close_position=False,
                quantity=float(q),
                order_type="TRAILING_STOP_MARKET",
                working_type=self.trail_working_type,
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

    # --- Trailing arming background thread ---

    def _arm_trailing_after_tp1_thread(
        self, *, sid: str, symbol: str, logical_side: str,
        tp1: float, callback_rate_pct: float, sl_order_id: int | None,
        client: BinanceFuturesClient,
        filters: FiltersCache,
    ) -> None:
        """Daemon thread: poll mark price → when TP1 touched, replace SL with trailing stop.

        Rationale:
          - Keep hard SL active until TP1 is reached (protects downside before runners)
          - After TP1: cancel hard SL, arm trailing stop for remainder
          - This matches the upstream trail_after_tp1 strategy pattern
        """
        try:
            deadline = time.time() + float(self.trail_arm_timeout_s)
            poll_s = max(0.2, float(self.trail_arm_poll_s))
            touched = False

            while time.time() < deadline:
                # Use mark price (less noisy than last trade price)
                mp = float(client.get_mark_price(symbol) or 0.0)
                if mp <= 0:
                    time.sleep(poll_s)
                    continue

                if logical_side == "LONG":
                    touched = mp >= tp1
                else:
                    touched = mp <= tp1

                if touched:
                    break
                time.sleep(poll_s)

            if not touched:
                # Timed out without TP1 touch — log and exit silently
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "trail_arm",
                    "status": "timeout", "trail_tp1": tp1,
                    "trail_callback_rate_pct": callback_rate_pct,
                })
                return

            # Verify position is still open; also grab margin + leverage for the notification
            qty, margin_usdt, leverage = self._get_position_info(symbol, logical_side=logical_side, client=client)
            if qty <= 0:
                self._exec_event({
                    "sid": sid, "symbol": symbol,
                    "action": "trail_arm", "status": "no_position",
                })
                return

            # Cancel the hard SL (best-effort: might already be cancelled by partial TP)
            if sl_order_id:
                try:
                    client.delete_order(symbol, order_id=int(sl_order_id))
                except Exception:
                    pass

            trail = self._place_trailing_stop(
                sid=sid, symbol=symbol, logical_side=logical_side,
                qty=qty, callback_rate_pct=callback_rate_pct,
                client=client, filters=filters,
            )

            ev = {
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "armed", "side": logical_side, "qty": qty,
                "trail_tp1": tp1, "trail_callback_rate_pct": callback_rate_pct,
                **trail,
            }
            self._exec_event(ev)

            # Update orders:state:{sid} with trail_order_id so lookup shows full picture
            self._save_order_state(sid, {
                "action": "trail_arm",
                "status": "armed",
                "symbol": symbol,
                "side": logical_side,
                "trail_algo_id": trail.get("trail_algo_id"),
                "trail_client_id": trail.get("trail_client_id"),
                "trail_tp1": tp1,
                "trail_callback_rate_pct": callback_rate_pct,
            })

            if self.tg is not None and self.trail_notify:
                with self._trail_arm_lock:
                    key = (symbol, logical_side)
                    self._trail_arm_counts[key] = self._trail_arm_counts.get(key, 0) + 1
                    count = self._trail_arm_counts[key]
                    should_notify = (count % 50 == 0)

                if should_notify:
                    margin_str = f"{margin_usdt:.2f} USDT" if margin_usdt > 0 else "n/a"
                    notional = qty * mp if mp > 0 else 0.0
                    notional_str = f"{notional:.2f} USDT" if notional > 0 else "n/a"
                    lev_str = f"{leverage}x" if leverage > 0 else "n/a"
                    self.tg.send_text(
                        f"🧷 BINANCE trailing armed\n"
                        f"symbol={symbol} side={logical_side}\n"
                        f"sid={sid[:24]}...\n"
                        f"tp1={tp1} cb={callback_rate_pct:.1f}%\n"
                        f"qty={qty} pos={notional_str} lev={lev_str}\n"
                        f"margin={margin_str}"
                    )
        except Exception as e:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "error", "msg": str(e)[:900],
            })

    def _maybe_start_trailing_after_tp1(
        self, *, payload: dict[str, Any], sid: str, symbol: str,
        logical_side: str, entry_price: float | None, initial_qty: float,
        sl_algo_id: int | None, tp_levels: list[float], tp1_working_type: str,
        policy: ExecutionPolicyDecision,
        client: BinanceFuturesClient,
        filters: FiltersCache,
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
        if policy.name == MAKER_FIRST:
            return {
                "trail_after_tp1": True,
                "trail_callback_rate_pct": cb,
                "trail_status": "managed_by_tp_watchdog",
                "trail_pending": True
            },

        tp1 = float(tp_levels[0]),
        t = threading.Thread(
            target=self._arm_trailing_after_tp1_thread,
            kwargs={
                "sid": sid, "symbol": symbol, "logical_side": logical_side,
                "tp1": tp1, "callback_rate_pct": cb, "sl_algo_id": sl_algo_id,
                "initial_qty": initial_qty, "tp1_working_type": tp1_working_type,
                "client": client, "filters": filters,
            },
            daemon=True,
        )
        t.start()
        return {
            "trail_after_tp1": True,
            "trail_tp1": tp1,
            "trail_callback_rate_pct": cb,
            "trail_status": "arming",
            "trail_pending": True,
        }

    # --- Action handlers ---

    def handle_open(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Open a new position: entry order → wait fill → SL/TP → trailing arming.

        Routing: payload[is_virtual]=true → demo/testnet client; else → prod client.
        """
        client, filters = self._resolve_client(payload)
        is_virtual = _truthy(payload.get("is_virtual")) or _truthy(payload.get("virtual"))

        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        self._guard_sid_not_quarantined(sid, symbol=symbol, action='open')
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")

        self._ensure_symbol_settings(symbol, client=client)

        side, logical = _normalize_side(payload)
        policy = self._resolve_execution_policy(payload, symbol)

        # --- Dirty Reversal & Orphan Cleanup ---
        # If opening a position, we must clean up old orphaned algo orders.
        # Furthermore, in One-Way mode, if we already have an opposing position,
        # we must cancel all its orders and explicitly close it to avoid
        # leaving orphaned reduceOnly orders behind.
        try:
            # 1. Check current position for the symbol
            risks = client.get_position_risk() or []
            current_amt = 0.0
            for pos in risks:
                if (pos.get("symbol") or "").upper() != symbol:
                    continue
                # In One-Way mode, we just look at the net amount.
                # In Hedge mode, this simple reversal logic doesn't cleanly apply
                # (you can have both LONG and SHORT), so we skip aggressive reversal.
                if self.position_mode == "oneway":
                    current_amt = _f(pos.get("positionAmt"), 0.0)
                    break

            is_opposing = False
            if current_amt != 0.0:
                current_logical = "LONG" if current_amt > 0 else "SHORT"
                if current_logical != logical:
                    is_opposing = True

            # 2. If it's an opposing position (dirty reversal), cancel all orders and close it.
            if is_opposing:
                # Cancel all orders (including Algo orders)
                self._cancel_by_token(symbol, "", client=client) # passing empty token cancels all orders for symbol via Binance API (if we implemented it/if we just use API wrapper)
                # Actually, _cancel_by_token filters by token. We should just cancel all.
                client.cancel_all_orders(symbol)

                # Market close the opposing position
                exit_side = "SELL" if current_amt > 0 else "BUY"
                close_qty = abs(current_amt)
                q_close, _ = self._quantize(symbol, close_qty, None, filters=filters)
                close_params: dict[str, Any] = {
                    "symbol": symbol,
                    "side": exit_side,
                    "type": "MARKET",
                    "quantity": q_close,
                    "reduceOnly": True,
                    "newClientOrderId": _make_cid(sid, "rev_close"),
                }
                client.post_order(close_params)

            # 3. If position is 0 (or just became 0), clean up any lingering Algo orders
            # that might be stuck as orphans.
            if current_amt == 0.0 or is_opposing:
                algo_orders = client.get_open_algo_orders(symbol) or []
                pos_side_cleanup = _position_side_for_mode(self.position_mode, logical)
                for o in algo_orders:
                    try:
                        p_side = (o.get("positionSide") or "").upper()
                        if self.position_mode == "hedge" and pos_side_cleanup and p_side and p_side != pos_side_cleanup:
                            continue
                        oid = _i(o.get("algoId"), 0)
                        if oid:
                            client.cancel_algo_order(symbol, algo_id=oid)
                    except Exception:
                        pass
        except Exception:
            pass
        # ---------------------------------------

        qty = _normalize_qty(payload, self.assume_lot_is_qty)
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
            details={"execution_policy": policy.name, "logical_side": logical}
        )
        if float(q) <= 0:
            raise ValueError("qty <= 0 after quantisation")

        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": q,
            "newClientOrderId": _make_cid(sid, "entry"),
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
        if status not in {"FILLED", "PARTIALLY_FILLED"}:
            raise RuntimeError(f"entry not filled: status={status} order={j_final}")
        if filled_qty <= 0:
            raise RuntimeError(f"filled_qty=0: order={j_final}")
        entry_fsm = FSM_ENTRY_PARTIAL if status == "PARTIALLY_FILLED" else FSM_ENTRY_FILLED
        self._transition_state(
            sid, symbol=symbol, action="open", next_state=entry_fsm,
            details={"filled_qty": filled_qty, "avg_price": avg_price, "entry_status": status}
        )

        sl = _f(payload.get("sl"), 0.0) if payload.get("sl") is not None else None
        sl = sl if sl and sl > 0 else None
        tps_raw = payload.get("tp_levels") or []
        tps = [float(x) for x in tps_raw if x not in (None, "")]
        tps = [tp for tp in tps if tp > 0]

        self._transition_state(sid, symbol=symbol, action="open", next_state=FSM_PROTECTION_ARMING)
        prot = self._place_protective(
            sid=sid, symbol=symbol, logical_side=logical,
            qty=filled_qty, sl=sl, tps=tps,
            client=client, filters=filters,
            ref_price=avg_price if avg_price > 0 else None,
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
            sl_order_id=_i(prot.get("sl_algo_id"), 0) or None,
            tp_levels=tps,
            client=client, filters=filters,
        )


        # Anti-blowup invariant: a filled entry must not remain naked. We treat
        # missing protection refs as a critical incident and immediately flatten.
        trail_enabled = _truthy(payload.get("trail_after_tp1")) and bool(tps)
        if not self._protection_confirmed(prot, tps, trail_enabled):
            self._emit_protection_incident(sid, symbol, "entry_filled_without_confirmed_protection")
            emerg = self._emergency_flatten_position(
                sid=sid, symbol=symbol, logical_side=logical, qty=filled_qty,
                client=client, filters=filters,
            )
            self._transition_state(sid, symbol=symbol, action="open", next_state=FSM_EMERGENCY_FLATTENED, details=emerg)
            prot = {**prot, **emerg, "protection_invariant_failed": True}

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
        })
        return result

    def handle_modify(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Modify existing position: cancel all our orders → re-place SL/TP → trailing arming."""
        client, filters = self._resolve_client(payload)
        self._sync_client_clock(client)

        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        self._guard_sid_not_quarantined(sid, symbol=symbol, action='modify')
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")

        canceled = self._cancel_by_token(symbol, sid, client=client)

        # Determine current position
        risks = client.get_position_risk() or []
        amt = 0.0
        for pos in risks:
            if (pos.get("symbol") or "").upper() != symbol:
                continue
            amt = _f(pos.get("positionAmt"), 0.0)
            break

        if amt == 0.0:
            return {
                "sid": sid, "symbol": symbol, "action": "modify",
                "status": "no_position", "canceled_orders": canceled,
            }

        logical = "LONG" if amt > 0 else "SHORT"
        qty = abs(amt)
        policy = self._resolve_execution_policy(payload, symbol)

        sl = _f(payload.get("sl"), 0.0) if payload.get("sl") is not None else None
        sl = sl if sl and sl > 0 else None
        tps_raw = payload.get("tp_levels") or []
        tps = [float(x) for x in tps_raw if x not in (None, "")]
        tps = [tp for tp in tps if tp > 0]

        # Fetch current mark price for protective order validation and trailing calc
        mark_price: float | None = None
        try:
            mp = float(client.get_mark_price(symbol) or 0.0)
            mark_price = mp if mp > 0 else None
        except Exception:
            pass

        prot = self._place_protective(
            sid=sid, symbol=symbol, logical_side=logical,
            qty=qty, sl=sl, tps=tps, policy=policy,
            client=client, filters=filters,
            ref_price=mark_price,
        )

        # Determine entry price for trailing callbackRate calculation
        # mark_price was already fetched above; use it as the fallback.
        entry_price: float | None = mark_price
        if payload.get("entry") not in (None, 0, "", "0"):
            try:
                ep = float(payload.get("entry"))
                if ep > 0:
                    entry_price = ep
            except Exception:
                pass  # keep mark_price fallback

        trail = self._maybe_start_trailing_after_tp1(
            payload=payload, sid=sid, symbol=symbol, logical_side=logical,
            entry_price=entry_price,
            sl_order_id=_i(prot.get("sl_algo_id"), 0) or None,
            tp_levels=tps,
            client=client, filters=filters,
        )
        if EXECUTION_EMERGENCY_FLATTEN_TOTAL:
            EXECUTION_EMERGENCY_FLATTEN_TOTAL.labels(symbol=symbol, reason="emerg").inc()
        self._transition_state(sid, symbol=symbol, action="emergency_flatten", next_state=FSM_EMERGENCY_FLATTENED, details=close)
        # P5: derive audit chain from payload and include in result
        audit_chain = self._derive_audit_chain_fields(payload, sid)
        audit_policies = self._derive_entry_exit_policies(execution_policy=policy.name)
        # P5: persist audit chain into state so it survives modify→cancel handoff
        self._save_order_state(sid, {
            "action": "modify",
            "status": "ok",
            "symbol": symbol,
            "side": logical,
            "qty": qty,
            "execution_policy": policy.name,
            **audit_chain, **audit_policies,
            **prot, **trail,
        })
        return {
            "sid": sid, "symbol": symbol, "action": "modify",
            "status": "ok", "side": logical, "qty": qty,
            "canceled_orders": canceled,
            **audit_chain, **audit_policies,
            **prot, **trail,
            "json": json.dumps(
                {"canceled": canceled, "protective": prot, "trailing": trail},
                ensure_ascii=False, default=str,
            )
        }

    def handle_cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Cancel: remove our orders + market-close any open position."""
        client, filters = self._resolve_client(payload)

        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")

        # P5: derive audit chain before cancel so exit refs are correctly attributed
        audit_chain = self._derive_audit_chain_fields(payload, sid)
        canceled = self._cancel_by_token(symbol, sid, client=client)

        # Close any open position
        risks = client.get_position_risk() or []
        amt = 0.0
        for pos in risks:
            if (pos.get("symbol") or "").upper() != symbol:
                continue
            amt = _f(pos.get("positionAmt"), 0.0)
            break

        closed = False
        close_order_id: int | None = None
        logical: str | None = None
        qty = 0.0
        if amt != 0.0:
            logical = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            close = self._submit_reduce_only_market_exit(
                sid=sid, symbol=symbol, logical_side=logical, qty=qty,
                reason_tag="close", client=client, filters=filters,
            )
            close_order_id = _i(close.get("close_order_id"), 0) or None
            closed = True

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
                **audit_chain,
            })
        return result

    def handle_resize(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Resize position (TODO: compute delta-qty and market order the difference).

        Currently intentionally unimplemented — requires careful delta calculation
        and safe partial-close/pyramid logic.
        """
        sid = (payload.get("sid") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        self._guard_sid_not_quarantined(sid, symbol=symbol, action='resize')
        raise RuntimeError(f"resize not implemented yet (sid={sid} symbol={symbol})")

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

        except Exception as e:
            cls = _classify_error(e)
            msg = str(e)
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": action,
                "severity": "error", "msg": msg, "error_class": cls,
            })
            if self.tg is not None:
                self.tg.send_text(
                    f"❌ BINANCE EXEC error\n"
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
                                try:
                                    _c.sync_time()
                                except Exception:
                                    pass
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

    def run_forever(self) -> None:
        """Blocking main loop. BRPOPLPUSH ensures at-least-once delivery."""
        print("🚀 BinanceExecutor starting")
        print(f"   queue={self.queue}")
        print(f"   processing={self.queue_processing}")
        print(f"   dlq={self.queue_dlq}")
        print(f"   exec_stream={self.exec_stream}")
        print(f"   position_mode={self.position_mode}")
        print(f"   trail_cb=[{self.trail_cb_min}%..{self.trail_cb_max}%] default={self.trail_cb_default}%")
        if self.allowlist:
            print(f"   allowlist={sorted(self.allowlist)}")

        while True:
            try:
                raw = self.r.brpoplpush(self.queue, self.queue_processing, timeout=5)
                if not raw:
                    continue
                self.process_one(raw)
            except Exception as e:
                # Global fail-open protection: never let a loop error stop the executor
                print(f"❌ executor loop error: {e}")
                time.sleep(1.0)


def main() -> None:
    BinanceExecutor().run_forever()


if __name__ == "__main__":
    main()
