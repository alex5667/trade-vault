#!/usr/bin/env python3
from __future__ import annotations

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
from typing import Any, Dict, List, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from services.binance_futures_client import BinanceAPIError, BinanceFuturesClient
from services.telegram.telegram_client import TelegramClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bool_env(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "on"}


def _ms_now() -> int:
    return int(time.time() * 1000)


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
    payload: Dict[str, Any],
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
        self._cache: Dict[str, SymbolFilters] = {}

    def get(self, symbol: str) -> SymbolFilters:
        s = symbol.upper()
        if s in self._cache:
            return self._cache[s]

        info = self.client.get_exchange_info()
        sym_list = info.get("symbols") or []
        by_symbol = {str(x.get("symbol")).upper(): x for x in sym_list if x.get("symbol")}
        if s not in by_symbol:
            raise RuntimeError(f"Unknown Binance symbol: {s}")

        filters = by_symbol[s].get("filters") or []
        tick = 0.0
        step = 0.0
        min_qty = 0.0
        min_notional = 0.0
        for f in filters:
            t = str(f.get("filterType") or "")
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

def _normalize_side(payload: Dict[str, Any]) -> Tuple[str, str]:
    """Return (binance_side, logical_side).

    Accepts: BUY/SELL (Binance native) or LONG/SHORT (strategy convention).
    """
    side = str(payload.get("side") or payload.get("direction") or "").upper().strip()
    if side in {"BUY", "SELL"}:
        return side, "LONG" if side == "BUY" else "SHORT"
    if side in {"LONG", "SHORT"}:
        return ("BUY" if side == "LONG" else "SELL"), side
    raise ValueError(f"bad side: {payload.get('side')!r}")


def _normalize_qty(payload: Dict[str, Any], assume_lot_is_qty: bool) -> float:
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


def _position_side_for_mode(position_mode: str, logical_side: str) -> Optional[str]:
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
            self.demo_client: Optional[BinanceFuturesClient] = BinanceFuturesClient.from_env(prefix="BINANCE_DEMO_")
            self.demo_filters = FiltersCache(self.demo_client)
            print(f"   demo_client: base_url={self.demo_client.base_url}")
        else:
            self.demo_client = None
            self.demo_filters = None
            print("   demo_client: not configured (BINANCE_DEMO_API_KEY not set)")

        # Production client — built from BINANCE_ prefix
        _prod_key = (os.getenv("BINANCE_API_KEY") or "").strip()
        if _prod_key:
            self.client: Optional[BinanceFuturesClient] = BinanceFuturesClient.from_env(prefix="BINANCE_")
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

        self.r = redis.from_url(self.redis_url, decode_responses=True)

        # --- orders:state:{sid} — fast lookup of Binance IDs by signal ID ---
        self.state_key_prefix = (os.getenv("ORDERS_STATE_KEY_PREFIX") or "orders:state:").rstrip(":") + ":"
        self.state_ttl = int(os.getenv("ORDERS_STATE_TTL_SEC", "86400"))  # default 24h

    def _resolve_client(
        self, payload: Dict[str, Any]
    ) -> Tuple["BinanceFuturesClient", "FiltersCache"]:
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


    # --- Redis event helpers ---

    def _exec_event(self, fields: Dict[str, Any]) -> None:
        """Write an execution fact to orders:exec stream (fail-open)."""
        fields = dict(fields)
        fields.setdefault("ts_ms", str(_ms_now()))
        fields.setdefault("venue", "binance")
        try:
            self.r.xadd(self.exec_stream, {k: str(v) for k, v in fields.items() if v is not None})
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

    def _requeue(self, payload: Dict[str, Any], raw: str, reason: str) -> None:
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

    def _save_order_state(self, sid: str, state: Dict[str, Any]) -> None:
        """Write orders:state:{sid} Redis key for fast SID→Binance ID lookup.

        Schema (fields written on open):
          ts_ms            — unix ms timestamp
          venue            — always 'binance'
          action           — open | cancel | modify
          status           — filled | closed | ...
          symbol, side, qty, exec_price
          binance_order_id — Binance entry order ID
          sl_order_id      — SL STOP_MARKET order ID
          tp1_order_id, tp2_order_id, ...  — TP order IDs
          trail_order_id   — TRAILING_STOP_MARKET order ID (written when arming fires)

        Consumers: r.get(f"orders:state:{sid}") → json.loads()
        TTL: ORDERS_STATE_TTL_SEC (default 86400 = 24h).
        Fail-open: all Redis errors silently swallowed.
        """
        try:
            doc = {"ts_ms": _ms_now(), "venue": "binance", **state}
            self.r.set(
                f"{self.state_key_prefix}{sid}",
                json.dumps(doc, ensure_ascii=False, default=str),
                ex=self.state_ttl if self.state_ttl > 0 else None,
            )
        except Exception:
            pass  # fail-open: state is best-effort; exec stream is authoritative

    # --- Symbol initialisation ---

    def _ensure_symbol_settings(
        self, symbol: str, *, client: "BinanceFuturesClient"
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
    def _parse_max_leverage(exc: "BinanceAPIError") -> int:
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
        self, symbol: str, qty: float, price: Optional[float],
        *, filters: "FiltersCache",
    ) -> Tuple[str, Optional[str]]:
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
        client: "BinanceFuturesClient",
    ) -> Dict[str, Any]:
        """Poll order status until FILLED or terminal state or timeout."""
        deadline = time.time() + timeout_s
        last: Dict[str, Any] = {}
        while time.time() < deadline:
            j = client.get_order(symbol, order_id=order_id)
            last = j
            st = str(j.get("status") or "").upper()
            if st == "FILLED":
                return j
            if st in {"CANCELED", "REJECTED", "EXPIRED"}:
                return j
            time.sleep(max(0.05, self.fill_poll_s))
        return last

    # --- TP qty splitting ---

    def _split_tp_qtys(
        self, symbol: str, total_qty: float, n: int,
        *, filters: "FiltersCache",
    ) -> List[float]:
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

    # --- Protective orders (SL + TPs) ---

    # Prices crossed by ≤ this fraction of mark are nudged instead of dropped.
    # Handles stale-signal case where mark moved a few bps during placement.
    _PROTECTIVE_NUDGE_THRESHOLD: float = 0.001   # 0.1 %
    _PROTECTIVE_NUDGE_OFFSET: float    = 0.0005  # 0.05 % cushion away from mark

    def _validate_protective_prices(
        self,
        symbol: str,
        logical_side: str,
        sl: Optional[float],
        tps: List[float],
        *,
        client: "BinanceFuturesClient",
        ref_price: Optional[float] = None,
    ) -> Tuple[Optional[float], List[float]]:
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
        mark: Optional[float] = ref_price
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

        valid_sl: Optional[float] = None
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

        valid_tps: List[float] = []
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

    def _place_protective(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, sl: Optional[float], tps: List[float],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        ref_price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Place SL (STOP_MARKET) and TP (TAKE_PROFIT_MARKET) orders.

        Pre-flight validation: any price already crossed vs current mark is
        skipped (would cause Binance -2021 "Order would immediately trigger").
        Skipped prices are logged as exec_event warnings.

        One-way mode:
          - SL: reduceOnly=True
          - TP: reduceOnly=True
        Hedge mode:
          - SL: quantity + positionSide (reduceOnly forbidden by Binance)
          - TP: positionSide
        """
        out: Dict[str, Any] = {}
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        reduce_only_allowed = self.position_mode == "oneway"

        # --- Pre-flight: validate SL/TP prices against mark price ---
        valid_sl, valid_tps = self._validate_protective_prices(
            symbol, logical_side, sl, tps,
            client=client, ref_price=ref_price,
        )

        # Log any prices that were dropped so they are traceable
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
        # -----------------------------------------------------------

        if valid_sl is not None and valid_sl > 0:
            q_sl, sl_q = self._quantize(symbol, qty, valid_sl, filters=filters)
            p: Dict[str, Any] = {
                "symbol": symbol,
                "side": exit_side,
                "type": "STOP_MARKET",
                "stopPrice": sl_q,
                "quantity": q_sl,
                "newClientOrderId": _make_cid(sid, "sl"),
            }
            if reduce_only_allowed:
                p["reduceOnly"] = True
            elif pos_side:
                p["positionSide"] = pos_side
            j = client.post_order(p)
            out["sl_order_id"] = j.get("orderId")
            out["sl_client_id"] = p["newClientOrderId"]

        if valid_tps:
            parts = self._split_tp_qtys(symbol, qty, len(valid_tps), filters=filters)
            for idx, (tp, q_tp) in enumerate(zip(valid_tps, parts), start=1):
                q_tp2, tp_q = self._quantize(symbol, q_tp, tp, filters=filters)
                p = {
                    "symbol": symbol,
                    "side": exit_side,
                    "type": "TAKE_PROFIT_MARKET",
                    "stopPrice": tp_q,
                    "quantity": q_tp2,
                    "newClientOrderId": _make_cid(sid, f"tp{idx}"),
                }
                if reduce_only_allowed:
                    p["reduceOnly"] = True
                elif pos_side:
                    p["positionSide"] = pos_side
                j = client.post_order(p)
                out[f"tp{idx}_order_id"] = j.get("orderId")
                out[f"tp{idx}_client_id"] = p["newClientOrderId"]

        # ── NAKED POSITION GUARD ─────────────────────────────────────────────
        # If nothing was placed at all (all prices failed validation) the
        # position is unprotected.  Emit a loud warning so operators can act.
        if not out:
            _naked_msg = (
                f"All SL/TP prices failed mark-price validation for "
                f"{symbol} {logical_side} sid={sid[:24]} — position is NAKED. "
                f"Send a 'modify' payload with fresh prices to protect it."
            )
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "protective_skip_all",
                "status": "warning", "msg": _naked_msg,
            })
            if self.tg is not None:
                self.tg.send_text(
                    f"\u26a0\ufe0f BINANCE naked position\n"
                    f"symbol={symbol} side={logical_side}\n"
                    f"sid={sid[:24]}...\n"
                    f"All SL/TP prices failed mark-price validation.\n"
                    f"Position has NO protective orders — manual action required!"
                )
        # ─────────────────────────────────────────────────────────────────────
        return out

    # --- Order cancellation by token ---

    def _cancel_by_token(
        self, symbol: str, sid: str, *, client: "BinanceFuturesClient"
    ) -> int:
        """Cancel all open orders for symbol whose clientOrderId contains our sid token."""
        token = _sha1_8(sid)
        canceled = 0
        orders = client.get_open_orders(symbol) or []
        for o in orders:
            cid = str(o.get("clientOrderId") or "")
            if token and token not in cid:
                continue
            try:
                oid = _i(o.get("orderId"), 0)
                if oid:
                    client.delete_order(symbol, order_id=oid)
                    canceled += 1
            except Exception:
                continue
                
        # Also clean up Algo Orders specifically to avoid max stop limit errors on Testnet
        try:
            algo_orders = client._request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol}, signed=True) or []
            for o in algo_orders:
                cid = str(o.get("clientAlgoId") or "")
                if token and token not in cid:
                    continue
                try:
                    oid = _i(o.get("algoId"), 0)
                    if oid:
                        client.delete_order(symbol, order_id=oid, is_algo=True)
                        canceled += 1
                except Exception:
                    continue
        except BinanceAPIError as e:
            if e.status not in (404, 400):
                pass
                
        return canceled

    # --- Position quantity / margin query ---

    def _get_position_info(
        self, symbol: str, logical_side: Optional[str] = None,
        *, client: "BinanceFuturesClient",
    ) -> Tuple[float, float, int]:
        """Return (abs_qty, margin_usdt, leverage) for the symbol position.

        margin_usdt is ``isolatedMargin`` for ISOLATED mode and
        ``initialMargin`` for CROSSED (best proxy available without
        account-level breakdown).  Returns (0.0, 0.0, 0) if no position found.
        """
        risks = client.get_position_risk() or []
        for p in risks:
            if str(p.get("symbol") or "").upper() != symbol.upper():
                continue
            amt = _f(p.get("positionAmt"))
            # margin: isolatedMargin > 0 for ISOLATED; fall back to initialMargin
            margin = _f(p.get("isolatedMargin")) or _f(p.get("initialMargin"))
            leverage = _i(p.get("leverage"), 0)
            if self.position_mode == "oneway":
                return abs(amt), margin, leverage
            # Hedge mode: match positionSide
            ps = str(p.get("positionSide") or "").upper().strip()
            if logical_side and ps and ps in {"LONG", "SHORT"}:
                if logical_side.upper() == ps and abs(amt) > 0:
                    return abs(amt), margin, leverage
                continue
            return abs(amt), margin, leverage
        return 0.0, 0.0, 0

    def _get_position_qty(
        self, symbol: str, logical_side: Optional[str] = None,
        *, client: "BinanceFuturesClient",
    ) -> float:
        """Return absolute position quantity. Returns 0.0 if no position."""
        qty, _, _lev = self._get_position_info(symbol, logical_side, client=client)
        return qty

    # --- Trailing stop placement ---

    def _place_trailing_stop(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, callback_rate_pct: float,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> Dict[str, Any]:
        """Place TRAILING_STOP_MARKET order.

        In one-way mode: reduceOnly=True ensures it only reduces the position.
        In hedge mode: positionSide is set; reduceOnly is forbidden by Binance.
        """
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        q, _ = self._quantize(symbol, qty, None, filters=filters)
        if float(q) <= 0:
            raise ValueError("trail qty <= 0")

        p: Dict[str, Any] = {
            "symbol": symbol,
            "side": exit_side,
            "type": "TRAILING_STOP_MARKET",
            "quantity": q,
            "callbackRate": float(callback_rate_pct),
            "newClientOrderId": _make_cid(sid, "trail"),
        }
        if self.position_mode == "oneway":
            p["reduceOnly"] = True
        if pos_side:
            p["positionSide"] = pos_side

        j = client.post_order(p)
        return {
            "trail_order_id": j.get("orderId"),
            "trail_client_id": p["newClientOrderId"],
        }

    # --- Trailing arming background thread ---

    def _arm_trailing_after_tp1_thread(
        self, *, sid: str, symbol: str, logical_side: str,
        tp1: float, callback_rate_pct: float, sl_order_id: Optional[int],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
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
                    touched = mp >= float(tp1)
                else:
                    touched = mp <= float(tp1)

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
                "trail_order_id": trail.get("trail_order_id"),
                "trail_client_id": trail.get("trail_client_id"),
                "trail_tp1": tp1,
                "trail_callback_rate_pct": callback_rate_pct,
            })

            if self.tg is not None and self.trail_notify:
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
        self, *, payload: Dict[str, Any], sid: str, symbol: str,
        logical_side: str, entry_price: Optional[float],
        sl_order_id: Optional[int], tp_levels: List[float],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> Dict[str, Any]:
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
        tp1 = float(tp_levels[0])

        t = threading.Thread(
            target=self._arm_trailing_after_tp1_thread,
            kwargs={
                "sid": sid, "symbol": symbol, "logical_side": logical_side,
                "tp1": tp1, "callback_rate_pct": cb,
                "sl_order_id": int(sl_order_id) if sl_order_id else None,
                "client": client,
                "filters": filters,
            },
            daemon=True,  # won't block process exit
        )
        t.start()

        return {
            "trail_after_tp1": True,
            "trail_tp1": tp1,
            "trail_callback_rate_pct": cb,
            "trail_status": "arming",
        }

    # --- Action handlers ---

    def handle_open(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Open a new position: entry order → wait fill → SL/TP → trailing arming.

        Routing: payload[is_virtual]=true → demo/testnet client; else → prod client.
        """
        client, filters = self._resolve_client(payload)
        is_virtual = _truthy(payload.get("is_virtual")) or _truthy(payload.get("virtual"))

        sid = str(payload.get("sid") or "").strip()
        symbol = str(payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")

        self._ensure_symbol_settings(symbol, client=client)

        side, logical = _normalize_side(payload)
        
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
                if str(pos.get("symbol") or "").upper() != symbol:
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
                close_params: Dict[str, Any] = {
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
                algo_orders = client._request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol}, signed=True) or []
                pos_side_cleanup = _position_side_for_mode(self.position_mode, logical)
                for o in algo_orders:
                    try:
                        p_side = str(o.get("positionSide") or "").upper()
                        if self.position_mode == "hedge" and pos_side_cleanup and p_side and p_side != pos_side_cleanup:
                            continue
                        oid = _i(o.get("algoId"), 0)
                        if oid:
                            client.delete_order(symbol, order_id=oid, is_algo=True)
                    except Exception:
                        pass
        except Exception:
            pass
        # ---------------------------------------

        qty = _normalize_qty(payload, self.assume_lot_is_qty)
        entry = payload.get("entry")
        price: Optional[float] = None
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
        if float(q) <= 0:
            raise ValueError("qty <= 0 after quantisation")

        params: Dict[str, Any] = {
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
            params["timeInForce"] = "GTC"

        j_entry = client.post_order(params)
        order_id = _i(j_entry.get("orderId"), 0)
        if not order_id:
            raise RuntimeError(f"no orderId in response: {j_entry}")

        j_final = self._wait_fill(symbol, order_id, timeout_s=self.fill_timeout_s, client=client)
        status = str(j_final.get("status") or "").upper()
        filled_qty = _f(j_final.get("executedQty"), 0.0)
        avg_price = _f(j_final.get("avgPrice"), 0.0)
        if status not in {"FILLED", "PARTIALLY_FILLED"}:
            raise RuntimeError(f"entry not filled: status={status} order={j_final}")
        if filled_qty <= 0:
            raise RuntimeError(f"filled_qty=0: order={j_final}")

        sl = _f(payload.get("sl"), 0.0) if payload.get("sl") is not None else None
        sl = sl if sl and sl > 0 else None
        tps_raw = payload.get("tp_levels") or []
        tps = [float(x) for x in tps_raw if x not in (None, "")]
        tps = [tp for tp in tps if tp > 0]

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
            sl_order_id=_i(prot.get("sl_order_id"), 0) or None,
            tp_levels=tps,
            client=client, filters=filters,
        )

        result = {
            "sid": sid, "symbol": symbol, "action": "open",
            "status": status.lower(), "side": logical,
            "qty": filled_qty, "exec_price": avg_price,
            "binance_order_id": order_id,
            "is_virtual": str(is_virtual).lower(),
            "venue": f"binance_{'demo' if is_virtual else 'prod'}",
            **prot, **trail,
            "json": json.dumps(
                {"entry": j_final, "protective": prot, "trailing": trail},
                ensure_ascii=False, default=str,
            ),
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
            **prot,
        })
        return result

    def handle_modify(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Modify existing position: cancel all our orders → re-place SL/TP → trailing arming."""
        client, filters = self._resolve_client(payload)

        sid = str(payload.get("sid") or "").strip()
        symbol = str(payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")

        canceled = self._cancel_by_token(symbol, sid, client=client)

        # Determine current position
        risks = client.get_position_risk() or []
        amt = 0.0
        for pos in risks:
            if str(pos.get("symbol") or "").upper() != symbol:
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

        sl = _f(payload.get("sl"), 0.0) if payload.get("sl") is not None else None
        sl = sl if sl and sl > 0 else None
        tps_raw = payload.get("tp_levels") or []
        tps = [float(x) for x in tps_raw if x not in (None, "")]
        tps = [tp for tp in tps if tp > 0]

        # Fetch current mark price for protective order validation and trailing calc
        mark_price: Optional[float] = None
        try:
            mp = float(client.get_mark_price(symbol) or 0.0)
            mark_price = mp if mp > 0 else None
        except Exception:
            pass

        prot = self._place_protective(
            sid=sid, symbol=symbol, logical_side=logical,
            qty=qty, sl=sl, tps=tps,
            client=client, filters=filters,
            ref_price=mark_price,
        )

        # Determine entry price for trailing callbackRate calculation
        # mark_price was already fetched above; use it as the fallback.
        entry_price: Optional[float] = mark_price
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
            sl_order_id=_i(prot.get("sl_order_id"), 0) or None,
            tp_levels=tps,
            client=client, filters=filters,
        )
        return {
            "sid": sid, "symbol": symbol, "action": "modify",
            "status": "ok", "side": logical, "qty": qty,
            "canceled_orders": canceled,
            **prot, **trail,
            "json": json.dumps(
                {"canceled": canceled, "protective": prot, "trailing": trail},
                ensure_ascii=False, default=str,
            ),
        }

    def handle_cancel(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Cancel: remove our orders + market-close any open position."""
        client, filters = self._resolve_client(payload)

        sid = str(payload.get("sid") or "").strip()
        symbol = str(payload.get("symbol") or "").strip().upper()
        if not sid or not symbol:
            raise ValueError("sid/symbol required")
        if self.allowlist and symbol not in self.allowlist:
            raise ValueError(f"symbol not in allowlist: {symbol}")

        canceled = self._cancel_by_token(symbol, sid, client=client)

        # Close any open position
        risks = client.get_position_risk() or []
        amt = 0.0
        for pos in risks:
            if str(pos.get("symbol") or "").upper() != symbol:
                continue
            amt = _f(pos.get("positionAmt"), 0.0)
            break

        closed = False
        close_order_id: Optional[int] = None
        logical: Optional[str] = None
        qty = 0.0
        if amt != 0.0:
            logical = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            exit_side = "SELL" if amt > 0 else "BUY"
            pos_side = _position_side_for_mode(self.position_mode, logical)
            q, _ = self._quantize(symbol, qty, None, filters=filters)
            p: Dict[str, Any] = {
                "symbol": symbol,
                "side": exit_side,
                "type": "MARKET",
                "quantity": q,
                "newClientOrderId": _make_cid(sid, "close"),
            }
            if self.position_mode == "oneway":
                p["reduceOnly"] = True
            if pos_side:
                p["positionSide"] = pos_side
            j = client.post_order(p)
            close_order_id = _i(j.get("orderId"), 0) or None
            closed = True

        result = {
            "sid": sid, "symbol": symbol, "action": "cancel",
            "status": "ok", "side": logical or "",
            "qty": qty, "canceled_orders": canceled,
            "close_order_id": close_order_id or "",
            "closed": str(closed).lower(),
        }
        # Update orders:state:{sid}: mark position as closed
        if closed:
            self._save_order_state(sid, {
                "action": "cancel",
                "status": "closed",
                "symbol": symbol,
                "side": logical or "",
                "close_order_id": close_order_id or "",
                "closed": True,
            })
        return result

    def handle_resize(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Resize position (TODO: compute delta-qty and market order the difference).

        Currently intentionally unimplemented — requires careful delta calculation
        and safe partial-close/pyramid logic.
        """
        sid = str(payload.get("sid") or "").strip()
        symbol = str(payload.get("symbol") or "").strip().upper()
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

        action = str(payload.get("action") or "").strip().lower()
        sid = str(payload.get("sid") or "").strip()
        symbol = str(payload.get("symbol") or "").strip().upper()

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
                    time.sleep(0.25)  # brief back-off before next iteration
                    return

            # Fatal OR max retries exceeded
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
