"""binance_order_mapper.py — Stateless helpers and order construction utilities.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Numeric helpers: _f, _i, _sha1_8, _make_cid, _round_down, _truthy,
  _format_float, _clamp, _round_half_up
- Payload normalization: _normalize_side, _normalize_qty
- Order quantisation: quantize_qty, _split_tp_qtys, _local_headroom_check
- Error classification: _classify_error
- Trailing helpers: compute_trailing_callback_rate_pct, compute_trailing_activate_price,
  compute_limit_tp_price
- FSM state constants
- PositionSide helpers: _position_side_for_mode, _tp_state_name

All functions are pure / stateless (no self, no external I/O).
"""
from __future__ import annotations

import contextlib
import hashlib
import math
import os
from typing import Any

try:
    from services.binance_futures_client import (
        TRADFI_PERPS_NOT_SIGNED,
        BinanceAPIError,
    )
except Exception:  # pragma: no cover
    from binance_futures_client import (  # type: ignore[no-redef]
        TRADFI_PERPS_NOT_SIGNED,
        BinanceAPIError,
    )

try:
    from common.normalization import get_side_int, normalize_direction, normalize_side
except Exception:
    try:
        from normalization import get_side_int, normalize_direction, normalize_side  # type: ignore[no-redef]
    except Exception:
        normalize_side = normalize_direction = get_side_int = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# FSM state constants (shared across execution sub-modules)
# ---------------------------------------------------------------------------

FSM_PENDING_RECONCILE = "PENDING_RECONCILE"
FSM_RECEIVED = "RECEIVED"
FSM_VALIDATED = "VALIDATED"
FSM_ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
FSM_ENTRY_ACKED = "ENTRY_ACKED"
FSM_ENTRY_PARTIAL = "ENTRY_PARTIAL"
FSM_ENTRY_FILLED = "ENTRY_FILLED"
FSM_PROTECTION_ARMING = "PROTECTION_ARMING"
FSM_PROTECTION_REPLACING = "PROTECTION_REPLACING"
FSM_PROTECTED = "PROTECTED"
FSM_TP_POLICY_ARMED = "TP_POLICY_ARMED"
FSM_TRAIL_ARMED = "TRAIL_ARMED"
FSM_EXIT_FILLED = "EXIT_FILLED"
FSM_EMERGENCY_FLATTENED = "EMERGENCY_FLATTENED"
FSM_FAILED = "FAILED"

TERMINAL_FSM_STATES: frozenset[str] = frozenset({
    FSM_EXIT_FILLED,
    FSM_EMERGENCY_FLATTENED,
    FSM_FAILED,
})

# Partial fill policies
PARTIAL_FILL_CANCEL_REMAINDER_AND_PROTECT_FILLED = "CANCEL_REMAINDER_AND_PROTECT_FILLED"
PARTIAL_FILL_CONVERT_REMAINDER_TO_MARKET = "CONVERT_REMAINDER_TO_MARKET"
PARTIAL_FILL_ABORT_AND_FLATTEN = "ABORT_AND_FLATTEN"
VALID_PARTIAL_FILL_POLICIES: frozenset[str] = frozenset({
    PARTIAL_FILL_CANCEL_REMAINDER_AND_PROTECT_FILLED,
    PARTIAL_FILL_CONVERT_REMAINDER_TO_MARKET,
    PARTIAL_FILL_ABORT_AND_FLATTEN,
})


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def _f(x: Any, default: float = 0.0) -> float:
    """Safe float cast."""
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    """Safe int cast via float."""
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _round_down(x: float, step: float) -> float:
    """Round x down to the nearest multiple of step (for LOT_SIZE quantisation)."""
    if step <= 0:
        return x
    return math.floor(x / step) * step


def _round_half_up(x: float, decimals: int = 1) -> float:
    """Round halves up (avoids banker's rounding).

    Binance callbackRate uses 0.1% steps. Python's built-in round(0.35, 1)
    can yield 0.3 due to binary floating-point; this helper keeps it stable.
    """
    p = 10 ** int(decimals)
    return math.floor(x * p + 0.5) / p


def _format_float(x: float, step: float) -> str:
    """Format float to the step dimension without scientific notation."""
    if step <= 0:
        return f"{x:f}".rstrip("0").rstrip(".") if "." in f"{x:f}" else f"{x:f}"
    s_step = f"{step:f}".rstrip("0").rstrip(".") if "." in f"{step:f}" else f"{step:f}"
    decimals = len(s_step.split(".")[1]) if "." in s_step else 0
    return f"{x:.{decimals}f}"


def _truthy(v: Any) -> bool:
    """Check if a value is truthy in payload context (handles string 'true'/'1' etc.)."""
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v) != 0.0
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _bool_env(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Client-order-ID helpers
# ---------------------------------------------------------------------------

def _sha1_8(s: str) -> str:
    """Short stable hash for building client order IDs (≤ Binance 36-char limit)."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]


def _make_cid(sid: str, tag: str, r: Any = None) -> str:
    """Build a deterministic clientOrderId ≤ 36 chars: <base>-<sha1[:8]>-<tag>.

    Optionally registers cid→sid mapping in Redis (TTL 3 days) for audit joins.
    """
    token = _sha1_8(sid)
    base = sid.replace(" ", "").replace(":", "-")
    base = base[: max(6, 36 - (len(tag) + len(token) + 2))]
    cid = f"{base}-{token}-{tag}"[:36]
    if r is not None:
        with contextlib.suppress(Exception):
            r.set(f"orders:cid_to_sid:{cid}", sid, ex=86400 * 3)
    return cid


def _tp_state_name(level: int, state: str) -> str:
    return f"TP{int(level)}_{str(state).strip().upper()}"


# ---------------------------------------------------------------------------
# Payload normalization
# ---------------------------------------------------------------------------

def _normalize_side(payload: dict[str, Any]) -> tuple[str, str, int]:
    """Return (binance_side, logical_side, side_int).

    binance_side:  BUY | SELL
    logical_side:  LONG | SHORT
    side_int:      1 | -1
    """
    raw = payload.get("side") or payload.get("direction") or ""
    side = normalize_side(raw) if normalize_side else type("_", (), {"value": "BUY"})()
    direction = normalize_direction(raw) if normalize_direction else type("_", (), {"value": "LONG"})()
    side_int: int = get_side_int(raw) if get_side_int else 1
    return str(side.value), str(direction.value), side_int  # type: ignore


def _normalize_qty(
    payload: dict[str, Any],
    assume_lot_is_qty: bool = True,
    symbol: str = "",
) -> float:
    """Extract trade quantity from payload.

    Priority: qty → quantity → lot (with contract_size expansion for MT5 payloads).
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
                from confidence_calculation.instrument_config import get_specs  # type: ignore[import]
                specs = get_specs(sym)
                c_size = getattr(specs, "contract_size", 1.0)
                return lot * float(c_size)
        except Exception:
            pass
        return lot
    raise ValueError(
        f"missing qty (payload provided no qty/quantity/lot, keys: {list(payload.keys())})"
    )


def _position_side_for_mode(position_mode: str, logical_side: str) -> str | None:
    """Return positionSide for hedge mode; None for one-way mode."""
    if position_mode != "hedge":
        return None
    return "LONG" if logical_side == "LONG" else "SHORT"


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

def _classify_error(e: Exception) -> str:
    """Return 'transient' or 'fatal' for error routing.

    Transient codes (retry-eligible):
      -1021  timestamp out of recvWindow → sync_time() and retry
      -1003  too many requests
      -1001  internal error
      -1007  timeout
      -1100  illegal chars (often transient duplicate)
    Fatal codes (no retry):
      -2021  Order would immediately trigger
      -4411  TradFi-Perps agreement not signed
    """
    if isinstance(e, BinanceAPIError):
        p = e.payload if isinstance(e.payload, dict) else {}
        code = p.get("code")
        msg = (p.get("msg") or "").lower()
        if p.get("ambiguous") is True or (e.status == 503 and "unknown" in msg):
            return "transient"
        if code == TRADFI_PERPS_NOT_SIGNED:
            return "fatal"
        if code in (-1021, -1003, -1001, -1007, -1100):
            return "transient"
        return "fatal"
    msg = str(e).lower()
    if "timed out" in msg or "temporary" in msg or "connection" in msg:
        return "transient"
    return "fatal"


# ---------------------------------------------------------------------------
# Trailing helpers
# ---------------------------------------------------------------------------

def compute_trailing_callback_rate_pct(
    payload: dict[str, Any],
    *,
    min_pct: float,
    max_pct: float,
    default_pct: float,
) -> float:
    """Extract callbackRate (%) for TRAILING_STOP_MARKET from signal payload.

    Priority:
      1. Explicit percent:  trail_callback_rate / trail_callback_pct
      2. Explicit bps:      trail_callback_bps  (30 bps = 0.30%)
      3. ENV default:       BINANCE_TRAIL_CALLBACK_DEFAULT

    Result is clamped to [min_pct, max_pct] and rounded to 0.1% (Binance req).
    """
    for k in ("trail_callback_rate", "trail_callback_pct", "trail_callback_percent"):
        v = payload.get(k)
        if v is not None:
            try:
                return _round_half_up(_clamp(float(v), min_pct, max_pct), 1)
            except Exception:
                pass
    bps = payload.get("trail_callback_bps")
    if bps is not None:
        try:
            return _round_half_up(_clamp(float(bps) / 100.0, min_pct, max_pct), 1)
        except Exception:
            pass
    return _round_half_up(_clamp(float(default_pct), min_pct, max_pct), 1)


def compute_limit_tp_price(
    tp_trigger_price: float,
    logical_side: str,
    *,
    offset_bps: float,
    tick_size: float,
) -> float:
    """Compute passive limit price for maker TP ladder.

    LONG (SELL): positive offset places limit ABOVE trigger.
    SHORT (BUY): positive offset places limit BELOW trigger.
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
      * BUY trailing (close SHORT): activatePrice < latest price
      * SELL trailing (close LONG): activatePrice > latest price
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
