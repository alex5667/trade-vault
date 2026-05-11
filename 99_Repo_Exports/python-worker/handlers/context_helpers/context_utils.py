"""
Context manipulation and type conversion utilities.

Extracted from BaseOrderFlowHandler to follow Single Responsibility Principle.
Provides fail-open utilities for working with signal contexts and type conversions.
"""

import math
from typing import Any
import contextlib


def get_attr(ctx: Any, name: str, default: Any = None) -> Any:
    """
    Get attribute or dict key, fail-open.
    
    Args:
        ctx: Context object or dictionary
        name: Attribute/key name
        default: Default value if not found
        
    Returns:
        Attribute value or default
    """
    if isinstance(ctx, dict):
        return ctx.get(name, default)
    return getattr(ctx, name, default)


def set_attr(ctx: Any, name: str, value: Any) -> bool:
    """
    Set attribute or dict key, fail-open.
    
    Args:
        ctx: Context object or dictionary
        name: Attribute/key name
        value: Value to set
        
    Returns:
        True if successful, False otherwise
    """
    try:
        if isinstance(ctx, dict):
            ctx[name] = value
            return True
        setattr(ctx, name, value)
        return True
    except Exception:
        return False


def safe_float_pos(x: Any) -> float | None:
    """
    Return finite float>0 else None (best-effort, never raises).
    
    Args:
        x: Value to convert
        
    Returns:
        Positive finite float or None
    """
    try:
        f = float(x)
        if not math.isfinite(f) or f <= 0.0:
            return None
        return f
    except Exception:
        return None


def first_item(x: Any) -> Any:
    """
    Return x[0] for list/tuple, else x (best-effort).
    
    Args:
        x: Value to extract from
        
    Returns:
        First item if list/tuple, otherwise x
    """
    if isinstance(x, (list, tuple)) and len(x) > 0:
        return x[0]
    return x


def normalize_side_int(side: Any) -> int | None:
    """
    Standardize side to internal format (+1/-1).
    
    Accepts:
      - ints: 1/-1, +1/-1
      - strings: "LONG"/"SHORT", "BUY"/"SELL", "BID"/"ASK"
      - legacy enums/objects with .value or .name (best-effort)
      
    Args:
        side: Side value in various formats
        
    Returns:
        +1 for long/buy, -1 for short/sell, None if cannot parse
    """
    if side is None:
        return None

    # numbers
    try:
        if isinstance(side, (int, float)):
            v = int(side)
            if v > 0:
                return 1
            if v < 0:
                return -1
            return None
    except Exception:
        pass

    # objects/enums
    for attr in ("value", "name"):
        try:
            vv = getattr(side, attr, None)
            if vv is not None and vv is not side:
                r = normalize_side_int(vv)
                if r in (1, -1):
                    return r
        except Exception:
            pass

    # strings
    try:
        s = side.strip().lower()  # type: ignore
    except Exception:
        return None

    if not s:
        return None

    if s in {"1", "+1", "long", "buy", "bid", "b", "l"}:
        return 1
    if s in {"-1", "short", "sell", "ask", "s"}:
        return -1

    return None


def side_int_to_payload(side_int: int | None) -> str | None:
    """
    Convert internal (+1/-1) to payload string (LONG/SHORT).
    
    Args:
        side_int: Internal side representation
        
    Returns:
        "LONG" for +1, "SHORT" for -1, None otherwise
    """
    if side_int == 1:
        return "LONG"
    if side_int == -1:
        return "SHORT"
    return None


def ensure_levels(ctx: Any, *, side: Any = None) -> None:
    """
    FAIL-OPEN: ensure minimal invariant fields on ctx for EV/Cost gates.

    What it does (best-effort, never raises):
      - normalizes/sets:
          ctx.entry_price
          ctx.tp1_price
          ctx.sl_price
          ctx.price (fallback to entry_price)
          ctx.side_int (+1/-1)  [internal]
          ctx.side (LONG/SHORT) [string mirror for envelope/audit]
      - if something cannot be ensured -> appends DQ flags into ctx.data_quality_flags

    IMPORTANT:
      - This function does NOT decide to veto/fail-close. It only stabilizes inputs.
      - Gates remain the single source of truth for decisions.
      
    Args:
        ctx: Signal context to normalize
        side: Optional side override
    """
    if ctx is None:
        return

    # Idempotency guard
    try:
        if bool(getattr(ctx, "_levels_attached", False)):
            return
    except Exception:
        pass

    # Import DQ flag functions
    try:
        from common.dq_flags import append_dq_flag  # type: ignore
    except ImportError:
        def append_dq_flag(c, f):
            pass

    # ---- side normalization (internal) ----
    try:
        si = normalize_side_int(
            side if side is not None
            else get_attr(ctx, "side_int", None)
            or get_attr(ctx, "side", None)
        )
        if si in (1, -1):
            set_attr(ctx, "side_int", int(si))
            # payload-friendly mirror (LONG/SHORT)
            ss = side_int_to_payload(si)
            if ss:
                set_attr(ctx, "side", ss)
        else:
            append_dq_flag(ctx, "side_missing_or_unparsed")
    except Exception:
        append_dq_flag(ctx, "side_normalize_failed")

    # ---- entry_price ----
    try:
        entry = safe_float_pos(get_attr(ctx, "entry_price", None))
        if entry is None:
            entry = safe_float_pos(get_attr(ctx, "entry", None))
        if entry is None:
            entry = safe_float_pos(get_attr(ctx, "price", None))
        if entry is None:
            entry = safe_float_pos(get_attr(ctx, "last_price", None))
        if entry is None:
            of = get_attr(ctx, "of", None)
            if of is not None:
                entry = (safe_float_pos(get_attr(of, "price", None))
                        or safe_float_pos(get_attr(of, "last_price", None)))
        if entry is not None:
            set_attr(ctx, "entry_price", entry)
        else:
            append_dq_flag(ctx, "levels_missing_entry_price")
    except Exception:
        append_dq_flag(ctx, "levels_entry_price_extract_failed")

    # ---- price (audit-friendly) ----
    try:
        price = (safe_float_pos(get_attr(ctx, "price", None))
                or safe_float_pos(get_attr(ctx, "last_price", None)))
        if price is None:
            price = safe_float_pos(get_attr(ctx, "entry_price", None))
        if price is not None:
            set_attr(ctx, "price", float(price))
            set_attr(ctx, "last_price", float(price))
        else:
            append_dq_flag(ctx, "levels_missing_price")
    except Exception:
        append_dq_flag(ctx, "levels_price_extract_failed")

    # ---- tp1_price ----
    try:
        tp1 = (safe_float_pos(get_attr(ctx, "tp1_price", None))
              or safe_float_pos(get_attr(ctx, "tp1", None)))
        if tp1 is None:
            for name in ("tp_levels", "tp_prices", "targets", "take_profits"):
                v = get_attr(ctx, name, None)
                v0 = first_item(v)
                tp1 = safe_float_pos(v0)
                if tp1 is not None:
                    break
        if tp1 is not None:
            set_attr(ctx, "tp1_price", tp1)
        else:
            append_dq_flag(ctx, "levels_missing_tp1_price")
    except Exception:
        append_dq_flag(ctx, "levels_tp1_extract_failed")

    # ---- sl_price ----
    try:
        sl = (safe_float_pos(get_attr(ctx, "sl_price", None))
             or safe_float_pos(get_attr(ctx, "sl", None)))
        if sl is None:
            for name in ("stop_price", "stop", "sl_level", "stop_level"):
                sl = get_attr(ctx, name, None)
                sl = safe_float_pos(sl)
                if sl is not None:
                    break
        if sl is not None:
            set_attr(ctx, "sl_price", sl)
        else:
            append_dq_flag(ctx, "levels_missing_sl_price")
    except Exception:
        append_dq_flag(ctx, "levels_sl_extract_failed")

    # Mark as attached
    with contextlib.suppress(Exception):
        ctx._levels_attached = True


def to_float_or_nan(x: Any) -> float:
    """
    Convert to float or NaN (never raises).
    
    Args:
        x: Value to convert
        
    Returns:
        Float value or NaN
    """
    try:
        f = float(x)
        if not math.isfinite(f):
            return float("nan")
        return f
    except Exception:
        return float("nan")


def to_opt_float(x: Any) -> float | None:
    """
    Convert to optional float (never raises).
    
    Args:
        x: Value to convert
        
    Returns:
        Float value or None
    """
    if x is None:
        return None
    try:
        f = float(x)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None
