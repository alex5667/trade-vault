from __future__ import annotations

"""Fill / trade-close event contract (A3).

Goal:
- Guarantee that post-trade fill events are joinable with decision_snapshot via `sid`.
- Standardize time semantics: ts_fill_ms must be epoch-ms.

This module does NOT perform IO. It is used by executors/archivers/writers to:
- normalize heterogeneous payloads into a canonical shape
- validate that required fields are present

Required fields (minimum):
- sid
- order_id
- ts_fill_ms
- px
- qty
- fee_bps
- venue
- symbol
- side (LONG|SHORT)

Optional (high value):
- bid_at_fill
- ask_at_fill
- mid_at_fill
"""


import math
from typing import Any


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return float(f)


def _safe_int(v: Any) -> int | None:
    try:
        i = int(float(v))
    except Exception:
        return None
    return int(i)


def normalize_fill_event(evt: dict[str, Any]) -> dict[str, Any]:
    """Return canonical dict with best-effort key mapping.

    All values in the output follow strict types (str|float|int|None).
    This function never raises.
    """
    out: dict[str, Any] = {}

    # Join key — must match decision_snapshot.sid
    sid = evt.get("sid") or evt.get("signal_id") or evt.get("client_order_id")
    out["sid"] = str(sid) if sid is not None else None

    # order_id — unique per fill (for MT5: deal id; for Binance: orderId)
    out["order_id"] = str(evt.get("order_id") or evt.get("oid") or evt.get("id") or "") or None

    # Timestamps: epoch ms
    out["ts_fill_ms"] = _safe_int(
        evt.get("ts_fill_ms")
        or evt.get("fill_ts_ms")
        or evt.get("ts_ms")
        or evt.get("ts")
    )

    # Price/qty/fees
    out["px"] = _safe_float(evt.get("px") or evt.get("price") or evt.get("fill_px"))
    out["qty"] = _safe_float(
        evt.get("qty") or evt.get("q") or evt.get("fill_qty") or evt.get("size")
    )
    out["fee_bps"] = _safe_float(
        evt.get("fee_bps") or evt.get("fees_bps") or evt.get("fee")
    )

    # Venue/symbol/side
    out["venue"] = str(evt.get("venue") or evt.get("exchange") or "") or None
    out["symbol"] = str(evt.get("symbol") or evt.get("sym") or "") or None

    side = evt.get("side") or evt.get("position_side") or evt.get("direction")
    if isinstance(side, str):
        s = side.upper()
        if s in {"LONG", "SHORT"}:
            out["side"] = s
        elif s == "BUY":
            out["side"] = "LONG"
        elif s == "SELL":
            out["side"] = "SHORT"
        else:
            out["side"] = s
    else:
        out["side"] = None

    # Optional BBO at fill (for TCA: effective slippage vs decision snapshot)
    out["bid_at_fill"] = _safe_float(evt.get("bid_at_fill") or evt.get("bid"))
    out["ask_at_fill"] = _safe_float(evt.get("ask_at_fill") or evt.get("ask"))
    out["mid_at_fill"] = _safe_float(evt.get("mid_at_fill") or evt.get("mid"))

    return out


def validate_fill_event(evt: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate required fields after normalization.

    Returns (ok, missing_fields).
    """
    missing: list[str] = []

    sid = evt.get("sid")
    if not sid:
        missing.append("sid")

    for k in ("order_id", "venue", "symbol", "side"):
        if not evt.get(k):
            missing.append(k)

    ts = evt.get("ts_fill_ms")
    if ts is None or int(ts) <= 0:
        missing.append("ts_fill_ms")

    for k in ("px", "qty", "fee_bps"):
        v = evt.get(k)
        if v is None or not isinstance(v, (int, float)):
            missing.append(k)

    return (len(missing) == 0), missing
