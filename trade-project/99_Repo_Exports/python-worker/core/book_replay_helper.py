"""book_replay_helper.py — ADR-0005 support module.

Synchronous helper for retrieving the bid/ask/mid price from `stream:book_{SYMBOL}`
at an arbitrary historical timestamp. Used by the TCA enricher to compute
realized_spread_{1s,5s}_bps and perm_impact_{1s,5s}_bps after a fill.

Strategy:
  - Use Redis stream id range search: id is `<ms>-<seq>`, so xrange with
    `min=ts_ms-tolerance, max=ts_ms+tolerance` returns entries near target.
  - Pick the entry closest to ts_ms (smallest |entry_ts - ts_ms|).
  - Returns None if no book data within tolerance.

Field name convention (from go-worker book publisher):
  - mid_price (preferred), micro_mid, mid
  - bid, ask, bid_size, ask_size
"""
from __future__ import annotations

import math
from typing import Any


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


def _safe_float(v: Any, default: float = float("nan")) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def get_mid_at(
    redis_client: Any,
    symbol: str,
    target_ts_ms: int,
    *,
    tolerance_ms: int = 500,
    book_stream_template: str = "stream:book_{symbol}",
) -> tuple[float | None, int | None]:
    """Return (mid_price, actual_ts_ms) closest to target_ts_ms within tolerance.

    Walks the book stream in [target-tolerance, target+tolerance] ms window.
    If multiple entries fall in the window, the one with smallest |Δt| wins.
    Returns (None, None) if no entries found.
    """
    if redis_client is None or not symbol:
        return None, None
    stream_key = book_stream_template.format(symbol=symbol)
    lo = max(0, target_ts_ms - tolerance_ms)
    hi = target_ts_ms + tolerance_ms
    try:
        # Redis stream IDs are <ms>-<seq>
        entries = redis_client.xrange(stream_key, min=f"{lo}-0", max=f"{hi}-0", count=64)
    except Exception:
        return None, None
    if not entries:
        return None, None

    best_mid: float | None = None
    best_ts: int | None = None
    best_dt = tolerance_ms + 1
    for raw_id, fields in entries:
        try:
            entry_id = _decode(raw_id)
            entry_ts = int(entry_id.split("-")[0])
            fields = {_decode(k): _decode(v) for k, v in fields.items()}
            mid = _safe_float(fields.get("mid_price") or fields.get("micro_mid") or fields.get("mid"))
            if not math.isfinite(mid):
                # Fallback: derive mid from bid/ask
                bid = _safe_float(fields.get("bid"))
                ask = _safe_float(fields.get("ask"))
                if math.isfinite(bid) and math.isfinite(ask) and bid > 0 and ask > 0:
                    mid = 0.5 * (bid + ask)
                else:
                    continue
            dt = abs(entry_ts - target_ts_ms)
            if dt < best_dt:
                best_mid = mid
                best_ts = entry_ts
                best_dt = dt
        except Exception:
            continue
    return best_mid, best_ts


def compute_tca_metrics(
    redis_client: Any,
    *,
    symbol: str,
    fill_price: float,
    arrival_mid: float,
    fill_ts_ms: int,
    side: str,
    horizons_ms: tuple[int, ...] = (1_000, 5_000),
    tolerance_ms: int = 500,
) -> dict[str, float]:
    """Compute TCA metrics around a fill event.

    Returns a dict with:
      eff_spread_bps           — 2 * sign * (fill_price - arrival_mid) / arrival_mid * 1e4
      mid_after_{H}s_bps       — book mid at fill_ts + H, in bps relative to arrival_mid
      realized_spread_{H}s_bps — sign * (fill_price - mid_after_H) / arrival_mid * 1e4 * 2
      perm_impact_{H}s_bps     — sign * (mid_after_H - arrival_mid) / arrival_mid * 1e4
      is_bps                   — implementation shortfall (alias for eff_spread for limit orders)

    Sign convention: BUY/LONG → +1, SELL/SHORT → −1.
    """
    sign = 1.0 if (side or "").upper() in ("BUY", "LONG") else -1.0
    eps = 1e-12
    out: dict[str, float] = {"eff_spread_bps": 0.0, "is_bps": 0.0}
    if arrival_mid <= 0 or not math.isfinite(arrival_mid) or not math.isfinite(fill_price):
        for h_ms in horizons_ms:
            sec = h_ms // 1000
            out[f"mid_after_{sec}s_bps"] = 0.0
            out[f"realized_spread_{sec}s_bps"] = 0.0
            out[f"perm_impact_{sec}s_bps"] = 0.0
        return out

    eff_spread_bps = 1e4 * 2.0 * sign * (fill_price - arrival_mid) / arrival_mid
    out["eff_spread_bps"] = eff_spread_bps
    out["is_bps"] = eff_spread_bps  # IS for taker order ≈ effective spread

    for h_ms in horizons_ms:
        sec = h_ms // 1000
        target_ts = fill_ts_ms + h_ms
        mid_h, _actual = get_mid_at(redis_client, symbol, target_ts, tolerance_ms=tolerance_ms)
        if mid_h is None or mid_h <= 0:
            out[f"mid_after_{sec}s_bps"] = 0.0
            out[f"realized_spread_{sec}s_bps"] = 0.0
            out[f"perm_impact_{sec}s_bps"] = 0.0
            continue
        # mid relative to arrival_mid in bps
        out[f"mid_after_{sec}s_bps"] = 1e4 * (mid_h - arrival_mid) / max(arrival_mid, eps)
        # realized spread = 2 * sign * (fill_price - mid_h) / arrival_mid
        out[f"realized_spread_{sec}s_bps"] = 1e4 * 2.0 * sign * (fill_price - mid_h) / max(arrival_mid, eps)
        # permanent impact = sign * (mid_h - arrival_mid) / arrival_mid
        out[f"perm_impact_{sec}s_bps"] = 1e4 * sign * (mid_h - arrival_mid) / max(arrival_mid, eps)
    return out
