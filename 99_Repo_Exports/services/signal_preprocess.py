from __future__ import annotations

import os
import time
import math
from typing import Any, Dict, List, Optional


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _dedup_str_list(xs: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in xs:
        s = str(x or "").strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def preprocess_signal_for_publish(signal: Dict[str, Any], symbol: str, source: str, logger: Any) -> Dict[str, Any]:
    """
    In-place normalize + attach *data-quality* flags that downstream gates can use.

    Contract goals:
      - deterministic time fields (epoch ms)
      - numeric coercions for price/entry
      - micro defaults (spread/book staleness)
      - `data_quality_flags`: list[str] (lowercase) for hard veto gates (optional)

    This function MUST be safe to call multiple times.
    """
    # Time
    now_ms = int(time.time() * 1000)
    ts = signal.get("tick_ts") or signal.get("ts_ms") or signal.get("ts") or now_ms
    try:
        ts_ms = int(ts)
    except Exception:
        ts_ms = now_ms
    if ts_ms <= 0:
        ts_ms = now_ms

    signal["symbol"] = str(symbol or signal.get("symbol") or "").upper()
    signal["ts_ms"] = int(ts_ms)
    signal["tick_ts"] = int(signal.get("tick_ts") or ts_ms)

    # Direction / side (keep legacy `side` for consumers)
    direction = str(signal.get("direction") or "").upper().strip()
    if direction in {"LONG", "SHORT"}:
        signal["direction"] = direction
        signal["side"] = "long" if direction == "LONG" else "short"

    # Numeric coercions
    if "price" in signal:
        signal["price"] = _safe_float(signal.get("price"), 0.0)
    if "entry" in signal:
        signal["entry"] = _safe_float(signal.get("entry"), signal.get("price") or 0.0)

    # Micro defaults
    micro = signal.get("micro")
    if not isinstance(micro, dict):
        micro = {}
        signal["micro"] = micro

    micro.setdefault("spread_bps", 0.0)
    micro.setdefault("book_stale_ms", 10**9)

    # Indicators may already hold DQ hints
    indicators = signal.get("indicators")
    if not isinstance(indicators, dict):
        indicators = {}
        signal["indicators"] = indicators

    # ------------------------------------------------------------------
    # Data-quality flags (fail-open by default; veto is controlled elsewhere)
    # ------------------------------------------------------------------
    flags: List[str] = []
    if isinstance(signal.get("data_quality_flags"), list):
        flags.extend([str(x) for x in signal.get("data_quality_flags") if x is not None])

    # Tick health hints from indicators
    if int(indicators.get("tick_ts_missing", 0) or 0) == 1:
        flags.append("tick_ts_missing")
    if int(indicators.get("tick_oood", 0) or 0) == 1:
        flags.append("tick_oood")
    if "tick_gap_ms" in indicators:
        try:
            gap = int(indicators.get("tick_gap_ms") or 0)
            if gap > _env_int("DQ_TICK_GAP_FLAG_MS", 5000):
                flags.append("tick_gap")
        except Exception:
            pass

    # L2/book freshness derived from micro
    try:
        book_stale = int(micro.get("book_stale_ms") or 0)
        if book_stale > _env_int("DQ_BOOK_STALE_FLAG_MS", 1500):
            flags.append("stale_l2")
    except Exception:
        pass

    # Spread widening flag
    try:
        spread_bps = float(micro.get("spread_bps") or 0.0)
        if spread_bps > _env_float("DQ_SPREAD_WIDE_FLAG_BPS", 12.0):
            flags.append("wide_spread")
    except Exception:
        pass

    # Optional: missing trade_id (not a veto by default; useful for diagnostics)
    if signal.get("trade_id") is None:
        flags.append("missing_trade_id")

    signal["data_quality_flags"] = _dedup_str_list(flags)
    return signal

