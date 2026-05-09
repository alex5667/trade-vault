from __future__ import annotations

import hashlib
import math
import os
from typing import Any

from utils.time_utils import get_ny_time_millis

# Cache environment variables at module level (Zero I/O in Hot Path)
_DQ_TICK_GAP_FLAG_MS = int(os.getenv("DQ_TICK_GAP_FLAG_MS", "5000"))
_DQ_BOOK_STALE_FLAG_MS = int(os.getenv("DQ_BOOK_STALE_FLAG_MS", "1500"))
_DQ_SPREAD_WIDE_FLAG_BPS = float(os.getenv("DQ_SPREAD_WIDE_FLAG_BPS", "12.0"))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _dedup_str_list(xs: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for x in xs:
        s = (x or "").strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def preprocess_signal_for_publish(signal: dict[str, Any], symbol: str, source: str, logger: Any) -> dict[str, Any]:
    """
    In-place normalize + attach *data-quality* flags that downstream gates can use.

    Contract goals:
      - deterministic time fields (epoch ms)
      - numeric coercions for price/entry
      - micro defaults (spread/book staleness)
      - `data_quality_flags`: list[str] (lowercase) for hard veto gates (optional)

    This function MUST be safe to call multiple times.
    """
    # Safe-check for dictionary interface
    if not hasattr(signal, "get") or not isinstance(signal, dict):
        return signal

    # Time
    now_ms = get_ny_time_millis()
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
    direction = (signal.get("direction") or "").upper().strip()
    if not direction:
        direction = (signal.get("side") or "").upper().strip()

    if direction in {"LONG", "SHORT", "BUY", "SELL"}:
        if direction in {"BUY", "LONG"}:
            norm_dir = "LONG"
            side_int = 1
        else:
            norm_dir = "SHORT"
            side_int = -1

        signal["direction"] = norm_dir
        signal["side"] = norm_dir.lower()
        signal["side_int"] = side_int

    # Signal ID / sid
    if not signal.get("signal_id"):
        # Deterministic but unique-ish ID if missing
        raw_id = f"{signal['symbol']}_{signal['ts_ms']}_{signal.get('price', 0)}"
        sig_id = hashlib.md5(raw_id.encode()).hexdigest()[:16]
        signal["signal_id"] = sig_id

    signal["sid"] = signal["signal_id"]

    # Confidence mirrors
    if "confidence" in signal:
        conf = _safe_float(signal["confidence"], 0.0)
        # normalize to 0..1 and 0..100
        if conf > 1.0:
            c01 = conf / 100.0
            cpct = conf
        else:
            c01 = conf
            cpct = conf * 100.0
        signal["confidence01"] = round(float(c01), 4)
        signal["confidence_pct"] = round(float(cpct), 2)

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
    flags: list[str] = []
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
            if gap > _DQ_TICK_GAP_FLAG_MS:
                flags.append("tick_gap")
        except Exception:
            pass

    # L2/book freshness derived from micro
    try:
        book_stale = int(micro.get("book_stale_ms") or 0)
        if book_stale > _DQ_BOOK_STALE_FLAG_MS:
            flags.append("stale_l2")
    except Exception:
        pass

    # Spread widening flag
    try:
        spread_bps = float(micro.get("spread_bps") or 0.0)
        if spread_bps > _DQ_SPREAD_WIDE_FLAG_BPS:
            flags.append("wide_spread")
    except Exception:
        pass

    # Optional: missing trade_id (not a veto by default; useful for diagnostics)
    if signal.get("trade_id") is None:
        flags.append("missing_trade_id")

    signal["data_quality_flags"] = _dedup_str_list(flags)
    return signal

