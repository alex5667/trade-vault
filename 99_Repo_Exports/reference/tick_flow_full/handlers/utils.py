# utils.py
"""
Utility functions extracted from base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Any, Optional, Dict, Tuple, Dict, Tuple
import math
import time
import json
from datetime import datetime, timezone

_EPOCH_S_MIN = 946684800        # 2000-01-01 UTC in seconds
_EPOCH_MS_MIN = 946684800000    # 2000-01-01 UTC in ms


def _coalesce(*vals, default=None):
    """Return first non-None value, or default."""
    for val in vals:
        if val is not None:
            return val
    return default


def _to_str(val: Any) -> str:
    """Convert value to string safely."""
    if val is None:
        return ""
    try:
        return str(val)
    except Exception:
        return ""


def _parse_bool(val: Any) -> bool:
    """Parse value as boolean."""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("true", "1", "yes", "y", "on", "t"):
            return True
        if s in ("false", "0", "no", "n", "off", "f", ""):
            return False
        # fallback: non-empty string -> True
        return True
    if isinstance(val, (int, float)):
        return bool(val)
    return False


def normalize_epoch_ms(ts: Any, *, now_ms: Optional[int] = None, strict: bool = False) -> int:
    """
    Normalize timestamp to milliseconds since epoch.
    Handles various input formats.
    NOTE: Intended ONLY for epoch timestamps. Intraday values (0..86400 etc) are rejected.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    if ts is None:
        if strict:
            raise ValueError("normalize_epoch_ms: ts is None")
        return int(now_ms)

    # datetime -> epoch ms (UTC)
    if isinstance(ts, datetime):
        try:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            ms = int(ts.timestamp() * 1000)
            if ms < _EPOCH_MS_MIN:
                if strict:
                    raise ValueError(f"normalize_epoch_ms: datetime too old: {ms}")
                return int(now_ms)
            return ms
        except Exception:
            if strict:
                raise
            return int(now_ms)

    try:
        # bytes -> str
        if isinstance(ts, (bytes, bytearray)):
            ts = ts.decode("utf-8", errors="replace")

        # If already int/float, check if it's seconds or milliseconds
        if isinstance(ts, (int, float)):
            ts_val = float(ts)
            if not math.isfinite(ts_val) or ts_val <= 0:
                if strict:
                    raise ValueError(f"normalize_epoch_ms: non-finite or <=0 ts={ts!r}")
                return int(now_ms)

            # Too small to be epoch seconds for modern data (likely intraday minutes/seconds).
            if ts_val < _EPOCH_S_MIN:
                if strict:
                    raise ValueError(f"normalize_epoch_ms: ts looks non-epoch (too small): {ts_val}")
                return int(now_ms)

            # Heuristic:
            #  - seconds epoch ~ 1.7e9 now
            #  - ms epoch ~ 1.7e12 now
            # Anything < 1e11 is almost certainly seconds (covers far future safely).
            if ts_val < 1e11:
                ms = int(ts_val * 1000)
            else:
                ms = int(ts_val)

            if ms < _EPOCH_MS_MIN:
                if strict:
                    raise ValueError(f"normalize_epoch_ms: ms looks non-epoch (too small): {ms}")
                return int(now_ms)
            return ms

        # If string, try to parse as float first
        if isinstance(ts, str):
            s = ts.strip()
            if not s:
                if strict:
                    raise ValueError("normalize_epoch_ms: empty string ts")
                return int(now_ms)
            # try numeric
            try:
                ts_val = float(s)
                if not math.isfinite(ts_val) or ts_val <= 0:
                    if strict:
                        raise ValueError(f"normalize_epoch_ms: non-finite or <=0 ts={ts!r}")
                    return int(now_ms)

                if ts_val < _EPOCH_S_MIN:
                    if strict:
                        raise ValueError(f"normalize_epoch_ms: ts looks non-epoch (too small): {ts_val}")
                    return int(now_ms)

                if ts_val < 1e11:
                    ms = int(ts_val * 1000)
                else:
                    ms = int(ts_val)
                if ms < _EPOCH_MS_MIN:
                    if strict:
                        raise ValueError(f"normalize_epoch_ms: ms looks non-epoch (too small): {ms}")
                    return int(now_ms)
                return ms
            except ValueError:
                # try ISO 8601 (best-effort)
                try:
                    # supports "YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]"
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    ms = int(dt.timestamp() * 1000)
                    if ms < _EPOCH_MS_MIN:
                        if strict:
                            raise ValueError(f"normalize_epoch_ms: parsed ISO but too small: {ms}")
                        return int(now_ms)
                    return ms
                except Exception:
                    if strict:
                        raise ValueError(f"normalize_epoch_ms: cannot parse ts={ts!r}")
                    return int(now_ms)

    except (ValueError, TypeError, AttributeError):
        if strict:
            raise

    # Fallback to current time
    return int(now_ms)


def minutes_of_day_from_epoch_ms(ts_ms: Any, *, strict: bool = False) -> int:
    """
    Convert epoch-ms timestamp to minutes-of-day [0..1439] in UTC.
    This is the ONLY correct place to derive intraday minutes from epoch.
    """
    ms = normalize_epoch_ms(ts_ms, strict=strict)
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.hour * 60 + dt.minute


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _iso_date_utc_from_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.date().isoformat()


def normalize_pivots_bundle(
    pivots: Any,
    *,
    now_ms: Optional[int] = None,
    strict: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Canonical pivots wrapper:
      {
        "ts_ms": int,          # epoch ms
        "date": "YYYY-MM-DD",  # UTC date derived from ts_ms (or provided)
        "hlc": {high,low,close} | None,
        "pivots": { ... }      # numeric levels
      }

    Accepts:
      - None -> None
      - raw dict with levels -> wrapped into bundle
      - bundle dict {"ts_ms","hlc","pivots",...} -> normalized
      - JSON str/bytes -> parsed then normalized
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    if pivots is None:
        return None

    # JSON input
    if isinstance(pivots, (bytes, bytearray)):
        pivots = pivots.decode("utf-8", errors="replace")
    if isinstance(pivots, str):
        s = pivots.strip()
        if not s:
            return None
        try:
            pivots = json.loads(s)
        except Exception:
            if strict:
                raise ValueError("normalize_pivots_bundle: invalid JSON")
            return None

    if not isinstance(pivots, dict):
        if strict:
            raise ValueError(f"normalize_pivots_bundle: expected dict, got {type(pivots).__name__}")
        return None

    is_bundle = ("pivots" in pivots) and isinstance(pivots.get("pivots"), dict)

    if is_bundle:
        raw_ts = pivots.get("ts_ms") or pivots.get("ts") or pivots.get("updated_ts_ms") or pivots.get("updated_at_ms")
        ts_ms = normalize_epoch_ms(raw_ts, now_ms=now_ms, strict=strict) if raw_ts is not None else int(now_ms)

        raw_hlc = pivots.get("hlc")
        hlc = None
        if isinstance(raw_hlc, dict):
            hi = _safe_float(raw_hlc.get("high"))
            lo = _safe_float(raw_hlc.get("low"))
            cl = _safe_float(raw_hlc.get("close"))
            if hi is not None and lo is not None and cl is not None:
                hlc = {"high": hi, "low": lo, "close": cl}

        raw_levels = pivots.get("pivots") or {}
        levels: Dict[str, float] = {}
        for k, v in raw_levels.items():
            fv = _safe_float(v)
            if fv is None:
                continue
            levels[str(k)] = fv

        # keep provided date if valid, else derive
        date = pivots.get("date")
        if not isinstance(date, str) or len(date) < 8:
            date = _iso_date_utc_from_ms(int(ts_ms))

        return {"ts_ms": int(ts_ms), "date": str(date), "hlc": hlc, "pivots": levels}

    # Raw dict levels -> wrap
    levels: Dict[str, float] = {}
    for k, v in pivots.items():
        fv = _safe_float(v)
        if fv is None:
            continue
        levels[str(k)] = fv

    if not levels:
        return None

    ts_ms = int(now_ms)
    return {"ts_ms": ts_ms, "date": _iso_date_utc_from_ms(ts_ms), "hlc": None, "pivots": levels}


def compute_daily_pivots(hlc: dict) -> dict:
    """
    Compute daily pivot points from HLC data.

    Args:
        hlc: Dict with 'high', 'low', 'close' keys

    Returns:
        Dict with pivot levels
    """
    if not isinstance(hlc, dict):
        return {}
    try:
        high = float(hlc.get("high", 0) or 0.0)
        low = float(hlc.get("low", 0) or 0.0)
        close = float(hlc.get("close", 0) or 0.0)
    except (TypeError, ValueError):
        return {}

    if not (math.isfinite(high) and math.isfinite(low) and math.isfinite(close)):
        return {}
    if high <= 0 or low <= 0 or close <= 0:
        return {}

    # Calculate pivot points
    pivot = (high + low + close) / 3
    r1 = 2 * pivot - low
    s1 = 2 * pivot - high
    r2 = pivot + (high - low)
    s2 = pivot - (high - low)

    return {
        'pivot': pivot,
        'r1': r1,
        's1': s1,
        'r2': r2,
        's2': s2,
        'high': high,
        'low': low,
        'close': close,
    }
