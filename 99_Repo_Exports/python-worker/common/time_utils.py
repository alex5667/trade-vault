from __future__ import annotations
# Stage1-P1: NormalizedEpochMs + normalize_epoch_ms_v2 aliases added
from dataclasses import dataclass

import time
from typing import Any, Optional

EPOCH_MS_MIN = 946684800000       # 2000-01-01
EPOCH_MS_MAX = 4102444800000      # 2100-01-01

def get_current_timestamp_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)

def get_current_timestamp_s() -> int:
    """Get current timestamp in seconds."""
    return int(time.time())

def format_timestamp_for_redis(ts: int) -> str:
    """Format timestamp for Redis."""
    return str(ts)

def parse_timestamp_from_redis(ts_str: str) -> int:
    """Parse timestamp from Redis."""
    return int(ts_str)

def extract_binance_close_time(data: Any) -> Optional[int]:
    """Extract close time from Binance data."""
    return None

def format_duration_ms(ms: int) -> str:
    """Format duration in milliseconds to string."""
    return f"{ms}ms"

def normalize_timestamp(ts: Any) -> Optional[int]:
    """Normalize timestamp."""
    try:
        return int(ts)
    except (TypeError, ValueError):
        return None

def format_timestamp_iso(ts: int) -> str:
    """Format timestamp to ISO string."""
    from datetime import datetime
    return datetime.fromtimestamp(ts / 1000).isoformat()

def normalize_epoch_ms_best_effort(ts: Any, *, now_ms: Optional[int] = None) -> int:
    """
    Защита от: ts_ms<=0, time-of-day, секунды вместо мс, мусор.
    Возвращает plausibly-epoch milliseconds.
    """
    if now_ms is None:
        import time
        now_ms = int(time.time() * 1000)
    try:
        v = int(ts or 0)
    except Exception:
        return int(now_ms)

    if v <= 0:
        return int(now_ms)

    # seconds epoch => ms
    # типично 1700000000..2000000000
    if 1_000_000_000 <= v < 100_000_000_000:
        v = v * 1000

    # time-of-day / minutes-of-day / прочий мелкий мусор
    if v < EPOCH_MS_MIN:
        return int(now_ms)

    if v > EPOCH_MS_MAX:
        return int(now_ms)

    return int(v)
from dataclasses import dataclass

@dataclass(frozen=True)
class NormalizedTime:
    ts_ms: int
    src_unit: str  # 'ms'|'s'|'us'|'ns'|'unknown'
    ok: bool
    err: str = ""

def normalize_epoch_ms(
    x: Any,
    *,
    now_ms: Optional[int] = None,
    max_future_ms: int = 2 * 24 * 3600_000,
    max_past_ms: int = 10 * 365 * 24 * 3600_000,
) -> NormalizedTime:
    """
    Normalize epoch-like timestamp to epoch milliseconds.

    Heuristics:
    - < 1e11 -> seconds
    - 1e14..1e17 -> microseconds
    - >= 1e17 -> nanoseconds
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    try:
        if x is None:
            return NormalizedTime(0, "unknown", False, "ts_missing")
        if isinstance(x, bool):
            return NormalizedTime(0, "unknown", False, "ts_bool")
        if isinstance(x, (int, float)):
            v = int(x)
        else:
            s = str(x).strip()
            if s == "":
                return NormalizedTime(0, "unknown", False, "ts_empty")
            v = int(float(s)) if "." in s else int(s)
    except Exception:
        return NormalizedTime(0, "unknown", False, "ts_parse")

    unit = "ms"
    ts_ms = v

    if ts_ms > 0 and ts_ms < 100_000_000_000:
        unit = "s"
        ts_ms *= 1000
    elif ts_ms >= 100_000_000_000_000 and ts_ms < 100_000_000_000_000_000:
        unit = "us"
        ts_ms //= 1000
    elif ts_ms >= 100_000_000_000_000_000:
        unit = "ns"
        ts_ms //= 1_000_000

    if ts_ms <= 0:
        return NormalizedTime(0, unit, False, "ts_nonpositive")
    if ts_ms > now_ms + max_future_ms:
        return NormalizedTime(ts_ms, unit, False, "ts_future")
    if ts_ms < now_ms - max_past_ms:
        return NormalizedTime(ts_ms, unit, False, "ts_too_old")

    return NormalizedTime(ts_ms, unit, True, "")


# ---------------------------------------------------------------------------
# Stage1-P1: simplified structured result + v2 compatibility alias
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class NormalizedEpochMs:
    """
    Structured result for normalize_epoch_ms_v2.

    Fields
    ------
    ts_ms   : int  — epoch milliseconds (best-effort)
    kind    : str  — 'ms' | 'sec' | 'now' (source classification)
    reason  : str  — short diagnostic string
    clamped : bool — True when the raw value was out-of-range and fallback was used
    """
    ts_ms: int
    kind: str
    reason: str
    clamped: bool = False


def normalize_epoch_ms_v2(value: Any, *, now_ms: Optional[int] = None) -> NormalizedEpochMs:
    """
    Best-effort normalization to epoch milliseconds.

    Accepts:
      - ms  (>= 1e11)          kept as-is
      - sec (< 1e11)           converted value * 1000
      - numeric strings
    Garbage / None / negative  -> now_ms (real-time fallback)

    Returns NormalizedEpochMs so callers can use:
        normalize_epoch_ms_v2(val).ts_ms   (attribute access)
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    if value is None:
        return NormalizedEpochMs(now_ms, "now", "none", clamped=True)

    try:
        if isinstance(value, str):
            s = value.strip()
            if s == "":
                return NormalizedEpochMs(now_ms, "now", "empty_str", clamped=True)
            v = float(s)
        else:
            v = float(value)
    except Exception:
        return NormalizedEpochMs(now_ms, "now", "non_numeric", clamped=True)

    if v <= 0:
        return NormalizedEpochMs(now_ms, "now", "non_positive", clamped=True)

    # heuristics: seconds vs milliseconds
    if v < 1e11:  # likely Unix seconds (e.g. 1700000000)
        ts_ms = int(v * 1000.0)
        return NormalizedEpochMs(ts_ms, "sec", "sec_to_ms")
    else:
        ts_ms = int(v)
        return NormalizedEpochMs(ts_ms, "ms", "as_ms")
