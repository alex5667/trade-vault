from __future__ import annotations

import time

# Stage1-P1: NormalizedEpochMs + normalize_epoch_ms_v2 aliases added
from dataclasses import dataclass
from typing import Any

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

def extract_binance_close_time(data: Any) -> int | None:
    """Extract close time from Binance data."""
    return None

def format_duration_ms(ms: int) -> str:
    """Format duration in milliseconds to string."""
    return f"{ms}ms"

def normalize_timestamp(ts: Any) -> int | None:
    """Normalize timestamp."""
    try:
        return int(ts)
    except Exception:
        return None

def format_timestamp_iso(ts: int) -> str:
    """Format timestamp to ISO string."""
    from datetime import datetime
    return datetime.fromtimestamp(ts / 1000).isoformat()

def normalize_epoch_ms_best_effort(ts: Any, *, now_ms: int | None = None) -> int:
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


class _NormalizedTs:
    """Minimal return object so callers can use .ts_ms attribute."""
    __slots__ = ("ts_ms",)

    def __init__(self, ts_ms: int) -> None:
        self.ts_ms = int(ts_ms)

    def __int__(self) -> int:
        return self.ts_ms


def normalize_epoch_ms(ts: Any, *, now_ms: int | None = None) -> _NormalizedTs:
    """
    Alias for normalize_epoch_ms_best_effort that returns a _NormalizedTs object.

    The result supports both:
    - int() conversion: int(normalize_epoch_ms(ts))
    - attribute access: normalize_epoch_ms(ts).ts_ms
    """
    return _NormalizedTs(normalize_epoch_ms_best_effort(ts, now_ms=now_ms))


# Convenience alias (positional: no 'now_ms' needed for most callers)
normalize_epoch_seconds = lambda ts: int((normalize_epoch_ms_best_effort(ts) or 0) // 1000)


# ---------------------------------------------------------------------------
# Stage1-P1: structured result type and v2 compatibility wrapper
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


def normalize_epoch_ms_v2(value: Any, *, now_ms: int | None = None) -> NormalizedEpochMs:
    """
    Best-effort normalization to epoch milliseconds.

    Accepts:
      - ms  (>= 1e11)             kept as-is
      - sec (< 1e11, >= 1e9)      converted  value * 1000
      - numeric strings
    Garbage / None / negative     -> now_ms (real-time fallback)

    Returns NormalizedEpochMs so callers can use either:
        normalize_epoch_ms_v2(val).ts_ms      (attribute)
        int(normalize_epoch_ms_v2(val))       (implicit via .ts_ms)
    """
    if now_ms is None:
        import time as _time
        now_ms = int(_time.time() * 1000)

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

    # heuristics: seconds vs milliseconds threshold = 1e11
    if v < 1e11:  # likely Unix seconds (e.g. 1700000000)
        ts_ms = int(v * 1000.0)
        return NormalizedEpochMs(ts_ms, "sec", "sec_to_ms")
    else:
        ts_ms = int(v)
        return NormalizedEpochMs(ts_ms, "ms", "as_ms")
