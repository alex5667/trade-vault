"""Core time utilities.

This module exists to provide a stable import path for time normalization across
services. It is intentionally thin and deterministic.

Public API:
- normalize_epoch_ms(x) -> int
- extract_tick_ts_ms(tick: dict) -> int
"""

from __future__ import annotations

from typing import Any, Dict
from common.time_norm import normalize_epoch_ms as _normalize_epoch_ms


def normalize_epoch_ms(x: Any) -> int:
    """Normalize timestamp to epoch milliseconds.

    Accepts int/float/str/datetime. Returns 0 if x is falsy/None/invalid.
    """
    try:
        if x is None or x is False:
            return 0
        ts_ms = int(_normalize_epoch_ms(x))
        return ts_ms if ts_ms > 0 else 0
    except Exception:
        return 0


def extract_tick_ts_ms(tick: Dict[str, Any]) -> int:
    """Extract best-effort event timestamp (ms) from a tick payload.
    
    Semantic priority chain:
    1. ts_ms / event_ts_ms: Explicitly normalized internal event time.
    2. ts / event_time: Common internal representation keys.
    3. E: Exchange raw event time (e.g., Binance Event Time - exactly when the event was dispatched).
    4. time / written_at: Infrastructure fallback times.
    
    NOTE: 'T' (Binance Trade Time) is intentionally excluded. Using trade execution time 
    as an event timestamp fallback creates timestamp confusion (e.g., artificial event age or 
    future skew) during backfill and replay scenarios.
    """
    if not tick or not isinstance(tick, dict):
        return 0
    return normalize_epoch_ms(
        tick.get("ts_ms")
        or tick.get("event_ts_ms")
        or tick.get("ts")
        or tick.get("event_time")
        or tick.get("tick_ts")
        or tick.get("E")
        or tick.get("time")
        or tick.get("written_at")
    )
