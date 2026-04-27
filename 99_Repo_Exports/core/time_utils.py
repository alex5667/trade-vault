"""Time helpers used across services.

This module intentionally re-exports normalize_epoch_ms so higher-level services
can import from a stable path (core.time_utils).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from common.time_norm import normalize_epoch_ms


def extract_tick_ts_ms(tick: Dict[str, Any]) -> Optional[int]:
    """Best-effort extraction of tick timestamp (epoch ms or seconds)."""
    if not isinstance(tick, dict):
        return None

    ts = tick.get("ts_ms")
    if ts is None:
        ts = tick.get("ts")
    if ts is None:
        # Some feeds may use alternative keys
        ts = tick.get("timestamp")

    try:
        return normalize_epoch_ms(int(ts)) if ts is not None else None
    except Exception:
        return None

