from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class SlqSnapshot:
    n: int
    sl_buffer_atr_q90: float
    post_sl_tp1_hit_rate: float
    ts_ms: int


def _sf(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def fetch_slq(redis: Any, *, key: str) -> Optional[SlqSnapshot]:
    """
    Redis GET -> SlqSnapshot
    Fail-open: returns None on any error or missing/invalid payload.
    """
    if redis is None:
        return None
    try:
        raw = redis.get(key)
        if not raw:
            return None
        obj = json.loads(raw)
        n = int(obj.get("n", 0) or 0)
        if n <= 0:
            return None
        return SlqSnapshot(
            n=n
            sl_buffer_atr_q90=_sf(obj.get("sl_buffer_atr_q90"), 0.0)
            post_sl_tp1_hit_rate=_sf(obj.get("post_sl_tp1_hit_rate"), 0.0)
            ts_ms=int(obj.get("ts_ms", 0) or 0)
        )
    except Exception:
        return None
