from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SlqSnapshot:
    n: int
    sl_buffer_atr_q90: float
    post_sl_tp1_hit_rate: float
    ts_ms: int
    # Observability fields (optional, default-safe for backward compat)
    bucket_level: str = "na"          # exact / sym_side_regime / sym_side / etc.
    ev_after_slq_bps: float = 0.0     # EV estimate after SLQ adjustment
    stop_atr_mult_before: float = 0.0 # STOP_ATR_MULT before SLQ
    stop_atr_mult_after: float = 0.0  # STOP_ATR_MULT after SLQ


def _sf(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def fetch_slq(redis: Any, *, key: str) -> SlqSnapshot | None:
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
            n=n,
            sl_buffer_atr_q90=_sf(obj.get("sl_buffer_atr_q90"), 0.0),
            post_sl_tp1_hit_rate=_sf(obj.get("post_sl_tp1_hit_rate"), 0.0),
            ts_ms=int(obj.get("ts_ms", 0) or 0),
            bucket_level=str(obj.get("bucket_level") or "na"),
            ev_after_slq_bps=_sf(obj.get("ev_after_slq_bps"), 0.0),
            stop_atr_mult_before=_sf(obj.get("stop_atr_mult_before"), 0.0),
            stop_atr_mult_after=_sf(obj.get("stop_atr_mult_after"), 0.0),
        )
    except Exception:
        return None
