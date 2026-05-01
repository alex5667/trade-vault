from __future__ import annotations
"""Promotion freeze registry.

Purpose:
  - Centralize a small Redis contract that blocks model promotions for a period of time.
  - Used by timers/bundles/exporters to provide deterministic, explainable safety behavior.

Redis key (hash): cfg:edge_stack:promote_freeze
Fields:
  - active: "1"
  - until_ts_ms: epoch ms
  - set_ts_ms: epoch ms
  - reason: short string
  - source: e.g., monitoring_smoke
"""

from utils.time_utils import get_ny_time_millis

import os
import time
from dataclasses import dataclass
from typing import Dict, Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


@dataclass
class FreezeState:
    active: bool
    until_ts_ms: int
    reason: str
    source: str


def freeze_key() -> str:
    return os.getenv("EDGE_STACK_PROMOTE_FREEZE_KEY", "cfg:edge_stack:promote_freeze")


def _client(redis_url: str):
    if redis is None:
        return None
    return redis.Redis.from_url(redis_url, decode_responses=True)


def read_freeze(redis_url: str) -> FreezeState:
    r = _client(redis_url)
    if r is None:
        return FreezeState(active=False, until_ts_ms=0, reason="redis_unavailable", source="")
    try:
        d = r.hgetall(freeze_key()) or {}
        until_ts_ms = int(float(d.get("until_ts_ms", "0") or 0))
        now_ms = get_ny_time_millis()
        active = until_ts_ms > now_ms
        if (not active) and until_ts_ms > 0:
            try:
                r.delete(freeze_key())
            except Exception:
                pass
        return FreezeState(
            active=active,
            until_ts_ms=until_ts_ms,
            reason=str(d.get("reason", "") or ""),
            source=str(d.get("source", "") or ""),
        )
    except Exception:
        return FreezeState(active=False, until_ts_ms=0, reason="read_error", source="")


def set_freeze(redis_url: str, duration_s: int, reason: str, source: str = "monitoring_smoke", extra: Optional[Dict[str, str]] = None) -> bool:
    r = _client(redis_url)
    if r is None:
        return False
    now_ms = get_ny_time_millis()
    until_ts_ms = now_ms + int(duration_s) * 1000
    payload = {
        "active": "1",
        "until_ts_ms": str(until_ts_ms),
        "set_ts_ms": str(now_ms),
        "reason": str(reason or "unspecified")[:400],
        "source": str(source or "")[:64],
    }
    if extra:
        for k, v in extra.items():
            if v is None:
                continue
            payload[str(k)[:64]] = str(v)[:800]
    try:
        r.hset(freeze_key(), mapping=payload)
        r.expire(freeze_key(), max(60, int(duration_s) + 172800))
        return True
    except Exception:
        return False


def clear_freeze(redis_url: str) -> bool:
    r = _client(redis_url)
    if r is None:
        return False
    try:
        r.delete(freeze_key())
        return True
    except Exception:
        return False
