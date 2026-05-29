"""Sweep-detector reader (P2.C, 2026-05-27).

EntryPolicyGate calls this to know whether recent stop-hunt sweep happened on
a symbol within the last N minutes. Writer: services/sweep_detector_writer_v1.py.

State shape (Redis HASH `ctx:sweep:{SYMBOL}`):
  last_sweep_ms          int — epoch ms of last detected sweep
  direction              "up"|"down" — sweep direction (took out highs vs lows)
  levels_swept           int — how many stop levels swept (≥1)
  magnitude_bps          float — sweep amplitude in bps
  ttl_ms                 int — expiration time

Active iff:
  - last_sweep_ms within `SWEEP_HUNT_WINDOW_SEC` seconds (default 900 = 15min)
  - levels_swept ≥ `SWEEP_HUNT_MIN_LEVELS` (default 2)
  - magnitude_bps ≥ `SWEEP_HUNT_MIN_BPS` (default 30)

LONG-block applies only when direction=="up" (took out highs = buyers trapped).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_KEY_PREFIX = "ctx:sweep:"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _redis_url() -> str:
    return (
        os.environ.get("SWEEP_DETECTOR_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://redis-worker-1:6379/0"
    )


_RC: Any = None
_RC_LOCK = threading.Lock()


def _get_redis() -> Any:
    global _RC
    if _RC is not None:
        return _RC
    with _RC_LOCK:
        if _RC is None:
            try:
                import redis  # type: ignore
                _RC = redis.from_url(_redis_url(), decode_responses=True, socket_timeout=0.5)
            except Exception as e:
                logger.debug("sweep_reader: redis init fail (fail-open): %s", e)
                _RC = None
        return _RC


def is_recent_sweep(ctx: Any, symbol: str) -> tuple[bool, str]:
    """Return (hit, notes). Hit=True iff recent sweep against LONG side
    (took out highs) within the configured window.

    Fail-open on any error.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return False, ""

    rc = None
    try:
        _ctx_rc = getattr(ctx, "redis", None) or getattr(ctx, "redis_client", None)
        if _ctx_rc is not None:
            _mod = type(_ctx_rc).__module__ or ""
            if "asyncio" not in _mod and "aioredis" not in _mod:
                rc = _ctx_rc
    except Exception:
        rc = None
    if rc is None:
        rc = _get_redis()
    if rc is None:
        return False, ""

    try:
        raw = rc.hgetall(_KEY_PREFIX + sym)
    except Exception:
        return False, ""
    if not raw:
        return False, ""

    state: dict[str, str] = {}
    for k, v in raw.items():
        ks = k.decode() if isinstance(k, bytes) else k
        vs = v.decode() if isinstance(v, bytes) else v
        state[str(ks)] = str(vs)

    try:
        last_ms = int(state.get("last_sweep_ms") or 0)
    except Exception:
        last_ms = 0
    if last_ms <= 0:
        return False, ""

    window_sec = max(60, int(os.environ.get("SWEEP_HUNT_WINDOW_SEC", "900") or 900))
    if _now_ms() - last_ms > window_sec * 1000:
        return False, "expired"

    direction = (state.get("direction") or "").strip().lower()
    # Only block LONG when sweep took out HIGHS (direction=up = buyers stop-hunted).
    if direction != "up":
        return False, f"dir={direction}"

    try:
        levels = int(state.get("levels_swept") or 0)
    except Exception:
        levels = 0
    min_levels = max(1, int(os.environ.get("SWEEP_HUNT_MIN_LEVELS", "2") or 2))
    if levels < min_levels:
        return False, f"levels={levels}/{min_levels}"

    try:
        magnitude = float(state.get("magnitude_bps") or 0.0)
    except Exception:
        magnitude = 0.0
    min_bps = float(os.environ.get("SWEEP_HUNT_MIN_BPS", "30") or 30)
    if magnitude < min_bps:
        return False, f"mag={magnitude:.1f}bps<{min_bps:.1f}"

    age_s = (_now_ms() - last_ms) // 1000
    return True, f"sweep_up age_s={age_s} levels={levels} mag={magnitude:.1f}bps"
