"""LONG-cooldown reader (P1.D, 2026-05-27).

EntryPolicyGate consumes this on every LONG signal. Cooldown state lives in
Redis HASH `risk:cooldown:long:{SYMBOL}` written by `services/long_cooldown_manager_v1.py`
on every loss (trade_closed event with r_multiple ≤ 0).

State shape:
  count:                 int — consecutive LONG losses streak
  last_loss_ms:          int — epoch ms of last loss
  expires_at_ms:         int — when cooldown auto-expires (TTL-driven)
  active:                "0"|"1" — armed flag

Active iff:
  - active == "1"
  - now_ms < expires_at_ms

Fail-open: any exception → not active.

ENV (read by gate, mirrored here for documentation):
  COOLDOWN_LONG_ENABLED          0|1     master switch in gate
  COOLDOWN_LONG_AFTER_LOSSES     3       streak to trigger (writer-side)
  COOLDOWN_LONG_TTL_SEC          1800    cooldown duration (writer-side)
  COOLDOWN_LONG_MODE             enforce|shadow
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_KEY_PREFIX = "risk:cooldown:long:"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _redis_url() -> str:
    return (
        os.environ.get("LONG_COOLDOWN_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://redis-worker-1:6379/0"
    )


# Lazy singleton — мы не хотим создавать клиент per-call.
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
                logger.debug("long_cooldown_reader: redis init fail (fail-open): %s", e)
                _RC = None
        return _RC


def _read_state(redis_client: Any, symbol: str) -> dict[str, str] | None:
    try:
        raw = redis_client.hgetall(_KEY_PREFIX + symbol.upper())
    except Exception:
        return None
    if not raw:
        return None
    norm: dict[str, str] = {}
    for k, v in raw.items():
        ks = k.decode() if isinstance(k, bytes) else k
        vs = v.decode() if isinstance(v, bytes) else v
        norm[str(ks)] = str(vs)
    return norm or None


def is_long_cooldown_active(ctx: Any, symbol: str) -> tuple[bool, str]:
    """Return (active, notes). Fail-open on any error."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return False, ""

    # Prefer the redis client on ctx (tests inject); fall back to lazy singleton.
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

    state = _read_state(rc, sym)
    if not state:
        return False, ""

    active = (state.get("active") or "0").strip()
    if active not in ("1", "true", "True"):
        return False, ""

    try:
        exp_ms = int(state.get("expires_at_ms") or 0)
    except Exception:
        exp_ms = 0

    now = _now_ms()
    if exp_ms > 0 and now >= exp_ms:
        return False, "expired"

    count = state.get("count") or "?"
    last_loss = state.get("last_loss_ms") or "?"
    remaining_s = max(0, (exp_ms - now) // 1000) if exp_ms > 0 else 0
    return True, f"streak={count} last_loss_ms={last_loss} remaining_s={remaining_s}"
