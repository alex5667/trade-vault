"""
core/order_execution_events.py — Plan 3 / Step 2 TCA lifecycle emitter API.

Pure-function library that callers (signal pipeline, executor, broker bridge)
use to record stage transitions. All emits go to a single bounded Redis stream
(`stream:order_exec_events`); a separate writer service drains it into the
`order_execution_events` Timescale hypertable.

Design constraints:
  * NO synchronous DB write on the hot path — emitter only XADDs.
  * Fail-open: on any error the call returns False; the trading path MUST NOT
    raise because of TCA bookkeeping.
  * Master switch ORDER_EXEC_EVENTS_ENABLED=0 turns every emit into a no-op
    that returns True (so callers don't change behavior when telemetry is off).
  * Validates the stage against ALLOWED_STAGES at runtime — typos become
    obvious in test, not in production grep.
"""
from __future__ import annotations

import json
import os
import time
from enum import StrEnum
from typing import Any

# Single source of truth for stage strings (matches the migration comment).
ALLOWED_STAGES: frozenset[str] = frozenset({
    "DECISION",
    "SIGNAL_PUBLISHED",
    "ORDER_QUEUE_XADD",
    "GATEWAY_READ",
    "BROKER_SEND",
    "BROKER_ACK",
    "FILL",
    "PARTIAL_FILL",
    "CLOSE",
    "REJECT",
    "CANCEL",
})


class Stage(StrEnum):
    DECISION = "DECISION"
    SIGNAL_PUBLISHED = "SIGNAL_PUBLISHED"
    ORDER_QUEUE_XADD = "ORDER_QUEUE_XADD"
    GATEWAY_READ = "GATEWAY_READ"
    BROKER_SEND = "BROKER_SEND"
    BROKER_ACK = "BROKER_ACK"
    FILL = "FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    CLOSE = "CLOSE"
    REJECT = "REJECT"
    CANCEL = "CANCEL"


_ENABLED_CACHE: dict[str, bool] = {}


def _is_enabled() -> bool:
    """Check ORDER_EXEC_EVENTS_ENABLED ENV (cached at process start)."""
    if "v" in _ENABLED_CACHE:
        return _ENABLED_CACHE["v"]
    raw = os.environ.get("ORDER_EXEC_EVENTS_ENABLED", "0").strip().lower()
    val = raw in ("1", "true", "yes", "on")
    _ENABLED_CACHE["v"] = val
    return val


def _reset_enabled_cache() -> None:
    """Test-only: re-read the env."""
    _ENABLED_CACHE.clear()


def build_event(
    *,
    sid: str,
    stage: str,
    symbol: str,
    side: int,
    status: str,
    ts_ms: int | None = None,
    seq: int = 0,
    venue: str | None = None,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    px: float | None = None,
    qty: float | None = None,
    notional_usd: float | None = None,
    reason_code: str | None = None,
    latency_ms: float | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the canonical event dict for XADD or DB insert.

    Pure — no side effects. Raises ValueError on malformed input so tests catch
    contract drift; production caller wraps in try/except and counts failures.
    """
    if stage not in ALLOWED_STAGES:
        raise ValueError(f"invalid stage: {stage!r}; allowed: {sorted(ALLOWED_STAGES)}")
    if not sid:
        raise ValueError("sid required")
    if not symbol:
        raise ValueError("symbol required")
    if side not in (-1, 1):
        raise ValueError(f"side must be +1 or -1, got {side!r}")
    if not status:
        raise ValueError("status required")

    out: dict[str, Any] = {
        "ts_ms": int(ts_ms if ts_ms is not None else time.time() * 1000),
        "sid": sid,
        "stage": stage,
        "seq": int(seq),
        "symbol": symbol.upper(),
        "side": int(side),
        "status": status,
    }
    if venue is not None:
        out["venue"] = venue
    if client_order_id is not None:
        out["client_order_id"] = client_order_id
    if exchange_order_id is not None:
        out["exchange_order_id"] = exchange_order_id
    if px is not None:
        out["px"] = float(px)
    if qty is not None:
        out["qty"] = float(qty)
    if notional_usd is not None:
        out["notional_usd"] = float(notional_usd)
    if reason_code is not None:
        out["reason_code"] = reason_code
    if latency_ms is not None:
        out["latency_ms"] = float(latency_ms)
    out["payload"] = json.dumps(payload or {}, default=str)
    return out


def emit(
    rc: Any,
    *,
    sid: str,
    stage: str,
    symbol: str,
    side: int,
    status: str,
    **kwargs: Any,
) -> bool:
    """Emit a stage event to the Redis stream (sync caller).

    Returns True on success (or when SHADOW mode short-circuits), False on any
    failure. Never raises — TCA bookkeeping must not break the trading path.
    """
    if not _is_enabled():
        return True  # SHADOW: caller-visible no-op
    if rc is None:
        return False
    try:
        ev = build_event(sid=sid, stage=stage, symbol=symbol, side=side, status=status, **kwargs)
    except ValueError:
        return False
    try:
        from core.redis_keys import RedisStreams as RS
        stream_key = RS.ORDER_EXEC_EVENTS
        rc.xadd(stream_key, ev, maxlen=30_000, approximate=True)
        return True
    except Exception:
        return False


async def async_emit(
    rc: Any,
    *,
    sid: str,
    stage: str,
    symbol: str,
    side: int,
    status: str,
    **kwargs: Any,
) -> bool:
    """Async variant of `emit` for callers that hold an aioredis client.

    Same fail-open contract as `emit`. Use this from coroutine code (e.g. the
    signal pipeline's `_publish_of_inputs`). Never raises.
    """
    if not _is_enabled():
        return True
    if rc is None:
        return False
    try:
        ev = build_event(sid=sid, stage=stage, symbol=symbol, side=side, status=status, **kwargs)
    except ValueError:
        return False
    try:
        from core.redis_keys import RedisStreams as RS
        stream_key = RS.ORDER_EXEC_EVENTS
        await rc.xadd(stream_key, ev, maxlen=30_000, approximate=True)
        return True
    except Exception:
        return False
