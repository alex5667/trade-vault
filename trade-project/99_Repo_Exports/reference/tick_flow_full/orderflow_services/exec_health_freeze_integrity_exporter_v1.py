#!/usr/bin/env python3
from __future__ import annotations

"""P8 ExecHealth Freeze Integrity Exporter.

Reads the freeze control hash, autoguard state hash, and freeze event stream
and evaluates integrity violations using the pure-logic evaluator from
services.orderflow.exec_health_freeze_integrity.

Prometheus metrics exposed on EXEC_HEALTH_FREEZE_INTEGRITY_EXPORTER_PORT (default 9828).

Violations detected:
  control_missing_pending_ack           — control hash deleted with pending nonce
  state_missing_pending_ack             — state hash deleted with pending nonce
  control_state_missing_without_valid_ack — both gone, trigger event still in stream
  thaw_without_valid_ack_event          — thaw in control but no valid signed event
  invalid_ack_event_signature           — ack event has invalid HMAC
  invalid_control_ack_signature         — thaw in control has invalid HMAC
  none                                  — no violations
"""

import os
import time
from typing import Any, Dict, List, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Gauge, start_http_server

from services.orderflow.exec_health_freeze_integrity import VIOLATION_KINDS, evaluate_freeze_integrity


def _now_ms() -> int:
    return int(time.time() * 1000)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


# ── Prometheus metrics ──────────────────────────────────────────────────────
UP = Gauge("exec_health_freeze_integrity_exporter_up", "1 if exporter can read freeze control integrity state")
CONTROL_PRESENT = Gauge("exec_health_freeze_integrity_control_present", "1 if freeze control hash is present")
STATE_PRESENT = Gauge("exec_health_freeze_integrity_state_present", "1 if autoguard state hash is present")
PENDING_ACK = Gauge("exec_health_freeze_integrity_pending_ack", "1 if a pending signed manual ack is still required")
VALID_ACK_EVENT = Gauge("exec_health_freeze_integrity_valid_ack_event_present", "1 if a valid signed ack event exists for the current nonce")
INVALID_ACK_EVENT = Gauge("exec_health_freeze_integrity_invalid_ack_event_present", "1 if an invalid ack event was observed")
VIOLATION = Gauge("exec_health_freeze_integrity_violation", "One-hot freeze integrity violations", ["kind"])
LAST_TRIGGER_TS_MS = Gauge("exec_health_freeze_integrity_last_trigger_ts_ms", "Latest trigger ts referenced by control/state or stream")
STATE_AGE_S = Gauge("exec_health_freeze_integrity_state_age_seconds", "Max age of control/state hashes")


def _read_events(r: Any, key: str, count: int) -> List[Tuple[str, Dict[str, Any]]]:
    """Read the most recent events from a Redis stream (reverse order, newest first)."""
    try:
        rows = r.xrevrange(key, count=max(1, int(count))) or []
        return [(str(eid), dict(payload or {})) for eid, payload in rows]
    except Exception:
        return []


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    control_key = os.getenv("EXEC_HEALTH_FREEZE_CONTROL_KEY", "cfg:orderflow:exec_health:freeze_control:v1")
    state_key = os.getenv("EXEC_HEALTH_SLO_AUTOGUARD_STATE_KEY", "metrics:exec_health:slo:autoguard:state")
    event_stream = os.getenv("EXEC_HEALTH_FREEZE_EVENT_STREAM", "ops:exec_health:freeze_events:v1")
    port = int(os.getenv("EXEC_HEALTH_FREEZE_INTEGRITY_EXPORTER_PORT", "9828"))
    interval_s = float(os.getenv("EXEC_HEALTH_FREEZE_INTEGRITY_EXPORTER_INTERVAL_S", "10") or 10)
    event_count = int(os.getenv("EXEC_HEALTH_FREEZE_INTEGRITY_EVENT_COUNT", "100") or 100)
    if redis is None:
        raise RuntimeError("redis dependency missing")
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)
    while True:
        try:
            control = r.hgetall(control_key) or {}
            state = r.hgetall(state_key) or {}
            events = _read_events(r, event_stream, event_count)
            res = evaluate_freeze_integrity(control_raw=control, state_raw=state, events=events, now_ms=_now_ms())
            UP.set(1.0)
            CONTROL_PRESENT.set(1.0 if res.control_present else 0.0)
            STATE_PRESENT.set(1.0 if res.state_present else 0.0)
            # pending ack: nonce exists but no valid ack event yet
            PENDING_ACK.set(1.0 if res.pending_ack_nonce and not res.valid_ack_event_present else 0.0)
            VALID_ACK_EVENT.set(1.0 if res.valid_ack_event_present else 0.0)
            INVALID_ACK_EVENT.set(1.0 if res.invalid_ack_event_present else 0.0)
            LAST_TRIGGER_TS_MS.set(float(res.pending_trigger_ts_ms or 0))
            # compute max age of control/state
            now_ms = _now_ms()
            ages = []
            if control:
                ages.append(max(0.0, float(now_ms - _i(control.get("updated_ts_ms"), 0)) / 1000.0))
            if state:
                ages.append(max(0.0, float(now_ms - _i(state.get("updated_ts_ms"), 0)) / 1000.0))
            STATE_AGE_S.set(max(ages) if ages else 0.0)
            # one-hot violation labels
            active = set(res.violation_kinds)
            for kind in VIOLATION_KINDS:
                VIOLATION.labels(kind=kind).set(1.0 if kind in active else 0.0)
        except Exception:
            UP.set(0.0)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
