#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""P9 dual-control thaw state exporter for Prometheus.

Reads the freeze control hash, state hash, and event stream from Redis,
evaluates the dual-control thaw chain via evaluate_freeze_dual_control,
and exposes metrics on EXEC_HEALTH_FREEZE_DUAL_CONTROL_EXPORTER_PORT (default 9829).

Metrics exposed:
  exec_health_freeze_dual_control_exporter_up           — 1 if Redis read succeeded
  exec_health_freeze_dual_control_pending_request       — 1 if thaw request is pending
  exec_health_freeze_dual_control_ready                 — 1 if prepare+approve done by distinct operators
  exec_health_freeze_dual_control_valid_commit_event_present — 1 if valid signed commit exists
  exec_health_freeze_dual_control_same_operator_violation    — 1 if preparer == approver
  exec_health_freeze_dual_control_status{status}        — one-hot by request status
  exec_health_freeze_dual_control_violation{kind}       — one-hot by violation kind
  exec_health_freeze_dual_control_request_age_seconds   — seconds since prepare-thaw was called
"""

import os
import time
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Gauge, start_http_server

from services.orderflow.exec_health_freeze_dual_control import (
    DUAL_CONTROL_VIOLATION_KINDS,
    evaluate_freeze_dual_control,
)


def _read_events(r: Any, key: str, count: int) -> list[tuple[str, dict[str, Any]]]:
    try:
        rows = r.xrevrange(key, count=max(1, int(count))) or []
        return [(str(eid), dict(payload or {})) for eid, payload in rows]
    except Exception:
        return []


UP = Gauge('exec_health_freeze_dual_control_exporter_up', '1 if exporter can read dual-control state')
PENDING = Gauge('exec_health_freeze_dual_control_pending_request', '1 if thaw request is pending')
READY = Gauge('exec_health_freeze_dual_control_ready', '1 if request has valid prepare+approve by distinct operators')
VALID_COMMIT = Gauge('exec_health_freeze_dual_control_valid_commit_event_present', '1 if a valid signed commit event exists')
SAME_OPERATOR = Gauge('exec_health_freeze_dual_control_same_operator_violation', '1 if preparer and approver are identical')
STATUS = Gauge('exec_health_freeze_dual_control_status', 'One-hot dual-control request status', ['status'])
VIOLATION = Gauge('exec_health_freeze_dual_control_violation', 'One-hot dual-control violations', ['kind'])
REQUEST_AGE = Gauge('exec_health_freeze_dual_control_request_age_seconds', 'Age of current thaw request in seconds')


def main() -> None:
    redis_url = os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')
    control_key = os.getenv('EXEC_HEALTH_FREEZE_CONTROL_KEY', 'cfg:orderflow:exec_health:freeze_control:v1')
    state_key = os.getenv('EXEC_HEALTH_SLO_AUTOGUARD_STATE_KEY', 'metrics:exec_health:slo:autoguard:state')
    event_stream = os.getenv('EXEC_HEALTH_FREEZE_EVENT_STREAM', 'ops:exec_health:freeze_events:v1')
    port = int(os.getenv('EXEC_HEALTH_FREEZE_DUAL_CONTROL_EXPORTER_PORT', '9829'))
    interval_s = float(os.getenv('EXEC_HEALTH_FREEZE_DUAL_CONTROL_EXPORTER_INTERVAL_S', '10') or 10)
    event_count = int(os.getenv('EXEC_HEALTH_FREEZE_DUAL_CONTROL_EVENT_COUNT', '100') or 100)
    if redis is None:
        raise RuntimeError('redis dependency missing')
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)
    while True:
        try:
            control = r.hgetall(control_key) or {}
            state = r.hgetall(state_key) or {}
            events = _read_events(r, event_stream, event_count)
            res = evaluate_freeze_dual_control(control_raw=control, state_raw=state, events=events)
            UP.set(1.0)
            PENDING.set(1.0 if res.pending_request else 0.0)
            READY.set(1.0 if res.dual_control_ready else 0.0)
            VALID_COMMIT.set(1.0 if res.valid_commit_event_present else 0.0)
            SAME_OPERATOR.set(1.0 if res.same_operator_violation else 0.0)
            for status in ['none', 'prepared', 'approved', 'committed', 'legacy_committed']:
                STATUS.labels(status=status).set(1.0 if (res.request_status or 'none') == status else 0.0)
            active = set(res.violation_kinds)
            for kind in DUAL_CONTROL_VIOLATION_KINDS:
                VIOLATION.labels(kind=kind).set(1.0 if kind in active else 0.0)
            age_s = 0.0
            for raw in (control, state):
                if raw and raw.get('thaw_prepare_ts_ms'):
                    age_s = max(age_s, max(0.0, (get_ny_time_millis() - float(raw.get('thaw_prepare_ts_ms', 0)))/1000.0))
            REQUEST_AGE.set(age_s)
        except Exception:
            UP.set(0.0)
        time.sleep(interval_s)


if __name__ == '__main__':
    main()
