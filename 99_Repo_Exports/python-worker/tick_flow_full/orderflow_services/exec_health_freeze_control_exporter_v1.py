#!/usr/bin/env python3
from __future__ import annotations

"""Prometheus exporter for ExecHealth freeze control state (P7).

Reads cfg:orderflow:exec_health:freeze_control:v1 (the latched control hash)
and exposes metrics on port EXEC_HEALTH_FREEZE_CONTROL_EXPORTER_PORT (default 9827).

Metrics
-------
exec_health_freeze_control_exporter_up         — 1 if exporter can read freeze control state
exec_health_freeze_control_effective_active    — 1 if system is currently freeze-blocked
exec_health_freeze_control_manual_ack_required — 1 if operator must ack before thaw
exec_health_freeze_control_manual_override_active — 1 if operator override is active
exec_health_freeze_control_state_age_seconds   — seconds since control hash was last written
exec_health_freeze_control_manual_ack_age_seconds — seconds since last manual ack
exec_health_freeze_control_trigger_total       — cumulative autoguard latches recorded
exec_health_freeze_control_thaw_total          — cumulative manual thaw acks
exec_health_freeze_control_manual_freeze_total — cumulative operator force-freezes
exec_health_freeze_control_source{source}      — one-hot: current control_source label

Run
---
python3 -m orderflow_services.exec_health_freeze_control_exporter_v1
"""

import os
import time
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from prometheus_client import Gauge, start_http_server

from services.orderflow.exec_health_freeze_control import parse_exec_health_freeze_control


def _now_s() -> float:
    return time.time()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


# ── Prometheus gauges ─────────────────────────────────────────────────────────
UP = Gauge("exec_health_freeze_control_exporter_up", "1 if exporter can read freeze control state")
EFFECTIVE_ACTIVE = Gauge("exec_health_freeze_control_effective_active", "Effective ExecHealth freeze state (1/0)")
MANUAL_ACK_REQUIRED = Gauge("exec_health_freeze_control_manual_ack_required", "Whether manual ack is required before thaw")
MANUAL_OVERRIDE_ACTIVE = Gauge("exec_health_freeze_control_manual_override_active", "Whether a manual operator override is active")
STATE_AGE_S = Gauge("exec_health_freeze_control_state_age_seconds", "Age of freeze control state in seconds")
MANUAL_ACK_AGE_S = Gauge("exec_health_freeze_control_manual_ack_age_seconds", "Age of the last manual ack in seconds")
TRIGGER_TOTAL = Gauge("exec_health_freeze_control_trigger_total", "Total autoguard latches recorded in control state")
THAW_TOTAL = Gauge("exec_health_freeze_control_thaw_total", "Total manual thaw acknowledgements")
MANUAL_FREEZE_TOTAL = Gauge("exec_health_freeze_control_manual_freeze_total", "Total manual freeze overrides")
SOURCE = Gauge("exec_health_freeze_control_source", "One-hot current freeze source", ["source"])

SOURCES = ["none", "autoguard", "manual_override_thaw", "manual_override_freeze"]


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    key = os.getenv("EXEC_HEALTH_FREEZE_CONTROL_KEY", "cfg:orderflow:exec_health:freeze_control:v1")
    port = int(os.getenv("EXEC_HEALTH_FREEZE_CONTROL_EXPORTER_PORT", "9827"))
    interval_s = float(os.getenv("EXEC_HEALTH_FREEZE_CONTROL_EXPORTER_INTERVAL_S", "10") or 10)

    if redis is None:
        raise RuntimeError("redis dependency missing")

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)

    while True:
        try:
            d: dict[str, Any] = r.hgetall(key) or {}
            st = parse_exec_health_freeze_control(d)
            now_s = _now_s()

            UP.set(1.0)

            updated_ms = _i(d.get("updated_ts_ms"), 0)
            STATE_AGE_S.set(max(0.0, now_s - (float(updated_ms) / 1000.0 if updated_ms > 0 else 0.0)))

            ack_ms = _i(d.get("manual_ack_ts_ms"), 0)
            # If no ack yet, show age as 0 (not since epoch)
            MANUAL_ACK_AGE_S.set(max(0.0, now_s - (float(ack_ms) / 1000.0 if ack_ms > 0 else now_s)))

            EFFECTIVE_ACTIVE.set(1.0 if st.effective_freeze_active else 0.0)
            MANUAL_ACK_REQUIRED.set(1.0 if st.manual_ack_required else 0.0)
            MANUAL_OVERRIDE_ACTIVE.set(1.0 if st.manual_override_active else 0.0)
            TRIGGER_TOTAL.set(float(_i(d.get("trigger_total"), 0)))
            THAW_TOTAL.set(float(_i(d.get("thaw_total"), 0)))
            MANUAL_FREEZE_TOTAL.set(float(_i(d.get("manual_freeze_total"), 0)))

            current_source = st.control_source or "none"
            for src in SOURCES:
                SOURCE.labels(source=src).set(1.0 if src == current_source else 0.0)

        except Exception:
            UP.set(0.0)

        time.sleep(interval_s)


if __name__ == "__main__":
    main()
