from __future__ import annotations

#!/usr/bin/env python3
# exec_health_slo_autoguard_exporter_v1.py
# P5 AutoGuard Prometheus exporter.
# Reads metrics:exec_health:slo:autoguard:state (written by exec_health_slo_autoguard_v1.py)
# and exposes Prometheus metrics on EXEC_HEALTH_SLO_AUTOGUARD_EXPORTER_PORT (default 9825).
import os
import time
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Gauge, start_http_server


def _now_s() -> float:
    return time.time()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


# Exporter liveness — 1 if last Redis read succeeded, 0 on error
UP = Gauge("exec_health_slo_autoguard_exporter_up", "1 if exporter can read autoguard state")

# Freeze state
FREEZE_ACTIVE = Gauge("exec_health_slo_autoguard_freeze_active", "Autoguard freeze active (1/0)")
FREEZE_REMAINING_S = Gauge(
    "exec_health_slo_autoguard_freeze_remaining_seconds", "Remaining freeze duration in seconds"
)

# Input condition booleans (as set by autoguard)
MODE_MISMATCH_ACTIVE = Gauge(
    "exec_health_slo_autoguard_mode_mismatch_active", "Mode mismatch currently active"
)
ROLLING_DRIFT_ACTIVE = Gauge(
    "exec_health_slo_autoguard_rollout_drift_active", "Rollout drift currently active"
)

# Trigger / rollback timestamps (ms epoch)
LAST_TRIGGER_MS = Gauge(
    "exec_health_slo_autoguard_last_trigger_ts_ms", "Last autoguard trigger ts_ms"
)
LAST_ROLLBACK_MS = Gauge(
    "exec_health_slo_autoguard_last_rollback_ts_ms", "Last rollback ts_ms"
)

# Cumulative rollback counter
ROLLBACK_TOTAL = Gauge(
    "exec_health_slo_autoguard_rollback_total", "Total rollbacks performed by autoguard"
)

# State freshness
STATE_AGE_S = Gauge(
    "exec_health_slo_autoguard_state_age_seconds", "Age of autoguard state in seconds"
)


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    key = os.getenv("EXEC_HEALTH_SLO_AUTOGUARD_STATE_KEY", "metrics:exec_health:slo:autoguard:state")
    port = int(os.getenv("EXEC_HEALTH_SLO_AUTOGUARD_EXPORTER_PORT", "9825"))
    interval_s = float(os.getenv("EXEC_HEALTH_SLO_AUTOGUARD_EXPORTER_INTERVAL_S", "10") or 10)
    if redis is None:
        raise RuntimeError("redis dependency missing")
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)
    while True:
        try:
            d: dict[str, Any] = r.hgetall(key) or {}
            now_s = _now_s()
            UP.set(1.0)
            updated_ms = _i(d.get("updated_ts_ms"), 0)
            age_s = max(0.0, now_s - (float(updated_ms) / 1000.0 if updated_ms > 0 else 0.0))
            STATE_AGE_S.set(age_s)
            freeze_until = _i(d.get("freeze_until_ts_ms"), 0)
            FREEZE_ACTIVE.set(float(_i(d.get("freeze_active"), 0)))
            FREEZE_REMAINING_S.set(
                max(0.0, (float(freeze_until) / 1000.0) - now_s) if freeze_until > 0 else 0.0
            )
            MODE_MISMATCH_ACTIVE.set(float(_i(d.get("mode_mismatch_active"), 0)))
            ROLLING_DRIFT_ACTIVE.set(float(_i(d.get("rollout_drift_active"), 0)))
            LAST_TRIGGER_MS.set(float(_i(d.get("last_trigger_ts_ms"), 0)))
            LAST_ROLLBACK_MS.set(float(_i(d.get("last_rollback_ts_ms"), 0)))
            ROLLBACK_TOTAL.set(float(_i(d.get("rollback_total"), 0)))
        except Exception:
            UP.set(0.0)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
