#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from typing import Any, Dict

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Gauge, start_http_server

SCOPES = ("edge", "pipeline", "entry_policy")
THR_METRICS = (
    "threshold_is_p95_bps",
    "threshold_perm_impact_p95_bps",
    "threshold_realized_spread_p50_bps",
)
OUTCOMES = ("apply", "veto", "pass")


def _now_s() -> float:
    return time.time()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


UP = Gauge("exec_health_slo_exporter_up", "1 if exporter can read Redis summary")
LAST_UPDATED_MS = Gauge("exec_health_slo_last_updated_ts_ms", "Last SLO summary updated_ts_ms")
LAST_AGE_S = Gauge("exec_health_slo_last_age_seconds", "Age of last SLO summary in seconds")
ACTIVE = Gauge("exec_health_slo_active_instances", "Active ExecHealth instances by scope", ["scope"])
STALE = Gauge("exec_health_slo_stale_instances", "Stale ExecHealth instances by scope", ["scope"])
SHARE = Gauge("exec_health_slo_share", "ExecHealth share by scope/outcome", ["scope", "outcome"])
MODE_DISTINCT = Gauge("exec_health_slo_scope_mode_distinct", "Distinct effective modes by scope", ["scope"])
DEPLOY_DISTINCT = Gauge("exec_health_slo_scope_deploy_distinct", "Distinct deploy ids by scope", ["scope"])
THRESH_DISTINCT = Gauge("exec_health_slo_scope_threshold_distinct", "Distinct threshold values by scope/metric", ["scope", "metric"])
CROSS_SCOPE_MODE_DISTINCT = Gauge("exec_health_slo_cross_scope_mode_distinct", "Distinct modal modes across scopes")
CROSS_SCOPE_THRESH_DISTINCT = Gauge("exec_health_slo_cross_scope_threshold_distinct", "Distinct modal thresholds across scopes", ["metric"])
DRIFT_TOTAL = Gauge("exec_health_slo_rollout_drift_instances_total", "Total instances with rollout drift")
DRIFT_SCOPE = Gauge("exec_health_slo_rollout_drift_instances", "Instances with rollout drift by scope", ["scope"])
STALE_TOTAL = Gauge("exec_health_slo_stale_instances_total", "Total stale instances")


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    key = os.getenv("EXEC_HEALTH_SLO_SUMMARY_KEY", "metrics:exec_health:slo:last")
    port = int(os.getenv("EXEC_HEALTH_SLO_EXPORTER_PORT", "9824"))
    interval_s = float(os.getenv("EXEC_HEALTH_SLO_EXPORTER_INTERVAL_S", "10") or 10)
    stale_s = float(os.getenv("EXEC_HEALTH_SLO_EXPORTER_STALE_S", "180") or 180)

    if redis is None:
        raise RuntimeError("redis dependency missing")
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)

    while True:
        try:
            m: Dict[str, Any] = r.hgetall(key) or {}
            UP.set(1.0)
            updated_ms = _i(m.get("updated_ts_ms"), 0)
            LAST_UPDATED_MS.set(float(updated_ms))
            age_s = max(0.0, _now_s() - (float(updated_ms) / 1000.0 if updated_ms > 0 else 0.0))
            LAST_AGE_S.set(age_s)

            ACTIVE.labels(scope="all").set(float(_i(m.get("active_instances_total"), 0)))
            STALE_TOTAL.set(float(_i(m.get("stale_instances_total"), 0)))
            DRIFT_TOTAL.set(float(_i(m.get("rollout_drift_instances_total"), 0)))
            CROSS_SCOPE_MODE_DISTINCT.set(float(_i(m.get("cross_scope_mode_distinct"), 0)))
            for metric in THR_METRICS:
                CROSS_SCOPE_THRESH_DISTINCT.labels(metric=metric).set(float(_i(m.get(f"cross_scope_distinct_{metric}"), 0)))

            for scope in SCOPES:
                ACTIVE.labels(scope=scope).set(float(_i(m.get(f"active_instances_{scope}"), 0)))
                STALE.labels(scope=scope).set(float(_i(m.get(f"stale_instances_{scope}"), 0)))
                MODE_DISTINCT.labels(scope=scope).set(float(_i(m.get(f"mode_distinct_{scope}"), 0)))
                DEPLOY_DISTINCT.labels(scope=scope).set(float(_i(m.get(f"deploy_distinct_{scope}"), 0)))
                DRIFT_SCOPE.labels(scope=scope).set(float(_i(m.get(f"rollout_drift_instances_{scope}"), 0)))
                for outcome in OUTCOMES:
                    SHARE.labels(scope=scope, outcome=outcome).set(float(_f(m.get(f"share_{outcome}_{scope}"), 0.0)))
                for metric in THR_METRICS:
                    THRESH_DISTINCT.labels(scope=scope, metric=metric).set(float(_i(m.get(f"threshold_distinct_{scope}_{metric}"), 0)))

            if age_s > stale_s:
                UP.set(0.0)
        except Exception:
            UP.set(0.0)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
