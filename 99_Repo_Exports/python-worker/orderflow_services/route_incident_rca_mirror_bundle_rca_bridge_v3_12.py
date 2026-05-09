from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None

APP_NAME = "route_incident_rca_mirror_bundle_rca_bridge_v3_12"
BUNDLES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_BUNDLES_STREAM", "stream:ml:route_incident_rca_mirror_incident_bundles")
VERTEX_RCA_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_STREAM", "stream:ml:route_incident_rca_mirror_vertex_rca_requests")
LOCAL_FALLBACK_STREAM = os.getenv("ML_LOCAL_FALLBACK_REQUESTS_STREAM", "stream:ml:local_fallback_requests")

DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_bridge_decisions")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_bridge_audit")
LAST_HASH = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_LAST_HASH", "metrics:ml:route_incident_rca_mirror_rca_bridge:last")
VERTEX_HEALTH_HASH = os.getenv("ML_VERTEX_HEALTH_LAST_HASH", "metrics:ml:vertex_health:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_PORT", "9930"))
MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_MODE", "AUTO")  # AUTO, VERTEX_ONLY, LOCAL_ONLY, DISABLED
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_MAXLEN", "1000"))
MAX_BUNDLE_BYTES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_MAX_BUNDLE_BYTES", "131072"))
REQUIRE_VERTEX_DEGRADED = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL", "1"))
POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_bridge_runs_total", "Bridge runs", ("status", "decision"))
ROUTED = _counter("ml_route_incident_rca_mirror_rca_bridge_routed_total", "Bundles routed", ("route", "severity"))
LAT = _hist("ml_route_incident_rca_mirror_rca_bridge_latency_seconds", "Bridge latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_bridge_up", "Bridge up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_bridge_last_run_ts_seconds", "Bridge last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: dict[Any, Any]) -> dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def is_vertex_degraded(r: Any) -> bool:
    try:
        health_data = await r.hgetall(VERTEX_HEALTH_HASH)
        health = decode_dict(health_data)
        return health.get("status", "healthy") == "degraded"
    except Exception:
        return True # Default to degraded if we can't fetch health

def trigger_decision(severity: str, mode: str, vertex_degraded: bool, bundle_size: int, max_bytes: int, require_vertex_degraded: bool) -> str:
    if mode == "DISABLED":
        return "REJECT"

    if bundle_size > max_bytes:
        return "REJECT"

    if mode == "VERTEX_ONLY":
        return "ROUTE_VERTEX"

    if mode == "LOCAL_ONLY":
        return "ROUTE_LOCAL"

    if mode == "AUTO":
        if vertex_degraded:
            return "ROUTE_LOCAL"
        else:
            return "ROUTE_VERTEX"

    return "REJECT"

async def route_vertex(r: Any, bundle_id: str, bundle_json: str, severity: str) -> None:
    payload = {
        "task_family": "route_incident_rca_mirror_rca",
        "task_type": "route_incident_rca_mirror_rca",
        "bundle_json": bundle_json,
    }
    await r.xadd(VERTEX_RCA_STREAM, payload, maxlen=MAXLEN, approximate=True)
    if ROUTED:
        ROUTED.labels(route="ROUTE_VERTEX", severity=severity).inc()

async def route_local(r: Any, bundle_id: str, bundle_json: str, severity: str) -> None:
    payload = {
        "task_type": "vertex_unavailable_fallback",
        "source": APP_NAME,
        "input_json": bundle_json,
    }
    await r.xadd(LOCAL_FALLBACK_STREAM, payload, maxlen=MAXLEN, approximate=True)
    if ROUTED:
        ROUTED.labels(route="ROUTE_LOCAL", severity=severity).inc()

async def persist_decision(db_url: str, bundle_id: str, decision: str, vertex_degraded: bool, severity: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_bridge_decisions (
                    bundle_id, ts_ms, decision, vertex_degraded, severity
                ) VALUES (
                    %(bundle_id)s, %(ts_ms)s, %(decision)s, %(vertex_degraded)s, %(severity)s
                )
                """,
                {
                    "bundle_id": bundle_id,
                    "ts_ms": now_ms(),
                    "decision": decision,
                    "vertex_degraded": vertex_degraded,
                    "severity": severity,
                }
            )
            conn.commit()

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)

    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    last_bundle_id = "$"

    while True:
        started = time.perf_counter()
        status = "ok"
        decision = "none"

        try:
            streams = {BUNDLES_STREAM: last_bundle_id}
            results = await r.xread(streams, count=5, block=int(POLL_INTERVAL_SEC*1000))
            if results:
                for stream_name, events in results:
                    for msg_id, fields in events:
                        m_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        decoded = decode_dict(fields)

                        bundle_id = decoded.get("bundle_id")
                        severity = decoded.get("severity", "info")
                        bundle_json = decoded.get("bundle_json")

                        if bundle_id and bundle_json:
                            v_degraded = await is_vertex_degraded(r)
                            bundle_size = len(bundle_json.encode('utf-8'))

                            decision = trigger_decision(
                                severity, MODE, v_degraded, bundle_size, MAX_BUNDLE_BYTES, REQUIRE_VERTEX_DEGRADED == 1
                            )

                            if decision == "ROUTE_VERTEX":
                                await route_vertex(r, bundle_id, bundle_json, severity)
                            elif decision == "ROUTE_LOCAL":
                                await route_local(r, bundle_id, bundle_json, severity)

                            await persist_decision(db_url, bundle_id, decision, v_degraded, severity)

                            await r.xadd(DECISIONS_STREAM, {"bundle_id": bundle_id, "decision": decision, "vertex_degraded": str(v_degraded)}, maxlen=MAXLEN, approximate=True)
                            await r.xadd(AUDIT_STREAM, {"event_type": "BUNDLE_ROUTED", "bundle_id": bundle_id, "decision": decision}, maxlen=MAXLEN, approximate=True)
                            await r.hset(LAST_HASH, mapping={"bundle_id": bundle_id, "decision": decision, "ts_ms": str(now_ms())})

                        last_bundle_id = m_id

            if LAST_RUN:
                LAST_RUN.set(time.time())

        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "BRIDGE_RCA_ROUTING_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
            await asyncio.sleep(2)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
