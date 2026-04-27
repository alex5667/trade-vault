from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import hashlib
import json
import os
import time
from typing import Any, Dict, Tuple, List, Optional

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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_bundle_rca_bridge_v3_20"

BUNDLES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_incident_bundles")
VERTEX_HEALTH_METRIC = os.getenv("ML_VERTEX_HEALTH_METRIC", "metrics:ml:vertex_health:last")

VERTEX_RCA_REQUESTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_REQUESTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_requests")
LOCAL_FALLBACK_STREAM = os.getenv("ML_LOCAL_FALLBACK_REQUESTS_STREAM", "stream:ml:local_fallback_requests")

DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rca_bridge_decisions")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rca_bridge_audit")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_PORT", "9941"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_MAXLEN", "1000"))

# AUTO, VERTEX_ONLY, LOCAL_ONLY, DISABLED
BRIDGE_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_MODE", "AUTO")
REQUIRE_VERTEX_DEGRADED = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL", "1"))
MAX_BUNDLE_BYTES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_MAX_BUNDLE_BYTES", "131072"))

POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_rca_bridge_runs_total", "Runs", ("status", "decision"))
ROUTED_TOTAL = _counter("ml_route_incident_rca_mirror_rca_winner_apply_rca_bridge_routed_total", "Routed total", ("route", "severity"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_rca_bridge_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_rca_bridge_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_rca_bridge_last_run_ts_seconds", "Last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def read_vertex_health(r: Any) -> bool:
    try:
        health_data = await r.hgetall(VERTEX_HEALTH_METRIC)
        if not health_data:
            return True
            
        decoded = decode_dict(health_data)
        status = decoded.get("status", "healthy")
        return status != "degraded"
    except Exception:
        return True # Default to healthy on err

def vertex_degraded_from_hash(bundle_id: str) -> bool:
    # Deterministic "is degraded?" mock for standalone testability without redis
    # Using SHA-256 just like the real system
    h = hashlib.sha256(bundle_id.encode()).hexdigest()
    # Simple trick: true if odd, false if even checksum prefix (in a real test)
    val = int(h[:4], 16)
    return val % 2 == 1

def decide_route(mode: str, vertex_is_healthy: bool, require_degraded: int, bundle_size: int, max_size: int) -> str:
    if mode == "DISABLED":
        return "REJECT"
        
    if bundle_size > max_size:
        return "REJECT"
        
    if mode == "VERTEX_ONLY":
        return "ROUTE_VERTEX"
        
    if mode == "LOCAL_ONLY":
        return "ROUTE_LOCAL"
        
    # AUTO mode
    if vertex_is_healthy:
        return "ROUTE_VERTEX"
    else:
        if require_degraded:
            return "ROUTE_LOCAL"
        return "ROUTE_VERTEX" # Fallback to vertex anyway? if require degraded is 0 and it's degraded, weird edge case

def build_vertex_payload(bundle_json: str) -> Dict[str, str]:
    return {
        "task_family": "route_incident_rca_mirror_rca_winner_apply_rca",
        "task_type": "route_incident_rca_mirror_rca_winner_apply_rca",
        "bundle_json": bundle_json,
        "ts_ms": str(now_ms())
    }

def build_local_fallback_payload(bundle_json: str) -> Dict[str, str]:
    return {
        "task_family": "route_incident_rca_mirror_rca_winner_apply_rca",
        "task_type": "vertex_unavailable_fallback",
        "source": APP_NAME,
        "input_json": bundle_json,
        "ts_ms": str(now_ms())
    }

async def persist_decision(db_url: str, bundle_id: str, decision: str, bundle_json: str, severity: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_rca_bridge_decisions (
                    bundle_id, decision, bundle_json, severity, ts_ms
                ) VALUES (
                    %(bundle_id)s, %(decision)s, %(bundle_json)s, %(severity)s, %(ts_ms)s
                )
                """,
                {
                    "bundle_id": bundle_id,
                    "decision": decision,
                    "bundle_json": bundle_json,
                    "severity": severity,
                    "ts_ms": now_ms(),
                },
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
    
    last_id = "0-0"
    
    # Try to resume from audit stream if available to prevent re-processing
    try:
        last_audit = await r.xrevrange(AUDIT_STREAM, count=1)
        if last_audit:
            # We don't have the original BUNDLES_STREAM ID in audit directly easily, 
            # so we just poll from '$' going forward in a real production setup
            last_id = "$"
    except Exception:
        pass

    while True:
        started = time.perf_counter()
        status = "ok"
        decision = "none"
        
        try:
            vertex_is_healthy = await read_vertex_health(r)
            
            res = await r.xread({BUNDLES_STREAM: last_id}, count=10, block=int(POLL_INTERVAL_SEC * 1000))
            if res:
                for stream_name, messages in res:
                    for msg_id, fields in messages:
                        last_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        
                        decoded = decode_dict(fields)
                        bundle_id = decoded.get("bundle_id", "unknown")
                        bundle_json = decoded.get("bundle_json", "{}")
                        
                        size = len(bundle_json.encode("utf-8"))
                        
                        try:
                            parsed = json.loads(bundle_json)
                            severity = parsed.get("trigger", {}).get("severity", "unknown")
                        except Exception:
                            severity = "unknown"
                            
                        decision = decide_route(BRIDGE_MODE, vertex_is_healthy, REQUIRE_VERTEX_DEGRADED, size, MAX_BUNDLE_BYTES)
                        
                        if decision == "ROUTE_VERTEX":
                            payload = build_vertex_payload(bundle_json)
                            await r.xadd(VERTEX_RCA_REQUESTS_STREAM, payload, maxlen=MAXLEN, approximate=True)
                            if ROUTED_TOTAL: ROUTED_TOTAL.labels(route="vertex", severity=severity).inc()
                            
                        elif decision == "ROUTE_LOCAL":
                            payload = build_local_fallback_payload(bundle_json)
                            await r.xadd(LOCAL_FALLBACK_STREAM, payload, maxlen=MAXLEN, approximate=True)
                            if ROUTED_TOTAL: ROUTED_TOTAL.labels(route="local", severity=severity).inc()
                            
                        await r.xadd(DECISIONS_STREAM, {
                            "bundle_id": bundle_id,
                            "decision": decision,
                            "ts_ms": str(now_ms())
                        }, maxlen=MAXLEN, approximate=True)
                        
                        await r.xadd(AUDIT_STREAM, {
                            "bundle_id": bundle_id,
                            "decision": decision,
                            "vertex_is_healthy": str(vertex_is_healthy),
                            "action": "BRIDGE_ROUTED",
                            "ts_ms": str(now_ms())
                        }, maxlen=MAXLEN, approximate=True)
                        
                        await persist_decision(db_url, bundle_id, decision, bundle_json, severity)
                
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            # If we didn't block on xread, sleep
            if not res:
                await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
