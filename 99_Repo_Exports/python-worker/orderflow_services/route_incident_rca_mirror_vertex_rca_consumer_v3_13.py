from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
import uuid
from typing import Any, Dict, Tuple

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

APP_NAME = "route_incident_rca_mirror_vertex_rca_consumer_v3_13"
REQUESTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_REQUESTS_STREAM", "stream:ml:route_incident_rca_mirror_vertex_rca_requests")
RESULTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_RESULTS_STREAM", "stream:ml:route_incident_rca_mirror_vertex_rca_results")
LAST_HASH = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_LAST_HASH", "metrics:ml:route_incident_rca_mirror_vertex_rca:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_PORT", "9931"))
MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_HANDLER_MODE", "DETERMINISTIC")
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_MAXLEN", "1000"))
POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_vertex_rca_runs_total", "Runs", ("status", "decision"))
RESULTS_G = _counter("ml_route_incident_rca_mirror_vertex_rca_results_total", "Results", ("severity", "provider_mode"))
LAT = _hist("ml_route_incident_rca_mirror_vertex_rca_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_vertex_rca_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_vertex_rca_last_run_ts_seconds", "Last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    },

def build_deterministic_result(bundle_json_str: str) -> Dict[str, Any]:
    try:
        bundle = json.loads(bundle_json_str)
    except Exception:
        bundle = {}
        
    severity = bundle.get("severity", "unknown")
    bundle_id = bundle.get("bundle_id", str(uuid.uuid4()))
    
    return {
        "request_id": str(uuid.uuid4()),
        "bundle_id": bundle_id,
        "severity": severity,
        "summary": "Deterministic RCA Result generated in mock mode.",
        "dominant_findings": ["Latency spike observed", "Service degraded"],
        "hypotheses": ["Network partition", "High CPU contention"],
        "next_actions": ["Investigate local metrics", "Wait for cooldown"],
        "confidence": 0.85,
        "quality_flags": ["valid_format", "mock_data"]
    },

async def process_request(r: Any, db_url: str, request_id: str, payload: Dict[str, Any]) -> None:
    bundle_json = payload.get("bundle_json", "{}")
    
    if MODE == "DETERMINISTIC":
        result = build_deterministic_result(bundle_json)
    else:
        # Future Vertex API call
        result = build_deterministic_result(bundle_json)
        result["summary"] = f"Vertex mode {MODE} not fully implemented. Mocked."

    bundle_id = result.get("bundle_id", "unknown")
    severity = result.get("severity", "unknown")
    
    await r.xadd(RESULTS_STREAM, {
        "request_id": result["request_id"],
        "bundle_id": bundle_id,
        "result_json": json.dumps(result),
        "ts_ms": str(now_ms()),
    }, maxlen=MAXLEN, approximate=True)
    
    await r.hset(LAST_HASH, mapping={
        "request_id": result["request_id"],
        "bundle_id": bundle_id,
        "status": "processed",
        "ts_ms": str(now_ms())
    })
    
    if RESULTS_G:
        RESULTS_G.labels(severity=severity, provider_mode=MODE).inc()

    if db_url and psycopg is not None:  # pragma: no cover
        try:
            with psycopg.connect(db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """,
                        INSERT INTO llm_route_incident_rca_mirror_vertex_rca_results (
                            request_id, bundle_id, result_json, ts_ms
                        ) VALUES (
                            %(request_id)s, %(bundle_id)s, %(result_json)s, %(ts_ms)s
                        )
                        """,
                        {
                            "request_id": result["request_id"],
                            "bundle_id": bundle_id,
                            "result_json": json.dumps(result),
                            "ts_ms": now_ms(),
                        },
                    )
                    conn.commit()
        except Exception as e:
            pass # logging could be added

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
        
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")
    
    last_req_id = "$"
    
    while True:
        started = time.perf_counter()
        status = "ok"
        decision = "none"
        
        try:
            streams = {REQUESTS_STREAM: last_req_id}
            results = await r.xread(streams, count=5, block=int(POLL_INTERVAL_SEC*1000))
            if results:
                for stream_name, events in results:
                    for msg_id, fields in events:
                        m_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        decoded = decode_dict(fields)
                        
                        task_family = decoded.get("task_family")
                        if task_family == "route_incident_rca_mirror_rca":
                            await process_request(r, db_url, m_id, decoded)
                            decision = "processed"
                        else:
                            decision = "ignored"
                        last_req_id = m_id
                            
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
            await asyncio.sleep(2)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
