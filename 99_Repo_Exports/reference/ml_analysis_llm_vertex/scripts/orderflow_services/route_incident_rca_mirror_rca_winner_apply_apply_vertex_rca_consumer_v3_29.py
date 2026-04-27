from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Tuple

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
        pass

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_consumer_v3_29"

IN_VERTEX_RCA = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_REQUESTS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_requests")
OUT_VERTEX_RCA_RESULTS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_RESULTS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_results")

LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca:last")

CG_NAME = "cg_apply_vertex_rca_consumer_v3_29"
CONS_NAME = f"cons_{os.getpid()}"

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_PORT", "9953"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_MAXLEN", "2000"))

MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_HANDLER_MODE", "DETERMINISTIC").upper()
POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_last_run_ts_seconds", "Last run")

RESULTS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_results_total", "Results", ("severity", "provider_mode"))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def persist_result(db_url: str, request_id: str, bundle_id: str, result_json: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_results (
                    request_id, bundle_id, result_json, ts_ms
                ) VALUES (
                    %(request_id)s, %(bundle_id)s, %(result_json)s, %(ts_ms)s
                )
                """,
                {
                    "request_id": request_id,
                    "bundle_id": bundle_id,
                    "result_json": result_json,
                    "ts_ms": now_ms(),
                },
            )
            conn.commit()

def build_deterministic_result(bundle_json_str: str) -> Dict[str, Any]:
    try:
        b = json.loads(bundle_json_str)
    except Exception:
        b = {}
    
    triggers = b.get("trigger", {}) 
    trig_type = triggers.get("type", "unknown")
    sev = triggers.get("severity", "unknown")
    
    findings = []
    actions = []
    
    if trig_type == "rollback":
        findings.append("ROLLBACK_TRIGGERED")
        findings.append("verify_keep_rate_dropped_or_mttr_breach")
        actions.append("review_apply_controller_shadow_performance")
    elif trig_type == "apply":
        findings.append("APPLY_CONTROLLER_TRIGGERED")
        findings.append("experiment_shifted_to_primary_or_single")
        actions.append("monitor_verification_loop_for_policy_mismatch")
    elif trig_type == "escalation":
        findings.append(f"ESCALATION_{sev.upper()}")
        actions.append("check_retry_exhaustion_in_governance")
    else:
        findings.append("UNKNOWN_TRIGGER")
        
    return {
        "summary": "Deterministic apply-governance RCA summary based on bundle headers.",
        "dominant_findings": findings,
        "hypotheses": [
            "policy_mismatch" if trig_type == "rollback" else "normal_transition"
        ],
        "next_actions": actions,
        "confidence": 0.85,
        "quality_flags": ["auto_generated", "deterministic_fallback"]
    }

async def process_msg(r: Any, db_url: str, request_id: str, fields: Dict[str, Any]) -> None:
    started = time.perf_counter()
    status = "ok"
    decision = "ACCEPT"
    
    try:
        bundle_id = fields.get("apply_id", "") 
        bundle_json = fields.get("bundle_json", "")
        if not bundle_id or not bundle_json:
            decision = "REJECT_INVALID_PAYLOAD"
            return
            
        b = json.loads(bundle_json)
        sev = b.get("trigger", {}).get("severity", "unknown")
            
        if MODE == "DETERMINISTIC":
            res_payload = build_deterministic_result(bundle_json)
        else:
            # Here real vertex call would happen
            res_payload = build_deterministic_result(bundle_json)
            
        rj = json.dumps(res_payload)
        
        await r.xadd(OUT_VERTEX_RCA_RESULTS, {
            "request_id": request_id,
            "bundle_id": bundle_id,
            "provider_mode": MODE,
            "result_json": rj,
            "ts_ms": str(now_ms())
        }, maxlen=MAXLEN, approximate=True)
        
        await r.hset(LAST_METRIC, "request_id", request_id)
        await r.hset(LAST_METRIC, "bundle_id", bundle_id)
        await r.hset(LAST_METRIC, "ts_ms", str(now_ms()))
        
        await persist_result(db_url, request_id, bundle_id, rj)
        
        if RESULTS:
            RESULTS.labels(severity=sev, provider_mode=MODE).inc()
            
    except Exception as exc:
        status = "error"
    finally:
        if RUNS:
            RUNS.labels(status=status, decision=decision).inc()
        if LAT:
            LAT.observe(max(time.perf_counter() - started, 0.0))

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)

    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    try:
        await r.xgroup_create(IN_VERTEX_RCA, CG_NAME, id="0", mkstream=True)
    except Exception:
        pass

    while True:
        try:
            resp = await r.xreadgroup(CG_NAME, CONS_NAME, {IN_VERTEX_RCA: ">"}, count=10, block=2000)
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
            if not resp:
                continue
                
            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    mid = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    f = decode_dict(fields)
                    await process_msg(r, db_url, mid, f)
                    await r.xack(stream_name, CG_NAME, msg_id)
        except Exception:
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
