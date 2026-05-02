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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_experiment_harness_v3_22"

BUNDLES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_incident_bundles")

EXPOSURES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_EXPOSURES_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_experiment_exposures")
DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_experiment_decisions")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_experiment_audit")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_experiment:last")

DETERMINISTIC_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_DETERMINISTIC_REQUESTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rca_requests")
VERTEX_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_REQUESTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_requests")
LOCAL_STREAM = os.getenv("ML_LOCAL_FALLBACK_REQUESTS_STREAM", "stream:ml:local_fallback_requests")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PORT", "9944"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MAXLEN", "2000"))

# DISABLED, SHADOW, SINGLE_ARM, MULTI_ARM
EXPERIMENT_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE", "SHADOW")
PRIMARY_ARM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM", "deterministic")
SHADOW_ARMS_JSON = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON", '["vertex_candidate","local_fallback_candidate"]')
ARM_WEIGHTS_JSON = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_ARM_WEIGHTS_JSON", '{"deterministic":70,"vertex_candidate":20,"local_fallback_candidate":10}')
ALLOW_SEVERITIES = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_ALLOW_SEVERITIES", "warning,critical").split(",")

HASH_SALT = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_HASH_SALT", "wa_rca_salt_v1")

POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_experiment_runs_total", "Runs", ("status", "decision"))
EXPOSURES = _counter("ml_route_incident_rca_mirror_rca_winner_apply_exposures_total", "Exposures total", ("arm", "severity", "mode"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_experiment_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_experiment_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_experiment_last_run_ts_seconds", "Last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

def resolve_multi_arm(bundle_id: str, weights: Dict[str, int]) -> str:
    total = sum(weights.values())
    if total <= 0:
        return PRIMARY_ARM
        
    h = hashlib.sha256(f"{bundle_id}:{HASH_SALT}".encode()).hexdigest()
    val = int(h[:8], 16) % total
    
    cumulative = 0
    # Sort for deterministic distribution logic
    for arm, w in sorted(weights.items()):
        cumulative += w
        if val < cumulative:
            return arm
            
    return PRIMARY_ARM

def compute_exposures(mode: str, bundle_id: str, weights: Dict[str, int]) -> List[Dict[str, str]]:
    if mode == "DISABLED":
        return []
        
    if mode == "SINGLE_ARM":
        return [{"arm": PRIMARY_ARM, "type": "primary", "decision": "SINGLE_ARM_ASSIGNED"}]
        
    if mode == "SHADOW":
        res = [{"arm": PRIMARY_ARM, "type": "primary", "decision": "SHADOW_PRIMARY_ASSIGNED"}]
        try:
            shadows = json.loads(SHADOW_ARMS_JSON)
            for sa in shadows:
                res.append({"arm": sa, "type": "shadow", "decision": "SHADOW_ARM_ASSIGNED"})
        except Exception:
            pass
        return res
        
    if mode == "MULTI_ARM":
        assigned = resolve_multi_arm(bundle_id, weights)
        return [{"arm": assigned, "type": "primary", "decision": "MULTI_ARM_ASSIGNED"}]
        
    return []

def get_stream_for_arm(arm: str) -> str:
    if arm == "deterministic":
        return DETERMINISTIC_STREAM
    if arm == "vertex_candidate":
        return VERTEX_STREAM
    if arm == "local_fallback_candidate":
        return LOCAL_STREAM
    return DETERMINISTIC_STREAM

def build_payload(arm: str, bundle_json: str) -> Dict[str, str]:
    base = {
        "task_family": "route_incident_rca_mirror_rca_winner_apply_rca",
        "bundle_json": bundle_json,
        "source": APP_NAME,
        "ts_ms": str(now_ms())
    }
    
    if arm == "local_fallback_candidate":
        base["task_type"] = "vertex_unavailable_fallback" # reuse local struct
    elif arm == "vertex_candidate":
        base["task_type"] = "route_incident_rca_mirror_rca_winner_apply_rca" 
    else:
        base["task_type"] = "route_incident_rca_mirror_rca_winner_apply_deterministic"
        
    return base

async def persist_exposure(db_url: str, bundle_id: str, arm: str, exp_type: str, severity: str, mode: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_experiment_exposures (
                    bundle_id, arm, exposure_type, severity, experiment_mode, ts_ms
                ) VALUES (
                    %(bundle_id)s, %(arm)s, %(exposure_type)s, %(severity)s, %(mode)s, %(ts_ms)s
                )
                """,
                {
                    "bundle_id": bundle_id,
                    "arm": arm,
                    "exposure_type": exp_type,
                    "severity": severity,
                    "mode": mode,
                    "ts_ms": now_ms(),
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
    
    last_id = "0-0"
    try:
        last_metric = await r.hgetall(LAST_METRIC)
        if last_metric:
            last_id = "$"
    except Exception:
        pass
        
    try:
        weights = json.loads(ARM_WEIGHTS_JSON)
    except Exception:
        weights = {"deterministic": 100}

    while True:
        started = time.perf_counter()
        status = "ok"
        decision = "none"
        
        try:
            res_stream = await r.xread({BUNDLES_STREAM: last_id}, count=10, block=int(POLL_INTERVAL_SEC * 1000))
            if res_stream:
                for stream_name, messages in res_stream:
                    for msg_id, fields in messages:
                        last_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        
                        decoded = decode_dict(fields)
                        bundle_id = decoded.get("bundle_id", "unknown")
                        bundle_json = decoded.get("bundle_json", "{}")
                        
                        try:
                            parsed = json.loads(bundle_json)
                            severity = parsed.get("trigger", {}).get("severity", "unknown")
                        except Exception:
                            severity = "unknown"
                            
                        # Gating
                        if severity not in ALLOW_SEVERITIES:
                            decision = "GATED_SEVERITY"
                        elif EXPERIMENT_MODE == "DISABLED":
                            decision = "DISABLED"
                        else:
                            exposures = compute_exposures(EXPERIMENT_MODE, bundle_id, weights)
                            if exposures:
                                decision = "COMPUTED_EXPOSURES"
                                
                            for exp in exposures:
                                arm = exp["arm"]
                                exp_type = exp["type"]
                                dec = exp["decision"]
                                
                                # Publish to target stream
                                target_stream = get_stream_for_arm(arm)
                                payload = build_payload(arm, bundle_json)
                                await r.xadd(target_stream, payload, maxlen=MAXLEN, approximate=True)
                                
                                # Log exposure
                                exp_payload = {
                                    "bundle_id": bundle_id,
                                    "arm": arm,
                                    "type": exp_type,
                                    "decision": dec,
                                    "severity": severity,
                                    "ts_ms": str(now_ms())
                                }
                                await r.xadd(EXPOSURES_STREAM, exp_payload, maxlen=MAXLEN, approximate=True)
                                await persist_exposure(db_url, bundle_id, arm, exp_type, severity, EXPERIMENT_MODE)
                                
                                if EXPOSURES: EXPOSURES.labels(arm=arm, severity=severity, mode=EXPERIMENT_MODE).inc()
                                
                            # Record overall decision
                            await r.xadd(DECISIONS_STREAM, {
                                "bundle_id": bundle_id,
                                "mode": EXPERIMENT_MODE,
                                "exposures": json.dumps(exposures),
                                "ts_ms": str(now_ms())
                            }, maxlen=MAXLEN, approximate=True)
                            
                        await r.hset(LAST_METRIC, "bundle_id", bundle_id)
                        await r.hset(LAST_METRIC, "decision", decision)
                        await r.hset(LAST_METRIC, "ts_ms", str(now_ms()))
                
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            if not res_stream:
                await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
