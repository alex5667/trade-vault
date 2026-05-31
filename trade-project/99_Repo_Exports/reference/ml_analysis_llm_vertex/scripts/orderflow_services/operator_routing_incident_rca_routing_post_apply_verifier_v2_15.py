from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import os
import time
from typing import Any, Dict

try:
    import redis.asyncio as redis
except Exception:
    redis = None

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None: return None

APP_NAME = "operator_routing_incident_rca_routing_post_apply_verifier_v2_15"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_VERIFY_PORT", "9894"))

MIN_USEFULNESS = float(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_VERIFY_MIN_USEFULNESS", "0.50"))
MIN_EXPOSURE = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_VERIFY_MIN_EXPOSURE", "3"))

IN_STREAM = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_RESULTS_STREAM", "stream:ml:operator_routing_incident_rca_routing_apply_results")
OUT_STREAM = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_VERIFY_RESULTS_STREAM", "stream:ml:operator_routing_incident_rca_routing_verify_results")
ROLLBACK_STREAM = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_ROLLBACK_REQUESTS_STREAM", "stream:ml:operator_routing_incident_rca_routing_rollback_requests")
AUDIT_STREAM = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_AUDIT_STREAM", "stream:ml:operator_routing_incident_rca_routing_apply_audit")
GLOBAL_POLICY_HASH = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_DEFAULT_POLICY", "cfg:ml:operator_routing_incident_rca_routing:default")

GROUP = "operator_routing_incident_rca_routing_verify_v2_15"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_VERIFY_POLL_INTERVAL", "5"))
MAX_BATCH = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_VERIFY_MAX_BATCH", "50"))
MAXLEN = 10000

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None
def _hist(name: str, doc: str, labels: tuple = ()) -> Any: return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_operator_routing_incident_rca_routing_verify_runs_total", "Runs", ("status",))
LAT = _hist("ml_operator_routing_incident_rca_routing_verify_latency_seconds", "Latency")
LAST_RUN_TS = _gauge("ml_operator_routing_incident_rca_routing_verify_last_run_ts_seconds", "Last run")
VERIFICATION_RESULTS = _counter("ml_operator_routing_incident_rca_routing_verify_results_total", "Results", ("conclusion",))

def now_ms() -> int: return get_ny_time_millis()

def as_dict(record: Dict[bytes, bytes]) -> Dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}

async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise

async def verify_loop(r: Any) -> None:
    started = time.perf_counter()
    status = "ok"
    try:
        await ensure_group(r, IN_STREAM, GROUP)
        messages = await r.xreadgroup(GROUP, CONSUMER, {IN_STREAM: ">"}, count=MAX_BATCH, block=10)
        if not messages: return

        for stream_name, records in messages:
            for msg_id, payload in records:
                try:
                    row = as_dict(payload)
                    exp_id = row.get("experiment_id", "unknown")
                    
                    feedback = await r.hgetall("metrics:ml:operator_routing_incident_rca_feedback:last")
                    fb = as_dict(feedback) if feedback else {}
                    
                    exposure = int(fb.get("exposures_n", "0"))
                    usefulness = float(fb.get("usefulness_avg", "1.0"))
                    
                    conclusion = "OK"
                    reason = "metrics_within_bounds"
                    
                    if exposure < MIN_EXPOSURE:
                        conclusion = "INCONCLUSIVE"
                        reason = "LOW_EXPOSURE"
                    elif usefulness < MIN_USEFULNESS:
                        conclusion = "ROLLBACK_REQUIRED"
                        reason = "USEFULNESS_DROP"
                        
                    result = {
                        "experiment_id": exp_id,
                        "conclusion": conclusion,
                        "reason": reason,
                        "observed_exposure": exposure,
                        "observed_usefulness": usefulness,
                        "ts_ms": now_ms()
                    }
                    
                    await r.xadd(OUT_STREAM, result, maxlen=MAXLEN, approximate=True)
                    await r.xadd(AUDIT_STREAM, result, maxlen=MAXLEN, approximate=True)
                    
                    if conclusion == "ROLLBACK_REQUIRED":
                        rollback_req = {
                            "route_change_id": f"rollback_{exp_id}_{now_ms()}",
                            "trigger": "post_apply_verifier",
                            "reason": reason,
                            "ts_ms": now_ms()
                        }
                        await r.xadd(ROLLBACK_STREAM, rollback_req, maxlen=MAXLEN, approximate=True)
                    
                    if VERIFICATION_RESULTS:
                        VERIFICATION_RESULTS.labels(conclusion=conclusion).inc()
                    
                    await r.xack(IN_STREAM, GROUP, msg_id)
                except Exception:
                    status = "error"
                    await r.xack(IN_STREAM, GROUP, msg_id)
                    
        if LAST_RUN_TS: LAST_RUN_TS.set(time.time())
    except Exception:
        status = "error"
    finally:
        if RUNS: RUNS.labels(status=status).inc()
        if LAT: LAT.observe(max(time.perf_counter() - started, 0.0))

async def main() -> None:
    start_http_server(PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    while True:
        await verify_loop(r)
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
