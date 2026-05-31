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

APP_NAME = "operator_routing_incident_rca_routing_rollback_executor_v2_15"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROLLBACK_PORT", "9895"))

ROLLBACK_MODE = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_ROLLBACK_MODE", "DRY_RUN")

IN_STREAM = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_ROLLBACK_REQUESTS_STREAM", "stream:ml:operator_routing_incident_rca_routing_rollback_requests")
OUT_STREAM = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_ROLLBACK_RESULTS_STREAM", "stream:ml:operator_routing_incident_rca_routing_rollback_results")
JOURNAL_STREAM = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_ROLLBACK_JOURNAL_STREAM", "stream:ml:operator_routing_incident_rca_routing_rollback_journal")
AUDIT_STREAM = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_AUDIT_STREAM", "stream:ml:operator_routing_incident_rca_routing_apply_audit")
GLOBAL_POLICY_HASH = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_DEFAULT_POLICY", "cfg:ml:operator_routing_incident_rca_routing:default")

GROUP = "operator_routing_incident_rca_routing_rollback_v2_15"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROLLBACK_POLL_INTERVAL", "5"))
MAX_BATCH = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROLLBACK_MAX_BATCH", "50"))
MAXLEN = 10000

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None
def _hist(name: str, doc: str, labels: tuple = ()) -> Any: return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_operator_routing_incident_rca_routing_rollback_runs_total", "Runs", ("status",))
LAT = _hist("ml_operator_routing_incident_rca_routing_rollback_latency_seconds", "Latency")
LAST_RUN_TS = _gauge("ml_operator_routing_incident_rca_routing_rollback_last_run_ts_seconds", "Last run")
ROLLBACKS = _counter("ml_operator_routing_incident_rca_routing_rollbacks_total", "Rollbacks", ("mode",))

def now_ms() -> int: return get_ny_time_millis()

def as_dict(record: Dict[bytes, bytes]) -> Dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}

async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise

async def rollback_loop(r: Any) -> None:
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
                    route_change_id = row.get("route_change_id", "unknown")
                    reason = row.get("reason", "unknown")
                    
                    action = "ROLLBACK_DRY_RUN"
                    
                    if ROLLBACK_MODE == "COMMIT":
                        action = "ROLLBACK_COMMITTED"
                        await r.hset(GLOBAL_POLICY_HASH, mapping={
                            "provider": "vertex",
                            "model_name": "gemini-2.5-flash-lite",
                            "prompt_version": "routing_incident_rca_v1",
                            "last_updated_ms": now_ms(),
                            "experiment_source": "rollback"
                        })
                    
                    result = {
                        "route_change_id": route_change_id,
                        "action": action,
                        "reason": reason,
                        "ts_ms": now_ms()
                    }
                    
                    await r.xadd(OUT_STREAM, result, maxlen=MAXLEN, approximate=True)
                    await r.xadd(JOURNAL_STREAM, result, maxlen=MAXLEN, approximate=True)
                    await r.xadd(AUDIT_STREAM, result, maxlen=MAXLEN, approximate=True)
                    
                    if ROLLBACKS: ROLLBACKS.labels(mode=action).inc()
                    
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
        await rollback_loop(r)
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
