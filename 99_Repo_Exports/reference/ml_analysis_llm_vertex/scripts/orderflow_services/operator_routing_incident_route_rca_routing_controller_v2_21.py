from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
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

APP_NAME = "operator_routing_incident_route_rca_routing_controller_v2_21"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_ROUTING_PORT", "9906"))

IN_STREAM = "stream:ml:operator_routing_incident_rca_route_rca_requests"
OUT_DECISIONS = "stream:ml:operator_routing_incident_route_rca_routing_decisions"
OUT_AUDIT = "stream:ml:operator_routing_incident_route_rca_routing_audit"
OUT_ROUTED = "stream:ml:operator_routing_incident_rca_route_rca_requests_routed"
GOVERNOR_STREAM = "stream:ml:operator_routing_incident_route_rca_governor_decisions"

GROUP = "operator_routing_incident_route_rca_routing_v2_21"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = 5
MAX_BATCH = 50
MAXLEN = 10000

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None
def _hist(name: str, doc: str, labels: tuple = ()) -> Any: return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_operator_routing_incident_route_rca_routing_runs_total", "Runs", ("status",))
LAT = _hist("ml_operator_routing_incident_route_rca_routing_latency_seconds", "Latency")
LAST_RUN = _gauge("ml_operator_routing_incident_route_rca_routing_last_run_ts_seconds", "Last timestamp")

def now_ms() -> int: return get_ny_time_millis()
def as_dict(record: Dict[bytes, bytes]) -> Dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}

async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e): raise

async def get_governor_policy(r: Any) -> Dict[str, str]:
    # In a real implementation, this would read from the governor's state in Redis
    # or the latest decisions. For this phase, we use defaults with governor overrides.
    return {
        "provider": os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_DEFAULT_PROVIDER", "vertex"),
        "model_name": os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_DEFAULT_MODEL", "gemini-2.5-flash-lite"),
        "prompt_version": os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_DEFAULT_PROMPT_VERSION", "routing_incident_route_rca_v1"),
        "policy_version": os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_DEFAULT_POLICY_VERSION", "policy_v1")
    }

async def run_loop(r: Any) -> None:
    started = time.perf_counter()
    status = "ok"
    try:
        await ensure_group(r, IN_STREAM, GROUP)
        messages = await r.xreadgroup(GROUP, CONSUMER, {IN_STREAM: ">"}, count=MAX_BATCH, block=10)
        if not messages: return

        policy = await get_governor_policy(r)

        for stream_name, records in messages:
            for msg_id, payload in records:
                try:
                    row = as_dict(payload)
                    row.update(policy)
                    row["routing_ts_ms"] = now_ms()
                    row["routing_mode"] = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_ROUTING_MODE", "DRY_RUN")

                    # Publish routed request
                    await r.xadd(OUT_ROUTED, row, maxlen=MAXLEN, approximate=True)
                    
                    # Publish decision and audit
                    decision = {
                        "incident_id": row.get("incident_id", "unknown"),
                        "provider": policy["provider"],
                        "model_name": policy["model_name"],
                        "prompt_version": policy["prompt_version"],
                        "policy_version": policy["policy_version"],
                        "ts_ms": now_ms()
                    }
                    await r.xadd(OUT_DECISIONS, decision, maxlen=MAXLEN, approximate=True)
                    await r.xadd(OUT_AUDIT, decision, maxlen=MAXLEN, approximate=True)
                    
                    # Save last routing metrics
                    metric_key = "metrics:ml:operator_routing_incident_route_rca_routing:last"
                    await r.hset(metric_key, mapping=decision)
                    
                    await r.xack(IN_STREAM, GROUP, msg_id)
                except Exception:
                    status = "error"
                    await r.xack(IN_STREAM, GROUP, msg_id)
                    
        if LAST_RUN: LAST_RUN.set(time.time())
    except Exception:
        status = "error"
    finally:
        if RUNS: RUNS.labels(status=status).inc()
        if LAT: LAT.observe(max(time.perf_counter() - started, 0.0))

async def main() -> None:
    start_http_server(PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    while True:
        await run_loop(r)
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
