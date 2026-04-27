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

from orderflow_services.providers.vertex_routing_incident_route_rca_provider_v2_18 import VertexRcaProvider

APP_NAME = "ml_operator_routing_incident_route_rca_orchestrator_v2_18"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_PORT", "9901"))

IN_STREAM = "stream:ml:operator_routing_incident_rca_route_rca_requests"
OUT_RESULTS = "stream:ml:operator_routing_incident_rca_route_rca_results"
OUT_PROPOSALS = "stream:ml:recommendation_proposals"
OUT_DLQ = "stream:ml:operator_routing_incident_rca_route_rca_dlq"

GROUP = "operator_routing_incident_route_rca_orchestrator_v2_18"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = 5
MAX_BATCH = 10
MAXLEN = 10000

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None
def _hist(name: str, doc: str, labels: tuple = ()) -> Any: return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_operator_routing_incident_route_rca_runs_total", "Runs", ("status",))
LAT = _hist("ml_operator_routing_incident_route_rca_latency_seconds", "Latency")
LAST_RUN = _gauge("ml_operator_routing_incident_route_rca_last_run_ts_seconds", "Last timestamp")

def now_ms() -> int: return get_ny_time_millis()
def as_dict(record: Dict[bytes, bytes]) -> Dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}

async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e): raise

async def run_loop(r: Any, provider: VertexRcaProvider) -> None:
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
                    
                    try:
                        rca_result = await provider.generate_rca(row)
                        
                        out_res = {
                            "incident_id": row.get("incident_id", "unknown"),
                            "analysis": rca_result.get("analysis", ""),
                            "confidence": rca_result.get("confidence", 0.0),
                            "action": rca_result.get("advisory_action", "MONITOR"),
                            "ts_ms": now_ms()
                        }
                        
                        await r.xadd(OUT_RESULTS, out_res, maxlen=MAXLEN, approximate=True)
                        
                        # Generate structured recommendation proposal
                        proposal = dict(out_res)
                        proposal["type"] = "ROUTE_RECOVERY_PROPOSAL"
                        await r.xadd(OUT_PROPOSALS, proposal, maxlen=MAXLEN, approximate=True)
                        
                    except Exception as pe:
                        dlq_entry = dict(row)
                        dlq_entry["error"] = str(pe)
                        await r.xadd(OUT_DLQ, dlq_entry, maxlen=MAXLEN, approximate=True)
                        
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
    provider = VertexRcaProvider()
    while True:
        await run_loop(r, provider)
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
