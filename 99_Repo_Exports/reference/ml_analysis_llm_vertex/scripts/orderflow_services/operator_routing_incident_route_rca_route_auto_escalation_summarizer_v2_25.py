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

APP_NAME = "operator_routing_incident_route_rca_route_auto_escalation_summarizer_v2_25"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_ROUTE_ESCALATIONS_PORT", "9914"))

STREAMS = [
    "stream:ml:operator_routing_incident_route_rca_routing_verify_results",
    "stream:ml:operator_routing_incident_route_rca_routing_rollback_results"
]
OUT_STREAM = "stream:ml:operator_routing_incident_route_rca_route_escalations"

GROUP = "operator_routing_incident_route_rca_escalations_v2_25"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = 5
MAX_BATCH = 50
MAXLEN = 10000

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None
def _hist(name: str, doc: str, labels: tuple = ()) -> Any: return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_operator_routing_incident_route_rca_route_escalations_runs_total", "Runs", ("status",))
LAT = _hist("ml_operator_routing_incident_route_rca_route_escalations_latency_seconds", "Latency")
LAST_RUN = _gauge("ml_operator_routing_incident_route_rca_route_escalations_last_run_ts_seconds", "Last timestamp")

def now_ms() -> int: return get_ny_time_millis()
def as_dict(record: Dict[bytes, bytes]) -> Dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}

async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e): raise

async def run_loop(r: Any) -> None:
    started = time.perf_counter()
    status = "ok"
    try:
        # Check streams
        for stream in STREAMS:
            await ensure_group(r, stream, GROUP)
            messages = await r.xreadgroup(GROUP, CONSUMER, {stream: ">"}, count=MAX_BATCH, block=10)
            if not messages: continue

            for stream_name, records in messages:
                for msg_id, payload in records:
                    try:
                        row = as_dict(payload)
                        # Mock escalation summarizer logic
                        escalation = {
                            "incident_id": row.get("incident_id", "unknown"),
                            "severity": "CRITICAL" if row.get("status") in {"ROLLBACK_REQUIRED", "FAILED"} else "INFO",
                            "summary": f"Route safety event: {row.get('status')}",
                            "ts_ms": now_ms()
                        }
                        await r.xadd(OUT_STREAM, escalation, maxlen=MAXLEN, approximate=True)
                        await r.xack(stream, GROUP, msg_id)
                    except Exception:
                        await r.xack(stream, GROUP, msg_id)
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
