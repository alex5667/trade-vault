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
    from prometheus_client import Counter, Gauge, start_http_server
except Exception:
    Counter = Gauge = None
    def start_http_server(*args: Any, **kwargs: Any) -> None: return None

APP_NAME = "operator_routing_incident_rca_route_auto_escalation_summarizer_v2_16"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTE_ESCALATION_PORT", "9898"))

IN_STREAM = "stream:ml:operator_routing_incident_rca_route_slo_rollups"
OUT_STREAM = "stream:ml:operator_routing_incident_rca_route_escalations"

GROUP = "operator_routing_incident_rca_route_escalation_v2_16"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = 5
MAX_BATCH = 50
MAXLEN = 10000

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None

RUNS = _counter("ml_operator_routing_incident_rca_route_escalation_runs_total", "Runs", ("status",))
LAST_RUN = _gauge("ml_operator_routing_incident_rca_route_escalation_last_run_ts_seconds", "Last timestamp")

def now_ms() -> int: return get_ny_time_millis()
def as_dict(record: Dict[bytes, bytes]) -> Dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}

async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e): raise

async def run_loop(r: Any) -> None:
    status = "ok"
    try:
        await ensure_group(r, IN_STREAM, GROUP)
        messages = await r.xreadgroup(GROUP, CONSUMER, {IN_STREAM: ">"}, count=MAX_BATCH, block=10)
        if not messages: return

        for stream_name, records in messages:
            for msg_id, payload in records:
                try:
                    row = as_dict(payload)
                    reasons = row.get("reasons", "")
                    
                    if reasons:
                        severity = "critical" if "SLO_BREACH" in reasons else "warning"
                        
                        esc = {
                            "reasons": reasons,
                            "severity": severity,
                            "action": "ESCALATED",
                            "ts_ms": now_ms()
                        }
                        await r.xadd(OUT_STREAM, esc, maxlen=MAXLEN, approximate=True)
                            
                    await r.xack(IN_STREAM, GROUP, msg_id)
                except Exception:
                    status = "error"
                    await r.xack(IN_STREAM, GROUP, msg_id)
                    
        if LAST_RUN: LAST_RUN.set(time.time())
    except Exception:
        status = "error"
    finally:
        if RUNS: RUNS.labels(status=status).inc()

async def main() -> None:
    start_http_server(PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    while True:
        await run_loop(r)
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
