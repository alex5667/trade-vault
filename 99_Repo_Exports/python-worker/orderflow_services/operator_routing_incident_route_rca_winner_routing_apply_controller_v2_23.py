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

APP_NAME = "operator_routing_incident_route_rca_winner_routing_apply_controller_v2_23"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_WINNER_APPLY_PORT", "9909"))

IN_STREAM = "stream:ml:operator_routing_incident_route_rca_experiment_winner_decisions"
OUT_STREAM = "stream:ml:operator_routing_incident_route_rca_routing_apply_results"
AUDIT_STREAM = "stream:ml:operator_routing_incident_route_rca_routing_apply_audit"

GROUP = "operator_routing_incident_route_rca_winner_apply_v2_23"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = 5
MAX_BATCH = 50
MAXLEN = 10000

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None
def _hist(name: str, doc: str, labels: tuple = ()) -> Any: return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_operator_routing_incident_route_rca_winner_routing_apply_runs_total", "Runs", ("status",))
LAT = _hist("ml_operator_routing_incident_route_rca_winner_routing_apply_latency_seconds", "Latency")
LAST_RUN = _gauge("ml_operator_routing_incident_route_rca_winner_routing_apply_last_run_ts_seconds", "Last timestamp")

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
        await ensure_group(r, IN_STREAM, GROUP)
        messages = await r.xreadgroup(GROUP, CONSUMER, {IN_STREAM: ">"}, count=MAX_BATCH, block=10)
        if not messages: return

        advisory_only = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_ROUTING_APPLY_ADVISORY_ONLY", "1") == "1"
        cooldown_sec = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_ROUTING_APPLY_COOLDOWN_SEC", "21600"))

        for stream_name, records in messages:
            for msg_id, payload in records:
                try:
                    row = as_dict(payload)
                    inc_id = row.get("incident_id", "unknown")
                    exp_id = row.get("experiment_id", "unknown")
                    winner_variant = row.get("winner_variant", "unknown")
                    
                    # Logic to apply winner
                    # In a real implementation, we'd check cooldowns and actual uplift metrics
                    # Here we simulate the apply result
                    apply_result = {
                        "incident_id": inc_id,
                        "experiment_id": exp_id,
                        "winner_variant": winner_variant,
                        "applied": "true" if not advisory_only else "false",
                        "mode": "ADVISORY" if advisory_only else "COMMIT",
                        "ts_ms": now_ms()
                    }
                    
                    if not advisory_only:
                        # Update default routing policy in Redis
                        policy_key = "cfg:ml:operator_routing_incident_route_rca_routing:default"
                        await r.hset(policy_key, mapping={
                            "provider": "vertex", # Mocking variant selection metadata
                            "model_name": "gemini-2.5-flash-lite",
                            "last_winner_exp_id": exp_id,
                            "last_winner_variant": winner_variant,
                            "applied_ts_ms": now_ms()
                        })
                    
                    await r.xadd(OUT_STREAM, apply_result, maxlen=MAXLEN, approximate=True)
                    await r.xadd(AUDIT_STREAM, apply_result, maxlen=MAXLEN, approximate=True)
                    
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
