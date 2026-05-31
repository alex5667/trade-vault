from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis.asyncio as redis
except Exception:
    redis = None

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None: return None

APP_NAME = "operator_routing_incident_route_rca_route_retry_controller_v2_25"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_ROUTE_RETRY_PORT", "9913"))

IN_STREAM = "stream:ml:operator_routing_incident_route_rca_routing_verify_results"
OUT_STREAM = "stream:ml:operator_routing_incident_route_rca_route_retry_requests"
AUDIT_STREAM = "stream:ml:operator_routing_incident_route_rca_routing_apply_audit"

GROUP = "operator_routing_incident_route_rca_route_retry_v2_25"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = 5
MAX_BATCH = 50
MAXLEN = 10000

RETRY_REASONS = {"LOW_EXPOSURE", "FEEDBACK_STALE", "ROUTE_MISMATCH"}
HARD_STOP_REASONS = {"BASELINE_ROUTE_MISSING", "ROUTING_POLICY_CORRUPTED"}

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None
def _hist(name: str, doc: str, labels: tuple = ()) -> Any: return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_operator_routing_incident_route_rca_route_retry_runs_total", "Runs", ("status",))
LAT = _hist("ml_operator_routing_incident_route_rca_route_retry_latency_seconds", "Latency")
LAST_RUN = _gauge("ml_operator_routing_incident_route_rca_route_retry_last_run_ts_seconds", "Last timestamp")

def now_ms() -> int: return get_ny_time_millis()
def as_dict(record: dict[bytes, bytes]) -> dict[str, str]:
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

        max_attempts = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_ROUTE_RCA_ROUTE_RETRY_MAX_ATTEMPTS", "3"))

        for stream_name, records in messages:
            for msg_id, payload in records:
                try:
                    row = as_dict(payload)
                    inc_id = row.get("incident_id", "unknown")
                    reason = row.get("reason", "unknown")

                    if reason in RETRY_REASONS:
                        retry_req = {
                            "incident_id": inc_id,
                            "reason": reason,
                            "attempt": "1",
                            "ts_ms": now_ms()
                        }
                        await r.xadd(OUT_STREAM, retry_req, maxlen=MAXLEN, approximate=True)
                        await r.xadd(AUDIT_STREAM, retry_req, maxlen=MAXLEN, approximate=True)

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
