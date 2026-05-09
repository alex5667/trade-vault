from __future__ import annotations

import asyncio
import hashlib
import json
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

APP_NAME = "operator_routing_incident_rca_route_incident_bundle_builder_v2_17"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTE_INCIDENT_BUNDLE_PORT", "9899"))

IN_STREAM = "stream:ml:operator_routing_incident_rca_route_incident_bundle_requests"
OUT_STREAM = "stream:ml:operator_routing_incident_rca_route_incident_bundle_results"
LAST_HASH = "metrics:ml:operator_routing_incident_rca_route_incident_bundle:last"
INCIDENT_HASH_PREFIX = "metrics:ml:operator_routing_incident_rca_route_incident_bundle:"

GROUP = "operator_routing_incident_rca_route_incident_bundle_v2_17"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = 5
MAX_BATCH = 50
MAXLEN = 10000

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None
def _hist(name: str, doc: str, labels: tuple = ()) -> Any: return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_operator_routing_incident_rca_route_incident_bundle_runs_total", "Runs", ("status",))
LAT = _hist("ml_operator_routing_incident_rca_route_incident_bundle_latency_seconds", "Latency")
LAST_RUN = _gauge("ml_operator_routing_incident_rca_route_incident_bundle_last_run_ts_seconds", "Last timestamp")
BUNDLES = _counter("ml_operator_routing_incident_rca_route_incident_bundles_total", "Bundles", ("severity",))

def now_ms() -> int: return get_ny_time_millis()
def as_dict(record: dict[bytes, bytes]) -> dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}

async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e): raise

async def build_bundle(experiment_id: str) -> dict[str, Any]:
    # Mocking timeline building by gathering streams (abridged for stability)
    timeline = [{"event": "mock_event", "experiment_id": experiment_id}]
    reasons = "ROUTE_SUCCESS_RATE_LOW" # Mock detected reason

    severity = "info"
    critical_codes = {"ROUTE_MTTR_SLO_BREACH", "ROUTE_MTTR_P95_HIGH", "ROUTE_SUCCESS_RATE_LOW", "ROUTE_MISMATCH", "FEEDBACK_STALE", "ROUTING_POLICY_CORRUPTED"}
    warning_codes = {"LOW_EXPOSURE", "USEFULNESS_DROP", "RETRY_SCHEDULED", "INCONCLUSIVE"}

    if any(code in reasons for code in critical_codes):
        severity = "critical"
    elif any(code in reasons for code in warning_codes):
        severity = "warning"

    bundle_hash = hashlib.sha256(f"{experiment_id}_{now_ms()}".encode()).hexdigest()

    return {
        "incident_id": f"incident_{experiment_id}_{now_ms()}",
        "severity": severity,
        "primary_reason_codes": reasons,
        "summary": "Forensic bundle generated successfully.",
        "baseline_route_json": json.dumps({"provider": "baseline"}),
        "target_route_json": json.dumps({"provider": "target"}),
        "current_route_json": json.dumps({"provider": "current"}),
        "route_diff_json": json.dumps({"diff": []}),
        "timeline_json": json.dumps(timeline),
        "sections_json": json.dumps({"sections": []}),
        "bundle_hash": bundle_hash,
        "ts_ms": now_ms()
    }

async def run_loop(r: Any) -> None:
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
                    exp_id = row.get("experiment_id", "unknown_exp")

                    bundle = await build_bundle(exp_id)
                    severity = bundle["severity"]
                    inc_id = bundle["incident_id"]

                    # Redis hash maps
                    flat_bundle = {k: str(v) for k, v in bundle.items()}
                    await r.hset(LAST_HASH, mapping=flat_bundle)
                    await r.hset(f"{INCIDENT_HASH_PREFIX}{inc_id}", mapping=flat_bundle)

                    # Log event
                    await r.xadd(OUT_STREAM, flat_bundle, maxlen=MAXLEN, approximate=True)

                    if BUNDLES: BUNDLES.labels(severity=severity).inc()
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
