from __future__ import annotations

import asyncio
import json
import os
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

APP_NAME = "phase2_integration_freeze_auditor_v2_if1"
PORT = int(os.getenv("ML_PHASE2_INTEGRATION_FREEZE_PORT", "9915"))

# Configuration for auditing
CRITICAL_STREAMS = [
    "stream:ml:operator_routing_incident_rca_experiment_winner_decisions",
    "stream:ml:operator_routing_incident_route_rca_routing_apply_results",
    "stream:ml:operator_routing_incident_route_rca_routing_verify_results"
]
OPTIONAL_STREAMS = [
    "stream:ml:operator_routing_incident_route_rca_route_slo_rollups",
    "stream:ml:operator_routing_incident_route_rca_route_retry_requests",
    "stream:ml:operator_routing_incident_route_rca_route_escalations"
]

OUT_STREAM = "stream:ml:phase2_integration_freeze_reports"
AUDIT_STREAM = "stream:ml:phase2_integration_freeze_audit"
METRIC_KEY = "metrics:ml:phase2_integration_freeze:last"

POLL_INTERVAL = int(os.getenv("ML_PHASE2_INTEGRATION_FREEZE_RUN_EVERY_SEC", "300"))
MAX_AGE_MS = int(os.getenv("ML_PHASE2_INTEGRATION_FREEZE_DEFAULT_MAX_AGE_MS", "7200000"))
CRITICAL_MAX_AGE_MS = int(os.getenv("ML_PHASE2_INTEGRATION_FREEZE_CRITICAL_MAX_AGE_MS", "3600000"))

def _counter(name: str, doc: str, labels: tuple = ()) -> Any: return Counter(name, doc, labels) if Counter else None
def _gauge(name: str, doc: str, labels: tuple = ()) -> Any: return Gauge(name, doc, labels) if Gauge else None

VERDICT_GAUGE = _gauge("ml_phase2_integration_freeze_verdict", "Verdict: 0=NO_GO, 1=WARN, 2=GO")
CHECKS_TOTAL = _counter("ml_phase2_integration_freeze_checks_total", "Checks conducted", ("status",))

def now_ms() -> int: return get_ny_time_millis()

async def check_stream(r: Any, stream: str, max_age_ms: int) -> dict[str, Any]:
    try:
        res = await r.xrevrange(stream, count=1)
        if not res:
            return {"status": "FAIL", "reason": f"Stream {stream} is empty"}

        msg_id, payload = res[0]
        ts_ms = int(msg_id.decode().split("-")[0])
        age = now_ms() - ts_ms
        if age > max_age_ms:
            return {"status": "WARN", "reason": f"Stream {stream} is stale ({age//1000}s)", "age_ms": age}

        return {"status": "OK", "stream": stream}
    except Exception as e:
        return {"status": "FAIL", "reason": f"Error checking {stream}: {str(e)}"}

async def run_audit(r: Any) -> None:
    results = []
    verdict = "GO"

    # Critical checks
    for s in CRITICAL_STREAMS:
        res = await check_stream(r, s, CRITICAL_MAX_AGE_MS)
        results.append({"check": s, "type": "CRITICAL", **res})
        if res["status"] == "FAIL": verdict = "NO_GO"
        elif res["status"] == "WARN" and verdict == "GO": verdict = "WARN"

    # Optional checks
    for s in OPTIONAL_STREAMS:
        res = await check_stream(r, s, MAX_AGE_MS)
        results.append({"check": s, "type": "OPTIONAL", **res})
        if res["status"] == "FAIL" and verdict == "GO" or res["status"] == "WARN" and verdict == "GO": verdict = "WARN"

    report = {
        "verdict": verdict,
        "results_json": json.dumps(results),
        "ts_ms": now_ms()
    }

    await r.xadd(OUT_STREAM, report, maxlen=100)
    await r.xadd(AUDIT_STREAM, report, maxlen=1000)
    await r.hset(METRIC_KEY, mapping=report)

    if VERDICT_GAUGE:
        val = 2 if verdict == "GO" else (1 if verdict == "WARN" else 0)
        VERDICT_GAUGE.set(val)
    if CHECKS_TOTAL:
        CHECKS_TOTAL.labels(status=verdict).inc()

async def main() -> None:
    start_http_server(PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    while True:
        await run_audit(r)
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
