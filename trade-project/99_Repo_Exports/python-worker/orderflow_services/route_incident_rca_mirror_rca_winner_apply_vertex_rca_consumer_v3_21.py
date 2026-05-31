from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Any

from utils.time_utils import get_ny_time_millis

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_vertex_rca_consumer_v3_21"

REQUESTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_REQUESTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_requests")
RESULTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_RESULTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_results")
LAST_RESULT_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_LAST_METRIC", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_PORT", "9942"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_MAXLEN", "2000"))

# DETERMINISTIC, MOCK_SLOW, LLM_CALL
HANDLER_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_HANDLER_MODE", "DETERMINISTIC")

POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_vertex_rca_runs_total", "Runs", ("status", "decision"))
RESULTS_TOTAL = _counter("ml_route_incident_rca_mirror_rca_winner_apply_vertex_rca_results_total", "Results total", ("severity", "provider_mode"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_vertex_rca_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_vertex_rca_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_vertex_rca_last_run_ts_seconds", "Last run")


def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: dict[Any, Any]) -> dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def generate_deterministic_result(request_id: str, bundle_json: str) -> dict[str, Any]:
    try:
        parsed = json.loads(bundle_json)
        desc = parsed.get("trigger", {}).get("description", "unknown")
        sev = parsed.get("trigger", {}).get("severity", "info")
    except Exception:
        desc = "unknown"
        sev = "info"

    dominant_finding = "policy_mismatch"
    if "ROLLBACK_MTTR" in desc:
        dominant_finding = "system_lag_or_persistence_issue"
    elif "verify_keep_rate" in desc:
        dominant_finding = "model_hallucination_or_mismatch"

    return {
        "result_id": f"rca_res_{uuid.uuid4().hex[:8]}",
        "request_id": request_id,
        "ts_ms": now_ms(),
        "provider": "deterministic",
        "severity": sev,
        "rca_payload": {
            "summary": f"Deterministic mock finding for winner apply failure: {desc}",
            "dominant_findings": [dominant_finding],
            "hypotheses": [
                f"Hypothesis 1 for {dominant_finding}",
                "Hypothesis 2: Check target allocation boundaries"
            ],
            "next_actions": ["Review target config", "Check DB loads"],
            "confidence": 0.95,
            "quality_flags": ["deterministic_mock"]
        }
    }

async def handle_request(mode: str, request_id: str, bundle_json: str) -> dict[str, Any]:
    if mode == "MOCK_SLOW":
        await asyncio.sleep(1.0)
        return await generate_deterministic_result(request_id, bundle_json)

    # Defaults to DETERMINISTIC (we don't have LLM call configured in this env)
    return await generate_deterministic_result(request_id, bundle_json)


async def persist_result(db_url: str, res: dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_vertex_rca_results (
                    result_id, request_id, provider, severity, result_json, ts_ms
                ) VALUES (
                    %(result_id)s, %(request_id)s, %(provider)s, %(severity)s, %(result_json)s, %(ts_ms)s
                )
                """,
                {
                    "result_id": res["result_id"],
                    "request_id": res["request_id"],
                    "provider": res["provider"],
                    "severity": res["severity"],
                    "result_json": json.dumps(res),
                    "ts_ms": res["ts_ms"],
                }
            )
            conn.commit()


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)

    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    last_id = "0-0"

    try:
        last_metric = await r.hgetall(LAST_RESULT_METRIC)
        if last_metric:
            # Similar logic as before, we poll from end
            last_id = "$"
    except Exception:
        pass

    while True:
        started = time.perf_counter()
        status = "ok"
        decision = "none"

        try:
            res_stream = await r.xread({REQUESTS_STREAM: last_id}, count=10, block=int(POLL_INTERVAL_SEC * 1000))
            if res_stream:
                for stream_name, messages in res_stream:
                    for msg_id, fields in messages:
                        last_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id

                        decoded = decode_dict(fields)
                        bundle_json = decoded.get("bundle_json", "{}")

                        # Generate ID for request for tracking (normally ID is from stream)
                        req_id = f"req_{msg_id.replace('-', '_')}"

                        result = await handle_request(HANDLER_MODE, req_id, bundle_json)
                        decision = "BUILT_RESULT"

                        result_json = json.dumps(result)

                        await r.xadd(RESULTS_STREAM, {
                            "request_id": req_id,
                            "result_id": result["result_id"],
                            "result_json": result_json,
                            "ts_ms": str(now_ms())
                        }, maxlen=MAXLEN, approximate=True)

                        await r.hset(LAST_RESULT_METRIC, "status", "built")
                        await r.hset(LAST_RESULT_METRIC, "request_id", req_id)
                        await r.hset(LAST_RESULT_METRIC, "ts_ms", str(now_ms()))

                        if RESULTS_TOTAL:
                            RESULTS_TOTAL.labels(severity=result["severity"], provider_mode=result["provider"]).inc()

                        await persist_result(db_url, result)

            if LAST_RUN:
                LAST_RUN.set(time.time())

        except Exception:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

            if not res_stream:
                await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
