from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
import uuid
from typing import Any, Dict

import redis.asyncio as redis
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from orderflow_services.llm_recommendation_guard_v1 import guard_recommendations
from orderflow_services.providers.vertex_genai_provider_v1 import VertexGenAIProviderV1, VertexProviderError


REQUESTS_STREAM = os.getenv("ML_ANALYSIS_REQUESTS_STREAM", "stream:ml:analysis_requests")
RESULTS_STREAM = os.getenv("ML_ANALYSIS_RESULTS_STREAM", "stream:ml:analysis_results")
PROPOSALS_STREAM = os.getenv("ML_RECOMMENDATION_PROPOSALS_STREAM", "stream:ml:recommendation_proposals")
DLQ_STREAM = os.getenv("ML_ANALYSIS_DLQ_STREAM", "stream:ml:analysis_dlq")
GROUP = os.getenv("ML_VERTEX_TRIAGE_GROUP", "cg:ml_vertex_triage_v1")
CONSUMER = os.getenv("HOSTNAME", "ml-vertex-triage-v1")
LAST_HASH = os.getenv("ML_ANALYSIS_RESULTS_LAST_HASH", "metrics:ml:analysis_results:last")

REQS = Counter("ml_analysis_requests_total", "LLM analysis requests", ["provider", "status", "task_type"])
LAT = Histogram("ml_analysis_latency_seconds", "LLM analysis latency", ["provider", "task_type"])
PARSE_FAIL = Counter("ml_analysis_parse_fail_total", "Parse/guard failures", ["provider"])
LAST_RUN_TS = Gauge("ml_vertex_triage_last_run_ts_seconds", "Last run ts")
UP = Gauge("ml_vertex_triage_up", "Health")
Q_LAG_MS = Gauge("ml_analysis_queue_lag_ms", "Queue lag ms")


def _s(x: Any) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", "replace")
    return str(x)


async def _write_result_db_if_possible(payload: Dict[str, Any]) -> None:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return
    try:
        import psycopg
        async with await psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO llm_analysis_runs (
                      analysis_run_id, ts_ms, provider, model_name, task_type,
                      scope_json, input_refs_json, output_json, status, latency_ms, cost_usd
                    ) VALUES (
                      %(analysis_run_id)s, %(ts_ms)s, %(provider)s, %(model_name)s, %(task_type)s,
                      %(scope_json)s::jsonb, %(input_refs_json)s::jsonb, %(output_json)s::jsonb,
                      %(status)s, %(latency_ms)s, %(cost_usd)s
                    )
                    ON CONFLICT (analysis_run_id) DO NOTHING
                    """,
                    payload,
                )
            await conn.commit()
    except Exception:
        return


async def main() -> None:
    start_http_server(int(os.getenv("ML_VERTEX_TRIAGE_METRICS_PORT", "9848")))
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    provider = VertexGenAIProviderV1()
    try:
        await r.xgroup_create(REQUESTS_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass

    UP.set(1.0)
    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {REQUESTS_STREAM: ">"}, count=20, block=5000)
        if not rows:
            await asyncio.sleep(1.0)
            continue
        for _stream, msgs in rows:
            for msg_id, fields in msgs:
                data = {_s(k): _s(v) for k, v in fields.items()}
                task_type = data.get("task_type", "unknown")
                try:
                    lag_ms = max(0, get_ny_time_millis() - int(data.get("ts_ms", "0") or "0"))
                    Q_LAG_MS.set(lag_ms)
                except Exception:
                    pass
                try:
                    t0 = time.perf_counter()
                    result = provider.analyze(data)
                    guarded = guard_recommendations(result.output_json)
                    analysis_run_id = str(result.output_json.get("analysis_run_id") or uuid.uuid4().hex)
                    out_row = {
                        "analysis_run_id": analysis_run_id,
                        "ts_ms": get_ny_time_millis(),
                        "provider": result.provider,
                        "model_name": result.model_name,
                        "task_type": task_type,
                        "scope_json": data.get("scope_json", "{}"),
                        "input_refs_json": json.dumps({"request_id": data.get("request_id")}, ensure_ascii=False),
                        "output_json": json.dumps(result.output_json, ensure_ascii=False),
                        "status": "ok" if guarded.get("valid") else "guard_error",
                        "latency_ms": result.latency_ms,
                        "cost_usd": 0.0,
                    }
                    await _write_result_db_if_possible(out_row)
                    await r.xadd(RESULTS_STREAM, {
                        "schema_version": 1,
                        "analysis_run_id": analysis_run_id,
                        "request_id": data.get("request_id", ""),
                        "provider": result.provider,
                        "model_name": result.model_name,
                        "task_type": task_type,
                        "ts_ms": out_row["ts_ms"],
                        "status": out_row["status"],
                        "output_json": out_row["output_json"],
                        "guarded_recommendations_json": json.dumps(guarded.get("guarded_recommendations", []), ensure_ascii=False),
                        "blocked_recommendations_json": json.dumps(guarded.get("blocked_recommendations", []), ensure_ascii=False),
                    }, maxlen=int(os.getenv("ML_ANALYSIS_RESULTS_STREAM_MAXLEN", "100000")), approximate=True)
                    for item in guarded.get("guarded_recommendations", []):
                        await r.xadd(PROPOSALS_STREAM, {
                            "schema_version": 1,
                            "analysis_run_id": analysis_run_id,
                            "request_id": data.get("request_id", ""),
                            "ts_ms": out_row["ts_ms"],
                            "action": item.get("action", ""),
                            "target": item.get("target", ""),
                            "risk": item.get("risk", "medium"),
                            "reason_code": item.get("reason_code", ""),
                            "apply_mode": "REVIEW_ONLY",
                            "recommendation_json": json.dumps(item, ensure_ascii=False),
                        }, maxlen=int(os.getenv("ML_RECOMMENDATION_PROPOSALS_STREAM_MAXLEN", "100000")), approximate=True)
                    await r.hset(LAST_HASH, mapping={
                        "analysis_run_id": analysis_run_id,
                        "request_id": data.get("request_id", ""),
                        "provider": result.provider,
                        "model_name": result.model_name,
                        "task_type": task_type,
                        "status": out_row["status"],
                        "latency_ms": out_row["latency_ms"],
                        "ts_ms": out_row["ts_ms"],
                    })
                    LAT.labels(provider=result.provider, task_type=task_type).observe(max(0.0, time.perf_counter() - t0))
                    REQS.labels(provider=result.provider, status="ok", task_type=task_type).inc()
                    LAST_RUN_TS.set(time.time())
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                except VertexProviderError as exc:
                    REQS.labels(provider="vertex", status="err_provider", task_type=task_type).inc()
                    await r.xadd(DLQ_STREAM, {
                        "schema_version": 1,
                        "request_id": data.get("request_id", ""),
                        "task_type": task_type,
                        "error_kind": "provider_error",
                        "error_text": str(exc),
                        "payload_json": json.dumps(data, ensure_ascii=False),
                        "ts_ms": get_ny_time_millis(),
                    }, maxlen=int(os.getenv("ML_ANALYSIS_DLQ_STREAM_MAXLEN", "50000")), approximate=True)
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                except Exception as exc:
                    PARSE_FAIL.labels(provider="vertex").inc()
                    REQS.labels(provider="vertex", status="err_runtime", task_type=task_type).inc()
                    await r.xadd(DLQ_STREAM, {
                        "schema_version": 1,
                        "request_id": data.get("request_id", ""),
                        "task_type": task_type,
                        "error_kind": "runtime_error",
                        "error_text": str(exc),
                        "payload_json": json.dumps(data, ensure_ascii=False),
                        "ts_ms": get_ny_time_millis(),
                    }, maxlen=int(os.getenv("ML_ANALYSIS_DLQ_STREAM_MAXLEN", "50000")), approximate=True)
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)


if __name__ == "__main__":
    asyncio.run(main())
