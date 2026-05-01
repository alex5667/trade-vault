from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from typing import Any, Dict

import redis.asyncio as redis
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from orderflow_services.llm_recommendation_guard_v1 import guard_recommendations
from orderflow_services.providers.vertex_genai_provider_v1_2 import VertexGenAIProviderV12, VertexProviderError
from orderflow_services.vertex_cost_accounting_v1 import CostRecord, record_cost


REQUESTS_STREAM = os.getenv("ML_ANALYSIS_BATCH_REQUESTS_STREAM", "stream:ml:analysis_batch_requests")
RESULTS_STREAM = os.getenv("ML_ANALYSIS_RESULTS_STREAM", "stream:ml:analysis_results")
PROPOSALS_STREAM = os.getenv("ML_RECOMMENDATION_PROPOSALS_STREAM", "stream:ml:recommendation_proposals")
DLQ_STREAM = os.getenv("ML_ANALYSIS_DLQ_STREAM", "stream:ml:analysis_dlq")
GROUP = os.getenv("ML_VERTEX_BATCH_REVIEW_GROUP", "cg:ml_vertex_batch_review_v1")
CONSUMER = os.getenv("HOSTNAME", "ml-vertex-batch-review-v1")
LAST_HASH = os.getenv("ML_ANALYSIS_RESULTS_LAST_HASH", "metrics:ml:analysis_results:last")

REQS = Counter("ml_batch_analysis_requests_total", "Batch analysis requests", ["provider", "status"])
ITEMS = Counter("ml_batch_analysis_items_total", "Batch analysis items", ["provider", "status"])
LAT = Histogram("ml_batch_analysis_latency_seconds", "Batch analysis latency", ["provider"])
LAST_RUN_TS = Gauge("ml_vertex_batch_review_last_run_ts_seconds", "Last run ts")
UP = Gauge("ml_vertex_batch_review_up", "Health")
Q_LAG_MS = Gauge("ml_batch_analysis_queue_lag_ms", "Queue lag ms")
COST_USD = Gauge("ml_batch_analysis_last_cost_usd", "Last accounted cost usd")


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
                    """,
                    INSERT INTO llm_analysis_runs (
                      analysis_run_id, ts_ms, provider, model_name, task_type,
                      scope_json, input_refs_json, output_json, status, latency_ms, cost_usd,
                      prompt_version, policy_version, compact_hash, batch_id,
                      input_chars, output_chars, estimated_cost_usd, actual_cost_usd, context_cache_ref
                    ) VALUES (
                      %(analysis_run_id)s, %(ts_ms)s, %(provider)s, %(model_name)s, %(task_type)s,
                      %(scope_json)s::jsonb, %(input_refs_json)s::jsonb, %(output_json)s::jsonb,
                      %(status)s, %(latency_ms)s, %(cost_usd)s,
                      %(prompt_version)s, %(policy_version)s, %(compact_hash)s, %(batch_id)s,
                      %(input_chars)s, %(output_chars)s, %(estimated_cost_usd)s, %(actual_cost_usd)s, %(context_cache_ref)s
                    )
                    ON CONFLICT (analysis_run_id) DO NOTHING
                    """,
                    payload,
                )
            await conn.commit()
    except Exception:
        return


async def main() -> None:
    start_http_server(int(os.getenv("ML_VERTEX_BATCH_REVIEW_METRICS_PORT", "9863")))
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    provider = VertexGenAIProviderV12()
    try:
        await r.xgroup_create(REQUESTS_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass
    UP.set(1.0)
    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {REQUESTS_STREAM: ">"}, count=10, block=5000)
        if not rows:
            await asyncio.sleep(1.0)
            continue
        for _stream, msgs in rows:
            for msg_id, fields in msgs:
                data = {_s(k): _s(v) for k, v in fields.items()}
                try:
                    payload = json.loads(data.get("payload") or "{}")
                except Exception:
                    payload = {}
                try:
                    lag_ms = max(0, get_ny_time_millis() - int(payload.get("ts_ms", 0) or 0))
                    Q_LAG_MS.set(lag_ms)
                except Exception:
                    pass
                try:
                    t0 = time.perf_counter()
                    results = provider.analyze_batch(payload)
                    REQS.labels(provider="vertex", status="ok").inc()
                    ITEMS.labels(provider="vertex", status="ok").inc(len(results))
                    for idx, res in enumerate(results):
                        item = (payload.get("items_json") or [])[idx] if idx < len(payload.get("items_json") or []) else {}
                        model_target = str(item.get("model_id") or res.request_id)
                        guarded = guard_recommendations(res.output_json)
                        analysis_run_id = str(res.output_json.get("analysis_run_id") or f"{res.batch_id}_{idx}")
                        out_row = {
                            "analysis_run_id": analysis_run_id,
                            "ts_ms": get_ny_time_millis(),
                            "provider": res.provider,
                            "model_name": res.model_name,
                            "task_type": payload.get("task_type", "fleet_batch_triage"),
                            "scope_json": json.dumps({"model_id": model_target, "batch_id": res.batch_id}, ensure_ascii=False),
                            "input_refs_json": json.dumps({"request_id": res.request_id, "batch_id": res.batch_id}, ensure_ascii=False),
                            "output_json": json.dumps(res.output_json, ensure_ascii=False),
                            "status": "ok" if guarded.get("valid") else "guard_error",
                            "latency_ms": res.latency_ms,
                            "cost_usd": res.actual_cost_usd,
                            "prompt_version": payload.get("prompt_version", "unknown"),
                            "policy_version": payload.get("policy_version", "unknown"),
                            "compact_hash": payload.get("batch_compact_hash", ""),
                            "batch_id": res.batch_id,
                            "input_chars": res.input_chars,
                            "output_chars": res.output_chars,
                            "estimated_cost_usd": res.estimated_cost_usd,
                            "actual_cost_usd": res.actual_cost_usd,
                            "context_cache_ref": res.context_cache_ref,
                        }
                        await _write_result_db_if_possible(out_row)
                        await r.xadd(RESULTS_STREAM, {
                            "schema_version": 1,
                            "analysis_run_id": analysis_run_id,
                            "request_id": res.request_id,
                            "batch_id": res.batch_id,
                            "provider": res.provider,
                            "model_name": res.model_name,
                            "task_type": out_row["task_type"],
                            "ts_ms": out_row["ts_ms"],
                            "status": out_row["status"],
                            "output_json": out_row["output_json"],
                            "guarded_recommendations_json": json.dumps(guarded.get("guarded_recommendations", []), ensure_ascii=False),
                            "blocked_recommendations_json": json.dumps(guarded.get("blocked_recommendations", []), ensure_ascii=False),
                            "estimated_cost_usd": res.estimated_cost_usd,
                            "actual_cost_usd": res.actual_cost_usd,
                            "context_cache_ref": res.context_cache_ref,
                        }, maxlen=int(os.getenv("ML_ANALYSIS_RESULTS_STREAM_MAXLEN", "100000") or 100000), approximate=True)
                        for rec in guarded.get("guarded_recommendations", []):
                            await r.xadd(PROPOSALS_STREAM, {
                                "schema_version": 1,
                                "analysis_run_id": analysis_run_id,
                                "request_id": res.request_id,
                                "batch_id": res.batch_id,
                                "ts_ms": out_row["ts_ms"],
                                "action": rec.get("action", ""),
                                "target": rec.get("target", model_target),
                                "risk": rec.get("risk", "medium"),
                                "reason_code": rec.get("reason_code", ""),
                                "prompt_version": out_row["prompt_version"],
                                "policy_version": out_row["policy_version"],
                                "apply_mode": "REVIEW_ONLY",
                                "recommendation_json": json.dumps(rec, ensure_ascii=False),
                            }, maxlen=int(os.getenv("ML_RECOMMENDATION_PROPOSALS_STREAM_MAXLEN", "100000") or 100000), approximate=True)
                        record_cost(os.getenv("REDIS_URL", "redis://localhost:6379/0"), CostRecord(
                            provider=res.provider,
                            model_name=res.model_name,
                            request_id=res.request_id,
                            batch_id=res.batch_id,
                            ts_ms=out_row["ts_ms"],
                            input_chars=res.input_chars,
                            output_chars=res.output_chars,
                            estimated_cost_usd=res.estimated_cost_usd,
                            actual_cost_usd=res.actual_cost_usd,
                            context_cache_ref=res.context_cache_ref,
                        ))
                        COST_USD.set(res.actual_cost_usd)
                    await r.hset(LAST_HASH, mapping={
                        "batch_id": payload.get("batch_id", ""),
                        "provider": "vertex",
                        "model_name": provider.model_name,
                        "task_type": payload.get("task_type", "fleet_batch_triage"),
                        "status": "ok",
                        "ts_ms": get_ny_time_millis(),
                    })
                    LAT.labels(provider="vertex").observe(max(0.0, time.perf_counter() - t0))
                    LAST_RUN_TS.set(time.time())
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                except VertexProviderError as exc:
                    REQS.labels(provider="vertex", status="err_provider").inc()
                    await r.xadd(DLQ_STREAM, {
                        "schema_version": 1,
                        "batch_id": payload.get("batch_id", ""),
                        "task_type": payload.get("task_type", "fleet_batch_triage"),
                        "error_kind": "provider_error",
                        "error_text": str(exc),
                        "payload_json": json.dumps(data, ensure_ascii=False),
                        "ts_ms": get_ny_time_millis(),
                    }, maxlen=int(os.getenv("ML_ANALYSIS_DLQ_STREAM_MAXLEN", "50000") or 50000), approximate=True)
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                except Exception as exc:
                    REQS.labels(provider="vertex", status="err_runtime").inc()
                    await r.xadd(DLQ_STREAM, {
                        "schema_version": 1,
                        "batch_id": payload.get("batch_id", ""),
                        "task_type": payload.get("task_type", "fleet_batch_triage"),
                        "error_kind": "runtime_error",
                        "error_text": str(exc),
                        "payload_json": json.dumps(data, ensure_ascii=False),
                        "ts_ms": get_ny_time_millis(),
                    }, maxlen=int(os.getenv("ML_ANALYSIS_DLQ_STREAM_MAXLEN", "50000") or 50000), approximate=True)
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)


if __name__ == "__main__":
    asyncio.run(main())
