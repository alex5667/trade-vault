from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, List

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from orderflow_services.llm_recommendation_guard_v1 import guard_recommendations
from orderflow_services.providers.vertex_genai_provider_v1_1 import VertexGenAIProviderV1_1

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


REQS = Counter("ml_vertex_triage_requests_total", "Vertex requests", ["status"])
LAT = Histogram("ml_vertex_triage_latency_seconds", "Vertex triage latency")
PARSE_FAIL = Counter("ml_vertex_triage_parse_fail_total", "Parse/guard failures")
QUEUE_LAG = Gauge("ml_vertex_triage_queue_lag_ms", "Queue lag")
LAST_RUN = Gauge("ml_vertex_triage_last_run_ts_seconds", "Last run ts")
EST_COST = Counter("ml_vertex_triage_estimated_cost_usd_total", "Estimated cost usd", ["model"])


def _write_pg(db_url: str, provider_result: Dict[str, Any], compact_pack: Dict[str, Any], guarded: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_analysis_runs (
                  analysis_run_id, ts_ms, provider, model_name, task_type,
                  scope_json, input_refs_json, output_json, status, latency_ms, cost_usd
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (analysis_run_id) DO NOTHING
                """,
                (
                    str(guarded.get("analysis_run_id") or compact_pack.get("request_id")),
                    int(compact_pack.get("ts_ms") or get_ny_time_millis()),
                    "vertex",
                    str(provider_result.get("model_name") or "unknown"),
                    str(compact_pack.get("task_type") or "root_cause_degradation"),
                    json.dumps(compact_pack.get("scope") or {}, ensure_ascii=False),
                    json.dumps({"compact_hash": compact_pack.get("compact_hash")}, ensure_ascii=False),
                    json.dumps(guarded, ensure_ascii=False),
                    str(guarded.get("status") or "unknown"),
                    int(provider_result.get("latency_ms") or 0),
                    float(provider_result.get("estimated_cost_usd") or 0.0),
                ),
            )
        conn.commit()


def main() -> None:
    if redis is None:
        raise RuntimeError("redis package is required")
    start_http_server(int(os.getenv("ML_VERTEX_TRIAGE_ORCH_PORT", "9860")))
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    provider = VertexGenAIProviderV1_1(redis_url=redis_url)
    in_stream = os.getenv("ML_ANALYSIS_REQUESTS_COMPACT_STREAM", "stream:ml:analysis_requests_compact")
    out_results = os.getenv("ML_ANALYSIS_RESULTS_STREAM", "stream:ml:analysis_results")
    out_recos = os.getenv("ML_RECOMMENDATION_PROPOSALS_STREAM", "stream:ml:recommendation_proposals")
    dlq = os.getenv("ML_ANALYSIS_DLQ_STREAM", "stream:ml:analysis_dlq")
    group = os.getenv("ML_VERTEX_TRIAGE_GROUP", "cg:ml_vertex_triage_v1_1")
    consumer = os.getenv("ML_VERTEX_TRIAGE_CONSUMER", "ml-vertex-triage-1")
    db_url = os.getenv("DATABASE_URL", "")
    try:
        r.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception:
        pass
    while True:
        rows = r.xreadgroup(groupname=group, consumername=consumer, streams={in_stream: ">"}, count=32, block=5000)
        if not rows:
            LAST_RUN.set(time.time())
            continue
        for _, msgs in rows:
            for msg_id, fields in msgs:
                try:
                    compact = json.loads(fields.get("payload", "{}"))
                    try:
                        QUEUE_LAG.set(max(0, get_ny_time_millis() - int(compact.get("ts_ms") or get_ny_time_millis())))
                    except Exception:
                        pass
                    t0 = time.time()
                    provider_result = provider.analyze(compact)
                    guarded = guard_recommendations(provider_result.parsed or {})
                    result_payload = {
                        "analysis_run_id": guarded.get("analysis_run_id") or compact.get("request_id"),
                        "request_id": compact.get("request_id"),
                        "task_type": compact.get("task_type"),
                        "provider": "vertex",
                        "model_name": provider_result.model_name,
                        "prompt_version": provider_result.prompt_version,
                        "policy_version": provider_result.policy_version,
                        "compact_hash": compact.get("compact_hash"),
                        "status": guarded.get("status", "ok"),
                        "payload": guarded,
                    }
                    r.xadd(out_results, {"payload": json.dumps(result_payload, ensure_ascii=False)}, maxlen=int(os.getenv("ML_ANALYSIS_RESULTS_MAXLEN", "200000")), approximate=True)
                    for reco in (guarded.get("recommendations") or []):
                        reco_payload = dict(reco)
                        reco_payload["analysis_run_id"] = result_payload["analysis_run_id"]
                        reco_payload["prompt_version"] = provider_result.prompt_version
                        reco_payload["policy_version"] = provider_result.policy_version
                        reco_payload["compact_hash"] = compact.get("compact_hash")
                        r.xadd(out_recos, {"payload": json.dumps(reco_payload, ensure_ascii=False)}, maxlen=int(os.getenv("ML_RECOMMENDATION_PROPOSALS_MAXLEN", "200000")), approximate=True)
                    _write_pg(db_url, provider_result.__dict__, compact, guarded)
                    LAT.observe(max(0.0, time.time() - t0))
                    EST_COST.labels(model=provider_result.model_name).inc(provider_result.estimated_cost_usd)
                    REQS.labels(status="ok").inc()
                except Exception as exc:
                    REQS.labels(status="err").inc()
                    PARSE_FAIL.inc()
                    r.xadd(dlq, {"error": str(exc), "payload": json.dumps(fields, ensure_ascii=False)}, maxlen=int(os.getenv("ML_ANALYSIS_DLQ_MAXLEN", "50000")), approximate=True)
                finally:
                    try:
                        r.xack(in_stream, group, msg_id)
                    except Exception:
                        pass
                    LAST_RUN.set(time.time())


if __name__ == "__main__":  # pragma: no cover
    main()

