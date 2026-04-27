from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, Tuple

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from orderflow_services.providers.vertex_incident_rca_provider_v2_0 import VertexIncidentRCAProviderV20


REQUEST_STREAM = os.getenv("ML_OPERATOR_RCA_REQUEST_STREAM", "stream:ml:operator_rca_requests")
RESULTS_STREAM = os.getenv("ML_OPERATOR_RCA_RESULTS_STREAM", "stream:ml:operator_rca_results")
PROPOSALS_STREAM = os.getenv("ML_OPERATOR_RCA_PROPOSALS_STREAM", "stream:ml:recommendation_proposals")
DLQ_STREAM = os.getenv("ML_OPERATOR_RCA_DLQ_STREAM", "stream:ml:operator_rca_dlq")
AUDIT_STREAM = os.getenv("ML_OPERATOR_RCA_AUDIT_STREAM", "stream:ml:recommendation_audit")
STATE_KEY = os.getenv("ML_OPERATOR_RCA_ORCH_STATE_KEY", "metrics:ml:operator_rca:last")
GROUP = os.getenv("ML_OPERATOR_RCA_GROUP", "cg:ml_operator_rca_orchestrator_v2_0")
CONSUMER = os.getenv("ML_OPERATOR_RCA_CONSUMER", "ml-operator-rca-v2-0")
PROM_PORT = int(os.getenv("ML_OPERATOR_RCA_PORT", "9869"))

RUNS = Counter("ml_operator_rca_runs_total", "Operator RCA orchestrator loops")
REQUESTS = Counter("ml_operator_rca_requests_total", "Operator RCA requests processed", ["status"])
LATENCY = Histogram("ml_operator_rca_latency_seconds", "Vertex RCA latency seconds")
LAST_RUN_TS = Gauge("ml_operator_rca_last_run_ts_seconds", "Last run ts")
QUEUE_LAG_MS = Gauge("ml_operator_rca_queue_lag_ms", "Approximate request queue lag")


def _b2s(v: Any) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def _loads(v: Any, default: Any) -> Any:
    try:
        if v in (None, "", b""):
            return default
        return json.loads(_b2s(v))
    except Exception:
        return default


def _now_ms() -> int:
    return get_ny_time_millis()


def _validate_output(output: Dict[str, Any]) -> Tuple[bool, str]:
    if not isinstance(output, dict):
        return False, "output_not_object"
    if not isinstance(output.get("findings", []), list):
        return False, "findings_not_list"
    recs = output.get("recommendations", [])
    if not isinstance(recs, list):
        return False, "recommendations_not_list"
    return True, "ok"


def _normalize_proposal(recommendation_id: str, output: Dict[str, Any]) -> Dict[str, Any]:
    recs = output.get("recommendations", [])
    first = recs[0] if recs else {}
    return {
        "schema_version": 1,
        "recommendation_id": recommendation_id,
        "ts_ms": _now_ms(),
        "source": "operator_rca_v2_0",
        "risk_level": str(first.get("risk", "low")),
        "action_type": str(first.get("action", "draft_postmortem")),
        "target_kind": "model",
        "target_ref": str(first.get("target", "")),
        "recommendation_json": json.dumps(output, separators=(",", ":"), sort_keys=True),
        "apply_status": "PENDING",
    }


async def _ensure_group(r: Any) -> None:
    try:
        await r.xgroup_create(REQUEST_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass


async def run_forever() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PROM_PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    await _ensure_group(r)
    provider = VertexIncidentRCAProviderV20()
    block_ms = int(os.getenv("ML_OPERATOR_RCA_BLOCK_MS", "5000"))
    count = int(os.getenv("ML_OPERATOR_RCA_READ_COUNT", "16"))

    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {REQUEST_STREAM: ">"}, count=count, block=block_ms)
        RUNS.inc()
        for _stream, messages in rows:
            for msg_id, fields in messages:
                ts_ms = int(_b2s(fields.get(b"ts_ms", b"0")) or "0")
                try:
                    QUEUE_LAG_MS.set(max(0, _now_ms() - ts_ms))
                except Exception:
                    pass
                recommendation_id = _b2s(fields.get(b"recommendation_id", b""))
                payload = _loads(fields.get(b"input_pack_json"), {})
                if not payload:
                    await r.xadd(DLQ_STREAM, {"reason": "missing_input_pack", "original_id": msg_id}, maxlen=200000, approximate=True)
                    REQUESTS.labels(status="dlq").inc()
                    await r.xack(REQUEST_STREAM, GROUP, msg_id)
                    continue
                try:
                    t0 = time.perf_counter()
                    resp = provider.analyze(payload)
                    LATENCY.observe(time.perf_counter() - t0)
                    ok, reason = _validate_output(resp.output_json)
                    if not ok:
                        await r.xadd(DLQ_STREAM, {"reason": reason, "recommendation_id": recommendation_id, "output_json": json.dumps(resp.output_json, separators=(",", ":"), sort_keys=True)}, maxlen=20000, approximate=True)
                        REQUESTS.labels(status="dlq").inc()
                    else:
                        result = {
                            "schema_version": 1,
                            "recommendation_id": recommendation_id,
                            "ts_ms": _now_ms(),
                            "provider": resp.provider,
                            "model_name": resp.model_name,
                            "status": resp.status,
                            "latency_ms": resp.latency_ms,
                            "estimated_cost_usd": resp.estimated_cost_usd,
                            "output_json": json.dumps(resp.output_json, separators=(",", ":"), sort_keys=True),
                        }
                        await r.xadd(RESULTS_STREAM, result, maxlen=200000, approximate=True)
                        await r.xadd(PROPOSALS_STREAM, _normalize_proposal(recommendation_id, resp.output_json), maxlen=100000, approximate=True)
                        await r.xadd(AUDIT_STREAM, {"ts_ms": _now_ms(), "event": "OPERATOR_RCA_COMPLETED", "recommendation_id": recommendation_id, "provider": resp.provider, "model_name": resp.model_name, "status": resp.status}, maxlen=200000, approximate=True)
                        await r.hset(STATE_KEY, mapping={"last_ts_ms": str(_now_ms()), "last_recommendation_id": recommendation_id, "last_provider": resp.provider, "last_model_name": resp.model_name, "last_status": resp.status})
                        REQUESTS.labels(status="ok").inc()
                except Exception as exc:
                    await r.xadd(DLQ_STREAM, {"reason": "provider_exception", "recommendation_id": recommendation_id, "error": str(exc)[:500]}, maxlen=20000, approximate=True)
                    REQUESTS.labels(status="error").inc()
                await r.xack(REQUEST_STREAM, GROUP, msg_id)
        LAST_RUN_TS.set(time.time())


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(run_forever())
