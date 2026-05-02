from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, Tuple

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

from orderflow_services.providers.vertex_routing_incident_rca_provider_v2_9 import (
    VertexRoutingIncidentRCAProviderV29,
)


APP_NAME = "ml_operator_routing_incident_rca_orchestrator_v2_9"
REQUESTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_REQUESTS_STREAM",
    "stream:ml:operator_rca_routing_rca_requests",
)
RESULTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_RESULTS_STREAM",
    "stream:ml:operator_rca_routing_rca_results",
)
PROPOSALS_STREAM = os.getenv(
    "ML_RECOMMENDATION_PROPOSALS_STREAM",
    "stream:ml:recommendation_proposals",
)
DLQ_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_DLQ_STREAM",
    "stream:ml:operator_rca_routing_rca_dlq",
)
AUDIT_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_AUDIT_STREAM",
    "stream:ml:operator_rca_routing_rca_audit",
)
GROUP = os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_PORT", "9885"))
MAXLEN = int(os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_MAXLEN", "20000"))
DRY_RUN = int(os.getenv("VERTEX_ROUTING_INCIDENT_RCA_DRY_RUN", "1"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_routing_incident_rca_runs_total",
    "Routing incident RCA runs",
    ("status", "provider"),
)
LAT = _hist(
    "ml_operator_routing_incident_rca_latency_seconds",
    "Routing incident RCA latency seconds",
)
UP = _gauge(
    "ml_operator_routing_incident_rca_up",
    "Routing incident RCA orchestrator up",
)
LAST_RUN_TS = _gauge(
    "ml_operator_routing_incident_rca_last_run_ts_seconds",
    "Routing incident RCA last run timestamp",
)


def now_ms() -> int:
    return get_ny_time_millis()


def as_dict(fields: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        if isinstance(v, (bytes, bytearray)):
            try:
                out[kk] = v.decode()
            except Exception:
                out[kk] = v.hex()
        else:
            out[kk] = v
    return out


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def persist_if_configured(db_url: str, row: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    try:  # pragma: no cover
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """

                    INSERT INTO llm_operator_rca_routing_incident_rca_results (
                        route_change_id,
                        ts_ms,
                        provider,
                        model_name,
                        prompt_version,
                        policy_version,
                        result_json
                    ) VALUES (
                        %(route_change_id)s,
                        %(ts_ms)s,
                        %(provider)s,
                        %(model_name)s,
                        %(prompt_version)s,
                        %(policy_version)s,
                        %(result_json)s
                    )
                    """,
                    {
                        "route_change_id": row["route_change_id"],
                        "ts_ms": row["ts_ms"],
                        "provider": row["provider"],
                        "model_name": row["model_name"],
                        "prompt_version": row["prompt_version"],
                        "policy_version": row["policy_version"],
                        "result_json": json.dumps(row["result"]),
                    }
                )
                conn.commit()
    except Exception:
        return


async def publish_recommendations(r: Any, route_change_id: str, result: Dict[str, Any], maxlen: int) -> None:
    for idx, reco in enumerate(result.get("recommendations", []) or []):
        payload = {
            "schema_version": 1,
            "source": "operator_rca_routing_incident_rca_v2_9",
            "route_change_id": route_change_id,
            "recommendation_id": f"{route_change_id}:{idx}",
            "action_type": reco.get("action", ""),
            "target_kind": "operator_rca_routing_policy",
            "target_ref": reco.get("target", "routing_default"),
            "risk_level": reco.get("risk", "low"),
            "recommendation_json": stable_json(reco),
            "review_only": "1",
            "ts_ms": str(now_ms()),
        }
        await r.xadd(PROPOSALS_STREAM, payload, maxlen=maxlen, approximate=True)


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    await ensure_group(r, REQUESTS_STREAM, GROUP)
    provider = VertexRoutingIncidentRCAProviderV29()
    db_url = os.getenv("DATABASE_URL", "")
    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {REQUESTS_STREAM: ">"}, count=16, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                row = as_dict(payload)
                status = "ok"
                provider_name = "vertex"
                try:
                    route_change_id = row.get("route_change_id", "")
                    request_payload = json.loads(row.get("payload_json", "{}"))
                    if DRY_RUN == 1:
                        result = {
                            "schema_version": 1,
                            "route_change_id": route_change_id,
                            "status": "dry_run",
                            "summary": "dry-run placeholder RCA result for routing incident",
                            "findings": [],
                            "recommendations": [],
                            "provider": "vertex",
                            "model_name": os.getenv("VERTEX_ROUTING_INCIDENT_RCA_MODEL", "gemini-2.5-flash-lite"),
                        }
                    else:
                        result = provider.analyze(request_payload)
                    out = {
                        "schema_version": 1,
                        "route_change_id": route_change_id,
                        "task_type": row.get("task_type", ""),
                        "compact_hash": row.get("compact_hash", ""),
                        "prompt_version": row.get("prompt_version", ""),
                        "policy_version": row.get("policy_version", ""),
                        "provider": result.get("provider", "vertex"),
                        "model_name": result.get("model_name", ""),
                        "result_json": stable_json(result),
                        "ts_ms": str(now_ms()),
                    }
                    await r.xadd(RESULTS_STREAM, out, maxlen=MAXLEN, approximate=True)
                    await publish_recommendations(r, route_change_id, result, MAXLEN)
                    await persist_if_configured(
                        db_url,
                        {
                            "route_change_id": route_change_id,
                            "ts_ms": now_ms(),
                            "provider": out["provider"],
                            "model_name": out["model_name"],
                            "prompt_version": out["prompt_version"],
                            "policy_version": out["policy_version"],
                            "result": result,
                        }
                    )
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        DLQ_STREAM,
                        {
                            "route_change_id": row.get("route_change_id", ""),
                            "payload_json": row.get("payload_json", "{}"),
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, provider=provider_name).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
