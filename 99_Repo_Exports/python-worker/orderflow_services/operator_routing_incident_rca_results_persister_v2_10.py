from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
from typing import Any, Dict, Tuple

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

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


APP_NAME = "operator_routing_incident_rca_results_persister_v2_10"
RESULTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_RESULTS_STREAM",
    "stream:ml:operator_rca_routing_rca_results",
)
QUALITY_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_QUALITY_STREAM",
    "stream:ml:operator_rca_routing_rca_quality",
)
GROUP = os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_PERSISTER_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_PERSISTER_PORT", "9886"))
MAXLEN = int(os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_PERSISTER_MAXLEN", "20000"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_routing_incident_rca_results_runs_total",
    "Routing incident RCA results persister runs",
    ("status",),
)
LAT = _hist(
    "ml_operator_routing_incident_rca_results_latency_seconds",
    "Routing incident RCA results persister latency seconds",
)
UP = _gauge(
    "ml_operator_routing_incident_rca_results_up",
    "Routing incident RCA results persister up",
)
LAST_RUN_TS = _gauge(
    "ml_operator_routing_incident_rca_results_last_run_ts_seconds",
    "Routing incident RCA results persister last run timestamp",
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


def compute_output_hash(payload: Dict[str, Any]) -> str:
    key_material = {
        "route_change_id": payload.get("route_change_id", ""),
        "provider": payload.get("provider", ""),
        "model_name": payload.get("model_name", ""),
        "result_json": payload.get("result_json", "{}"),
    }
    return hashlib.sha256(stable_json(key_material).encode("utf-8")).hexdigest()[:16]


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def persist_result(db_url: str, row: Dict[str, Any], output_hash: str) -> None:
    if not db_url or psycopg is None:
        return
    try:  # pragma: no cover
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """

                    INSERT INTO llm_operator_routing_incident_rca_results (
                        output_hash,
                        route_change_id,
                        task_type,
                        compact_hash,
                        prompt_version,
                        policy_version,
                        provider,
                        model_name,
                        result_json,
                        ts_ms
                    ) VALUES (
                        %(output_hash)s,
                        %(route_change_id)s,
                        %(task_type)s,
                        %(compact_hash)s,
                        %(prompt_version)s,
                        %(policy_version)s,
                        %(provider)s,
                        %(model_name)s,
                        %(result_json)s,
                        %(ts_ms)s
                    )
                    ON CONFLICT(output_hash) DO NOTHING,
                    """,
                    {
                        "output_hash": output_hash,
                        "route_change_id": row.get("route_change_id", ""),
                        "task_type": row.get("task_type", ""),
                        "compact_hash": row.get("compact_hash", ""),
                        "prompt_version": row.get("prompt_version", ""),
                        "policy_version": row.get("policy_version", ""),
                        "provider": row.get("provider", "vertex"),
                        "model_name": row.get("model_name", ""),
                        "result_json": row.get("result_json", "{}"),
                        "ts_ms": row.get("ts_ms", now_ms()),
                    }
                )
                conn.commit()
    except Exception:
        return


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    await ensure_group(r, RESULTS_STREAM, GROUP)
    db_url = os.getenv("DATABASE_URL", "")
    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {RESULTS_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                row = as_dict(payload)
                status = "ok"
                try:
                    output_hash = compute_output_hash(row)
                    await persist_result(db_url, row, output_hash)
                    
                    quality_req = {
                        "schema_version": 1,
                        "output_hash": output_hash,
                        "route_change_id": row.get("route_change_id", ""),
                        "compact_hash": row.get("compact_hash", ""),
                        "provider": row.get("provider", ""),
                        "model_name": row.get("model_name", ""),
                        "prompt_version": row.get("prompt_version", ""),
                        "policy_version": row.get("policy_version", ""),
                        "result_json": row.get("result_json", "{}"),
                        "ts_ms": str(now_ms()),
                    }
                    await r.xadd(
                        QUALITY_STREAM,
                        quality_req,
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(RESULTS_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception:
                    status = "error"
                    await r.xack(RESULTS_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
