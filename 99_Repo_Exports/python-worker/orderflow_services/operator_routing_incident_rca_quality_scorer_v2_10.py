from __future__ import annotations
from utils.time_utils import get_ny_time_millis

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


APP_NAME = "operator_routing_incident_rca_quality_scorer_v2_10"
QUALITY_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_QUALITY_STREAM",
    "stream:ml:operator_rca_routing_rca_quality",
)
QUALITY_RESULTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_QUALITY_RESULTS_STREAM",
    "stream:ml:operator_rca_routing_rca_quality_results",
)
GROUP = os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_QUALITY_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_QUALITY_PORT", "9887"))
MAXLEN = int(os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_QUALITY_MAXLEN", "20000"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_routing_incident_rca_quality_runs_total",
    "Routing incident RCA quality scorer runs",
    ("status",),
)
LAT = _hist(
    "ml_operator_routing_incident_rca_quality_latency_seconds",
    "Routing incident RCA quality scorer latency seconds",
)
UP = _gauge(
    "ml_operator_routing_incident_rca_quality_up",
    "Routing incident RCA quality scorer up",
)
LAST_RUN_TS = _gauge(
    "ml_operator_routing_incident_rca_quality_last_run_ts_seconds",
    "Routing incident RCA quality scorer last run timestamp",
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


def evaluate_quality(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        result = json.loads(payload.get("result_json", "{}"))
    except Exception:
        result = {}

    score = 0.0
    reasons = []

    if result.get("summary"):
        score += 0.2
    else:
        reasons.append("Missing summary")

    findings = result.get("findings", [])
    if isinstance(findings, list) and len(findings) > 0:
        score += 0.3
        evidence_n = sum(len(f.get("evidence", [])) for f in findings if isinstance(f, dict))
        if evidence_n > 0:
            score += 0.2
        else:
            reasons.append("Findings lack evidence")
    else:
        reasons.append("Missing findings")

    recos = result.get("recommendations", [])
    if isinstance(recos, list) and len(recos) > 0:
        score += 0.3
    else:
        reasons.append("Missing recommendations")

    return {
        "quality_score": min(score, 1.0),
        "quality_reasons": reasons,
        "ts_ms": now_ms(),
    }


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def persist_quality(db_url: str, output_hash: str, assessment: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    try:  # pragma: no cover
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO llm_operator_routing_incident_rca_quality (
                        output_hash,
                        quality_score,
                        quality_reasons_json,
                        ts_ms
                    ) VALUES (
                        %(output_hash)s,
                        %(quality_score)s,
                        %(quality_reasons_json)s,
                        %(ts_ms)s
                    )
                    ON CONFLICT(output_hash) DO NOTHING
                    """,
                    {
                        "output_hash": output_hash,
                        "quality_score": assessment["quality_score"],
                        "quality_reasons_json": stable_json(assessment["quality_reasons"]),
                        "ts_ms": assessment["ts_ms"],
                    },
                )
                cur.execute(
                    """
                    UPDATE llm_operator_routing_incident_rca_results
                    SET quality_score = %(quality_score)s
                    WHERE output_hash = %(output_hash)s
                    """,
                    {
                        "quality_score": assessment["quality_score"],
                        "output_hash": output_hash,
                    },
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
    await ensure_group(r, QUALITY_STREAM, GROUP)
    db_url = os.getenv("DATABASE_URL", "")
    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {QUALITY_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                row = as_dict(payload)
                status = "ok"
                try:
                    output_hash = row.get("output_hash", "")
                    assessment = evaluate_quality(row)
                    await persist_quality(db_url, output_hash, assessment)
                    
                    out = dict(row)
                    out.update(assessment)
                    out["quality_reasons_json"] = stable_json(assessment["quality_reasons"])
                    out.pop("quality_reasons", None)
                    
                    await r.xadd(QUALITY_RESULTS_STREAM, out, maxlen=MAXLEN, approximate=True)
                    await r.xack(QUALITY_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    status = "error"
                    await r.xack(QUALITY_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
