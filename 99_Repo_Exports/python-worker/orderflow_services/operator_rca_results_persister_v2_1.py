from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server
import contextlib

RESULTS_STREAM = os.getenv("ML_OPERATOR_RCA_RESULTS_STREAM", "stream:ml:operator_rca_results")
QUALITY_STREAM = os.getenv("ML_OPERATOR_RCA_QUALITY_STREAM", "stream:ml:operator_rca_quality")
STATE_KEY = os.getenv("ML_OPERATOR_RCA_RESULTS_PERSISTER_STATE_KEY", "metrics:ml:operator_rca_results_persister:last")
RESULT_HASH_PREFIX = os.getenv("ML_OPERATOR_RCA_RESULT_HASH_PREFIX", "metrics:ml:operator_rca_result:")
GROUP = os.getenv("ML_OPERATOR_RCA_RESULTS_PERSISTER_GROUP", "cg:ml_operator_rca_results_persister_v2_1")
CONSUMER = os.getenv("ML_OPERATOR_RCA_RESULTS_PERSISTER_CONSUMER", "ml-operator-rca-results-persist-v2-1")
PROM_PORT = int(os.getenv("ML_OPERATOR_RCA_RESULTS_PERSISTER_PORT", "9870"))

UPSERTED = Counter("ml_operator_rca_results_upserted_total", "RCA results upserted", ["status"])
DEDUPED = Counter("ml_operator_rca_results_deduped_total", "RCA results deduped")
DB_ERRORS = Counter("ml_operator_rca_results_db_errors_total", "RCA DB errors")
LAST_RUN_TS = Gauge("ml_operator_rca_results_persister_last_run_ts_seconds", "Last persister run ts")
QUEUE_LAG_MS = Gauge("ml_operator_rca_results_persister_queue_lag_ms", "Approximate queue lag")
LOOP_SECONDS = Histogram("ml_operator_rca_results_persister_loop_seconds", "Persister loop seconds")


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


def _json_hash(output_json: dict[str, Any]) -> str:
    payload = json.dumps(output_json, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


@dataclass
class RCAResult:
    recommendation_id: str
    ts_ms: int
    provider: str
    model_name: str
    status: str
    latency_ms: int
    estimated_cost_usd: float
    output_json: dict[str, Any]
    prompt_version: str
    policy_version: str

    @property
    def output_hash(self) -> str:
        return _json_hash(self.output_json)


def parse_result(fields: dict[Any, Any]) -> RCAResult:
    return RCAResult(
        recommendation_id=_b2s(fields.get(b"recommendation_id", b"")),
        ts_ms=int(_b2s(fields.get(b"ts_ms", b"0")) or "0"),
        provider=_b2s(fields.get(b"provider", b"")),
        model_name=_b2s(fields.get(b"model_name", b"")),
        status=_b2s(fields.get(b"status", b"")),
        latency_ms=int(_b2s(fields.get(b"latency_ms", b"0")) or "0"),
        estimated_cost_usd=float(_b2s(fields.get(b"estimated_cost_usd", b"0.0")) or "0.0"),
        output_json=_loads(fields.get(b"output_json"), {}),
        prompt_version=_b2s(fields.get(b"prompt_version", b"")),
        policy_version=_b2s(fields.get(b"policy_version", b"")),
    )


def build_quality_event(result: RCAResult) -> dict[str, Any]:
    findings = result.output_json.get("findings", []) if isinstance(result.output_json, dict) else []
    recs = result.output_json.get("recommendations", []) if isinstance(result.output_json, dict) else []
    return {
        "schema_version": 1,
        "recommendation_id": result.recommendation_id,
        "ts_ms": str(_now_ms()),
        "provider": result.provider,
        "model_name": result.model_name,
        "status": result.status,
        "output_hash": result.output_hash,
        "prompt_version": result.prompt_version,
        "policy_version": result.policy_version,
        "findings_n": str(len(findings) if isinstance(findings, list) else 0),
        "recommendations_n": str(len(recs) if isinstance(recs, list) else 0),
        "output_json": json.dumps(result.output_json, separators=(",", ":"), sort_keys=True),
    }


async def _ensure_group(r: Any) -> None:
    with contextlib.suppress(Exception):
        await r.xgroup_create(RESULTS_STREAM, GROUP, id="0", mkstream=True)


async def _upsert_sql(conn: Any, result: RCAResult) -> None:
    if conn is None:
        return
    output_json = json.dumps(result.output_json, separators=(",", ":"), sort_keys=True)
    await conn.execute(
        """

        INSERT INTO llm_incident_rca_runs (
            analysis_run_id, recommendation_id, ts_ms, provider, model_name,
            prompt_version, policy_version, status, latency_ms, estimated_cost_usd, output_json
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
        ON CONFLICT (analysis_run_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            latency_ms = EXCLUDED.latency_ms,
            estimated_cost_usd = EXCLUDED.estimated_cost_usd,
            output_json = EXCLUDED.output_json,
        """,
        f"{result.recommendation_id}:{result.ts_ms}:{result.output_hash}",
        result.recommendation_id,
        result.ts_ms,
        result.provider,
        result.model_name,
        result.prompt_version or None,
        result.policy_version or None,
        result.status,
        result.latency_ms,
        result.estimated_cost_usd,
        output_json,
    )
    await conn.execute(
        """

        INSERT INTO llm_incident_rca_results (
            recommendation_id, latest_analysis_run_id, latest_ts_ms, provider, model_name,
            severity, prompt_version, policy_version, output_hash, quality_score,
            usefulness_score, output_json
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,NULL,$10::jsonb)
        ON CONFLICT (recommendation_id)
        DO UPDATE SET
            latest_analysis_run_id = EXCLUDED.latest_analysis_run_id,
            latest_ts_ms = EXCLUDED.latest_ts_ms,
            provider = EXCLUDED.provider,
            model_name = EXCLUDED.model_name,
            severity = EXCLUDED.severity,
            prompt_version = EXCLUDED.prompt_version,
            policy_version = EXCLUDED.policy_version,
            output_hash = EXCLUDED.output_hash,
            output_json = EXCLUDED.output_json
        WHERE llm_incident_rca_results.output_hash IS DISTINCT FROM EXCLUDED.output_hash
           OR llm_incident_rca_results.latest_ts_ms < EXCLUDED.latest_ts_ms,
        """,
        result.recommendation_id,
        f"{result.recommendation_id}:{result.ts_ms}:{result.output_hash}",
        result.ts_ms,
        result.provider,
        result.model_name,
        str(result.output_json.get("severity", "")) if isinstance(result.output_json, dict) else None,
        result.prompt_version or None,
        result.policy_version or None,
        result.output_hash,
        output_json,
    )


async def run_forever() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PROM_PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    await _ensure_group(r)
    dsn = os.getenv("DATABASE_URL", "").strip()
    conn = None
    if dsn and asyncpg is not None:
        conn = await asyncpg.connect(dsn)
    block_ms = int(os.getenv("ML_OPERATOR_RCA_RESULTS_PERSISTER_BLOCK_MS", "5000"))
    count = int(os.getenv("ML_OPERATOR_RCA_RESULTS_PERSISTER_READ_COUNT", "32"))
    while True:
        t0 = time.perf_counter()
        rows = await r.xreadgroup(GROUP, CONSUMER, {RESULTS_STREAM: ">"}, count=count, block=block_ms)
        for _stream, messages in rows:
            for msg_id, fields in messages:
                result = parse_result(fields)
                with contextlib.suppress(Exception):
                    QUEUE_LAG_MS.set(max(0, _now_ms() - result.ts_ms))
                latest_key = f"{RESULT_HASH_PREFIX}{result.recommendation_id}"
                prev_hash = _b2s(await r.hget(latest_key, b"output_hash") or "")
                if prev_hash and prev_hash == result.output_hash:
                    DEDUPED.inc()
                    await r.xack(RESULTS_STREAM, GROUP, msg_id)
                    continue
                try:
                    await _upsert_sql(conn, result)
                    await r.hset(
                        latest_key,
                        mapping={
                            "recommendation_id": result.recommendation_id,
                            "latest_ts_ms": str(result.ts_ms),
                            "provider": result.provider,
                            "model_name": result.model_name,
                            "status": result.status,
                            "output_hash": result.output_hash,
                            "prompt_version": result.prompt_version,
                            "policy_version": result.policy_version,
                        }
                    )
                    await r.hset(
                        STATE_KEY,
                        mapping={
                            "last_recommendation_id": result.recommendation_id,
                            "last_ts_ms": str(result.ts_ms),
                            "last_output_hash": result.output_hash,
                            "last_provider": result.provider,
                            "last_model_name": result.model_name,
                        }
                    )
                    await r.xadd(QUALITY_STREAM, build_quality_event(result), maxlen=50000, approximate=True)
                    UPSERTED.labels(status=result.status or "unknown").inc()
                except Exception:
                    DB_ERRORS.inc()
                    raise
                await r.xack(RESULTS_STREAM, GROUP, msg_id)
        LAST_RUN_TS.set(time.time())
        LOOP_SECONDS.observe(time.perf_counter() - t0)


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(run_forever())
