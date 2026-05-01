from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server

QUALITY_STREAM = os.getenv("ML_OPERATOR_RCA_QUALITY_STREAM", "stream:ml:operator_rca_quality")
QUALITY_RESULTS_STREAM = os.getenv("ML_OPERATOR_RCA_QUALITY_RESULTS_STREAM", "stream:ml:operator_rca_quality_results")
STATE_KEY = os.getenv("ML_OPERATOR_RCA_QUALITY_STATE_KEY", "metrics:ml:operator_rca_quality:last")
GROUP = os.getenv("ML_OPERATOR_RCA_QUALITY_GROUP", "cg:ml_operator_rca_quality_v2_1")
CONSUMER = os.getenv("ML_OPERATOR_RCA_QUALITY_CONSUMER", "ml-operator-rca-quality-v2-1")
PROM_PORT = int(os.getenv("ML_OPERATOR_RCA_QUALITY_PORT", "9871"))

SCORED = Counter("ml_operator_rca_quality_scored_total", "RCA quality scores computed")
LOW_QUALITY = Counter("ml_operator_rca_quality_low_total", "Low RCA quality count")
LAST_RUN_TS = Gauge("ml_operator_rca_quality_last_run_ts_seconds", "Last RCA quality run ts")
LAST_SCORE = Gauge("ml_operator_rca_quality_last_score", "Last RCA quality score")
LOOP_SECONDS = Histogram("ml_operator_rca_quality_loop_seconds", "RCA quality loop seconds")


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


@dataclass
class RCAQualityInput:
    recommendation_id: str
    ts_ms: int
    provider: str
    model_name: str
    status: str
    output_hash: str
    prompt_version: str
    policy_version: str
    findings_n: int
    recommendations_n: int
    output_json: Dict[str, Any]


def parse_input(fields: Dict[Any, Any]) -> RCAQualityInput:
    return RCAQualityInput(
        recommendation_id=_b2s(fields.get(b"recommendation_id", b"")),
        ts_ms=int(_b2s(fields.get(b"ts_ms", b"0")) or "0"),
        provider=_b2s(fields.get(b"provider", b"")),
        model_name=_b2s(fields.get(b"model_name", b"")),
        status=_b2s(fields.get(b"status", b"")),
        output_hash=_b2s(fields.get(b"output_hash", b"")),
        prompt_version=_b2s(fields.get(b"prompt_version", b"")),
        policy_version=_b2s(fields.get(b"policy_version", b"")),
        findings_n=int(_b2s(fields.get(b"findings_n", b"0")) or "0"),
        recommendations_n=int(_b2s(fields.get(b"recommendations_n", b"0")) or "0"),
        output_json=_loads(fields.get(b"output_json"), {}),
    )


def score_output(payload: Dict[str, Any]) -> Tuple[float, List[str], Dict[str, float]]:
    reasons: List[str] = []
    parts: Dict[str, float] = {}
    total = 0.0
    if isinstance(payload, dict):
        total += 20.0
        parts["object"] = 20.0
    else:
        return 0.0, ["NOT_OBJECT"], {"object": 0.0}

    summary = str(payload.get("summary", "")).strip()
    if summary:
        total += 15.0
        parts["summary"] = 15.0
    else:
        reasons.append("MISSING_SUMMARY")
        parts["summary"] = 0.0

    findings = payload.get("findings", []) if isinstance(payload.get("findings", []), list) else []
    recs = payload.get("recommendations", []) if isinstance(payload.get("recommendations", []), list) else []
    if findings:
        add = min(20.0, 5.0 * len(findings))
        total += add
        parts["findings"] = add
    else:
        reasons.append("NO_FINDINGS")
        parts["findings"] = 0.0

    if recs:
        add = min(15.0, 5.0 * len(recs))
        total += add
        parts["recommendations"] = add
    else:
        reasons.append("NO_RECOMMENDATIONS")
        parts["recommendations"] = 0.0

    evidence_hits = 0
    for finding in findings[:10]:
        if isinstance(finding, dict) and isinstance(finding.get("evidence", []), list) and finding.get("evidence"):
            evidence_hits += 1
    evidence_add = min(15.0, 5.0 * evidence_hits)
    total += evidence_add
    parts["evidence_density"] = evidence_add
    if evidence_add == 0.0:
        reasons.append("NO_EVIDENCE")

    allowed_actions = {
        "require_shadow_retrain",
        "freeze_candidate",
        "unfreeze_candidate",
        "request_calibration_refresh",
        "propose_threshold_canary",
        "open_incident",
        "draft_postmortem",
    },
    allowed_n = 0
    for rec in recs[:10]:
        if isinstance(rec, dict) and str(rec.get("action", "")) in allowed_actions:
            allowed_n += 1
    allowed_add = min(15.0, 5.0 * allowed_n)
    total += allowed_add
    parts["allowed_actions"] = allowed_add
    if recs and allowed_n == 0:
        reasons.append("NO_ALLOWED_ACTIONS")

    total = max(0.0, min(100.0, total))
    if total < 60.0:
        reasons.append("QUALITY_BELOW_THRESHOLD")
    return total, reasons, parts


async def _ensure_group(r: Any) -> None:
    try:
        await r.xgroup_create(QUALITY_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass


async def _persist(conn: Any, item: RCAQualityInput, score: float, reasons: List[str], parts: Dict[str, float]) -> None:
    if conn is None:
        return
    await conn.execute(
        """,
        INSERT INTO llm_incident_rca_quality (
            recommendation_id, ts_ms, output_hash, quality_score, quality_reasons_json,
            parts_json, prompt_version, policy_version
        ) VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8)
        ON CONFLICT (recommendation_id, output_hash)
        DO UPDATE SET
            quality_score = EXCLUDED.quality_score,
            quality_reasons_json = EXCLUDED.quality_reasons_json,
            parts_json = EXCLUDED.parts_json
        """,
        item.recommendation_id,
        item.ts_ms,
        item.output_hash,
        score,
        json.dumps(reasons, separators=(",", ":")),
        json.dumps(parts, separators=(",", ":"), sort_keys=True),
        item.prompt_version or None,
        item.policy_version or None,
    )
    await conn.execute(
        """,
        UPDATE llm_incident_rca_results
        SET quality_score = $2
        WHERE recommendation_id = $1
        """,
        item.recommendation_id,
        score,
    )


async def run_forever() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PROM_PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    await _ensure_group(r)
    dsn = os.getenv("DATABASE_URL", "").strip()
    conn = await asyncpg.connect(dsn) if dsn and asyncpg is not None else None
    block_ms = int(os.getenv("ML_OPERATOR_RCA_QUALITY_BLOCK_MS", "5000"))
    count = int(os.getenv("ML_OPERATOR_RCA_QUALITY_READ_COUNT", "32"))
    while True:
        t0 = time.perf_counter()
        rows = await r.xreadgroup(GROUP, CONSUMER, {QUALITY_STREAM: ">"}, count=count, block=block_ms)
        for _stream, messages in rows:
            for msg_id, fields in messages:
                item = parse_input(fields)
                score, reasons, parts = score_output(item.output_json)
                await _persist(conn, item, score, reasons, parts)
                await r.xadd(
                    QUALITY_RESULTS_STREAM,
                    {
                        "schema_version": 1,
                        "recommendation_id": item.recommendation_id,
                        "ts_ms": str(_now_ms()),
                        "output_hash": item.output_hash,
                        "quality_score": f"{score:.2f}",
                        "quality_reasons_json": json.dumps(reasons, separators=(",", ":")),
                        "parts_json": json.dumps(parts, separators=(",", ":"), sort_keys=True),
                    },
                    maxlen=50000,
                    approximate=True,
                )
                await r.hset(STATE_KEY, mapping={"last_recommendation_id": item.recommendation_id, "last_ts_ms": str(_now_ms()), "last_quality_score": f"{score:.2f}"})
                SCORED.inc()
                LAST_SCORE.set(score)
                if score < 60.0:
                    LOW_QUALITY.inc()
                await r.xack(QUALITY_STREAM, GROUP, msg_id)
        LAST_RUN_TS.set(time.time())
        LOOP_SECONDS.observe(time.perf_counter() - t0)


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(run_forever())
