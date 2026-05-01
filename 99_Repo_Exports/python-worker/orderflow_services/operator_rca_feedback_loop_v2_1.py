from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server

FEEDBACK_STREAM = os.getenv("ML_OPERATOR_RCA_FEEDBACK_STREAM", "stream:ml:operator_rca_feedback")
FEEDBACK_SUMMARY_STREAM = os.getenv("ML_OPERATOR_RCA_FEEDBACK_SUMMARY_STREAM", "stream:ml:operator_rca_feedback_summary")
STATE_KEY = os.getenv("ML_OPERATOR_RCA_FEEDBACK_STATE_KEY", "metrics:ml:operator_rca_feedback:last")
SUMMARY_HASH_PREFIX = os.getenv("ML_OPERATOR_RCA_FEEDBACK_SUMMARY_HASH_PREFIX", "metrics:ml:operator_rca_feedback:")
GROUP = os.getenv("ML_OPERATOR_RCA_FEEDBACK_GROUP", "cg:ml_operator_rca_feedback_v2_1")
CONSUMER = os.getenv("ML_OPERATOR_RCA_FEEDBACK_CONSUMER", "ml-operator-rca-feedback-v2-1")
PROM_PORT = int(os.getenv("ML_OPERATOR_RCA_FEEDBACK_PORT", "9872"))

INGESTED = Counter("ml_operator_rca_feedback_ingested_total", "RCA feedback ingested", ["decision"])
LAST_RUN_TS = Gauge("ml_operator_rca_feedback_last_run_ts_seconds", "Last feedback run ts")
LAST_USEFULNESS = Gauge("ml_operator_rca_feedback_last_usefulness_score", "Last usefulness score")
LOOP_SECONDS = Histogram("ml_operator_rca_feedback_loop_seconds", "RCA feedback loop seconds")


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


def usefulness_from_feedback(decision: str) -> float:
    d = (decision or "").upper()
    if d == "VERY_USEFUL":
        return 1.0
    if d == "USEFUL":
        return 0.75
    if d == "MIXED":
        return 0.50
    if d == "NOT_USEFUL":
        return 0.0
    return 0.25


@dataclass
class FeedbackEvent:
    recommendation_id: str
    ts_ms: int
    reviewer: str
    decision: str
    action_type: str
    note: str


def parse_feedback(fields: Dict[Any, Any]) -> FeedbackEvent:
    return FeedbackEvent(
        recommendation_id=_b2s(fields.get(b"recommendation_id", b"")),
        ts_ms=int(_b2s(fields.get(b"ts_ms", b"0")) or "0"),
        reviewer=_b2s(fields.get(b"reviewer", b"")),
        decision=_b2s(fields.get(b"decision", b"MIXED")),
        action_type=_b2s(fields.get(b"action_type", b"")),
        note=_b2s(fields.get(b"note", b"")),
    )


async def _ensure_group(r: Any) -> None:
    try:
        await r.xgroup_create(FEEDBACK_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass


async def _persist(conn: Any, fb: FeedbackEvent, usefulness_score: float) -> None:
    if conn is None:
        return
    await conn.execute(
        """,
        INSERT INTO llm_incident_rca_feedback (
            recommendation_id, ts_ms, reviewer, decision, action_type, usefulness_score, note
        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
        fb.recommendation_id,
        fb.ts_ms,
        fb.reviewer,
        fb.decision,
        fb.action_type or None,
        usefulness_score,
        fb.note or None,
    )
    await conn.execute(
        """,
        UPDATE llm_incident_rca_results r
        SET usefulness_score = sub.avg_usefulness
        FROM (
            SELECT recommendation_id, AVG(usefulness_score)::double precision AS avg_usefulness
            FROM llm_incident_rca_feedback
            WHERE recommendation_id = $1
            GROUP BY recommendation_id
        ) sub
        WHERE r.recommendation_id = sub.recommendation_id
        """,
        fb.recommendation_id,
    )


async def _update_summary(r: Any, action_type: str, usefulness_score: float) -> Dict[str, Any]:
    key = f"{SUMMARY_HASH_PREFIX}{action_type or 'unknown'}"
    current = await r.hgetall(key)
    total_n = int(_b2s(current.get(b"total_n", b"0")) or "0") + 1
    prev_avg = float(_b2s(current.get(b"avg_usefulness", b"0.0")) or "0.0")
    new_avg = ((prev_avg * (total_n - 1)) + usefulness_score) / max(1, total_n)
    mapping = {
        "action_type": action_type or "unknown",
        "total_n": str(total_n),
        "avg_usefulness": f"{new_avg:.4f}",
        "last_ts_ms": str(_now_ms()),
    }
    await r.hset(key, mapping=mapping)
    return mapping


async def run_forever() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PROM_PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    await _ensure_group(r)
    dsn = os.getenv("DATABASE_URL", "").strip()
    conn = await asyncpg.connect(dsn) if dsn and asyncpg is not None else None
    block_ms = int(os.getenv("ML_OPERATOR_RCA_FEEDBACK_BLOCK_MS", "5000"))
    count = int(os.getenv("ML_OPERATOR_RCA_FEEDBACK_READ_COUNT", "32"))
    while True:
        t0 = time.perf_counter()
        rows = await r.xreadgroup(GROUP, CONSUMER, {FEEDBACK_STREAM: ">"}, count=count, block=block_ms)
        for _stream, messages in rows:
            for msg_id, fields in messages:
                fb = parse_feedback(fields)
                usefulness_score = usefulness_from_feedback(fb.decision)
                await _persist(conn, fb, usefulness_score)
                summary = await _update_summary(r, fb.action_type, usefulness_score)
                await r.xadd(FEEDBACK_SUMMARY_STREAM, summary, maxlen=20000, approximate=True)
                await r.hset(STATE_KEY, mapping={"last_recommendation_id": fb.recommendation_id, "last_usefulness_score": f"{usefulness_score:.2f}", "last_action_type": fb.action_type, "last_ts_ms": str(_now_ms())})
                LAST_USEFULNESS.set(usefulness_score)
                INGESTED.labels(decision=fb.decision.upper()).inc()
                await r.xack(FEEDBACK_STREAM, GROUP, msg_id)
        LAST_RUN_TS.set(time.time())
        LOOP_SECONDS.observe(time.perf_counter() - t0)


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(run_forever())
