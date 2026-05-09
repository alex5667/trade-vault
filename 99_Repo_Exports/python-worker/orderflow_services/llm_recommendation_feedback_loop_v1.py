from __future__ import annotations

import asyncio
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import contextlib

STREAM = os.getenv("ML_RECOMMENDATION_FEEDBACK_STREAM", "stream:ml:recommendation_feedback")
GROUP = os.getenv("ML_RECOMMENDATION_FEEDBACK_GROUP", "cg:ml_recommendation_feedback_v1")
CONSUMER = os.getenv("HOSTNAME", "ml-recommendation-feedback-v1")
LAST_HASH = os.getenv("ML_RECOMMENDATION_FEEDBACK_LAST_HASH", "metrics:ml:recommendation_feedback:last")

RUNS = Counter("ml_recommendation_feedback_events_total", "Feedback events", ["verdict", "action"])
LAST_RUN = Gauge("ml_recommendation_feedback_last_run_ts_seconds", "Last run ts")
UP = Gauge("ml_recommendation_feedback_up", "Health")
LOOP_LAT = Histogram("ml_recommendation_feedback_loop_seconds", "Loop latency")


@dataclass
class RecommendationFeedback:
    recommendation_id: str
    analysis_run_id: str
    ts_ms: int
    verdict: str
    action: str
    target: str
    reviewer: str
    reason_code: str
    prompt_version: str
    policy_version: str
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_feedback(fields: dict[str, Any]) -> RecommendationFeedback:
    def _s(v: Any) -> str:
        return "" if v is None else str(v)
    return RecommendationFeedback(
        recommendation_id=_s(fields.get("recommendation_id") or fields.get("id")),
        analysis_run_id=_s(fields.get("analysis_run_id")),
        ts_ms=int(fields.get("ts_ms") or get_ny_time_millis()),
        verdict=_s(fields.get("verdict") or "unknown").lower(),
        action=_s(fields.get("action") or "unknown"),
        target=_s(fields.get("target") or ""),
        reviewer=_s(fields.get("reviewer") or "system"),
        reason_code=_s(fields.get("reason_code") or ""),
        prompt_version=_s(fields.get("prompt_version") or "unknown"),
        policy_version=_s(fields.get("policy_version") or "unknown"),
        notes=_s(fields.get("notes") or ""),
    )


def update_summary(summary: dict[str, int], feedback: RecommendationFeedback) -> dict[str, int]:
    verdict = feedback.verdict
    summary = dict(summary)
    summary[verdict] = int(summary.get(verdict, 0)) + 1
    summary["total"] = int(summary.get("total", 0)) + 1
    return summary


async def _write_db_if_possible(payload: RecommendationFeedback) -> None:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return
    try:
        import psycopg
        async with await psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """

                    INSERT INTO llm_recommendation_feedback (
                      recommendation_id, analysis_run_id, ts_ms, verdict, action, target,
                      reviewer, reason_code, prompt_version, policy_version, notes
                    ) VALUES (
                      %(recommendation_id)s, %(analysis_run_id)s, %(ts_ms)s, %(verdict)s, %(action)s, %(target)s,
                      %(reviewer)s, %(reason_code)s, %(prompt_version)s, %(policy_version)s, %(notes)s
                    )
                    ON CONFLICT DO NOTHING,
                    """,
                    payload.to_dict(),
                )
            await conn.commit()
    except Exception:
        return


async def main() -> None:
    if redis is None:
        raise RuntimeError("redis package is required")
    start_http_server(int(os.getenv("ML_RECOMMENDATION_FEEDBACK_PORT", "9864")))
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    with contextlib.suppress(Exception):
        await r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    UP.set(1.0)
    while True:
        t0 = time.perf_counter()
        rows = await r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=64, block=5000)
        if not rows:
            LAST_RUN.set(time.time())
            await asyncio.sleep(1.0)
            continue
        for _, msgs in rows:
            for msg_id, fields in msgs:
                data = {(_k.decode() if isinstance(_k, bytes) else str(_k)): (_v.decode() if isinstance(_v, bytes) else str(_v)) for _k, _v in fields.items()}
                try:
                    fb = normalize_feedback(data)
                    await _write_db_if_possible(fb)
                    key = f"metrics:ml:recommendation_feedback:summary:{fb.action}"
                    raw = await r.hgetall(key)
                    summary = {str(k): int(v) for k, v in raw.items()} if raw else {}
                    summary = update_summary(summary, fb)
                    await r.hset(key, mapping={k: int(v) for k, v in summary.items()})
                    await r.hset(LAST_HASH, mapping=fb.to_dict())
                    RUNS.labels(verdict=fb.verdict, action=fb.action).inc()
                    await r.xack(STREAM, GROUP, msg_id)
                except Exception:
                    await r.xack(STREAM, GROUP, msg_id)
        LAST_RUN.set(time.time())
        LOOP_LAT.observe(max(0.0, time.perf_counter() - t0))


if __name__ == "__main__":
    asyncio.run(main())
