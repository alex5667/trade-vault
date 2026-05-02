from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from prometheus_client import Counter, Gauge, Histogram, start_http_server


VERIFIER_UP = Gauge("ml_rollback_verifier_up", "Rollback verifier liveness")
VERIFIER_LAST_RUN_TS = Gauge("ml_rollback_verifier_last_run_ts_seconds", "Last verifier loop time")
VERIFIER_EVENTS = Counter("ml_rollback_verifier_events_total", "Verification outcomes", ["status"])
VERIFIER_LOOP_SECONDS = Histogram("ml_rollback_verifier_loop_seconds", "Verifier loop latency")
VERIFIER_QUEUE_LAG_MS = Gauge("ml_rollback_verifier_queue_lag_ms", "Verifier queue lag")


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass
class VerificationDecision:
    verification_status: str
    reason_codes: List[str]
    details: Dict[str, Any]


def compute_rollback_verification(
    baseline_snapshot: Dict[str, Any],
    post_rollback_snapshot: Dict[str, Any],
    cfg: Dict[str, Any],
) -> VerificationDecision:
    reasons: List[str] = []
    details: Dict[str, Any] = {}

    if not baseline_snapshot:
        return VerificationDecision(
            verification_status="INCONCLUSIVE",
            reason_codes=["MISSING_BASELINE_SNAPSHOT"],
            details={},
        )
    if not post_rollback_snapshot:
        return VerificationDecision(
            verification_status="INCONCLUSIVE",
            reason_codes=["MISSING_POST_ROLLBACK_SNAPSHOT"],
            details={},
        )

    max_err_delta = _safe_float(cfg.get("ROLLBACK_VERIFY_MAX_ERROR_RATE_DELTA", 0.01))
    max_lat_p95_delta = _safe_float(cfg.get("ROLLBACK_VERIFY_MAX_LATENCY_P95_DELTA_MS", 1.5))
    max_missing_delta = _safe_float(cfg.get("ROLLBACK_VERIFY_MAX_MISSING_CRITICAL_DELTA", 0.01))
    max_allow_drop = _safe_float(cfg.get("ROLLBACK_VERIFY_MAX_ALLOW_RATE_DROP", 0.08))

    baseline_error = _safe_float(baseline_snapshot.get("error_rate_max"))
    after_error = _safe_float(post_rollback_snapshot.get("error_rate_max"))
    baseline_p95 = _safe_float(baseline_snapshot.get("latency_p95_max_ms"))
    after_p95 = _safe_float(post_rollback_snapshot.get("latency_p95_max_ms"))
    baseline_missing = _safe_float(baseline_snapshot.get("missing_critical_rate_max"))
    after_missing = _safe_float(post_rollback_snapshot.get("missing_critical_rate_max"))
    baseline_allow = _safe_float(baseline_snapshot.get("allow_rate_avg"))
    after_allow = _safe_float(post_rollback_snapshot.get("allow_rate_avg"))

    details.update(
        baseline_error_rate_max=baseline_error,
        after_error_rate_max=after_error,
        baseline_latency_p95_max_ms=baseline_p95,
        after_latency_p95_max_ms=after_p95,
        baseline_missing_critical_rate_max=baseline_missing,
        after_missing_critical_rate_max=after_missing,
        baseline_allow_rate_avg=baseline_allow,
        after_allow_rate_avg=after_allow,
    )

    if after_error > baseline_error + max_err_delta:
        reasons.append("ERROR_RATE_SPIKE")
    if after_p95 > baseline_p95 + max_lat_p95_delta:
        reasons.append("LATENCY_P95_REGRESSION")
    if after_missing > baseline_missing + max_missing_delta:
        reasons.append("MISSING_CRITICAL_REGRESSION")
    if after_allow < max(0.0, baseline_allow - max_allow_drop):
        reasons.append("ALLOW_RATE_DROP")

    hard = {"ERROR_RATE_SPIKE", "LATENCY_P95_REGRESSION"}
    if not reasons:
        return VerificationDecision("PASS", [], details)
    if any(r in hard for r in reasons):
        return VerificationDecision("FAIL", reasons, details)
    return VerificationDecision("INCONCLUSIVE", reasons, details)


async def _persist_pg(database_url: str, payload: Dict[str, Any]) -> None:
    import asyncpg  # type: ignore

    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute(
            """

            INSERT INTO llm_rollback_verifications (
                recommendation_id, verification_ts_ms, verification_status,
                reason_codes_json, details_json
            ) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
            """,
            payload["recommendation_id"],
            int(payload["verification_ts_ms"]),
            payload["verification_status"],
            json.dumps(payload.get("reason_codes", [])),
            json.dumps(payload.get("details", {})),
        )
        await conn.execute(
            """,
            UPDATE llm_recommendations
               SET rollback_verification_status = $2,
                   rollback_verified_at_ms = $3,
                   rollback_failure_reason = $4
             WHERE recommendation_id = $1,
            """,
            payload["recommendation_id"],
            payload["verification_status"],
            int(payload["verification_ts_ms"]),
            ",".join(payload.get("reason_codes", [])),
        )
    finally:
        await conn.close()


async def main() -> None:
    try:
        import redis.asyncio as redis  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise SystemExit(f"redis.asyncio is required: {exc}")

    port = int(os.getenv("ML_ROLLBACK_VERIFIER_METRICS_PORT", "9862"))
    start_http_server(port)

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    stream_results = os.getenv("ML_ROLLBACK_RESULTS_STREAM", "stream:ml:recommendation_rollback_results")
    stream_verify = os.getenv("ML_ROLLBACK_VERIFY_RESULTS_STREAM", "stream:ml:recommendation_rollback_verification_results")
    stream_audit = os.getenv("ML_RECOMMENDATION_AUDIT_STREAM", "stream:ml:recommendation_audit")
    stream_snapshot_prefix = os.getenv("ML_MODEL_SNAPSHOT_PREFIX", "metrics:ml:model_snapshot:")
    rollback_requests_stream = os.getenv("ML_ROLLBACK_REQUESTS_STREAM", "stream:ml:recommendation_rollback_requests")
    group = os.getenv("ML_ROLLBACK_VERIFIER_GROUP", "cg:ml_rollback_verifier")
    consumer = os.getenv("ML_ROLLBACK_VERIFIER_CONSUMER", os.getenv("HOSTNAME", "ml-rollback-verifier-1"))
    database_url = os.getenv("DATABASE_URL", "")

    try:
        await r.xgroup_create(stream_results, group, id="0", mkstream=True)
    except Exception:
        pass

    cfg = {
        "ROLLBACK_VERIFY_MAX_ERROR_RATE_DELTA": os.getenv("ROLLBACK_VERIFY_MAX_ERROR_RATE_DELTA", "0.01"),
        "ROLLBACK_VERIFY_MAX_LATENCY_P95_DELTA_MS": os.getenv("ROLLBACK_VERIFY_MAX_LATENCY_P95_DELTA_MS", "1.5"),
        "ROLLBACK_VERIFY_MAX_MISSING_CRITICAL_DELTA": os.getenv("ROLLBACK_VERIFY_MAX_MISSING_CRITICAL_DELTA", "0.01"),
        "ROLLBACK_VERIFY_MAX_ALLOW_RATE_DROP": os.getenv("ROLLBACK_VERIFY_MAX_ALLOW_RATE_DROP", "0.08"),
    }

    while True:
        VERIFIER_UP.set(1)
        t0 = time.perf_counter()
        rows = await r.xreadgroup(group, consumer, {stream_results: ">"}, count=50, block=5000)
        now_ms = _now_ms()
        VERIFIER_LAST_RUN_TS.set(time.time())
        for _stream, msgs in rows:
            for msg_id, payload in msgs:
                try:
                    ts_ms = int(payload.get("ts_ms", now_ms))
                    VERIFIER_QUEUE_LAG_MS.set(max(0, now_ms - ts_ms))

                    recommendation_id = str(payload.get("recommendation_id", "") or "")
                    model_id = str(payload.get("target_ref", "") or payload.get("model_id", "") or "")
                    baseline_snapshot_json = payload.get("baseline_snapshot_json", "") or ""
                    if baseline_snapshot_json:
                        baseline = json.loads(baseline_snapshot_json)
                    else:
                        baseline = {}

                    post = {}
                    if model_id:
                        post = await r.hgetall(f"{stream_snapshot_prefix}{model_id}")

                    decision = compute_rollback_verification(baseline, post, cfg)

                    verify_payload = {
                        "recommendation_id": recommendation_id,
                        "model_id": model_id,
                        "verification_ts_ms": _now_ms(),
                        "verification_status": decision.verification_status,
                        "reason_codes": json.dumps(decision.reason_codes, ensure_ascii=False),
                        "details": json.dumps(decision.details, ensure_ascii=False),
                    }
                    await r.xadd(stream_verify, verify_payload, maxlen=200_000, approximate=True)
                    await r.xadd(
                        stream_audit,
                        {
                            "event": "ROLLBACK_VERIFIED",
                            "recommendation_id": recommendation_id,
                            "model_id": model_id,
                            "verification_status": decision.verification_status,
                            "reason_codes": json.dumps(decision.reason_codes, ensure_ascii=False),
                            "ts_ms": _now_ms(),
                        }, maxlen=500_000,
                        approximate=True,
                    )

                    if decision.verification_status == "FAIL":
                        hard = [x for x in decision.reason_codes if x in {"ERROR_RATE_SPIKE", "LATENCY_P95_REGRESSION"}]
                        if hard:
                            await r.xadd(
                                rollback_requests_stream,
                                {
                                    "recommendation_id": recommendation_id,
                                    "target_ref": model_id,
                                    "reason": "POST_ROLLBACK_VERIFY_FAIL",
                                    "replay_status": "PASS",
                                    "ts_ms": _now_ms(),
                                }, maxlen=100_000,
                                approximate=True,
                            )

                    if database_url and recommendation_id:
                        await _persist_pg(database_url, verify_payload)

                    VERIFIER_EVENTS.labels(decision.verification_status).inc()
                finally:
                    await r.xack(stream_results, group, msg_id)
        VERIFIER_LOOP_SECONDS.observe(time.perf_counter() - t0)
        await asyncio.sleep(float(os.getenv("ML_ROLLBACK_VERIFIER_IDLE_SLEEP_SEC", "0.5")))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
