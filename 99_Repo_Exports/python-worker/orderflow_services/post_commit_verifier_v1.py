from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis

try:  # pragma: no cover
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:  # pragma: no cover
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server
import contextlib

VERIFY_RUNS = Counter(
    "ml_post_commit_verifier_runs_total",
    "Post-commit verifier runs",
    ["status"],
)
VERIFY_RESULTS = Counter(
    "ml_post_commit_verify_results_total",
    "Verification results by action/status",
    ["action_type", "status"],
)
LAST_RUN_TS = Gauge(
    "ml_post_commit_verifier_last_run_ts_seconds",
    "Last successful verifier run ts",
)
QUEUE_LAG_MS = Gauge(
    "ml_post_commit_verifier_queue_lag_ms",
    "Lag between commit event ts and verification time",
)
WINDOW_LAT = Histogram(
    "ml_post_commit_verifier_window_seconds",
    "Verification loop duration",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)


@dataclass
class CommitResult:
    recommendation_id: str
    action_type: str
    target_kind: str
    target_ref: str
    ts_ms: int
    apply_status: str
    executor_mode: str
    previous_value: str | None
    new_value: str | None
    replay_status: str
    reason_codes_json: str


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _j(x: Any, d: Any) -> Any:
    try:
        if x is None:
            return d
        if isinstance(x, (dict, list)):
            return x
        return json.loads(x)
    except Exception:
        return d


def parse_commit_result(fields: dict[Any, Any]) -> CommitResult:
    d = {str(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in fields.items()}
    return CommitResult(
        recommendation_id=(d.get("recommendation_id", "")),
        action_type=(d.get("action_type", "unknown")),
        target_kind=(d.get("target_kind", "unknown")),
        target_ref=(d.get("target_ref", "")),
        ts_ms=_i(d.get("ts_ms", 0), 0),
        apply_status=(d.get("apply_status", "UNKNOWN")),
        executor_mode=(d.get("executor_mode", "DRY_RUN")),
        previous_value=None if d.get("previous_value") in (None, "", "null") else (d.get("previous_value")),
        new_value=None if d.get("new_value") in (None, "", "null") else (d.get("new_value")),
        replay_status=(d.get("replay_status", "UNKNOWN")),
        reason_codes_json=(d.get("reason_codes_json", "[]")),
    )


def build_verification_policy(action_type: str) -> dict[str, Any]:
    base = {
        "verify_delay_sec": _i(os.getenv("ML_POST_COMMIT_VERIFY_DELAY_SEC", "300"), 300),
        "max_negative_delta": _f(os.getenv("ML_POST_COMMIT_MAX_NEG_DELTA", "0.05"), 0.05),
        "max_error_rate": _f(os.getenv("ML_POST_COMMIT_MAX_ERROR_RATE", "0.05"), 0.05),
        "max_latency_p95_ms": _f(os.getenv("ML_POST_COMMIT_MAX_LATENCY_P95_MS", "8.0"), 8.0),
    }
    if action_type == "propose_threshold_canary":
        base.update(
            {
                "required_signals_min": _i(os.getenv("ML_POST_COMMIT_CANARY_MIN_SIGNALS", "50"), 50),
                "rollback_on_allow_rate_drop": True,
                "rollback_on_error_spike": True,
            }
        )
    elif action_type in {"freeze_candidate", "unfreeze_candidate"}:
        base.update(
            {
                "required_signals_min": _i(os.getenv("ML_POST_COMMIT_FREEZE_MIN_SIGNALS", "10"), 10),
                "rollback_on_allow_rate_drop": False,
                "rollback_on_error_spike": True,
            }
        )
    else:
        base.update({"required_signals_min": 10, "rollback_on_allow_rate_drop": False, "rollback_on_error_spike": True})
    return base


def evaluate_post_commit(
    *,
    action_type: str,
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    policy: dict[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    before_allow = _f(before_snapshot.get("allow_rate_avg"), 0.0)
    after_allow = _f(after_snapshot.get("allow_rate_avg"), 0.0)
    after_err = _f(after_snapshot.get("error_rate_max"), 0.0)
    after_p95 = _f(after_snapshot.get("latency_p95_max_ms"), 0.0)
    signals_n = _i(after_snapshot.get("signals_n", after_snapshot.get("symbols_seen_n", 0)), 0)

    if signals_n < _i(policy.get("required_signals_min", 0), 0):
        reasons.append("INSUFFICIENT_SIGNALS")
    if _f(after_err, 0.0) > _f(policy.get("max_error_rate", 1.0), 1.0):
        reasons.append("ERROR_RATE_SPIKE")
    if _f(after_p95, 0.0) > _f(policy.get("max_latency_p95_ms", 999.0), 999.0):
        reasons.append("LATENCY_P95_REGRESSION")

    if bool(policy.get("rollback_on_allow_rate_drop", False)):
        delta = before_allow - after_allow
        if delta > _f(policy.get("max_negative_delta", 1.0), 1.0):
            reasons.append("ALLOW_RATE_DROP")

    if reasons:
        hard = {"ERROR_RATE_SPIKE", "LATENCY_P95_REGRESSION"}
        status = "ROLLBACK_REQUIRED" if any(r in hard for r in reasons) else "REVIEW_REQUIRED"
        return status, reasons
    return "PASS", []


async def fetch_snapshot(conn: Any, model_id: str, since_ts_ms: int) -> dict[str, Any]:
    sql = """,
        SELECT
            COALESCE(MAX(latency_p95_max_ms), 0.0) AS latency_p95_max_ms,
            COALESCE(MAX(error_rate_max), 0.0) AS error_rate_max,
            COALESCE(AVG(allow_rate_avg), 0.0) AS allow_rate_avg,
            COALESCE(MAX(signals_n), 0) AS signals_n,
            COALESCE(MAX(symbols_seen_n), 0) AS symbols_seen_n
        FROM ml_model_snapshots
        WHERE model_id = %s AND snapshot_ts_ms >= %s,
    """,
    async with conn.cursor() as cur:
        await cur.execute(sql, (model_id, since_ts_ms))
        row = await cur.fetchone()
        if not row:
            return {}
        cols = [d.name for d in cur.description]
        return {cols[i]: row[i] for i in range(len(cols))}


async def write_verification_result(conn: Any, rec: CommitResult, verification_status: str, reasons: list[str]) -> None:
    sql = """

        INSERT INTO llm_post_commit_verifications (
            recommendation_id, ts_ms, action_type, target_kind, target_ref,
            verification_status, reasons_json, executor_mode, replay_status
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (recommendation_id) DO UPDATE SET
            ts_ms = EXCLUDED.ts_ms,
            verification_status = EXCLUDED.verification_status,
            reasons_json = EXCLUDED.reasons_json,
            executor_mode = EXCLUDED.executor_mode,
            replay_status = EXCLUDED.replay_status,
    """,
    async with conn.cursor() as cur:
        await cur.execute(
            sql,
            (
                rec.recommendation_id,
                get_ny_time_millis(),
                rec.action_type,
                rec.target_kind,
                rec.target_ref,
                verification_status,
                json.dumps(reasons),
                rec.executor_mode,
                rec.replay_status,
            ),
        )


async def maybe_emit_rollback(redis_cli: Any, rec: CommitResult, reasons: list[str], verification_status: str) -> None:
    if verification_status != "ROLLBACK_REQUIRED":
        return
    payload = {
        "schema_version": 1,
        "recommendation_id": rec.recommendation_id,
        "ts_ms": get_ny_time_millis(),
        "action_type": rec.action_type,
        "target_kind": rec.target_kind,
        "target_ref": rec.target_ref,
        "rollback_reason_codes_json": json.dumps(reasons),
        "requested_by": "ml_post_commit_verifier_v1",
    }
    await redis_cli.xadd(
        os.getenv("ML_RECOMMENDATION_ROLLBACK_REQUESTS_STREAM", "stream:ml:recommendation_rollback_requests"),
        payload,
        maxlen=_i(os.getenv("ML_ROLLBACK_REQUESTS_MAXLEN", "50000"), 50000),
        approximate=True,
    )


async def run_once() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    if psycopg is None:
        raise RuntimeError("psycopg is required")

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream = os.getenv("ML_COMMIT_RESULTS_STREAM", "stream:ml:recommendation_apply_results")
    group = os.getenv("ML_POST_COMMIT_GROUP", "ml_post_commit_verifier_v1")
    consumer = os.getenv("ML_POST_COMMIT_CONSUMER", os.uname().nodename)
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required")

    redis_cli = redis.from_url(redis_url, decode_responses=False)
    with contextlib.suppress(Exception):
        await redis_cli.xgroup_create(stream, group, id="0", mkstream=True)

    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        rows = await redis_cli.xreadgroup(group, consumer, {stream: ">"}, count=100, block=1000)
        if not rows:
            VERIFY_RUNS.labels(status="idle").inc()
            return

        t0 = time.perf_counter()
        for _, messages in rows:
            for msg_id, fields in messages:
                rec = parse_commit_result(fields)
                if rec.apply_status not in {"COMMIT_APPLIED", "COMMIT_OK", "APPLIED"}:
                    await redis_cli.xack(stream, group, msg_id)
                    continue
                if rec.executor_mode != "COMMIT":
                    await redis_cli.xack(stream, group, msg_id)
                    continue

                policy = build_verification_policy(rec.action_type)
                delay_sec = _i(policy.get("verify_delay_sec", 300), 300)
                if get_ny_time_millis() - rec.ts_ms < delay_sec * 1000:
                    continue

                model_id = rec.target_ref
                before_snapshot = await fetch_snapshot(conn, model_id, max(0, rec.ts_ms - (delay_sec * 1000)))
                after_snapshot = await fetch_snapshot(conn, model_id, rec.ts_ms)
                verification_status, reasons = evaluate_post_commit(
                    action_type=rec.action_type,
                    before_snapshot=before_snapshot,
                    after_snapshot=after_snapshot,
                    policy=policy,
                )
                await write_verification_result(conn, rec, verification_status, reasons)
                await maybe_emit_rollback(redis_cli, rec, reasons, verification_status)
                await conn.commit()
                VERIFY_RESULTS.labels(action_type=rec.action_type, status=verification_status).inc()
                QUEUE_LAG_MS.set(max(0, get_ny_time_millis() - rec.ts_ms))
                await redis_cli.xadd(
                    os.getenv("ML_RECOMMENDATION_AUDIT_STREAM", "stream:ml:recommendation_audit"),
                    {
                        "schema_version": 1,
                        "event_type": "POST_COMMIT_VERIFICATION",
                        "recommendation_id": rec.recommendation_id,
                        "ts_ms": get_ny_time_millis(),
                        "verification_status": verification_status,
                        "reason_codes_json": json.dumps(reasons),
                    }, maxlen=_i(os.getenv("ML_RECOMMENDATION_AUDIT_MAXLEN", "100000"), 100000),
                    approximate=True,
                )
                await redis_cli.xack(stream, group, msg_id)

        LAST_RUN_TS.set(time.time())
        WINDOW_LAT.observe(time.perf_counter() - t0)
        VERIFY_RUNS.labels(status="ok").inc()


def main() -> None:
    start_http_server(_i(os.getenv("ML_POST_COMMIT_VERIFIER_METRICS_PORT", "9872"), 9872))
    import asyncio

    while True:
        try:
            asyncio.run(run_once())
        except Exception:
            VERIFY_RUNS.labels(status="error").inc()
            time.sleep(5)


if __name__ == "__main__":
    main()
