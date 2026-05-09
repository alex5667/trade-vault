from __future__ import annotations

import json
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis

try:  # pragma: no cover
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from core.redis_stream_consumer import AsyncRedisStreamHelper
import contextlib

try:
    from orderflow_services.recommendation_action_adapters_v1 import (  # type: ignore
        ACTION_WHITELIST,
        execute_action,
        rollback_action,
    )
except Exception:  # pragma: no cover
    ACTION_WHITELIST = {
        "propose_threshold_canary",
        "request_calibration_refresh",
        "freeze_candidate",
        "unfreeze_candidate",
    }

    def execute_action(*, action_type: str, recommendation: dict[str, Any], mode: str, redis_client: Any = None) -> dict[str, Any]:
        return {
            "status": "ok",
            "mode": mode,
            "action_type": action_type,
            "target_ref": recommendation.get("target_ref", ""),
            "change_summary": json.dumps(recommendation, sort_keys=True),
        }

    def rollback_action(*, action_type: str, journal_payload: dict[str, Any], redis_client: Any = None) -> dict[str, Any]:
        return {"status": "ok", "action_type": action_type, "rolled_back": True}


INPUT_STREAM = os.getenv(
    "ML_RECOMMENDATION_COMMIT_EXECUTOR_INPUT_STREAM",
    "stream:ml:recommendation_commit_requests",
)
RESULT_STREAM = os.getenv(
    "ML_RECOMMENDATION_COMMIT_EXECUTOR_RESULT_STREAM",
    "stream:ml:recommendation_apply_results",
)
ROLLBACK_REQUESTS_STREAM = os.getenv(
    "ML_RECOMMENDATION_COMMIT_EXECUTOR_ROLLBACK_INPUT_STREAM",
    "stream:ml:recommendation_rollback_requests",
)
ROLLBACK_JOURNAL_STREAM = os.getenv(
    "ML_RECOMMENDATION_COMMIT_EXECUTOR_ROLLBACK_STREAM",
    "stream:ml:recommendation_rollback_journal",
)
AUDIT_STREAM = os.getenv(
    "ML_RECOMMENDATION_COMMIT_EXECUTOR_AUDIT_STREAM",
    "stream:ml:recommendation_audit",
)
GROUP = os.getenv("ML_RECOMMENDATION_COMMIT_EXECUTOR_GROUP", "cg:ml_recommendation_commit_executor")
CONSUMER = os.getenv("ML_RECOMMENDATION_COMMIT_EXECUTOR_CONSUMER", os.getenv("HOSTNAME", "ml-commit-executor-1"))

RUNS = Counter("ml_recommendation_commit_executor_runs_total", "Executor runs", ["status"])
APPLY_TOTAL = Counter("ml_recommendation_commit_executor_apply_total", "Apply total", ["action", "mode", "status"])
ROLLBACK_TOTAL = Counter("ml_recommendation_commit_executor_rollback_total", "Rollback total", ["action", "status"])
LAST_RUN = Gauge("ml_recommendation_commit_executor_last_run_ts_seconds", "Last run ts")
QUEUE_LAG_MS = Gauge("ml_recommendation_commit_executor_queue_lag_ms", "Queue lag ms")
LOOP_SECONDS = Histogram("ml_recommendation_commit_executor_loop_seconds", "Loop duration")


def _s(v: Any, d: str = "") -> str:
    return d if v is None else str(v)


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _parse_msg(fields: dict[bytes, bytes]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else v
        out[kk] = vv
    return out


async def _emit(r: redis.Redis, stream: str, payload: dict[str, Any]) -> None:
    try:
        await r.xadd(stream, payload, maxlen=200_000, approximate=True)
    except Exception:
        return


async def _process_apply(r: redis.Redis, payload: dict[str, Any]) -> None:
    now_ms = get_ny_time_millis()
    action_type = _s(payload.get("action_type", "unknown"))
    recommendation_id = _s(payload.get("recommendation_id", ""))
    target_ref = _s(payload.get("target_ref", ""))
    mode = _s(payload.get("executor_mode", "DRY_RUN")).upper()
    if _s(payload.get("dry_run_only", "0")) == "1":
        mode = "DRY_RUN"

    if action_type not in ACTION_WHITELIST:
        result = {
            "ts_ms": str(now_ms),
            "recommendation_id": recommendation_id,
            "action_type": action_type,
            "target_ref": target_ref,
            "executor_mode": mode,
            "status": "blocked",
            "reason": "action_not_allowed",
        }
        await _emit(r, RESULT_STREAM, result)
        await _emit(r, AUDIT_STREAM, {"ts_ms": str(now_ms), "event": "executor_block", **result})
        APPLY_TOTAL.labels(action=action_type, mode=mode, status="blocked").inc()
        return

    exec_result = execute_action(action_type=action_type, recommendation=payload, mode=mode, redis_client=r)
    result = {
        "ts_ms": str(now_ms),
        "recommendation_id": recommendation_id,
        "action_type": action_type,
        "target_ref": target_ref,
        "executor_mode": mode,
        "status": _s(exec_result.get("status", "ok"), "ok"),
        "reason": _s(exec_result.get("reason", "applied"), "applied"),
        "change_summary": _s(exec_result.get("change_summary", ""), ""),
    }
    await _emit(r, RESULT_STREAM, result)
    await _emit(r, AUDIT_STREAM, {"ts_ms": str(now_ms), "event": "executor_apply", **result})

    if result["status"] == "ok" and mode == "COMMIT":
        journal_payload = {
            "ts_ms": str(now_ms),
            "recommendation_id": recommendation_id,
            "action_type": action_type,
            "target_ref": target_ref,
            "executor_mode": mode,
            "change_summary": result["change_summary"],
            "rollback_payload_json": json.dumps(exec_result, sort_keys=True),
        }
        await _emit(r, ROLLBACK_JOURNAL_STREAM, journal_payload)
    APPLY_TOTAL.labels(action=action_type, mode=mode, status=result["status"]).inc()


async def _process_rollback(r: redis.Redis, payload: dict[str, Any]) -> None:
    now_ms = get_ny_time_millis()
    action_type = _s(payload.get("action_type", "unknown"))
    journal_payload = payload
    rb = rollback_action(action_type=action_type, journal_payload=journal_payload, redis_client=r)
    status = _s(rb.get("status", "ok"), "ok")
    await _emit(
        r,
        AUDIT_STREAM,
        {
            "ts_ms": str(now_ms),
            "event": "executor_rollback",
            "recommendation_id": _s(payload.get("recommendation_id", "")),
            "action_type": action_type,
            "status": status,
        }
    )
    ROLLBACK_TOTAL.labels(action=action_type, status=status).inc()


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(_i(os.getenv("ML_RECOMMENDATION_COMMIT_EXECUTOR_METRICS_PORT", 9871), 9871))
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    helper = AsyncRedisStreamHelper(client=r, group=GROUP, consumer=CONSUMER)
    await helper.ensure_groups([INPUT_STREAM, ROLLBACK_REQUESTS_STREAM], start_id="0")

    pel_state = {INPUT_STREAM: "0-0", ROLLBACK_REQUESTS_STREAM: "0-0"}

    while True:
        t0 = time.perf_counter()
        try:
            # PEL Recovery
            pending_input_start, pending_input = await helper.claim_pending(
                INPUT_STREAM, min_idle_ms=5000, count=50, start_id=pel_state[INPUT_STREAM]
            )
            pel_state[INPUT_STREAM] = pending_input_start

            pending_rollback_start, pending_rollback = await helper.claim_pending(
                ROLLBACK_REQUESTS_STREAM, min_idle_ms=5000, count=50, start_id=pel_state[ROLLBACK_REQUESTS_STREAM]
            )
            pel_state[ROLLBACK_REQUESTS_STREAM] = pending_rollback_start

            resp = []
            if pending_input:
                resp.append([INPUT_STREAM, [(m.msg_id, m.fields) for m in pending_input]])
            if pending_rollback:
                resp.append([ROLLBACK_REQUESTS_STREAM, [(m.msg_id, m.fields) for m in pending_rollback]])

            if not resp:
                resp = await helper.read(
                    {INPUT_STREAM: ">", ROLLBACK_REQUESTS_STREAM: ">"},
                    count=50,
                    block=5000,
                ) or []

            if not resp:
                LAST_RUN.set(time.time())
                LOOP_SECONDS.observe(time.perf_counter() - t0)
                continue

            for stream_name, items in resp:
                sname = stream_name.decode() if isinstance(stream_name, (bytes, bytearray)) else str(stream_name)
                for msg_id, fields in items:
                    payload = _parse_msg(fields)
                    ts_ms = _i(payload.get("ts_ms", get_ny_time_millis()), get_ny_time_millis())
                    QUEUE_LAG_MS.set(max(0, get_ny_time_millis() - ts_ms))
                    if sname == INPUT_STREAM:
                        await _process_apply(r, payload)
                    else:
                        await _process_rollback(r, payload)
                    with contextlib.suppress(Exception):
                        await helper.ack(sname, msg_id)
            LAST_RUN.set(time.time())
            LOOP_SECONDS.observe(time.perf_counter() - t0)
            RUNS.labels(status="ok").inc()
        except Exception:
            RUNS.labels(status="error").inc()
            time.sleep(1.0)


if __name__ == "__main__":  # pragma: no cover
    import asyncio; asyncio.run(main())

