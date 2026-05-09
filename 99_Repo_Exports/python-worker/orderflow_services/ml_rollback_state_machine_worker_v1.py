from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from core.redis_stream_consumer import AsyncRedisStreamHelper
from orderflow_services.rollback_state_machine_v1 import (
    EVENT_REQUEST_CREATED,
    EVENT_ROLLBACK_ERROR,
    EVENT_ROLLBACK_EXECUTED,
    EVENT_VERIFY_FAIL,
    EVENT_VERIFY_INCONCLUSIVE,
    EVENT_VERIFY_PASS,
    apply_event,
)
from utils.time_utils import get_ny_time_millis
import contextlib

SM_UP = Gauge("ml_rollback_state_machine_up", "Rollback state machine liveness")
SM_LAST_RUN_TS = Gauge("ml_rollback_state_machine_last_run_ts_seconds", "Last state machine loop time")
SM_TRANSITIONS = Counter("ml_rollback_state_machine_transitions_total", "Rollback transitions", ["event", "state"])
SM_INVALID = Counter("ml_rollback_state_machine_invalid_total", "Invalid rollback transitions", ["event"])
SM_LOOP_SECONDS = Histogram("ml_rollback_state_machine_loop_seconds", "Rollback state loop latency")


def _now_ms() -> int:
    return get_ny_time_millis()


def _state_key(prefix: str, recommendation_id: str) -> str:
    return f"{prefix}:{recommendation_id}"


def _reason_codes(payload: dict[str, Any]) -> str:
    if "reason_codes" in payload:
        return (payload.get("reason_codes", "") or "")
    if "reason_codes_json" in payload:
        return (payload.get("reason_codes_json", "") or "")
    return ""


def _event_from_request(_payload: dict[str, Any]) -> str:
    return EVENT_REQUEST_CREATED


def _event_from_result(payload: dict[str, Any]) -> str:
    status = (payload.get("status", "") or "").upper()
    if status in {"OK", "SUCCESS", "DONE", "EXECUTED"}:
        return EVENT_ROLLBACK_EXECUTED
    return EVENT_ROLLBACK_ERROR


def _event_from_verify(payload: dict[str, Any]) -> str:
    status = str(payload.get("verification_status", "") or payload.get("status", "")).upper()
    if status in {"PASS", "ROLLBACK_SUCCESS"}:
        return EVENT_VERIFY_PASS
    if status in {"FAIL", "ROLLBACK_FAILED"}:
        return EVENT_VERIFY_FAIL
    return EVENT_VERIFY_INCONCLUSIVE


async def _persist_pg(database_url: str, payload: dict[str, Any]) -> None:
    import asyncpg  # type: ignore

    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute(
            """

            INSERT INTO llm_rollback_state_history (
                recommendation_id, ts_ms, prev_state, event, next_state, reason_codes_json
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            """,
            payload["recommendation_id"],
            int(payload["ts_ms"]),
            payload.get("prev_state"),
            payload["event"],
            payload["next_state"],
            json.dumps(payload.get("reason_codes", [])),
        )
        await conn.execute(
            """,
            UPDATE llm_recommendations
               SET rollback_state = $2
             WHERE recommendation_id = $1,
            """,
            payload["recommendation_id"],
            payload["next_state"],
        )
    finally:
        await conn.close()


async def _transition(
    r: Any,
    *,
    database_url: str,
    audit_stream: str,
    state_stream: str,
    state_prefix: str,
    recommendation_id: str,
    model_id: str,
    event: str,
    reason_codes: str,
) -> None:
    skey = _state_key(state_prefix, recommendation_id)
    current = await r.hgetall(skey)
    prev_state = current.get("rollback_state")
    try:
        tr = apply_event(prev_state, event)
    except ValueError:
        SM_INVALID.labels(event).inc()
        await r.xadd(
            audit_stream,
            {
                "event": "ROLLBACK_STATE_INVALID",
                "recommendation_id": recommendation_id,
                "model_id": model_id,
                "prev_state": prev_state or "",
                "attempted_event": event,
                "reason_codes": reason_codes,
                "ts_ms": _now_ms(),
            }, maxlen=500_000,
            approximate=True,
        )
        return

    ts_ms = _now_ms()
    body = {
        "recommendation_id": recommendation_id,
        "model_id": model_id,
        "prev_state": tr.prev_state or "",
        "event": tr.event,
        "next_state": tr.next_state,
        "reason_codes": reason_codes,
        "ts_ms": ts_ms,
    }

    await r.hset(
        skey,
        mapping={
            "recommendation_id": recommendation_id,
            "model_id": model_id,
            "rollback_state": tr.next_state,
            "last_event": tr.event,
            "updated_at_ms": ts_ms,
            "reason_codes": reason_codes,
        }
    )
    await r.expire(skey, int(os.getenv("ML_ROLLBACK_STATE_TTL_SEC", "604800")))
    await r.xadd(state_stream, body, maxlen=200_000, approximate=True)
    await r.xadd(
        audit_stream,
        {
            "event": "ROLLBACK_STATE_TRANSITION",
            **body,
        }, maxlen=500_000,
        approximate=True,
    )
    SM_TRANSITIONS.labels(event=tr.event, state=tr.next_state).inc()

    if database_url:
        await _persist_pg(
            database_url,
            {
                "recommendation_id": recommendation_id,
                "ts_ms": ts_ms,
                "prev_state": tr.prev_state,
                "event": tr.event,
                "next_state": tr.next_state,
                "reason_codes": [x.strip() for x in reason_codes.split(",") if x.strip()],
            }
        )


async def _ensure_group(r: Any, stream_name: str, group: str) -> None:
    with contextlib.suppress(Exception):
        await r.xgroup_create(stream_name, group, id="0", mkstream=True)


async def main() -> None:
    try:
        import redis.asyncio as redis  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise SystemExit(f"redis.asyncio is required: {exc}")

    port = int(os.getenv("ML_ROLLBACK_STATE_MACHINE_METRICS_PORT", "9863"))
    start_http_server(port)

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)
    database_url = os.getenv("DATABASE_URL", "")

    request_stream = os.getenv("ML_ROLLBACK_REQUESTS_STREAM", "stream:ml:recommendation_rollback_requests")
    result_stream = os.getenv("ML_ROLLBACK_RESULTS_STREAM", "stream:ml:recommendation_rollback_results")
    verify_stream = os.getenv("ML_ROLLBACK_VERIFY_RESULTS_STREAM", "stream:ml:recommendation_rollback_verification_results")
    audit_stream = os.getenv("ML_RECOMMENDATION_AUDIT_STREAM", "stream:ml:recommendation_audit")
    state_stream = os.getenv("ML_ROLLBACK_STATE_STREAM", "stream:ml:recommendation_rollback_state")
    state_prefix = os.getenv("ML_ROLLBACK_STATE_PREFIX", "ml:rollback_state")
    group = os.getenv("ML_ROLLBACK_STATE_GROUP", "cg:ml_rollback_state_machine")
    consumer = os.getenv("ML_ROLLBACK_STATE_CONSUMER", os.getenv("HOSTNAME", "ml-rollback-state-machine-1"))

    helper = AsyncRedisStreamHelper(client=r, group=group, consumer=consumer)
    await helper.ensure_groups([request_stream, result_stream, verify_stream], start_id="0")

    pel_state = {request_stream: "0-0", result_stream: "0-0", verify_stream: "0-0"}

    while True:
        SM_UP.set(1)
        t0 = time.perf_counter()
        SM_LAST_RUN_TS.set(time.time())

        # 1. PEL Recovery
        pending_req_start, pending_req = await helper.claim_pending(
            request_stream, min_idle_ms=5000, count=100, start_id=pel_state[request_stream]
        )
        pel_state[request_stream] = pending_req_start

        pending_res_start, pending_res = await helper.claim_pending(
            result_stream, min_idle_ms=5000, count=100, start_id=pel_state[result_stream]
        )
        pel_state[result_stream] = pending_res_start

        pending_ver_start, pending_ver = await helper.claim_pending(
            verify_stream, min_idle_ms=5000, count=100, start_id=pel_state[verify_stream]
        )
        pel_state[verify_stream] = pending_ver_start

        rows = []
        if pending_req:
            rows.append([request_stream, [(m.msg_id, m.fields) for m in pending_req]])
        if pending_res:
            rows.append([result_stream, [(m.msg_id, m.fields) for m in pending_res]])
        if pending_ver:
            rows.append([verify_stream, [(m.msg_id, m.fields) for m in pending_ver]])

        if not rows:
            rows = await helper.read(
                {request_stream: ">", result_stream: ">", verify_stream: ">"},
                count=100,
                block=5000,
            ) or []

        for stream_name, msgs in rows:
            for msg_id, payload in msgs:
                try:
                    recommendation_id = (payload.get("recommendation_id", "") or "")
                    model_id = str(payload.get("target_ref", "") or payload.get("model_id", "") or "")
                    if not recommendation_id:
                        continue
                    if stream_name == request_stream:
                        event = _event_from_request(payload)
                    elif stream_name == result_stream:
                        event = _event_from_result(payload)
                    else:
                        event = _event_from_verify(payload)
                    await _transition(
                        r,
                        database_url=database_url,
                        audit_stream=audit_stream,
                        state_stream=state_stream,
                        state_prefix=state_prefix,
                        recommendation_id=recommendation_id,
                        model_id=model_id,
                        event=event,
                        reason_codes=_reason_codes(payload),
                    )
                finally:
                    await helper.ack(stream_name, msg_id)
        SM_LOOP_SECONDS.observe(time.perf_counter() - t0)
        await asyncio.sleep(float(os.getenv("ML_ROLLBACK_STATE_IDLE_SLEEP_SEC", "0.5")))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
