from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, start_http_server

from orderflow_services.recommendation_action_adapters_v1 import (
    ALLOWED_ACTIONS,
    REPLAY_REQUIRED_ACTIONS,
    apply_recommendation_adapter,
    stable_json,
)


APPLY_REQ_STREAM = os.getenv("ML_RECOMMENDATION_APPLY_REQUESTS_STREAM", "stream:ml:recommendation_apply_requests")
APPLY_RESULTS_STREAM = os.getenv("ML_RECOMMENDATION_APPLY_RESULTS_STREAM", "stream:ml:recommendation_apply_results")
AUDIT_STREAM = os.getenv("ML_RECOMMENDATION_AUDIT_STREAM", "stream:ml:recommendation_audit")
ROLLBACK_STREAM = os.getenv("ML_RECOMMENDATION_ROLLBACK_STREAM", "stream:ml:recommendation_rollback_journal")
ROLLBACK_REQ_STREAM = os.getenv("ML_RECOMMENDATION_ROLLBACK_REQUESTS_STREAM", "stream:ml:recommendation_rollback_requests")
GROUP = os.getenv("ML_RECOMMENDATION_EXECUTOR_GROUP", "cg:ml_recommendation_executor_v1")
CONSUMER = os.getenv("HOSTNAME", "ml-recommendation-executor-v1")
STATE_PREFIX = os.getenv("ML_RECOMMENDATION_EXECUTOR_STATE_PREFIX", "state:ml:target")
MODE = os.getenv("ML_RECOMMENDATION_EXECUTOR_MODE", "DRY_RUN").upper()

EXECUTOR_UP = Gauge("ml_recommendation_executor_up", "Executor up")
EXECUTOR_LAST_RUN = Gauge("ml_recommendation_executor_last_run_ts_seconds", "Last executor loop")
EXECUTOR_APPLY_TOTAL = Counter("ml_recommendation_executor_apply_total", "Apply decisions", ["status", "mode", "action_type"])
EXECUTOR_ROLLBACK_TOTAL = Counter("ml_recommendation_executor_rollback_total", "Rollback decisions", ["status", "action_type"])


@dataclass
class ApplyDecision:
    ok: bool
    status: str
    reason_code: str
    result_payload: Dict[str, Any]


def _decode(fields: Dict[Any, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        out[kk] = vv
    return out


def _json(v: Any, default: Any) -> Any:
    if v in (None, "", b""):
        return default
    try:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            return json.loads(v)
        return v
    except Exception:
        return default


def state_key(target_kind: str, target_ref: str) -> str:
    return f"{STATE_PREFIX}:{target_kind}:{target_ref}"


def evaluate_apply_request(payload: Dict[str, Any], current_state: Dict[str, Any], *, mode: str = MODE) -> ApplyDecision:
    action_type = str(payload.get("action_type", ""))
    replay_status = str(payload.get("replay_status", "UNKNOWN")).upper()
    approval_status = str(payload.get("approval_status", "PENDING")).upper()
    recommendation_json = _json(payload.get("recommendation_json", {}), {})
    target_kind = str(payload.get("target_kind", ""))
    target_ref = str(payload.get("target_ref", ""))

    if action_type not in ALLOWED_ACTIONS:
        return ApplyDecision(False, "BLOCKED", "ACTION_NOT_ALLOWED", {"action_type": action_type})
    if approval_status != "APPROVED":
        return ApplyDecision(False, "BLOCKED", "NOT_APPROVED", {"action_type": action_type})
    if action_type in REPLAY_REQUIRED_ACTIONS and replay_status != "PASS":
        return ApplyDecision(False, "BLOCKED", "REPLAY_REQUIRED", {"action_type": action_type, "replay_status": replay_status})

    dry_run = mode != "COMMIT"
    result = apply_recommendation_adapter(
        action_type=action_type,
        target_kind=target_kind,
        target_ref=target_ref,
        recommendation_json=recommendation_json,
        current_state=current_state,
        dry_run=dry_run,
    )
    status = "APPLIED" if (result.ok and not dry_run) else ("DRY_RUN" if result.ok else "BLOCKED")
    return ApplyDecision(result.ok, status, result.reason_code, {
        "action_type": action_type,
        "target_kind": target_kind,
        "target_ref": target_ref,
        "dry_run": dry_run,
        "before_json": result.before_json,
        "after_json": result.after_json,
        "patch_json": result.patch_json,
        "rollback_json": result.rollback_json,
        "reason_code": result.reason_code,
    })


def evaluate_rollback_request(payload: Dict[str, Any], current_state: Dict[str, Any], *, mode: str = MODE) -> ApplyDecision:
    rollback_json = _json(payload.get("rollback_json", {}), {})
    if not rollback_json or "before" not in rollback_json:
        return ApplyDecision(False, "BLOCKED", "ROLLBACK_PAYLOAD_INVALID", {})
    action_type = str(rollback_json.get("action_type", "rollback"))
    target_kind = str(rollback_json.get("target_kind", payload.get("target_kind", "")))
    target_ref = str(rollback_json.get("target_ref", payload.get("target_ref", "")))
    before = rollback_json.get("before", {})
    status = "ROLLED_BACK" if mode == "COMMIT" else "DRY_RUN"
    return ApplyDecision(True, status, "OK", {
        "action_type": action_type,
        "target_kind": target_kind,
        "target_ref": target_ref,
        "dry_run": mode != "COMMIT",
        "before_json": stable_json(current_state),
        "after_json": stable_json(before),
        "patch_json": stable_json(before),
        "rollback_json": stable_json(rollback_json),
        "reason_code": "OK",
    })


async def _process_apply(redis_cli: Any, msg_id: str, payload: Dict[str, str]) -> None:
    target_kind = payload.get("target_kind", "")
    target_ref = payload.get("target_ref", "")
    key = state_key(target_kind, target_ref)
    current_state = _json(await redis_cli.get(key), {})
    decision = evaluate_apply_request(payload, current_state, mode=MODE)
    action_type = payload.get("action_type", "")
    EXECUTOR_APPLY_TOTAL.labels(status=decision.status, mode=MODE, action_type=action_type).inc()
    out = {
        "schema_version": 1,
        "ts_ms": get_ny_time_millis(),
        "request_id": payload.get("request_id", msg_id),
        "recommendation_id": payload.get("recommendation_id", ""),
        "status": decision.status,
        "reason_code": decision.reason_code,
        **{k: stable_json(v) if isinstance(v, (dict, list)) else v for k, v in decision.result_payload.items()},
    }
    if decision.ok and MODE == "COMMIT":
        await redis_cli.set(key, decision.result_payload.get("after_json", "{}"))
        await redis_cli.xadd(ROLLBACK_STREAM, {
            "schema_version": 1,
            "ts_ms": get_ny_time_millis(),
            "recommendation_id": payload.get("recommendation_id", ""),
            "request_id": payload.get("request_id", msg_id),
            "action_type": action_type,
            "target_kind": target_kind,
            "target_ref": target_ref,
            "rollback_json": decision.result_payload.get("rollback_json", "{}"),
        }, maxlen=200000, approximate=True)
    await redis_cli.xadd(APPLY_RESULTS_STREAM, out, maxlen=200000, approximate=True)
    await redis_cli.xadd(AUDIT_STREAM, {
        "schema_version": 1,
        "ts_ms": get_ny_time_millis(),
        "event": "apply_decision",
        "recommendation_id": payload.get("recommendation_id", ""),
        "request_id": payload.get("request_id", msg_id),
        "status": decision.status,
        "reason_code": decision.reason_code,
        "action_type": action_type,
    }, maxlen=200000, approximate=True)


async def _process_rollback(redis_cli: Any, msg_id: str, payload: Dict[str, str]) -> None:
    target_kind = payload.get("target_kind", "")
    target_ref = payload.get("target_ref", "")
    key = state_key(target_kind, target_ref)
    current_state = _json(await redis_cli.get(key), {})
    decision = evaluate_rollback_request(payload, current_state, mode=MODE)
    action_type = payload.get("action_type", "rollback")
    EXECUTOR_ROLLBACK_TOTAL.labels(status=decision.status, action_type=action_type).inc()
    if decision.ok and MODE == "COMMIT":
        await redis_cli.set(key, decision.result_payload.get("after_json", "{}"))
    await redis_cli.xadd(AUDIT_STREAM, {
        "schema_version": 1,
        "ts_ms": get_ny_time_millis(),
        "event": "rollback_decision",
        "request_id": payload.get("request_id", msg_id),
        "status": decision.status,
        "reason_code": decision.reason_code,
        "action_type": action_type,
    }, maxlen=200000, approximate=True)


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    port = int(os.getenv("ML_RECOMMENDATION_EXECUTOR_METRICS_PORT", "9869"))
    start_http_server(port)
    EXECUTOR_UP.set(1)
    redis_cli = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    try:
        await redis_cli.xgroup_create(APPLY_REQ_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass
    try:
        await redis_cli.xgroup_create(ROLLBACK_REQ_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass
    while True:
        EXECUTOR_LAST_RUN.set(time.time())
        rows = await redis_cli.xreadgroup(GROUP, CONSUMER, {APPLY_REQ_STREAM: ">", ROLLBACK_REQ_STREAM: ">"}, count=100, block=5000)
        for stream_name, messages in rows:
            sname = stream_name.decode() if isinstance(stream_name, (bytes, bytearray)) else str(stream_name)
            for msg_id, fields in messages:
                payload = _decode(fields)
                try:
                    if sname == APPLY_REQ_STREAM:
                        await _process_apply(redis_cli, msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id), payload)
                    else:
                        await _process_rollback(redis_cli, msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id), payload)
                finally:
                    await redis_cli.xack(sname, GROUP, msg_id)


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
