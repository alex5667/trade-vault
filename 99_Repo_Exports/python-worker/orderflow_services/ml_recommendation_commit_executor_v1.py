from __future__ import annotations

import contextlib
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
from orderflow_services.recommendation_action_adapters_v1 import (
    ALLOWED_ACTIONS,
    apply_recommendation_adapter,
    stable_json,
)

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
ROLLBACK_RESULTS_STREAM = os.getenv(
    "ML_RECOMMENDATION_COMMIT_EXECUTOR_ROLLBACK_RESULTS_STREAM",
    "stream:ml:recommendation_rollback_results",
)
MODEL_SNAPSHOT_PREFIX = os.getenv(
    "ML_RECOMMENDATION_COMMIT_EXECUTOR_SNAPSHOT_PREFIX",
    "metrics:ml:model_snapshot:",
)
AUDIT_STREAM = os.getenv(
    "ML_RECOMMENDATION_COMMIT_EXECUTOR_AUDIT_STREAM",
    "stream:ml:recommendation_audit",
)
GROUP = os.getenv("ML_RECOMMENDATION_COMMIT_EXECUTOR_GROUP", "cg:ml_recommendation_commit_executor")
CONSUMER = os.getenv("ML_RECOMMENDATION_COMMIT_EXECUTOR_CONSUMER", os.getenv("HOSTNAME", "ml-commit-executor-1"))
STATE_PREFIX = os.getenv("ML_RECOMMENDATION_EXECUTOR_STATE_PREFIX", "state:ml:target")

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


def _json(v: Any, default: Any) -> Any:
    try:
        if v is None:
            return default
        if isinstance(v, (list, dict)):
            return v
        return json.loads(v)
    except Exception:
        return default


def state_key(target_kind: str, target_ref: str) -> str:
    return f"{STATE_PREFIX}:{target_kind}:{target_ref}"




async def _process_apply_batch(r: Any, payloads: list[dict[str, Any]]) -> None:
    now_ms = get_ny_time_millis()

    # 1. Prepare keys and extract basic info
    keys_to_fetch = []
    parsed_items = []

    for payload in payloads:
        action_type = _s(payload.get("action_type", "unknown"))
        recommendation_id = _s(payload.get("recommendation_id", ""))
        target_kind = _s(payload.get("target_kind", ""))
        target_ref = _s(payload.get("target_ref", ""))
        recommendation_json = _json(payload.get("recommendation_json", {}), {})

        mode = _s(payload.get("executor_mode", "DRY_RUN")).upper()
        if _s(payload.get("dry_run_only", "0")) == "1":
            mode = "DRY_RUN"

        is_blocked = action_type not in ALLOWED_ACTIONS
        key = state_key(target_kind, target_ref) if target_kind and target_ref else ""

        parsed_items.append({
            "action_type": action_type,
            "recommendation_id": recommendation_id,
            "target_kind": target_kind,
            "target_ref": target_ref,
            "recommendation_json": recommendation_json,
            "mode": mode,
            "is_blocked": is_blocked,
            "key": key,
            "payload": payload
        })
        if not is_blocked and key:
            keys_to_fetch.append(key)

    # 2. Fetch all states in one pipelined call
    states_dict = {}
    if keys_to_fetch:
        # mget returns list of values in same order as keys
        states = await r.mget(keys_to_fetch)
        for k, v in zip(keys_to_fetch, states, strict=False):
            states_dict[k] = _json(v, {})

    # 3. Process CPU logic synchronously
    results = []
    for item in parsed_items:
        if item["is_blocked"]:
            result = {
                "ts_ms": str(now_ms),
                "recommendation_id": item["recommendation_id"],
                "action_type": item["action_type"],
                "target_kind": item["target_kind"],
                "target_ref": item["target_ref"],
                "executor_mode": item["mode"],
                "status": "blocked",
                "reason": "action_not_allowed",
            }
            results.append({"item": item, "result": result, "adapter_result": None, "status": "blocked"})
            continue

        current_state = states_dict.get(item["key"], {})
        dry_run = item["mode"] != "COMMIT"

        adapter_result = apply_recommendation_adapter(
            action_type=item["action_type"],
            target_kind=item["target_kind"],
            target_ref=item["target_ref"],
            recommendation_json=item["recommendation_json"],
            current_state=current_state,
            dry_run=dry_run,
        )

        status = "ok" if adapter_result.ok else "blocked"
        result = {
            "ts_ms": str(now_ms),
            "recommendation_id": item["recommendation_id"],
            "action_type": item["action_type"],
            "target_kind": item["target_kind"],
            "target_ref": item["target_ref"],
            "executor_mode": item["mode"],
            "status": status,
            "reason": adapter_result.reason_code,
            "change_summary": adapter_result.patch_json,
            "before_json": adapter_result.before_json,
            "after_json": adapter_result.after_json,
        }
        results.append({"item": item, "result": result, "adapter_result": adapter_result, "status": status})

    # 4. Pipeline all writes
    pipe = r.pipeline()
    metrics_updates = {"APPLY_TOTAL": []}

    for r_obj in results:
        item = r_obj["item"]
        result = r_obj["result"]
        adapter_result = r_obj["adapter_result"]
        status = r_obj["status"]

        metrics_updates["APPLY_TOTAL"].append((item["action_type"], item["mode"], status))

        if adapter_result is None:
            # Blocked early
            pipe.xadd(RESULT_STREAM, result, maxlen=200_000, approximate=True)
            pipe.xadd(
                AUDIT_STREAM,
                {"ts_ms": str(now_ms), "event": "executor_block", **result},
                maxlen=200_000,
                approximate=True,
            )
            continue

        dry_run = item["mode"] != "COMMIT"

        if adapter_result.ok and not dry_run:
            pipe.set(item["key"], adapter_result.after_json)

        pipe.xadd(RESULT_STREAM, result, maxlen=200_000, approximate=True)
        pipe.xadd(
            AUDIT_STREAM,
            {"ts_ms": str(now_ms), "event": "executor_apply", **result},
            maxlen=200_000,
            approximate=True,
        )

        if status == "ok" and item["mode"] == "COMMIT":
            journal_payload = {
                "ts_ms": str(now_ms),
                "recommendation_id": item["recommendation_id"],
                "action_type": item["action_type"],
                "target_kind": item["target_kind"],
                "target_ref": item["target_ref"],
                "executor_mode": item["mode"],
                "change_summary": adapter_result.patch_json,
                "rollback_json": adapter_result.rollback_json,
            }
            pipe.xadd(ROLLBACK_JOURNAL_STREAM, journal_payload, maxlen=200_000, approximate=True)

            if item["recommendation_id"] and adapter_result.rollback_json:
                rb_key = f"ml:rollback_payload:{item['recommendation_id']}"
                pipe.hset(rb_key, "rollback_json", adapter_result.rollback_json)
                pipe.expire(rb_key, 604800)

    if results:
        await pipe.execute()

    # 5. Update metrics
    for action, mode, status in metrics_updates["APPLY_TOTAL"]:
        APPLY_TOTAL.labels(action=action, mode=mode, status=status).inc()


async def _process_rollback_batch(r: Any, payloads: list[dict[str, Any]]) -> None:
    now_ms = get_ny_time_millis()

    # 1. Check which payloads need hget from redis
    keys_to_fetch = []
    indices_to_fetch = []

    for idx, payload in enumerate(payloads):
        rollback_json_raw = payload.get("rollback_json")
        recommendation_id = _s(payload.get("recommendation_id", ""))
        if not rollback_json_raw and recommendation_id:
            rb_key = f"ml:rollback_payload:{recommendation_id}"
            keys_to_fetch.append(rb_key)
            indices_to_fetch.append(idx)

    # 2. Fetch all required hgets
    if keys_to_fetch:
        pipe = r.pipeline()
        for key in keys_to_fetch:
            pipe.hget(key, "rollback_json")
        with contextlib.suppress(Exception):
            hget_results = await pipe.execute()
            for idx, hget_res in zip(indices_to_fetch, hget_results, strict=False):
                payloads[idx]["_fetched_rollback_json"] = hget_res

    # 2b. Fetch baseline snapshots for verifier
    baseline_snapshots = {}
    snapshot_keys = []
    snapshot_refs = []
    for payload in payloads:
        target_ref = _s(payload.get("target_ref", ""))
        if not target_ref:
            # Fallback from rollback_json
            rollback_json_raw = payload.get("rollback_json") or payload.get("_fetched_rollback_json")
            if rollback_json_raw:
                with contextlib.suppress(Exception):
                    rj = json.loads(rollback_json_raw)
                    target_ref = rj.get("target_ref", "")
        if target_ref:
            snapshot_keys.append(f"{MODEL_SNAPSHOT_PREFIX}{target_ref}")
            snapshot_refs.append(target_ref)

    if snapshot_keys:
        pipe = r.pipeline()
        for key in snapshot_keys:
            pipe.hgetall(key)
        with contextlib.suppress(Exception):
            snapshot_results = await pipe.execute()
            for ref, snap_raw in zip(snapshot_refs, snapshot_results, strict=False):
                if snap_raw:
                    snapshot = {
                        (k.decode() if isinstance(k, bytes) else str(k)): (
                            v.decode() if isinstance(v, bytes) else str(v)
                        )
                        for k, v in snap_raw.items()
                    }
                    baseline_snapshots[ref] = stable_json(snapshot)

    # 3. Process logic and pipeline writes
    pipe = r.pipeline()
    metrics_updates = []

    for payload in payloads:
        action_type = _s(payload.get("action_type", "unknown"))
        recommendation_id = _s(payload.get("recommendation_id", ""))
        target_kind = _s(payload.get("target_kind", ""))
        target_ref = _s(payload.get("target_ref", ""))

        rollback_json_raw = payload.get("rollback_json") or payload.get("_fetched_rollback_json")
        rollback_json = _json(rollback_json_raw, {})

        status = "failed"
        reason = "invalid_rollback_json"

        if rollback_json and "before" in rollback_json:
            if not target_kind:
                target_kind = rollback_json.get("target_kind", "")
            if not target_ref:
                target_ref = rollback_json.get("target_ref", "")

            key = state_key(target_kind, target_ref)
            before_state = rollback_json["before"]
            pipe.set(key, stable_json(before_state))
            status = "ok"
            reason = "rolled_back"

        baseline_snapshot_json = baseline_snapshots.get(target_ref, "{}")

        pipe.xadd(
            ROLLBACK_RESULTS_STREAM,
            {
                "ts_ms": str(now_ms),
                "recommendation_id": recommendation_id,
                "action_type": action_type,
                "target_kind": target_kind,
                "target_ref": target_ref,
                "status": status,
                "reason": reason,
                "baseline_snapshot_json": baseline_snapshot_json,
            },
            maxlen=200_000,
            approximate=True
        )

        pipe.xadd(
            AUDIT_STREAM,
            {
                "ts_ms": str(now_ms),
                "event": "executor_rollback",
                "recommendation_id": recommendation_id,
                "action_type": action_type,
                "target_kind": target_kind,
                "target_ref": target_ref,
                "status": status,
                "reason": reason,
            },
            maxlen=200_000,
            approximate=True
        )
        metrics_updates.append((action_type, status))

    if payloads:
        await pipe.execute()

    for action, status in metrics_updates:
        ROLLBACK_TOTAL.labels(action=action, status=status).inc()


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(_i(os.getenv("ML_RECOMMENDATION_COMMIT_EXECUTOR_METRICS_PORT", 9871), 9871))
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    helper = AsyncRedisStreamHelper(client=r, group=GROUP, consumer=CONSUMER)
    await helper.ensure_groups([INPUT_STREAM, ROLLBACK_REQUESTS_STREAM], start_id="0")

    pel_state = {INPUT_STREAM: "0-0", ROLLBACK_REQUESTS_STREAM: "0-0"}

    try:
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

                    payloads = []
                    msg_ids = []

                    for msg_id, fields in items:
                        payload = _parse_msg(fields)
                        ts_ms = _i(payload.get("ts_ms", get_ny_time_millis()), get_ny_time_millis())
                        QUEUE_LAG_MS.set(max(0, get_ny_time_millis() - ts_ms))
                        payloads.append(payload)
                        msg_ids.append(msg_id)

                    if payloads:
                        if sname == INPUT_STREAM:
                            await _process_apply_batch(r, payloads)
                        else:
                            await _process_rollback_batch(r, payloads)

                        await helper.ack_many(sname, msg_ids)
                LAST_RUN.set(time.time())
                LOOP_SECONDS.observe(time.perf_counter() - t0)
                RUNS.labels(status="ok").inc()
            except Exception:
                RUNS.labels(status="error").inc()
                import asyncio
                await asyncio.sleep(1.0)
    finally:
        import inspect
        close = getattr(r, "aclose", None) or getattr(r, "close", None)
        if close:
            res = close()
            if inspect.isawaitable(res):
                await res


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(main())

