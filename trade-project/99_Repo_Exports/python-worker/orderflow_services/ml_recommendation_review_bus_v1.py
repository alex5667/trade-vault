from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any

from core.redis_stream_consumer import AsyncRedisStreamHelper
from utils.time_utils import get_ny_time_millis

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    class _Metric:
        def labels(self, *args, **kwargs):
            return self
        def inc(self, *args, **kwargs):
            return None
        def set(self, *args, **kwargs):
            return None
        def observe(self, *args, **kwargs):
            return None
    Counter = Gauge = Histogram = lambda *a, **k: _Metric()  # type: ignore
    def start_http_server(*args, **kwargs):  # type: ignore
        return None

PROPOSALS_STREAM = os.getenv("ML_RECOMMENDATION_PROPOSALS_STREAM", "stream:ml:recommendation_proposals")
REVIEW_REQUESTS_STREAM = os.getenv("ML_RECOMMENDATION_REVIEW_REQUESTS_STREAM", "stream:ml:recommendation_review_requests")
REVIEWS_STREAM = os.getenv("ML_RECOMMENDATION_REVIEWS_STREAM", "stream:ml:recommendation_reviews")
APPLY_REQUESTS_STREAM = os.getenv("ML_RECOMMENDATION_APPLY_REQUESTS_STREAM", "stream:ml:recommendation_apply_requests")
AUDIT_STREAM = os.getenv("ML_RECOMMENDATION_AUDIT_STREAM", "stream:ml:recommendation_audit")
GROUP_PROPOSALS = os.getenv("ML_RECOMMENDATION_REVIEW_PROPOSALS_GROUP", "cg:ml_recommendation_review_proposals")
GROUP_REVIEWS = os.getenv("ML_RECOMMENDATION_REVIEW_REVIEWS_GROUP", "cg:ml_recommendation_reviews")
CONSUMER = os.getenv("ML_RECOMMENDATION_REVIEW_CONSUMER", os.getenv("HOSTNAME", "ml-recommendation-review-bus-1"))

ALLOWED_ACTIONS = {
    "require_shadow_retrain",
    "freeze_candidate",
    "unfreeze_candidate",
    "request_calibration_refresh",
    "propose_threshold_canary",
    "open_incident",
    "draft_postmortem",
}

REVIEW_REQ_TOTAL = Counter("ml_recommendation_review_requests_total", "Review requests emitted", ["action_type"])
REVIEW_EVENTS_TOTAL = Counter("ml_recommendation_review_events_total", "Incoming review events", ["decision"])
REVIEW_APPLY_REQ_TOTAL = Counter("ml_recommendation_apply_requests_emitted_total", "Apply requests emitted after review", ["action_type"])
REVIEW_UP = Gauge("ml_recommendation_review_bus_up", "Up metric for recommendation review bus")
REVIEW_LAST_RUN_TS = Gauge("ml_recommendation_review_bus_last_run_ts_seconds", "Last run ts for recommendation review bus")
REVIEW_LOOP_SECONDS = Histogram("ml_recommendation_review_bus_loop_seconds", "Loop time for recommendation review bus")


def _now_ms() -> int:
    return get_ny_time_millis()


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def _to_bool(x: Any, default: bool = False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _norm_action(x: Any) -> str:
    return (x or "").strip()


def _norm_risk(x: Any) -> str:
    return (x or "unknown").strip().lower() or "unknown"


def _split_reason_codes(payload: dict[str, Any]) -> list[str]:
    raw = payload.get("reason_codes_json") or payload.get("reason_codes") or "[]"
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    s = str(raw)
    try:
        if s.startswith("["):
            data = json.loads(s)
            if isinstance(data, list):
                return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    return [x.strip() for x in s.split(",") if x.strip()]


def proposal_to_review_request(payload: dict[str, Any]) -> dict[str, Any]:
    action_type = _norm_action(payload.get("action_type"))
    recommendation_id = (payload.get("recommendation_id") or "").strip()
    if not recommendation_id:
        recommendation_id = hashlib.sha1(
            f"{action_type}|{payload.get('target_kind')}|{payload.get('target_ref')}|{payload.get('ts_ms')}".encode()
        ).hexdigest()
    replay_required = 1 if action_type in {"require_shadow_retrain", "request_calibration_refresh", "propose_threshold_canary"} else 0
    reason_codes = _split_reason_codes(payload)
    return {
        "schema_version": 1,
        "recommendation_id": recommendation_id,
        "analysis_run_id": (payload.get("analysis_run_id", "") or ""),
        "ts_ms": _to_int(payload.get("ts_ms", _now_ms()), _now_ms()),
        "action_type": action_type,
        "target_kind": (payload.get("target_kind", "") or ""),
        "target_ref": (payload.get("target_ref", "") or ""),
        "risk_level": _norm_risk(payload.get("risk_level")),
        "recommendation_json": payload.get("recommendation_json") if isinstance(payload.get("recommendation_json"), str) else json.dumps(payload.get("recommendation_json", {}), ensure_ascii=False, separators=(",", ":")),
        "review_status": "PENDING",
        "replay_required": replay_required,
        "replay_status": (payload.get("replay_status", "UNKNOWN") or "UNKNOWN").upper(),
        "approved_count": 0,
        "rejected_count": 0,
        "reason_codes_json": json.dumps(reason_codes, ensure_ascii=False, separators=(",", ":")),
    }


def apply_request_from_review_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "recommendation_id": state["recommendation_id"],
        "analysis_run_id": state.get("analysis_run_id", ""),
        "ts_ms": _now_ms(),
        "action_type": state["action_type"],
        "target_kind": state["target_kind"],
        "target_ref": state["target_ref"],
        "risk_level": state.get("risk_level", "unknown"),
        "review_status": "READY_FOR_APPLY",
        "approved_count": _to_int(state.get("approved_count", 0), 0),
        "rejected_count": _to_int(state.get("rejected_count", 0), 0),
        "replay_required": _to_int(state.get("replay_required", 0), 0),
        "replay_status": (state.get("replay_status", "UNKNOWN") or "UNKNOWN").upper(),
        "reason_codes_json": state.get("reason_codes_json", "[]"),
        "recommendation_json": state.get("recommendation_json", "{}"),
    }


def build_audit_event(recommendation_id: str, event_type: str, actor: str, payload: dict[str, Any]) -> dict[str, Any]:
    ts_ms = _now_ms()
    audit_id = hashlib.sha1(f"{recommendation_id}|{event_type}|{actor}|{ts_ms}".encode()).hexdigest()
    return {
        "schema_version": 1,
        "audit_id": audit_id,
        "recommendation_id": recommendation_id,
        "ts_ms": ts_ms,
        "event_type": event_type,
        "actor": actor,
        "payload_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    }


def review_state_key(recommendation_id: str) -> str:
    return f"ml:recommendation:review_state:{recommendation_id}"


def should_emit_apply_request(state: dict[str, Any], *, min_approvals: int) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    action_type = (state.get("action_type", "") or "")
    if action_type not in ALLOWED_ACTIONS:
        reasons.append("ACTION_NOT_ALLOWED")
    if _to_int(state.get("approved_count", 0), 0) < min_approvals:
        reasons.append("INSUFFICIENT_APPROVALS")
    if _to_int(state.get("rejected_count", 0), 0) > 0:
        reasons.append("HAS_REJECTIONS")
    risk_level = _norm_risk(state.get("risk_level", "unknown"))
    if risk_level == "high" and not _to_bool(os.getenv("ML_RECOMMENDATION_ALLOW_HIGH_RISK", "0"), False):
        reasons.append("HIGH_RISK_BLOCKED")
    if _to_int(state.get("replay_required", 0), 0) == 1 and (state.get("replay_status", "UNKNOWN")).upper() != "PASS":
        reasons.append("REPLAY_REQUIRED_NOT_PASS")
    return (len(reasons) == 0, reasons)


async def _handle_proposals(client: Any, helper: AsyncRedisStreamHelper, pel_state: dict) -> None:
    pending_start, pending_msgs = await helper.claim_pending(
        PROPOSALS_STREAM, min_idle_ms=5000, count=_to_int(os.getenv("ML_RECOMMENDATION_REVIEW_BATCH", "64"), 64), start_id=pel_state.get(PROPOSALS_STREAM, "0-0")
    )
    pel_state[PROPOSALS_STREAM] = pending_start
    pending_formatted = [(m.msg_id, m.fields) for m in pending_msgs]

    if pending_formatted:
        rows = [[PROPOSALS_STREAM, pending_formatted]]
    else:
        rows = await helper.read({PROPOSALS_STREAM: ">"}, count=_to_int(os.getenv("ML_RECOMMENDATION_REVIEW_BATCH", "64"), 64), block=1000) or []

    for _, messages in rows:
        for msg_id, fields in messages:
            proposal = dict(fields)
            review_req = proposal_to_review_request(proposal)
            key = review_state_key(review_req["recommendation_id"])
            if not await client.exists(key):
                await client.hset(key, mapping=review_req)
            await client.xadd(REVIEW_REQUESTS_STREAM, review_req, maxlen=100_000, approximate=True)
            await client.xadd(AUDIT_STREAM, build_audit_event(review_req["recommendation_id"], "REVIEW_REQUEST_CREATED", "ml_recommendation_review_bus_v1", review_req), maxlen=200_000, approximate=True)
            REVIEW_REQ_TOTAL.labels(review_req["action_type"] or "unknown").inc()
            await helper.ack(PROPOSALS_STREAM, msg_id)


async def _handle_reviews(client: Any, helper: AsyncRedisStreamHelper, pel_state: dict) -> None:
    pending_start, pending_msgs = await helper.claim_pending(
        REVIEWS_STREAM, min_idle_ms=5000, count=_to_int(os.getenv("ML_RECOMMENDATION_REVIEW_BATCH", "64"), 64), start_id=pel_state.get(REVIEWS_STREAM, "0-0")
    )
    pel_state[REVIEWS_STREAM] = pending_start
    pending_formatted = [(m.msg_id, m.fields) for m in pending_msgs]

    if pending_formatted:
        rows = [[REVIEWS_STREAM, pending_formatted]]
    else:
        rows = await helper.read({REVIEWS_STREAM: ">"}, count=_to_int(os.getenv("ML_RECOMMENDATION_REVIEW_BATCH", "64"), 64), block=1000) or []

    min_approvals = _to_int(os.getenv("ML_RECOMMENDATION_MIN_APPROVALS", "1"), 1)
    for _, messages in rows:
        for msg_id, fields in messages:
            review = dict(fields)
            recommendation_id = (review.get("recommendation_id", "") or "")
            if not recommendation_id:
                await helper.ack(REVIEWS_STREAM, msg_id)
                continue
            state_key = review_state_key(recommendation_id)
            state = await client.hgetall(state_key)
            if not state:
                await client.xadd(AUDIT_STREAM, build_audit_event(recommendation_id, "REVIEW_EVENT_DROPPED", "ml_recommendation_review_bus_v1", {"reason": "UNKNOWN_RECOMMENDATION", "review": review}), maxlen=200_000, approximate=True)
                await helper.ack(REVIEWS_STREAM, msg_id)
                continue
            decision = (review.get("decision", "COMMENT") or "COMMENT").upper()
            reviewer = (review.get("reviewer", "unknown") or "unknown")
            replay_status = (review.get("replay_status", state.get("replay_status", "UNKNOWN")) or "UNKNOWN").upper()
            approved_count = _to_int(state.get("approved_count", 0), 0)
            rejected_count = _to_int(state.get("rejected_count", 0), 0)

            if decision == "APPROVE":
                approved_count += 1
            elif decision == "REJECT":
                rejected_count += 1

            state["approved_count"] = approved_count
            state["rejected_count"] = rejected_count
            state["replay_status"] = replay_status
            state["last_review_ts_ms"] = _now_ms()
            state["review_status"] = "APPROVED" if approved_count >= min_approvals and rejected_count == 0 else ("REJECTED" if rejected_count > 0 else "UNDER_REVIEW")
            await client.hset(state_key, mapping={k: str(v) for k, v in state.items()})

            audit_payload = {"reviewer": reviewer, "decision": decision, "replay_status": replay_status, "approved_count": approved_count, "rejected_count": rejected_count}
            REVIEW_EVENTS_TOTAL.labels(decision).inc()
            await client.xadd(AUDIT_STREAM, build_audit_event(recommendation_id, "REVIEW_EVENT", reviewer, audit_payload), maxlen=200_000, approximate=True)

            emit_apply, reasons = should_emit_apply_request(state, min_approvals=min_approvals)
            if emit_apply:
                apply_req = apply_request_from_review_state(state)
                await client.xadd(APPLY_REQUESTS_STREAM, apply_req, maxlen=100_000, approximate=True)
                await client.xadd(AUDIT_STREAM, build_audit_event(recommendation_id, "APPLY_REQUEST_EMITTED", "ml_recommendation_review_bus_v1", apply_req), maxlen=200_000, approximate=True)
                REVIEW_APPLY_REQ_TOTAL.labels(apply_req["action_type"] or "unknown").inc()
            elif reasons:
                await client.xadd(AUDIT_STREAM, build_audit_event(recommendation_id, "APPLY_REQUEST_BLOCKED", "ml_recommendation_review_bus_v1", {"reasons": reasons, "state": state}), maxlen=200_000, approximate=True)
            await helper.ack(REVIEWS_STREAM, msg_id)


async def _run() -> None:
    try:
        import redis.asyncio as redis  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("redis.asyncio is required to run the review bus") from exc

    metrics_port = _to_int(os.getenv("ML_RECOMMENDATION_REVIEW_METRICS_PORT", "9865"), 9865)
    start_http_server(metrics_port)
    REVIEW_UP.set(1)
    client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

    helper_proposals = AsyncRedisStreamHelper(client=client, group=GROUP_PROPOSALS, consumer=CONSUMER)
    await helper_proposals.ensure_groups([PROPOSALS_STREAM])

    helper_reviews = AsyncRedisStreamHelper(client=client, group=GROUP_REVIEWS, consumer=CONSUMER)
    await helper_reviews.ensure_groups([REVIEWS_STREAM])

    pel_state = {PROPOSALS_STREAM: "0-0", REVIEWS_STREAM: "0-0"}

    while True:
        t0 = time.perf_counter()
        REVIEW_LAST_RUN_TS.set(time.time())
        await _handle_proposals(client, helper_proposals, pel_state)
        await _handle_reviews(client, helper_reviews, pel_state)
        REVIEW_LOOP_SECONDS.observe(time.perf_counter() - t0)


def main() -> None:
    import asyncio
    asyncio.run(_run())


if __name__ == "__main__":
    main()
