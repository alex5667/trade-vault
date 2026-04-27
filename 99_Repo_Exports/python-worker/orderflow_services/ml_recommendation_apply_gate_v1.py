
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List

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

APPLY_REQUESTS_STREAM = os.getenv("ML_RECOMMENDATION_APPLY_REQUESTS_STREAM", "stream:ml:recommendation_apply_requests")
APPLY_RESULTS_STREAM = os.getenv("ML_RECOMMENDATION_APPLY_RESULTS_STREAM", "stream:ml:recommendation_apply_results")
AUDIT_STREAM = os.getenv("ML_RECOMMENDATION_AUDIT_STREAM", "stream:ml:recommendation_audit")
GROUP = os.getenv("ML_RECOMMENDATION_APPLY_GROUP", "cg:ml_recommendation_apply_gate")
CONSUMER = os.getenv("ML_RECOMMENDATION_APPLY_CONSUMER", os.getenv("HOSTNAME", "ml-recommendation-apply-gate-1"))

ALLOWED_ACTIONS = {
    "require_shadow_retrain",
    "freeze_candidate",
    "unfreeze_candidate",
    "request_calibration_refresh",
    "propose_threshold_canary",
    "open_incident",
    "draft_postmortem",
}

APPLYABLE_ACTIONS = {
    "require_shadow_retrain",
    "freeze_candidate",
    "unfreeze_candidate",
    "request_calibration_refresh",
    "propose_threshold_canary",
}

REPLAY_REQUIRED_ACTIONS = {
    "require_shadow_retrain",
    "request_calibration_refresh",
    "propose_threshold_canary",
}

ALLOWED_TARGET_KINDS = {
    "edge_stack_v1",
    "meta_lr",
    "ml_scorer_v2",
    "ml_scorer_v3",
    "confidence_cal",
    "feature_drift",
    "governance",
}

APPLY_REQ_TOTAL = Counter("ml_recommendation_apply_requests_total", "Recommendation apply requests seen", ["action_type", "status"])
APPLY_DECISION_TOTAL = Counter("ml_recommendation_apply_decision_total", "Recommendation apply decisions", ["action_type", "decision"])
APPLY_LAST_RUN_TS = Gauge("ml_recommendation_apply_last_run_ts_seconds", "Last run timestamp of recommendation apply gate")
APPLY_LOOP_SECONDS = Histogram("ml_recommendation_apply_loop_seconds", "Loop duration for recommendation apply gate")
APPLY_UP = Gauge("ml_recommendation_apply_up", "Up metric for recommendation apply gate")


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


def _split_csv(s: Any) -> List[str]:
    if s is None:
        return []
    if isinstance(s, (list, tuple)):
        return [str(x).strip() for x in s if str(x).strip()]
    return [x.strip() for x in str(s).split(",") if x.strip()]


@dataclass
class ApplyDecision:
    recommendation_id: str
    action_type: str
    target_kind: str
    target_ref: str
    decision: str
    allow: int
    status: str
    reason_codes_json: str
    replay_required: int
    replay_status: str
    approved_count: int
    rejected_count: int
    apply_mode: str
    dry_run: int
    actor: str
    ts_ms: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def parse_apply_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    out["recommendation_id"] = str(payload.get("recommendation_id", "") or "")
    out["action_type"] = str(payload.get("action_type", "") or "")
    out["target_kind"] = str(payload.get("target_kind", "") or "")
    out["target_ref"] = str(payload.get("target_ref", "") or "")
    out["risk_level"] = str(payload.get("risk_level", "unknown") or "unknown").lower()
    out["approved_count"] = _to_int(payload.get("approved_count", 0), 0)
    out["rejected_count"] = _to_int(payload.get("rejected_count", 0), 0)
    out["replay_required"] = 1 if _to_bool(payload.get("replay_required", False), False) else 0
    out["replay_status"] = str(payload.get("replay_status", "UNKNOWN") or "UNKNOWN").upper()
    out["review_status"] = str(payload.get("review_status", "PENDING") or "PENDING").upper()
    out["reason_codes"] = _split_csv(payload.get("reason_codes_json") or payload.get("reason_codes"))
    return out


def evaluate_apply_request(payload: Dict[str, Any]) -> ApplyDecision:
    req = parse_apply_request(payload)
    action_type = req["action_type"]
    apply_mode = str(os.getenv("ML_RECOMMENDATION_APPLY_MODE", "REVIEW_ONLY") or "REVIEW_ONLY").upper()
    dry_run = 1 if _to_bool(os.getenv("ML_RECOMMENDATION_APPLY_DRY_RUN", "1"), True) else 0
    min_approvals = _to_int(os.getenv("ML_RECOMMENDATION_MIN_APPROVALS", "1"), 1)
    allow_high_risk = _to_bool(os.getenv("ML_RECOMMENDATION_ALLOW_HIGH_RISK", "0"), False)

    reason_codes: List[str] = []
    allow = 0
    status = "BLOCKED"
    decision = "BLOCK"

    if not req["recommendation_id"]:
        reason_codes.append("MISSING_RECOMMENDATION_ID")
    if action_type not in ALLOWED_ACTIONS:
        reason_codes.append("ACTION_NOT_ALLOWED")
    if req["target_kind"] not in ALLOWED_TARGET_KINDS:
        reason_codes.append("TARGET_KIND_NOT_ALLOWED")
    if req["review_status"] not in {"APPROVED", "READY_FOR_APPLY"}:
        reason_codes.append("NOT_APPROVED")
    if req["approved_count"] < min_approvals:
        reason_codes.append("INSUFFICIENT_APPROVALS")
    if req["rejected_count"] > 0:
        reason_codes.append("HAS_REJECTIONS")
    if action_type not in APPLYABLE_ACTIONS:
        reason_codes.append("REVIEW_ONLY_ACTION")
    if req["risk_level"] == "high" and not allow_high_risk:
        reason_codes.append("HIGH_RISK_BLOCKED")
    replay_required = 1 if action_type in REPLAY_REQUIRED_ACTIONS or req["replay_required"] == 1 else 0
    replay_status = req["replay_status"]
    if replay_required and replay_status != "PASS":
        reason_codes.append("REPLAY_REQUIRED_NOT_PASS")
    if apply_mode == "OFF":
        reason_codes.append("APPLY_MODE_OFF")

    if not reason_codes:
        allow = 1
        if apply_mode == "REVIEW_ONLY":
            status = "REVIEW_ONLY"
            decision = "ALLOW_REVIEW_ONLY"
        elif dry_run == 1:
            status = "DRY_RUN_ALLOWED"
            decision = "ALLOW_DRY_RUN"
        else:
            status = "READY_FOR_EXECUTOR"
            decision = "ALLOW_EXECUTOR"

    return ApplyDecision(
        recommendation_id=req["recommendation_id"],
        action_type=action_type,
        target_kind=req["target_kind"],
        target_ref=req["target_ref"],
        decision=decision,
        allow=allow,
        status=status,
        reason_codes_json=json.dumps(reason_codes, ensure_ascii=False, separators=(",", ":")),
        replay_required=replay_required,
        replay_status=replay_status,
        approved_count=req["approved_count"],
        rejected_count=req["rejected_count"],
        apply_mode=apply_mode,
        dry_run=dry_run,
        actor="ml_recommendation_apply_gate_v1",
        ts_ms=_now_ms(),
    )


def build_audit_event(decision: ApplyDecision) -> Dict[str, Any]:
    audit_id = hashlib.sha1(f"{decision.recommendation_id}|{decision.ts_ms}|{decision.decision}".encode("utf-8")).hexdigest()
    return {
        "schema_version": 1,
        "audit_id": audit_id,
        "recommendation_id": decision.recommendation_id,
        "ts_ms": decision.ts_ms,
        "event_type": "APPLY_GATE_DECISION",
        "actor": decision.actor,
        "payload_json": json.dumps(decision.to_dict(), ensure_ascii=False, separators=(",", ":")),
    }


async def _run() -> None:
    try:
        import redis.asyncio as redis  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("redis.asyncio is required to run the apply gate") from exc

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    metrics_port = _to_int(os.getenv("ML_RECOMMENDATION_APPLY_METRICS_PORT", "9868"), 9868)
    start_http_server(metrics_port)
    APPLY_UP.set(1)

    client = redis.from_url(redis_url, decode_responses=True)
    try:
        await client.xgroup_create(APPLY_REQUESTS_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass

    while True:
        t0 = time.perf_counter()
        APPLY_LAST_RUN_TS.set(time.time())
        rows = await client.xreadgroup(
            GROUP,
            CONSUMER,
            {APPLY_REQUESTS_STREAM: ">"},
            count=_to_int(os.getenv("ML_RECOMMENDATION_APPLY_BATCH", "64"), 64),
            block=_to_int(os.getenv("ML_RECOMMENDATION_APPLY_BLOCK_MS", "5000"), 5000),
        )
        if not rows:
            APPLY_LOOP_SECONDS.observe(time.perf_counter() - t0)
            continue
        for _, messages in rows:
            for msg_id, fields in messages:
                req = dict(fields)
                dec = evaluate_apply_request(req)
                APPLY_REQ_TOTAL.labels(dec.action_type or "unknown", dec.status).inc()
                APPLY_DECISION_TOTAL.labels(dec.action_type or "unknown", dec.decision).inc()
                await client.xadd(APPLY_RESULTS_STREAM, dec.to_dict(), maxlen=100_000, approximate=True)
                await client.xadd(AUDIT_STREAM, build_audit_event(dec), maxlen=200_000, approximate=True)
                await client.xack(APPLY_REQUESTS_STREAM, GROUP, msg_id)
        APPLY_LOOP_SECONDS.observe(time.perf_counter() - t0)


def main() -> None:
    import asyncio
    asyncio.run(_run())


if __name__ == "__main__":
    main()
