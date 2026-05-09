from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server
import contextlib

STREAM_APPLY_RESULTS = os.getenv("ML_OPERATOR_RCA_ROUTING_APPLY_RESULTS_STREAM", "stream:ml:operator_rca_routing_apply_results")
STREAM_VERIFY_RESULTS = os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_RESULTS_STREAM", "stream:ml:operator_rca_routing_verify_results")
STREAM_ROLLBACK_REQUESTS = os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_REQUESTS_STREAM", "stream:ml:operator_rca_routing_rollback_requests")
STREAM_AUDIT = os.getenv("ML_OPERATOR_RCA_ROUTING_AUDIT_STREAM", "stream:ml:operator_rca_routing_apply_audit")
HASH_ROUTE_DEFAULT = os.getenv("ML_OPERATOR_RCA_DEFAULT_ROUTE_KEY", "cfg:ml:operator_rca_routing:default")
GROUP = os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_GROUP", "cg:ml_operator_rca_routing_verify_v2_6")
CONSUMER = os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_CONSUMER", os.getenv("HOSTNAME", "ml-operator-rca-routing-verify-v2-6"))

PROM_PORT = int(os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_METRICS_PORT", "9878"))
ROLLING_WINDOW_SEC = int(os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_WINDOW_SEC", "1800"))
MIN_EXPECTED_EXPOSURES = int(os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_MIN_EXPOSURES", "3"))
MAX_VERIFY_ERROR_RATE = float(os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_MAX_ERROR_RATE", "0.20"))
MIN_VERIFY_USEFULNESS = float(os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_MIN_USEFULNESS", "0.40"))
MAX_VERIFY_PARSE_FAIL_RATE = float(os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_MAX_PARSE_FAIL_RATE", "0.15"))


VERIFY_TOTAL = Counter(
    "ml_operator_rca_routing_verify_total",
    "Total routing post-apply verifications",
    ["status", "reason"],
)
LAST_RUN_TS = Gauge(
    "ml_operator_rca_routing_verify_last_run_ts_seconds",
    "Last successful routing verification run timestamp",
)
LOOP_SECONDS = Histogram(
    "ml_operator_rca_routing_verify_loop_seconds",
    "Loop duration for routing post-apply verifier",
)
PENDING = Gauge(
    "ml_operator_rca_routing_verify_pending_total",
    "Pending routing apply results awaiting verification",
)


@dataclass
class RoutingApplyResult:
    recommendation_id: str
    ts_ms: int
    status: str
    apply_mode: str
    provider: str
    model_name: str
    prompt_version: str
    policy_version: str
    baseline_route_json: str
    applied_route_json: str
    experiment_id: str
    reason_codes_json: str


def _decode(fields: dict[Any, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        out[kk] = vv
    return out


def _safe_json(raw: str, default: Any) -> Any:
    try:
        return json.loads(raw) if raw else default
    except Exception:
        return default


def _parse_apply_result(fields: dict[Any, Any]) -> RoutingApplyResult:
    d = _decode(fields)
    return RoutingApplyResult(
        recommendation_id=d.get("recommendation_id", ""),
        ts_ms=int(float(d.get("ts_ms", "0") or 0)),
        status=d.get("status", "UNKNOWN"),
        apply_mode=d.get("apply_mode", "DRY_RUN"),
        provider=d.get("provider", "vertex"),
        model_name=d.get("model_name", ""),
        prompt_version=d.get("prompt_version", ""),
        policy_version=d.get("policy_version", ""),
        baseline_route_json=d.get("baseline_route_json", "{}"),
        applied_route_json=d.get("applied_route_json", "{}"),
        experiment_id=d.get("experiment_id", ""),
        reason_codes_json=d.get("reason_codes_json", "[]"),
    )


def _score_snapshot(snapshot: dict[str, Any]) -> tuple[int, float, float, float]:
    exposures = int(snapshot.get("exposures_n", 0) or 0)
    usefulness = float(snapshot.get("usefulness_avg", 0.0) or 0.0)
    error_rate = float(snapshot.get("error_rate", 0.0) or 0.0)
    parse_fail_rate = float(snapshot.get("parse_fail_rate", 0.0) or 0.0)
    return exposures, usefulness, error_rate, parse_fail_rate


def evaluate_verification(snapshot: dict[str, Any]) -> tuple[str, list[str], bool]:
    reason_codes: list[str] = []
    exposures, usefulness, error_rate, parse_fail_rate = _score_snapshot(snapshot)
    rollback_required = False
    if exposures < MIN_EXPECTED_EXPOSURES:
        reason_codes.append("LOW_EXPOSURE")
        return "INCONCLUSIVE", reason_codes, rollback_required
    if error_rate > MAX_VERIFY_ERROR_RATE:
        reason_codes.append("ERROR_RATE_SPIKE")
        rollback_required = True
    if parse_fail_rate > MAX_VERIFY_PARSE_FAIL_RATE:
        reason_codes.append("PARSE_FAIL_RATE_HIGH")
        rollback_required = True
    if usefulness < MIN_VERIFY_USEFULNESS:
        reason_codes.append("USEFULNESS_DROP")
        rollback_required = True
    if rollback_required:
        return "ROLLBACK_REQUIRED", reason_codes, True
    return "PASS", ["VERIFY_PASS"], False


async def _load_current_route(r: Any) -> dict[str, Any]:
    try:
        current = await r.hgetall(HASH_ROUTE_DEFAULT)
        return {k.decode() if isinstance(k, (bytes, bytearray)) else str(k): v.decode() if isinstance(v, (bytes, bytearray)) else str(v) for k, v in current.items()}
    except Exception:
        return {}


async def _build_live_snapshot(r: Any) -> dict[str, Any]:
    last = await r.hgetall("metrics:ml:operator_rca_feedback:last")
    decoded = _decode(last)
    return {
        "exposures_n": int(float(decoded.get("sample_n", "0") or 0)),
        "usefulness_avg": float(decoded.get("usefulness_avg", "0") or 0.0),
        "error_rate": float(decoded.get("error_rate", "0") or 0.0),
        "parse_fail_rate": float(decoded.get("parse_fail_rate", "0") or 0.0),
    }


async def main() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PROM_PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    with contextlib.suppress(Exception):
        await r.xgroup_create(STREAM_APPLY_RESULTS, GROUP, id="0", mkstream=True)

    while True:
        t0 = time.perf_counter()
        rows = await r.xreadgroup(GROUP, CONSUMER, {STREAM_APPLY_RESULTS: ">"}, count=50, block=5000)
        processed = 0
        for _stream, items in rows:
            for msg_id, fields in items:
                processed += 1
                apply_result = _parse_apply_result(fields)
                if apply_result.apply_mode != "COMMIT" or apply_result.status not in {"COMMIT_APPLIED", "APPLIED"}:
                    await r.xack(STREAM_APPLY_RESULTS, GROUP, msg_id)
                    continue

                snapshot = await _build_live_snapshot(r)
                verify_status, reason_codes, rollback_required = evaluate_verification(snapshot)
                verification_payload = {
                    "schema_version": 1,
                    "recommendation_id": apply_result.recommendation_id,
                    "ts_ms": get_ny_time_millis(),
                    "provider": apply_result.provider,
                    "model_name": apply_result.model_name,
                    "prompt_version": apply_result.prompt_version,
                    "policy_version": apply_result.policy_version,
                    "verify_status": verify_status,
                    "reason_codes_json": json.dumps(reason_codes, ensure_ascii=False),
                    "baseline_route_json": apply_result.baseline_route_json,
                    "applied_route_json": apply_result.applied_route_json,
                    "live_snapshot_json": json.dumps(snapshot, ensure_ascii=False),
                    "rollback_required": 1 if rollback_required else 0,
                }
                await r.xadd(STREAM_VERIFY_RESULTS, verification_payload, maxlen=100000, approximate=True)
                await r.xadd(STREAM_AUDIT, {
                    "event": "ROUTING_POST_APPLY_VERIFY",
                    "recommendation_id": apply_result.recommendation_id,
                    "verify_status": verify_status,
                    "reason_codes_json": json.dumps(reason_codes, ensure_ascii=False),
                    "ts_ms": get_ny_time_millis(),
                }, maxlen=100000, approximate=True)
                if rollback_required:
                    await r.xadd(STREAM_ROLLBACK_REQUESTS, {
                        "schema_version": 1,
                        "recommendation_id": apply_result.recommendation_id,
                        "ts_ms": get_ny_time_millis(),
                        "rollback_type": "ROUTE_DEFAULT_ROLLBACK",
                        "baseline_route_json": apply_result.baseline_route_json,
                        "applied_route_json": apply_result.applied_route_json,
                        "reason_codes_json": json.dumps(reason_codes, ensure_ascii=False),
                    }, maxlen=100000, approximate=True)
                VERIFY_TOTAL.labels(status=verify_status, reason=(reason_codes[0] if reason_codes else "NONE")).inc()
                LAST_RUN_TS.set(time.time())
                await r.xack(STREAM_APPLY_RESULTS, GROUP, msg_id)
        PENDING.set(processed)
        LOOP_SECONDS.observe(time.perf_counter() - t0)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
