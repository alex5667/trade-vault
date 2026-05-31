from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, start_http_server


STREAM_IN = os.getenv("ML_OPERATOR_RCA_ROUTED_IN_STREAM", "stream:ml:operator_rca_requests_routed")
STREAM_OUT = os.getenv("ML_OPERATOR_RCA_EXPERIMENT_OUT_STREAM", "stream:ml:operator_rca_requests_experimented")
STREAM_EXPOSURES = os.getenv("ML_OPERATOR_RCA_EXPOSURES_STREAM", "stream:ml:operator_rca_exposures")
STREAM_AUDIT = os.getenv("ML_OPERATOR_RCA_EXPERIMENT_AUDIT_STREAM", "stream:ml:operator_rca_experiment_audit")
GROUP = os.getenv("ML_OPERATOR_RCA_EXPERIMENT_GROUP", "cg:ml_operator_rca_experiment_router")
CONSUMER = os.getenv("ML_OPERATOR_RCA_EXPERIMENT_CONSUMER", os.getenv("HOSTNAME", "ml-operator-rca-experiment-router"))

EXPERIMENTS_TOTAL = Counter(
    "ml_operator_rca_experiment_assignments_total",
    "Experiment arm assignments",
    ["experiment", "arm", "provider", "model_name", "prompt_version"],
)
EXPERIMENT_LAST_RUN_TS = Gauge(
    "ml_operator_rca_experiment_router_last_run_ts_seconds",
    "Last successful router loop timestamp",
)
EXPERIMENT_UP = Gauge(
    "ml_operator_rca_experiment_router_up",
    "Router heartbeat",
)


@dataclass(frozen=True)
class ArmSpec:
    name: str
    weight: float
    provider: str
    model_name: str
    prompt_version: str
    policy_version: str


def _safe_json_loads(raw: str, default: Any) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return default


def _hash_bucket(seed: str) -> float:
    h = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return int(h, 16) / float(0xFFFFFFFFFFFFFFFF)


def _normalize_weights(arms: Iterable[ArmSpec]) -> List[ArmSpec]:
    lst = [a for a in arms if a.weight > 0.0]
    total = sum(a.weight for a in lst)
    if total <= 0:
        return []
    return [ArmSpec(**{**a.__dict__, "weight": a.weight / total}) for a in lst]


def parse_experiment_arms(raw_json: str) -> List[ArmSpec]:
    data = _safe_json_loads(raw_json, [])
    out: List[ArmSpec] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        out.append(
            ArmSpec(
                name=str(item.get("name", "control")),
                weight=float(item.get("weight", 0.0) or 0.0),
                provider=str(item.get("provider", "vertex")),
                model_name=str(item.get("model_name", os.getenv("ML_OPERATOR_RCA_DEFAULT_MODEL", "gemini-2.5-flash-lite"))),
                prompt_version=str(item.get("prompt_version", os.getenv("ML_OPERATOR_RCA_DEFAULT_PROMPT_VERSION", "ml_triage_v1"))),
                policy_version=str(item.get("policy_version", os.getenv("ML_OPERATOR_RCA_DEFAULT_POLICY_VERSION", "policy_v1"))),
            )
        )
    return _normalize_weights(out)


def choose_arm(experiment_id: str, request_id: str, arms: List[ArmSpec]) -> Optional[ArmSpec]:
    if not arms:
        return None
    x = _hash_bucket(f"{experiment_id}:{request_id}")
    acc = 0.0
    for arm in arms:
        acc += arm.weight
        if x <= acc:
            return arm
    return arms[-1]


def build_experiment_assignment(payload: Dict[str, Any], experiment_id: str, arm: ArmSpec) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    request_id = str(payload.get("request_id") or payload.get("analysis_run_id") or payload.get("recommendation_id") or "na")
    ts_ms = int(payload.get("ts_ms") or get_ny_time_millis())
    assigned = dict(payload)
    assigned["experiment_id"] = experiment_id
    assigned["experiment_arm"] = arm.name
    assigned["provider"] = arm.provider
    assigned["model_name"] = arm.model_name
    assigned["prompt_version"] = arm.prompt_version
    assigned["policy_version"] = arm.policy_version
    assigned["experiment_assigned_ts_ms"] = ts_ms
    exposure = {
        "schema_version": 1,
        "ts_ms": ts_ms,
        "request_id": request_id,
        "experiment_id": experiment_id,
        "arm": arm.name,
        "provider": arm.provider,
        "model_name": arm.model_name,
        "prompt_version": arm.prompt_version,
        "policy_version": arm.policy_version,
        "incident_id": str(payload.get("incident_id", "")),
        "recommendation_id": str(payload.get("recommendation_id", "")),
    }
    return assigned, exposure


async def _ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception:
        return


async def run() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(int(os.getenv("ML_OPERATOR_RCA_EXPERIMENT_ROUTER_METRICS_PORT", "9875")))
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    await _ensure_group(r, STREAM_IN, GROUP)
    EXPERIMENT_UP.set(1)
    enabled = int(os.getenv("ML_OPERATOR_RCA_EXPERIMENT_ENABLE", "1") or 1)
    experiment_id = os.getenv("ML_OPERATOR_RCA_EXPERIMENT_ID", "operator_rca_ab_v1")
    arms = parse_experiment_arms(os.getenv(
        "ML_OPERATOR_RCA_EXPERIMENT_ARMS_JSON",
        '[{"name":"control","weight":0.5,"provider":"vertex","model_name":"gemini-2.5-flash-lite","prompt_version":"ml_triage_v1","policy_version":"policy_v1"},'
        '{"name":"challenger","weight":0.5,"provider":"vertex","model_name":"gemini-2.5-flash","prompt_version":"ml_triage_v2","policy_version":"policy_v1"}]'
    ))
    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {STREAM_IN: ">"}, count=100, block=5000)
        now_ts = time.time()
        for _stream, messages in rows:
            for msg_id, payload in messages:
                try:
                    if not enabled:
                        await r.xack(STREAM_IN, GROUP, msg_id)
                        continue
                    request_id = str(payload.get("request_id") or payload.get("analysis_run_id") or msg_id)
                    arm = choose_arm(experiment_id, request_id, arms)
                    if arm is None:
                        await r.xadd(STREAM_AUDIT, {"ts_ms": int(now_ts * 1000), "event": "NO_ARM", "request_id": request_id, "experiment_id": experiment_id}, maxlen=50000, approximate=True)
                        await r.xack(STREAM_IN, GROUP, msg_id)
                        continue
                    assigned, exposure = build_experiment_assignment(payload, experiment_id, arm)
                    await r.xadd(STREAM_OUT, assigned, maxlen=200000, approximate=True)
                    await r.xadd(STREAM_EXPOSURES, exposure, maxlen=200000, approximate=True)
                    await r.xadd(STREAM_AUDIT, {"ts_ms": int(now_ts * 1000), "event": "ASSIGNED", "request_id": request_id, "experiment_id": experiment_id, "arm": arm.name}, maxlen=50000, approximate=True)
                    EXPERIMENTS_TOTAL.labels(experiment_id, arm.name, arm.provider, arm.model_name, arm.prompt_version).inc()
                    await r.xack(STREAM_IN, GROUP, msg_id)
                except Exception as exc:
                    await r.xadd(STREAM_AUDIT, {"ts_ms": get_ny_time_millis(), "event": "ERROR", "message_id": msg_id, "error": str(exc)[:500]}, maxlen=50000, approximate=True)
                    await r.xack(STREAM_IN, GROUP, msg_id)
        EXPERIMENT_LAST_RUN_TS.set(now_ts)


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(run())
