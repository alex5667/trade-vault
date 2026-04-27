from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server


INCIDENT_BUNDLE_STREAM = os.getenv("ML_INCIDENT_BUNDLE_STREAM", "stream:ml:incident_bundle_results")
RCA_REQUEST_STREAM = os.getenv("ML_OPERATOR_RCA_REQUEST_STREAM", "stream:ml:operator_rca_requests")
STATE_KEY = os.getenv("ML_OPERATOR_RCA_BRIDGE_STATE_KEY", "metrics:ml:operator_rca_bridge:last")
GROUP = os.getenv("ML_OPERATOR_RCA_BRIDGE_GROUP", "cg:ml_operator_rca_bridge_v1")
CONSUMER = os.getenv("ML_OPERATOR_RCA_BRIDGE_CONSUMER", "ml-operator-rca-bridge-v1")
PROM_PORT = int(os.getenv("ML_OPERATOR_RCA_BRIDGE_PORT", "9868"))

RUNS = Counter("ml_operator_rca_bridge_runs_total", "Bridge loop runs")
REQUESTS = Counter("ml_operator_rca_bridge_requests_total", "RCA requests built", ["severity"])
SKIPPED = Counter("ml_operator_rca_bridge_skipped_total", "Skipped bundles", ["reason"])
LAST_RUN_TS = Gauge("ml_operator_rca_bridge_last_run_ts_seconds", "Last successful bridge loop timestamp")
QUEUE_LAG_MS = Gauge("ml_operator_rca_bridge_queue_lag_ms", "Approximate queue lag in ms")
LOOP_SECONDS = Histogram("ml_operator_rca_bridge_loop_seconds", "Bridge loop latency")


def _b2s(v: Any) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def _loads_maybe(v: Any, default: Any) -> Any:
    if v in (None, "", b""):
        return default
    try:
        return json.loads(_b2s(v))
    except Exception:
        return default


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass
class IncidentBundle:
    recommendation_id: str
    ts_ms: int
    model_id: str
    family: str
    severity: str
    primary_reason_codes: List[str]
    summary: str
    snapshot_before: Dict[str, Any]
    snapshot_after: Dict[str, Any]
    snapshot_diff: Dict[str, Any]
    timeline: List[Dict[str, Any]]


def parse_bundle(fields: Dict[Any, Any]) -> IncidentBundle:
    d = {_b2s(k): v for k, v in fields.items()}
    return IncidentBundle(
        recommendation_id=_b2s(d.get("recommendation_id", "")),
        ts_ms=int(_b2s(d.get("ts_ms", "0")) or "0"),
        model_id=_b2s(d.get("model_id", "")),
        family=_b2s(d.get("family", "")),
        severity=_b2s(d.get("severity", "warning")) or "warning",
        primary_reason_codes=_loads_maybe(d.get("primary_reason_codes_json"), []),
        summary=_b2s(d.get("summary", "")),
        snapshot_before=_loads_maybe(d.get("snapshot_before_json"), {}),
        snapshot_after=_loads_maybe(d.get("snapshot_after_json"), {}),
        snapshot_diff=_loads_maybe(d.get("snapshot_diff_json"), {}),
        timeline=_loads_maybe(d.get("timeline_json"), []),
    )


def should_bridge(bundle: IncidentBundle) -> Tuple[bool, str]:
    if not bundle.recommendation_id:
        return False, "missing_recommendation_id"
    if bundle.severity.lower() not in {"warning", "critical"}:
        return False, "severity_below_threshold"
    if not bundle.model_id:
        return False, "missing_model_id"
    if not bundle.timeline:
        return False, "missing_timeline"
    return True, "ok"


def _top_diff_items(snapshot_diff: Dict[str, Any], limit: int) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    if not isinstance(snapshot_diff, dict):
        return items
    for k, v in snapshot_diff.items():
        if isinstance(v, dict):
            item = {"field": k}
            item.update(v)
            items.append(item)
        else:
            items.append({"field": k, "value": v})

    def _key(x: Dict[str, Any]) -> Tuple[float, str]:
        dv = x.get("delta")
        try:
            score = abs(float(dv))
        except Exception:
            score = -1.0
        return (-score, str(x.get("field", "")))

    items.sort(key=_key)
    return items[:limit]


def build_rca_input_pack(bundle: IncidentBundle, *, prompt_version: str, policy_version: str, diff_limit: int = 12) -> Dict[str, Any]:
    compact_timeline = []
    for item in bundle.timeline[:50]:
        if not isinstance(item, dict):
            continue
        compact_timeline.append(
            {
                "ts_ms": item.get("ts_ms"),
                "event": item.get("event"),
                "state": item.get("state"),
                "reason_codes": item.get("reason_codes"),
                "status": item.get("status"),
            }
        )
    return {
        "schema_version": 1,
        "task_type": "incident_rca",
        "prompt_version": prompt_version,
        "policy_version": policy_version,
        "recommendation_id": bundle.recommendation_id,
        "model_id": bundle.model_id,
        "family": bundle.family,
        "severity": bundle.severity,
        "summary": bundle.summary,
        "primary_reason_codes": bundle.primary_reason_codes,
        "top_snapshot_diff": _top_diff_items(bundle.snapshot_diff, diff_limit),
        "snapshot_before": bundle.snapshot_before,
        "snapshot_after": bundle.snapshot_after,
        "timeline": compact_timeline,
        "instructions": {
            "response_format": "json_only",
            "advisory_only": True,
            "no_auto_apply": True,
            "allowed_actions": [
                "require_shadow_retrain",
                "freeze_candidate",
                "unfreeze_candidate",
                "request_calibration_refresh",
                "propose_threshold_canary",
                "open_incident",
                "draft_postmortem",
            ],
        },
    }


async def _ensure_group(r: Any) -> None:
    try:
        await r.xgroup_create(INCIDENT_BUNDLE_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass


async def run_forever() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PROM_PORT)
    prompt_version = os.getenv("ML_OPERATOR_RCA_PROMPT_VERSION", "incident_rca_v1")
    policy_version = os.getenv("ML_OPERATOR_RCA_POLICY_VERSION", "policy_v1")
    block_ms = int(os.getenv("ML_OPERATOR_RCA_BRIDGE_BLOCK_MS", "5000"))
    count = int(os.getenv("ML_OPERATOR_RCA_BRIDGE_READ_COUNT", "32"))
    diff_limit = int(os.getenv("ML_OPERATOR_RCA_DIFF_LIMIT", "12"))
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    await _ensure_group(r)

    while True:
        t0 = time.perf_counter()
        rows = await r.xreadgroup(GROUP, CONSUMER, {INCIDENT_BUNDLE_STREAM: ">"}, count=count, block=block_ms)
        RUNS.inc()
        for _stream, messages in rows:
            for msg_id, fields in messages:
                bundle = parse_bundle(fields)
                ok, reason = should_bridge(bundle)
                if not ok:
                    SKIPPED.labels(reason=reason).inc()
                    await r.xack(INCIDENT_BUNDLE_STREAM, GROUP, msg_id)
                    continue
                req = build_rca_input_pack(bundle, prompt_version=prompt_version, policy_version=policy_version, diff_limit=diff_limit)
                payload = {
                    "schema_version": 1,
                    "request_id": f"{bundle.recommendation_id}:{bundle.ts_ms}",
                    "ts_ms": _now_ms(),
                    "task_type": "incident_rca",
                    "priority": "high" if bundle.severity.lower() == "critical" else "normal",
                    "recommendation_id": bundle.recommendation_id,
                    "model_id": bundle.model_id,
                    "family": bundle.family,
                    "severity": bundle.severity,
                    "prompt_version": prompt_version,
                    "policy_version": policy_version,
                    "input_pack_json": json.dumps(req, separators=(",", ":"), sort_keys=True),
                }
                await r.xadd(RCA_REQUEST_STREAM, payload, maxlen=100000, approximate=True)
                await r.hset(
                    STATE_KEY,
                    mapping={
                        "last_request_id": payload["request_id"],
                        "last_recommendation_id": bundle.recommendation_id,
                        "last_model_id": bundle.model_id,
                        "last_ts_ms": str(payload["ts_ms"]),
                        "last_prompt_version": prompt_version,
                        "last_policy_version": policy_version,
                    },
                )
                REQUESTS.labels(severity=bundle.severity.lower()).inc()
                try:
                    QUEUE_LAG_MS.set(max(0, _now_ms() - bundle.ts_ms))
                except Exception:
                    pass
                await r.xack(INCIDENT_BUNDLE_STREAM, GROUP, msg_id)
        LAST_RUN_TS.set(time.time())
        LOOP_SECONDS.observe(time.perf_counter() - t0)


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(run_forever())
