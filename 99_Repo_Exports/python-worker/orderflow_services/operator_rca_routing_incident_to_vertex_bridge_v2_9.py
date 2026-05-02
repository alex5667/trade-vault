from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Tuple

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "operator_rca_routing_incident_to_vertex_bridge_v2_9"
REQUESTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_RESULTS_STREAM",
    "stream:ml:operator_rca_routing_incident_bundle_results",
)
RCA_REQUESTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_REQUESTS_STREAM",
    "stream:ml:operator_rca_routing_rca_requests",
)
RCA_AUDIT_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_AUDIT_STREAM",
    "stream:ml:operator_rca_routing_rca_audit",
)
GROUP = os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_BRIDGE_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_BRIDGE_PORT", "9884"))
MAXLEN = int(os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_BRIDGE_MAXLEN", "20000"))
PROMPT_VERSION = os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_PROMPT_VERSION", "routing_incident_rca_v1")
POLICY_VERSION = os.getenv("ML_OPERATOR_RCA_ROUTING_RCA_POLICY_VERSION", "policy_v1")
RCA_TASK_TYPE = "routing_incident_root_cause_analysis"


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_rca_routing_rca_bridge_runs_total",
    "Routing incident RCA bridge runs",
    ("status", "severity"),
)
LAT = _hist(
    "ml_operator_rca_routing_rca_bridge_latency_seconds",
    "Routing incident RCA bridge latency seconds",
)
UP = _gauge(
    "ml_operator_rca_routing_rca_bridge_up",
    "Routing incident RCA bridge up",
)
LAST_RUN_TS = _gauge(
    "ml_operator_rca_routing_rca_bridge_last_run_ts_seconds",
    "Routing incident RCA bridge last run timestamp",
)


def now_ms() -> int:
    return get_ny_time_millis()


def as_dict(fields: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        if isinstance(v, (bytes, bytearray)):
            try:
                out[kk] = v.decode()
            except Exception:
                out[kk] = v.hex()
        else:
            out[kk] = v
    return out


def maybe_json(v: Any, default: Any = None) -> Any:
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return default


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compact_timeline(timeline: List[Dict[str, Any]], limit: int = 25) -> List[Dict[str, Any]]:
    timeline = sorted(timeline, key=lambda x: int(x.get("ts_ms", 0) or 0))
    if len(timeline) <= limit:
        return timeline
    head = timeline[: max(limit // 2, 1)]
    tail = timeline[-max(limit - len(head), 1):]
    return head + tail


def build_routing_incident_rca_pack(bundle_row: Dict[str, Any]) -> Dict[str, Any]:
    bundle = maybe_json(bundle_row.get("bundle_json"), {}) or {}
    route_change_id = str(bundle.get("route_change_id") or bundle_row.get("route_change_id") or "")
    primary_reason_codes = bundle.get("primary_reason_codes") or maybe_json(bundle_row.get("primary_reason_codes_json"), [])
    summary = bundle.get("summary") or maybe_json(bundle_row.get("summary_json"), {})
    timeline = compact_timeline(bundle.get("timeline_json") or [])
    route_diff = bundle.get("route_diff_json") or maybe_json(bundle_row.get("route_diff_json"), {})
    sections = bundle.get("sections_json") or {}
    severity = str(bundle.get("severity") or bundle_row.get("severity") or "info")

    key_material = {
        "route_change_id": route_change_id,
        "severity": severity,
        "primary_reason_codes": primary_reason_codes,
        "route_diff": route_diff,
        "timeline": timeline,
        "prompt_version": PROMPT_VERSION,
        "policy_version": POLICY_VERSION,
    }
    compact_hash = hashlib.sha256(stable_json(key_material).encode("utf-8")).hexdigest()[:16]

    return {
        "schema_version": 1,
        "task_type": RCA_TASK_TYPE,
        "route_change_id": route_change_id,
        "severity": severity,
        "primary_reason_codes": primary_reason_codes,
        "summary": summary,
        "route_diff": route_diff,
        "timeline": timeline,
        "section_counts": {k: len(v) for k, v in sections.items() if isinstance(v, list)},
        "baseline_route": bundle.get("baseline_route_json") or {},
        "current_route": bundle.get("current_route_json") or {},
        "bundle_hash": bundle.get("bundle_hash") or bundle_row.get("bundle_id") or "",
        "compact_hash": compact_hash,
        "prompt_version": PROMPT_VERSION,
        "policy_version": POLICY_VERSION,
        "ts_ms": now_ms(),
    }


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    await ensure_group(r, REQUESTS_STREAM, GROUP)
    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {REQUESTS_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                row = as_dict(payload)
                status = "ok"
                severity = row.get("severity", "info")
                try:
                    pack = build_routing_incident_rca_pack(row)
                    await r.xadd(
                        RCA_REQUESTS_STREAM,
                        {
                            "schema_version": 1,
                            "route_change_id": pack["route_change_id"],
                            "task_type": pack["task_type"],
                            "compact_hash": pack["compact_hash"],
                            "prompt_version": pack["prompt_version"],
                            "policy_version": pack["policy_version"],
                            "severity": pack["severity"],
                            "payload_json": stable_json(pack),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        RCA_AUDIT_STREAM,
                        {
                            "event_type": "ROUTING_INCIDENT_RCA_BRIDGE_FAILED",
                            "route_change_id": row.get("route_change_id", ""),
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, severity=str(severity)).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
