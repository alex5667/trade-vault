from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from core.redis_keys import RedisKeyPrefixes as RK
from utils.time_utils import get_ny_time_millis

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None

    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_bundle_rca_bridge_v3_36"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles",
)
VERTEX_OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_requests",
)
LOCAL_OUTPUT_STREAM = os.getenv(
    "ML_LOCAL_FALLBACK_REQUESTS_STREAM",
    "stream:ml:local_fallback_requests",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge:global",
)
VERTEX_HEALTH_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_VERTEX_HEALTH_HASH",
    "metrics:ml:vertex_health:last",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_PORT", "9963"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_MAXLEN", "20000"))

DEFAULT_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_MODE", "AUTO").upper()
DEFAULT_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL = int(
    os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL", "1")
)
DEFAULT_MAX_BUNDLE_BYTES = int(
    os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_MAX_BUNDLE_BYTES", "131072")
)
DEFAULT_MAX_PROMPT_CHARS = int(
    os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_MAX_PROMPT_CHARS", "12000")
)
ALLOWED_MODES = {"AUTO", "VERTEX_ONLY", "LOCAL_ONLY", "DISABLED"}


def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_runs_total",
    "Winner-apply apply governance bundle RCA bridge runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_latency_seconds",
    "Winner-apply apply governance bundle RCA bridge latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_up",
    "Winner-apply apply governance bundle RCA bridge up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_last_run_ts_seconds",
    "Winner-apply apply governance bundle RCA bridge last run timestamp",
)
ROUTED = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_routed_total",
    "Winner-apply apply governance bundle RCA bridge routed total",
    ("route", "severity"),
)


def now_ms() -> int:
    return get_ny_time_millis()


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def as_dict(fields: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
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


def maybe_json(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def policy_from_hash(raw: dict[str, Any]) -> dict[str, Any]:
    mode = str(raw.get("mode") or DEFAULT_MODE).upper()
    if mode not in ALLOWED_MODES:
        mode = DEFAULT_MODE
    allow_severities = maybe_json(raw.get("allow_severities_json"), ["warning", "critical"])
    if not isinstance(allow_severities, list):
        allow_severities = ["warning", "critical"]
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "mode": mode,
        "require_vertex_degraded_for_local": parse_int(
            raw.get("require_vertex_degraded_for_local"),
            DEFAULT_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL,
        ),
        "allow_severities": {str(x).lower() for x in allow_severities},
        "max_bundle_bytes": parse_int(raw.get("max_bundle_bytes"), DEFAULT_MAX_BUNDLE_BYTES),
        "max_prompt_chars": parse_int(raw.get("max_prompt_chars"), DEFAULT_MAX_PROMPT_CHARS),
    }


def vertex_degraded_from_hash(raw: dict[str, Any]) -> bool:
    status = (raw.get("status") or "").lower()
    degraded = parse_int(raw.get("degraded"), 0)
    if degraded == 1:
        return True
    if status in {"degraded", "down", "error", "unavailable"}:
        return True
    err_rate = raw.get("err_rate")
    if err_rate is not None:
        try:
            if float(err_rate) >= 0.5:
                return True
        except Exception:
            pass
    return False


def build_vertex_prompt(bundle: dict[str, Any]) -> str:
    return (
        "Analyze this route_incident_rca mirror RCA winner-apply apply governance incident bundle. "
        "Focus on apply-controller decisions, verification failures, rollback causes, retry outcomes, "
        "SLO and MTTR pressure, escalation severity, and bounded next actions."
    )


def build_local_prompt(bundle: dict[str, Any]) -> str:
    return (
        "Vertex primary path is unavailable or degraded. "
        "Perform bounded RCA summarization for this route_incident_rca mirror RCA winner-apply apply governance incident bundle. "
        "Focus on apply-controller decisions, verification failures, rollback causes, retry outcomes, "
        "SLO and MTTR pressure, escalation severity, and bounded next checks."
    )


def evaluate_route(bundle: dict[str, Any], policy: dict[str, Any], vertex_degraded: bool) -> dict[str, Any]:
    severity = (bundle.get("trigger_severity") or "").lower()
    bundle_json = stable_json(bundle)
    out = {
        "decision": "REJECT",
        "reason_code": "REJECTED",
        "route": "reject",
        "severity": severity,
    }

    if policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if policy["mode"] == "DISABLED":
        out["reason_code"] = "MODE_DISABLED"
        return out
    if severity not in policy["allow_severities"]:
        out["reason_code"] = "SEVERITY_NOT_ALLOWED"
        return out
    if len(bundle_json.encode("utf-8")) > policy["max_bundle_bytes"]:
        out["reason_code"] = "BUNDLE_TOO_LARGE"
        return out

    if policy["mode"] == "VERTEX_ONLY":
        out["decision"] = "ROUTE_VERTEX"
        out["reason_code"] = "MODE_VERTEX_ONLY"
        out["route"] = "vertex"
        return out
    if policy["mode"] == "LOCAL_ONLY":
        out["decision"] = "ROUTE_LOCAL"
        out["reason_code"] = "MODE_LOCAL_ONLY"
        out["route"] = "local"
        return out

    if vertex_degraded:
        out["decision"] = "ROUTE_LOCAL"
        out["reason_code"] = "VERTEX_DEGRADED"
        out["route"] = "local"
        return out

    if policy["require_vertex_degraded_for_local"] == 1:
        out["decision"] = "ROUTE_VERTEX"
        out["reason_code"] = "PRIMARY_VERTEX"
        out["route"] = "vertex"
        return out

    out["decision"] = "ROUTE_VERTEX"
    out["reason_code"] = "PRIMARY_VERTEX"
    out["route"] = "vertex"
    return out


def build_vertex_request(bundle: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "request_id": (bundle.get("bundle_id") or ""),
        "task_family": "route_incident_rca_mirror_rca_winner_apply_apply_governance_rca",
        "task_type": "route_incident_rca_mirror_rca_winner_apply_apply_governance_rca",
        "severity": (bundle.get("trigger_severity") or "warning"),
        "source": APP_NAME,
        "prompt": build_vertex_prompt(bundle),
        "bundle_json": stable_json(bundle),
        "ts_ms": str(now_ms()),
    }


def build_local_request(bundle: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "request_id": (bundle.get("bundle_id") or ""),
        "task_family": "route_incident_rca_mirror_rca_winner_apply_apply_governance_rca",
        "task_type": "vertex_unavailable_fallback",
        "severity": (bundle.get("trigger_severity") or "warning"),
        "source": APP_NAME,
        "vertex_unavailable": "1",
        "force_local": "1",
        "prompt": build_local_prompt(bundle),
        "input_json": stable_json(bundle),
        "ts_ms": str(now_ms()),
    }


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> dict[str, Any]:
    raw = await r.hgetall(key)
    return as_dict(raw)


async def persist_if_configured(
    db_url: str,
    bundle: dict[str, Any],
    decision: dict[str, Any],
    destination_stream: str,
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_governance_rca_bridge_decisions (
                    bundle_id,
                    ts_ms,
                    trigger_type,
                    trigger_severity,
                    decision,
                    reason_code,
                    route,
                    destination_stream,
                    bundle_json
                ) VALUES (
                    %(bundle_id)s,
                    %(ts_ms)s,
                    %(trigger_type)s,
                    %(trigger_severity)s,
                    %(decision)s,
                    %(reason_code)s,
                    %(route)s,
                    %(destination_stream)s,
                    %(bundle_json)s
                )
                """,
                {
                    "bundle_id": bundle.get("bundle_id", ""),
                    "ts_ms": now_ms(),
                    "trigger_type": bundle.get("trigger_type", ""),
                    "trigger_severity": bundle.get("trigger_severity", ""),
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "route": decision["route"],
                    "destination_stream": destination_stream,
                    "bundle_json": json.dumps(bundle),
                }
            )
            conn.commit()


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    await ensure_group(r, INPUT_STREAM, GROUP)
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {INPUT_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                decision_label = "REJECT"
                try:
                    row = as_dict(payload)
                    bundle = maybe_json(row.get("bundle_json"), {})
                    if not isinstance(bundle, dict):
                        bundle = {}
                    if not bundle and row.get("bundle_id"):
                        bundle = {
                            "bundle_id": row.get("bundle_id", ""),
                            "trigger_type": row.get("trigger_type", ""),
                            "trigger_severity": row.get("trigger_severity", ""),
                        }
                    policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    try:
                        exec_kill = await r.get(RK.EXEC_KILL_SWITCH)
                        if exec_kill and exec_kill.decode().strip() == '1':
                            policy['kill_switch'] = 1
                    except Exception: pass
                    vertex_health = await read_hash(r, VERTEX_HEALTH_HASH)
                    vertex_degraded = vertex_degraded_from_hash(vertex_health)
                    decision = evaluate_route(bundle, policy, vertex_degraded)
                    decision_label = decision["decision"]
                    destination_stream = ""

                    if decision["decision"] == "ROUTE_VERTEX":
                        destination_stream = VERTEX_OUTPUT_STREAM
                        await r.xadd(
                            VERTEX_OUTPUT_STREAM,
                            build_vertex_request(bundle),
                            maxlen=MAXLEN,
                            approximate=True,
                        )
                    elif decision["decision"] == "ROUTE_LOCAL":
                        destination_stream = LOCAL_OUTPUT_STREAM
                        await r.xadd(
                            LOCAL_OUTPUT_STREAM,
                            build_local_request(bundle),
                            maxlen=MAXLEN,
                            approximate=True,
                        )

                    await persist_if_configured(db_url, bundle, decision, destination_stream)

                    out = {
                        "schema_version": 1,
                        "bundle_id": (bundle.get("bundle_id") or ""),
                        "trigger_type": (bundle.get("trigger_type") or ""),
                        "trigger_severity": (bundle.get("trigger_severity") or ""),
                        "decision": decision["decision"],
                        "reason_code": decision["reason_code"],
                        "route": decision["route"],
                        "destination_stream": destination_stream,
                        "vertex_degraded": "1" if vertex_degraded else "0",
                        "ts_ms": str(now_ms()),
                    }
                    await r.xadd(DECISIONS_STREAM, out, maxlen=MAXLEN, approximate=True)
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_DECIDED",
                            **out,
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "bundle_id": (bundle.get("bundle_id") or ""),
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "route": decision["route"],
                            "destination_stream": destination_stream,
                            "vertex_degraded": "1" if vertex_degraded else "0",
                            "ts_ms": str(now_ms()),
                        }
                    )
                    if ROUTED:
                        ROUTED.labels(route=decision["route"], severity=decision["severity"] or "unknown").inc()
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_FAILED",
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, decision=decision_label).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
