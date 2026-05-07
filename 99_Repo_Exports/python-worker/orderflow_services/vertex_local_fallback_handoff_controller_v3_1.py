from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
from core.redis_keys import RedisKeyPrefixes as RK
import time
from typing import Any, Dict, Tuple

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


APP_NAME = "vertex_local_fallback_handoff_controller_v3_1"
INPUT_STREAM = os.getenv(
    "ML_VERTEX_LOCAL_HANDOFF_INPUT_STREAM",
    "stream:ml:vertex_local_handoff_requests",
)
LOCAL_OUTPUT_STREAM = os.getenv(
    "ML_LOCAL_FALLBACK_REQUESTS_STREAM",
    "stream:ml:local_fallback_requests",
)
DECISIONS_STREAM = os.getenv(
    "ML_VERTEX_LOCAL_HANDOFF_DECISIONS_STREAM",
    "stream:ml:vertex_local_handoff_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_VERTEX_LOCAL_HANDOFF_AUDIT_STREAM",
    "stream:ml:vertex_local_handoff_audit",
)
LAST_HASH = os.getenv(
    "ML_VERTEX_LOCAL_HANDOFF_LAST_HASH",
    "metrics:ml:vertex_local_handoff:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_VERTEX_LOCAL_HANDOFF_GLOBAL_POLICY_KEY",
    "cfg:ml:vertex_local_handoff:global",
)
VERTEX_HEALTH_HASH = os.getenv(
    "ML_VERTEX_LOCAL_HANDOFF_VERTEX_HEALTH_HASH",
    "metrics:ml:vertex_health:last",
)
GROUP = os.getenv("ML_VERTEX_LOCAL_HANDOFF_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_VERTEX_LOCAL_HANDOFF_PORT", "9917"))
MAXLEN = int(os.getenv("ML_VERTEX_LOCAL_HANDOFF_MAXLEN", "20000"))

DEFAULT_MODE = os.getenv("ML_VERTEX_LOCAL_HANDOFF_MODE", "AUTO").upper()
DEFAULT_REQUIRE_VERTEX_DEGRADED = int(os.getenv("ML_VERTEX_LOCAL_HANDOFF_REQUIRE_VERTEX_DEGRADED", "1"))

PRIMARY_STREAM_BY_FAMILY = {
    "routing_incident_rca": os.getenv(
        "ML_VERTEX_LOCAL_HANDOFF_ROUTING_INCIDENT_RCA_STREAM",
        "stream:ml:operator_rca_routing_rca_requests",
    ),
    "route_incident_rca": os.getenv(
        "ML_VERTEX_LOCAL_HANDOFF_ROUTE_INCIDENT_RCA_STREAM",
        "stream:ml:operator_routing_incident_rca_route_rca_requests",
    ),
}

LOCAL_TASK_BY_FAMILY = {
    "routing_incident_rca": "vertex_unavailable_fallback",
    "route_incident_rca": "vertex_unavailable_fallback",
    "local_report": "local_report",
    "offline_debug": "offline_debug",
    "emergency_summarize": "emergency_summarize",
}

SUPPORTED_FAMILIES = set(LOCAL_TASK_BY_FAMILY.keys())
LOCAL_ONLY_FAMILIES = {"local_report", "offline_debug", "emergency_summarize"}


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_vertex_local_handoff_runs_total",
    "Vertex local fallback handoff runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_vertex_local_handoff_latency_seconds",
    "Vertex local fallback handoff latency seconds",
)
UP = _gauge(
    "ml_vertex_local_handoff_up",
    "Vertex local fallback handoff up",
)
LAST_RUN_TS = _gauge(
    "ml_vertex_local_handoff_last_run_ts_seconds",
    "Vertex local fallback handoff last run timestamp",
)
ROUTED = _counter(
    "ml_vertex_local_handoff_routed_total",
    "Vertex local handoff routed requests",
    ("route", "task_family"),
)


def now_ms() -> int:
    return get_ny_time_millis()


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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


def maybe_json(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    allow_families = maybe_json(raw.get("allow_families_json"), sorted(SUPPORTED_FAMILIES))
    if not isinstance(allow_families, list):
        allow_families = sorted(SUPPORTED_FAMILIES)
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "mode": str(raw.get("mode") or DEFAULT_MODE).upper(),
        "require_vertex_degraded": parse_int(raw.get("require_vertex_degraded"), DEFAULT_REQUIRE_VERTEX_DEGRADED),
        "allow_families": {str(x) for x in allow_families},
    }


def vertex_degraded_from_hash(raw: Dict[str, Any]) -> bool:
    status = str(raw.get("status") or "").lower()
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


def build_local_prompt(task_family: str, row: Dict[str, Any]) -> str:
    payload_json = row.get("payload_json", "{}")
    source = row.get("source", "unknown")
    severity = row.get("severity", "info")
    compact_hash = row.get("compact_hash", "")
    if task_family == "local_report":
        return (
            "Prepare a concise local operational report.\n"
            f"source={source}\nseverity={severity}\ncompact_hash={compact_hash}\n"
            f"payload={payload_json}"
        )
    if task_family == "offline_debug":
        return (
            "Analyze this snapshot for offline debugging. Keep bounded hypotheses and checks only.\n"
            f"source={source}\nseverity={severity}\ncompact_hash={compact_hash}\n"
            f"payload={payload_json}"
        )
    return (
        "Vertex primary path is unavailable or degraded. Produce an emergency bounded summary only.\n"
        f"task_family={task_family}\nsource={source}\nseverity={severity}\ncompact_hash={compact_hash}\n"
        f"payload={payload_json}"
    )


def evaluate_handoff(row: Dict[str, Any], policy: Dict[str, Any], vertex_degraded: bool) -> Dict[str, Any]:
    family = str(row.get("task_family") or "")
    force_local = parse_int(row.get("force_local"), 0)
    vertex_unavailable = parse_int(row.get("vertex_unavailable"), 0)

    out = {
        "decision": "REJECT",
        "reason_code": "REJECTED",
        "task_family": family,
        "route": "reject",
    }

    if policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if family not in SUPPORTED_FAMILIES:
        out["reason_code"] = "TASK_FAMILY_NOT_SUPPORTED"
        return out
    if family not in policy["allow_families"]:
        out["reason_code"] = "TASK_FAMILY_NOT_ALLOWED"
        return out

    mode = policy["mode"]
    if mode == "DISABLED":
        out["reason_code"] = "MODE_DISABLED"
        return out

    if family in LOCAL_ONLY_FAMILIES:
        out["decision"] = "ROUTE_LOCAL"
        out["reason_code"] = "LOCAL_ONLY_TASK"
        out["route"] = "local"
        return out

    if force_local == 1:
        out["decision"] = "ROUTE_LOCAL"
        out["reason_code"] = "FORCED_LOCAL"
        out["route"] = "local"
        return out

    if mode == "LOCAL_ONLY":
        out["decision"] = "ROUTE_LOCAL"
        out["reason_code"] = "MODE_LOCAL_ONLY"
        out["route"] = "local"
        return out

    if mode == "FALLBACK_ONLY":
        if policy["require_vertex_degraded"] == 1 and not (vertex_degraded or vertex_unavailable == 1):
            out["reason_code"] = "VERTEX_NOT_DEGRADED"
            return out
        out["decision"] = "ROUTE_LOCAL"
        out["reason_code"] = "VERTEX_FALLBACK"
        out["route"] = "local"
        return out

    # AUTO
    if vertex_degraded or vertex_unavailable == 1:
        out["decision"] = "ROUTE_LOCAL"
        out["reason_code"] = "VERTEX_DEGRADED"
        out["route"] = "local"
        return out

    out["decision"] = "ROUTE_VERTEX"
    out["reason_code"] = "PRIMARY_VERTEX"
    out["route"] = "vertex"
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    raw = await r.hgetall(key)
    return as_dict(raw)


async def persist_if_configured(db_url: str, row: Dict[str, Any], decision: Dict[str, Any], destination_stream: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_vertex_local_fallback_handoff_decisions (
                    request_id,
                    ts_ms,
                    task_family,
                    decision,
                    reason_code,
                    route,
                    destination_stream,
                    payload_json
                ) VALUES (
                    %(request_id)s,
                    %(ts_ms)s,
                    %(task_family)s,
                    %(decision)s,
                    %(reason_code)s,
                    %(route)s,
                    %(destination_stream)s,
                    %(payload_json)s
                )
                """,
                {
                    "request_id": row.get("request_id", ""),
                    "ts_ms": now_ms(),
                    "task_family": row.get("task_family", ""),
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "route": decision["route"],
                    "destination_stream": destination_stream,
                    "payload_json": json.dumps(row),
                }
            )
            conn.commit()


async def route_local(r: Any, row: Dict[str, Any], family: str) -> None:
    local_task_type = LOCAL_TASK_BY_FAMILY[family]
    payload = {
        "schema_version": 1,
        "request_id": row.get("request_id", ""),
        "task_type": local_task_type,
        "severity": row.get("severity", "info"),
        "source": row.get("source", "vertex_local_handoff_v3_1"),
        "vertex_unavailable": str(max(parse_int(row.get("vertex_unavailable"), 0), 1 if family in {"routing_incident_rca", "route_incident_rca"} else 0)),
        "force_local": "1",
        "prompt": build_local_prompt(family, row),
        "input_json": row.get("payload_json", "{}"),
        "ts_ms": str(now_ms()),
    }
    await r.xadd(LOCAL_OUTPUT_STREAM, payload, maxlen=MAXLEN, approximate=True)


async def route_vertex(r: Any, row: Dict[str, Any], family: str) -> str:
    dst = PRIMARY_STREAM_BY_FAMILY[family]
    payload = {
        "schema_version": 1,
        "incident_id": row.get("incident_id", ""),
        "task_type": row.get("task_type", ""),
        "compact_hash": row.get("compact_hash", ""),
        "payload_json": row.get("payload_json", "{}"),
        "severity": row.get("severity", "info"),
        "source": row.get("source", "vertex_local_handoff_v3_1"),
        "ts_ms": str(now_ms()),
    }
    await r.xadd(dst, payload, maxlen=MAXLEN, approximate=True)
    return dst


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
                    family = str(row.get("task_family") or "")
                    policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    try:
                        exec_kill = await r.get(RK.EXEC_KILL_SWITCH)
                        if exec_kill and exec_kill.decode().strip() == '1':
                            policy['kill_switch'] = 1
                    except: pass
                    vertex_health = await read_hash(r, VERTEX_HEALTH_HASH)
                    vertex_degraded = vertex_degraded_from_hash(vertex_health)
                    decision = evaluate_handoff(row, policy, vertex_degraded)
                    decision_label = decision["decision"]
                    destination_stream = ""

                    if decision["decision"] == "ROUTE_LOCAL":
                        await route_local(r, row, family)
                        destination_stream = LOCAL_OUTPUT_STREAM
                    elif decision["decision"] == "ROUTE_VERTEX":
                        destination_stream = await route_vertex(r, row, family)

                    await persist_if_configured(db_url, row, decision, destination_stream)

                    out = {
                        "schema_version": 1,
                        "request_id": row.get("request_id", ""),
                        "task_family": family,
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
                            "event_type": "VERTEX_LOCAL_HANDOFF_DECIDED",
                            **out,
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "request_id": row.get("request_id", ""),
                            "task_family": family,
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "route": decision["route"],
                            "destination_stream": destination_stream,
                            "ts_ms": str(now_ms()),
                        }
                    )
                    if ROUTED:
                        ROUTED.labels(route=decision["route"], task_family=family).inc()
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "VERTEX_LOCAL_HANDOFF_FAILED",
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
    import asyncio
    asyncio.run(main())
