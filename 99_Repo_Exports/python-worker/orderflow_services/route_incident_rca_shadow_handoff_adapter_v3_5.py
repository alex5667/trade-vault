from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
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


APP_NAME = "route_incident_rca_shadow_handoff_adapter_v3_5"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_SOURCE_STREAM",
    "stream:ml:route_incident_rca_handoff_shadow_source",
)
HANDOFF_SHADOW_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_HANDOFF_SHADOW_STREAM",
    "stream:ml:vertex_local_handoff_shadow_requests",
)
LEGACY_SHADOW_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_LEGACY_SHADOW_STREAM",
    "stream:ml:route_incident_rca_legacy_shadow_requests",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_shadow_handoff_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_AUDIT_STREAM",
    "stream:ml:route_incident_rca_shadow_handoff_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_LAST_HASH",
    "metrics:ml:route_incident_rca_shadow_handoff:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_shadow_handoff:global",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_PORT", "9921"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_MAXLEN", "20000"))

DEFAULT_ENABLED = int(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_ENABLED", "1"))
DEFAULT_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_MODE", "AUDIT_ONLY").upper()
DEFAULT_MAX_PAYLOAD_BYTES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_MAX_PAYLOAD_BYTES", "131072"))
DEFAULT_MAX_PROMPT_CHARS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_MAX_PROMPT_CHARS", "16000"))

ALLOWED_MODES = {"DISABLED", "AUDIT_ONLY", "MIRROR", "HANDOFF_ONLY", "LEGACY_ONLY"}

# Database URL: project convention uses ANALYTICS_DB_DSN via PgBouncer
DB_URL = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_shadow_handoff_runs_total",
    "Route incident RCA shadow handoff runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_shadow_handoff_latency_seconds",
    "Route incident RCA shadow handoff latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_shadow_handoff_up",
    "Route incident RCA shadow handoff adapter up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_shadow_handoff_last_run_ts_seconds",
    "Route incident RCA shadow handoff adapter last run timestamp",
)
ROUTED = _counter(
    "ml_route_incident_rca_shadow_handoff_routed_total",
    "Route incident RCA shadow handoff routed rows",
    ("decision", "mode"),
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


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(raw.get("mode") or DEFAULT_MODE).upper()
    if mode not in ALLOWED_MODES:
        mode = DEFAULT_MODE
    return {
        "enabled": parse_int(raw.get("enabled"), DEFAULT_ENABLED),
        "mode": mode,
        "max_payload_bytes": parse_int(raw.get("max_payload_bytes"), DEFAULT_MAX_PAYLOAD_BYTES),
        "max_prompt_chars": parse_int(raw.get("max_prompt_chars"), DEFAULT_MAX_PROMPT_CHARS),
    },


def build_payload_json(row: Dict[str, Any]) -> str:
    payload_json = row.get("payload_json")
    if payload_json:
        return str(payload_json)
    prompt = str(row.get("prompt") or "")
    summary = str(row.get("summary") or "")
    primary_reason_codes = maybe_json(row.get("primary_reason_codes_json"), [])
    context = maybe_json(row.get("context_json"), {})
    if prompt or summary or primary_reason_codes or context:
        return stable_json({
            "prompt": prompt,
            "summary": summary,
            "primary_reason_codes": primary_reason_codes,
            "context": context,
        })
    return stable_json(row)


def evaluate_row(row: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
    request_id = str(row.get("request_id") or "")
    incident_id = str(row.get("incident_id") or "")
    prompt = str(row.get("prompt") or "")
    payload_json = build_payload_json(row)

    out = {
        "decision": "REJECT",
        "reason_code": "REJECTED",
        "request_id": request_id,
        "incident_id": incident_id,
        "mode": policy["mode"],
    },
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if policy["mode"] == "DISABLED":
        out["reason_code"] = "MODE_DISABLED"
        return out
    if not incident_id and not request_id:
        out["reason_code"] = "IDENTIFIER_MISSING"
        return out
    if len(prompt) > policy["max_prompt_chars"]:
        out["reason_code"] = "PROMPT_TOO_LARGE"
        return out
    if len(payload_json.encode("utf-8")) > policy["max_payload_bytes"]:
        out["reason_code"] = "PAYLOAD_TOO_LARGE"
        return out
    out["decision"] = "ROUTE_SHADOW"
    out["reason_code"] = "OK"
    return out


def build_handoff_shadow_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "request_id": str(row.get("request_id") or row.get("incident_id") or ""),
        "task_family": "route_incident_rca",
        "incident_id": str(row.get("incident_id") or ""),
        "task_type": str(row.get("task_type") or "route_incident_rca"),
        "severity": str(row.get("severity") or "warning"),
        "source": "route_incident_rca_shadow_handoff_v3_5",
        "payload_json": build_payload_json(row),
        "compact_hash": str(row.get("compact_hash") or ""),
        "vertex_unavailable": str(parse_int(row.get("vertex_unavailable"), 0)),
        "force_local": str(parse_int(row.get("force_local"), 0)),
        "shadow_mode": "1",
        "ts_ms": str(now_ms()),
    },


def build_legacy_shadow_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "incident_id": str(row.get("incident_id") or ""),
        "task_type": str(row.get("task_type") or "route_incident_rca"),
        "compact_hash": str(row.get("compact_hash") or ""),
        "payload_json": build_payload_json(row),
        "severity": str(row.get("severity") or "warning"),
        "source": "route_incident_rca_shadow_handoff_v3_5",
        "shadow_mode": "1",
        "ts_ms": str(now_ms()),
    },


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_policy(r: Any) -> Dict[str, Any]:
    raw = await r.hgetall(GLOBAL_POLICY_KEY)
    return policy_from_hash(as_dict(raw))


async def persist_if_configured(
    db_url: str,
    row: Dict[str, Any],
    decision: Dict[str, Any],
    handoff_shadow_row: Dict[str, Any] | None,
    legacy_shadow_row: Dict[str, Any] | None,
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """,
                INSERT INTO llm_route_incident_rca_shadow_handoff_decisions (
                    request_id,
                    incident_id,
                    ts_ms,
                    decision,
                    reason_code,
                    mode,
                    source_stream,
                    handoff_shadow_stream,
                    legacy_shadow_stream,
                    handoff_payload_json,
                    legacy_payload_json,
                    original_payload_json
                ) VALUES (
                    %(request_id)s,
                    %(incident_id)s,
                    %(ts_ms)s,
                    %(decision)s,
                    %(reason_code)s,
                    %(mode)s,
                    %(source_stream)s,
                    %(handoff_shadow_stream)s,
                    %(legacy_shadow_stream)s,
                    %(handoff_payload_json)s,
                    %(legacy_payload_json)s,
                    %(original_payload_json)s
                )
                """,
                {
                    "request_id": row.get("request_id", ""),
                    "incident_id": row.get("incident_id", ""),
                    "ts_ms": now_ms(),
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "mode": decision["mode"],
                    "source_stream": INPUT_STREAM,
                    "handoff_shadow_stream": HANDOFF_SHADOW_STREAM if handoff_shadow_row else "",
                    "legacy_shadow_stream": LEGACY_SHADOW_STREAM if legacy_shadow_row else "",
                    "handoff_payload_json": json.dumps(handoff_shadow_row or {}),
                    "legacy_payload_json": json.dumps(legacy_shadow_row or {}),
                    "original_payload_json": json.dumps(row),
                },
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
    db_url = DB_URL

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
                    policy = await read_policy(r)
                    decision = evaluate_row(row, policy)
                    decision_label = decision["decision"]
                    handoff_shadow_row = None
                    legacy_shadow_row = None

                    if decision["decision"] == "ROUTE_SHADOW":
                        if policy["mode"] in {"MIRROR", "HANDOFF_ONLY"}:
                            handoff_shadow_row = build_handoff_shadow_row(row)
                            await r.xadd(HANDOFF_SHADOW_STREAM, handoff_shadow_row, maxlen=MAXLEN, approximate=True)
                        if policy["mode"] in {"MIRROR", "LEGACY_ONLY"}:
                            legacy_shadow_row = build_legacy_shadow_row(row)
                            await r.xadd(LEGACY_SHADOW_STREAM, legacy_shadow_row, maxlen=MAXLEN, approximate=True)

                    await persist_if_configured(db_url, row, decision, handoff_shadow_row, legacy_shadow_row)

                    decision_payload = {
                        "schema_version": 1,
                        "request_id": str(row.get("request_id") or ""),
                        "incident_id": str(row.get("incident_id") or ""),
                        "decision": decision["decision"],
                        "reason_code": decision["reason_code"],
                        "mode": decision["mode"],
                        "source_stream": INPUT_STREAM,
                        "handoff_shadow_stream": HANDOFF_SHADOW_STREAM if handoff_shadow_row else "",
                        "legacy_shadow_stream": LEGACY_SHADOW_STREAM if legacy_shadow_row else "",
                        "ts_ms": str(now_ms()),
                    },
                    await r.xadd(DECISIONS_STREAM, decision_payload, maxlen=MAXLEN, approximate=True)
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_SHADOW_HANDOFF_DECIDED",
                            **decision_payload,
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "request_id": str(row.get("request_id") or ""),
                            "incident_id": str(row.get("incident_id") or ""),
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "mode": decision["mode"],
                            "source_stream": INPUT_STREAM,
                            "handoff_shadow_stream": HANDOFF_SHADOW_STREAM if handoff_shadow_row else "",
                            "legacy_shadow_stream": LEGACY_SHADOW_STREAM if legacy_shadow_row else "",
                            "ts_ms": str(now_ms()),
                        },
                    )
                    if ROUTED:
                        ROUTED.labels(decision=decision["decision"], mode=decision["mode"]).inc()
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_SHADOW_HANDOFF_FAILED",
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
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
