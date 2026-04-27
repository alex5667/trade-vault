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


APP_NAME = "local_report_handoff_rewire_adapter_v3_2"
INPUT_STREAM = os.getenv(
    "ML_LOCAL_REPORT_HANDOFF_SOURCE_STREAM",
    "stream:ml:local_report_handoff_source",
)
OUTPUT_STREAM = os.getenv(
    "ML_VERTEX_LOCAL_HANDOFF_INPUT_STREAM",
    "stream:ml:vertex_local_handoff_requests",
)
DECISIONS_STREAM = os.getenv(
    "ML_LOCAL_REPORT_HANDOFF_REWIRE_DECISIONS_STREAM",
    "stream:ml:local_report_handoff_rewire_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_LOCAL_REPORT_HANDOFF_REWIRE_AUDIT_STREAM",
    "stream:ml:local_report_handoff_rewire_audit",
)
LAST_HASH = os.getenv(
    "ML_LOCAL_REPORT_HANDOFF_REWIRE_LAST_HASH",
    "metrics:ml:local_report_handoff_rewire:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_LOCAL_REPORT_HANDOFF_REWIRE_GLOBAL_POLICY_KEY",
    "cfg:ml:local_report_handoff_rewire:global",
)
GROUP = os.getenv("ML_LOCAL_REPORT_HANDOFF_REWIRE_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_LOCAL_REPORT_HANDOFF_REWIRE_PORT", "9918"))
MAXLEN = int(os.getenv("ML_LOCAL_REPORT_HANDOFF_REWIRE_MAXLEN", "20000"))

DEFAULT_ENABLED = int(os.getenv("ML_LOCAL_REPORT_HANDOFF_REWIRE_ENABLED", "1"))
DEFAULT_MODE = os.getenv("ML_LOCAL_REPORT_HANDOFF_REWIRE_MODE", "ENABLED").upper()
DEFAULT_MAX_PAYLOAD_BYTES = int(os.getenv("ML_LOCAL_REPORT_HANDOFF_REWIRE_MAX_PAYLOAD_BYTES", "65536"))
DEFAULT_MAX_PROMPT_CHARS = int(os.getenv("ML_LOCAL_REPORT_HANDOFF_REWIRE_MAX_PROMPT_CHARS", "12000"))

# Database URL: project convention uses ANALYTICS_DB_DSN via PgBouncer
DB_URL = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_local_report_handoff_rewire_runs_total",
    "Local report handoff rewire runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_local_report_handoff_rewire_latency_seconds",
    "Local report handoff rewire latency seconds",
)
UP = _gauge(
    "ml_local_report_handoff_rewire_up",
    "Local report handoff rewire adapter up",
)
LAST_RUN_TS = _gauge(
    "ml_local_report_handoff_rewire_last_run_ts_seconds",
    "Local report handoff rewire adapter last run timestamp",
)
ROUTED = _counter(
    "ml_local_report_handoff_rewire_routed_total",
    "Local report handoff rewire routed rows",
    ("decision",),
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
    return {
        "enabled": parse_int(raw.get("enabled"), DEFAULT_ENABLED),
        "mode": str(raw.get("mode") or DEFAULT_MODE).upper(),
        "max_payload_bytes": parse_int(raw.get("max_payload_bytes"), DEFAULT_MAX_PAYLOAD_BYTES),
        "max_prompt_chars": parse_int(raw.get("max_prompt_chars"), DEFAULT_MAX_PROMPT_CHARS),
        "force_local": parse_int(raw.get("force_local"), 0),
    }


def build_payload_json(row: Dict[str, Any]) -> str:
    payload_json = row.get("payload_json")
    if payload_json:
        return str(payload_json)
    prompt = str(row.get("prompt") or "")
    title = str(row.get("title") or "")
    context = maybe_json(row.get("context_json"), {})
    if prompt or title or context:
        return stable_json({
            "title": title,
            "prompt": prompt,
            "context": context,
        })
    return stable_json(row)


def evaluate_row(row: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
    request_id = str(row.get("request_id") or "")
    prompt = str(row.get("prompt") or "")
    payload_json = build_payload_json(row)

    out = {
        "decision": "REJECT",
        "reason_code": "REJECTED",
        "request_id": request_id,
    }
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if policy["mode"] == "DISABLED":
        out["reason_code"] = "MODE_DISABLED"
        return out
    if not request_id:
        out["reason_code"] = "REQUEST_ID_MISSING"
        return out
    if len(prompt) > policy["max_prompt_chars"]:
        out["reason_code"] = "PROMPT_TOO_LARGE"
        return out
    if len(payload_json.encode("utf-8")) > policy["max_payload_bytes"]:
        out["reason_code"] = "PAYLOAD_TOO_LARGE"
        return out
    out["decision"] = "ROUTE_HANDOFF"
    out["reason_code"] = "OK"
    return out


def build_handoff_row(row: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
    severity = str(row.get("severity") or "info")
    return {
        "schema_version": 1,
        "request_id": str(row.get("request_id") or ""),
        "task_family": "local_report",
        "incident_id": str(row.get("incident_id") or ""),
        "task_type": "local_report",
        "severity": severity,
        "source": "local_report_handoff_rewire_v3_2",
        "payload_json": build_payload_json(row),
        "compact_hash": str(row.get("compact_hash") or ""),
        "vertex_unavailable": str(parse_int(row.get("vertex_unavailable"), 0)),
        "force_local": str(max(parse_int(row.get("force_local"), 0), policy["force_local"])),
        "ts_ms": str(now_ms()),
    }


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_policy(r: Any) -> Dict[str, Any]:
    raw = await r.hgetall(GLOBAL_POLICY_KEY)
    return policy_from_hash(as_dict(raw))


async def persist_if_configured(db_url: str, row: Dict[str, Any], decision: Dict[str, Any], handoff_row: Dict[str, Any] | None) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_local_report_handoff_rewire_decisions (
                    request_id,
                    ts_ms,
                    decision,
                    reason_code,
                    source_stream,
                    output_stream,
                    handoff_payload_json,
                    original_payload_json
                ) VALUES (
                    %(request_id)s,
                    %(ts_ms)s,
                    %(decision)s,
                    %(reason_code)s,
                    %(source_stream)s,
                    %(output_stream)s,
                    %(handoff_payload_json)s,
                    %(original_payload_json)s
                )
                """,
                {
                    "request_id": row.get("request_id", ""),
                    "ts_ms": now_ms(),
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "source_stream": INPUT_STREAM,
                    "output_stream": OUTPUT_STREAM if handoff_row else "",
                    "handoff_payload_json": json.dumps(handoff_row or {}),
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
                    handoff_row = None

                    if decision["decision"] == "ROUTE_HANDOFF":
                        handoff_row = build_handoff_row(row, policy)
                        await r.xadd(OUTPUT_STREAM, handoff_row, maxlen=MAXLEN, approximate=True)

                    await persist_if_configured(db_url, row, decision, handoff_row)

                    decision_payload = {
                        "schema_version": 1,
                        "request_id": str(row.get("request_id") or ""),
                        "decision": decision["decision"],
                        "reason_code": decision["reason_code"],
                        "source_stream": INPUT_STREAM,
                        "output_stream": OUTPUT_STREAM if handoff_row else "",
                        "ts_ms": str(now_ms()),
                    }
                    await r.xadd(DECISIONS_STREAM, decision_payload, maxlen=MAXLEN, approximate=True)
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "LOCAL_REPORT_HANDOFF_REWIRE_DECIDED",
                            **decision_payload,
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "request_id": str(row.get("request_id") or ""),
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "source_stream": INPUT_STREAM,
                            "output_stream": OUTPUT_STREAM if handoff_row else "",
                            "ts_ms": str(now_ms()),
                        },
                    )
                    if ROUTED:
                        ROUTED.labels(decision=decision["decision"]).inc()
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "LOCAL_REPORT_HANDOFF_REWIRE_FAILED",
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
