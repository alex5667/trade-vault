from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, Tuple

from orderflow_services.providers.ollama_gpu_lock import (
    OllamaGpuLock,
    OllamaGpuLockTimeout,
)

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

from orderflow_services.providers.ollama_local_fallback_provider_v3_0 import (
    OllamaLocalFallbackProviderV30,
)


APP_NAME = "local_fallback_plane_gateway_v3_0"
INPUT_STREAM = os.getenv(
    "ML_LOCAL_FALLBACK_REQUESTS_STREAM",
    "stream:ml:local_fallback_requests",
)
RESULTS_STREAM = os.getenv(
    "ML_LOCAL_FALLBACK_RESULTS_STREAM",
    "stream:ml:local_fallback_results",
)
REJECTIONS_STREAM = os.getenv(
    "ML_LOCAL_FALLBACK_REJECTIONS_STREAM",
    "stream:ml:local_fallback_rejections",
)
AUDIT_STREAM = os.getenv(
    "ML_LOCAL_FALLBACK_AUDIT_STREAM",
    "stream:ml:local_fallback_audit",
)
LAST_HASH = os.getenv(
    "ML_LOCAL_FALLBACK_LAST_HASH",
    "metrics:ml:local_fallback:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_LOCAL_FALLBACK_GLOBAL_POLICY_KEY",
    "cfg:ml:local_fallback:global",
)
GROUP = os.getenv("ML_LOCAL_FALLBACK_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_LOCAL_FALLBACK_PORT", "9916"))
MAXLEN = int(os.getenv("ML_LOCAL_FALLBACK_MAXLEN", "20000"))

DEFAULT_MODE = os.getenv("ML_LOCAL_FALLBACK_MODE", "FALLBACK_ONLY").upper()
DEFAULT_MAX_PROMPT_CHARS = int(os.getenv("ML_LOCAL_FALLBACK_MAX_PROMPT_CHARS", "12000"))
DEFAULT_MAX_INPUT_JSON_BYTES = int(os.getenv("ML_LOCAL_FALLBACK_MAX_INPUT_JSON_BYTES", "65536"))
DEFAULT_MAX_SCHEMA_BYTES = int(os.getenv("ML_LOCAL_FALLBACK_MAX_SCHEMA_BYTES", "16384"))
DEFAULT_REQUIRE_VERTEX_DEGRADED = int(os.getenv("ML_LOCAL_FALLBACK_REQUIRE_VERTEX_DEGRADED", "1"))

ALLOWED_TASKS = {
    "emergency_summarize",
    "offline_debug",
    "local_report",
    "vertex_unavailable_fallback",
},


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_local_fallback_runs_total",
    "Local fallback plane runs",
    ("status", "task_type"),
)
LAT = _hist(
    "ml_local_fallback_latency_seconds",
    "Local fallback plane latency seconds",
)
UP = _gauge(
    "ml_local_fallback_up",
    "Local fallback plane up",
)
LAST_RUN_TS = _gauge(
    "ml_local_fallback_last_run_ts_seconds",
    "Local fallback plane last run timestamp",
)
ACCEPTED = _counter(
    "ml_local_fallback_accepted_total",
    "Local fallback accepted requests",
    ("task_type",),
)
REJECTED = _counter(
    "ml_local_fallback_rejected_total",
    "Local fallback rejected requests",
    ("reason_code", "task_type"),
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


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


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
    allowlist = maybe_json(raw.get("task_allowlist_json"), sorted(ALLOWED_TASKS))
    if not isinstance(allowlist, list):
        allowlist = sorted(ALLOWED_TASKS)
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "mode": str(raw.get("mode") or DEFAULT_MODE).upper(),
        "max_prompt_chars": parse_int(raw.get("max_prompt_chars"), DEFAULT_MAX_PROMPT_CHARS),
        "max_input_json_bytes": parse_int(raw.get("max_input_json_bytes"), DEFAULT_MAX_INPUT_JSON_BYTES),
        "max_schema_bytes": parse_int(raw.get("max_schema_bytes"), DEFAULT_MAX_SCHEMA_BYTES),
        "require_vertex_degraded": parse_int(raw.get("require_vertex_degraded"), DEFAULT_REQUIRE_VERTEX_DEGRADED),
        "task_allowlist": {str(x) for x in allowlist},
    }


def build_prompt(row: Dict[str, Any]) -> str:
    task_type = str(row.get("task_type") or "")
    prompt = str(row.get("prompt") or "")
    input_json = maybe_json(row.get("input_json"), {})
    if not prompt and input_json:
        prompt = stable_json(input_json)
    if task_type == "local_report":
        return f"Build a concise local operational report.\n\nInput:\n{prompt}"
    if task_type == "offline_debug":
        return f"Analyze this offline debugging snapshot and suggest bounded next checks.\n\nInput:\n{prompt}"
    if task_type == "vertex_unavailable_fallback":
        return f"Vertex is unavailable. Produce an emergency fallback structured summary.\n\nInput:\n{prompt}"
    return f"Produce a compact emergency summary.\n\nInput:\n{prompt}"


def evaluate_request(row: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
    task_type = str(row.get("task_type") or "")
    prompt = str(row.get("prompt") or "")
    input_json_raw = row.get("input_json") or "{}"
    schema_json_raw = row.get("schema_json") or "{}"
    vertex_unavailable = parse_int(row.get("vertex_unavailable"), 0)
    force_local = parse_int(row.get("force_local"), 0)
    severity = str(row.get("severity") or "info")

    out = {
        "accepted": 0,
        "reason_code": "REJECTED",
        "task_type": task_type,
        "task_mode": policy["mode"],
    }

    if policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if task_type not in ALLOWED_TASKS:
        out["reason_code"] = "TASK_NOT_SUPPORTED"
        return out
    if task_type not in policy["task_allowlist"]:
        out["reason_code"] = "TASK_NOT_ALLOWED"
        return out
    if len(prompt) > policy["max_prompt_chars"]:
        out["reason_code"] = "PROMPT_TOO_LARGE"
        return out
    if len(str(input_json_raw).encode("utf-8")) > policy["max_input_json_bytes"]:
        out["reason_code"] = "INPUT_JSON_TOO_LARGE"
        return out
    if len(str(schema_json_raw).encode("utf-8")) > policy["max_schema_bytes"]:
        out["reason_code"] = "SCHEMA_TOO_LARGE"
        return out

    if policy["mode"] == "DISABLED":
        out["reason_code"] = "MODE_DISABLED"
        return out

    if task_type in {"offline_debug", "local_report"}:
        out["accepted"] = 1
        out["reason_code"] = "OK"
        return out

    if task_type == "emergency_summarize" and severity in {"critical", "warning"}:
        out["accepted"] = 1
        out["reason_code"] = "OK"
        return out

    if force_local == 1:
        out["accepted"] = 1
        out["reason_code"] = "OK"
        return out

    if task_type == "vertex_unavailable_fallback":
        if policy["mode"] == "LOCAL_ONLY":
            out["accepted"] = 1
            out["reason_code"] = "OK"
            return out
        if policy["require_vertex_degraded"] == 1 and vertex_unavailable != 1:
            out["reason_code"] = "VERTEX_NOT_DEGRADED"
            return out
        out["accepted"] = 1
        out["reason_code"] = "OK"
        return out

    out["reason_code"] = "NOT_ELIGIBLE"
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_policy(r: Any) -> Dict[str, Any]:
    raw = await r.hgetall(GLOBAL_POLICY_KEY)
    return policy_from_hash(as_dict(raw))


async def persist_result_if_configured(db_url: str, row: Dict[str, Any], result: Dict[str, Any], meta: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_local_fallback_results (
                    request_id,
                    ts_ms,
                    task_type,
                    provider,
                    model_name,
                    result_json,
                    meta_json
                ) VALUES (
                    %(request_id)s,
                    %(ts_ms)s,
                    %(task_type)s,
                    %(provider)s,
                    %(model_name)s,
                    %(result_json)s,
                    %(meta_json)s
                )
                """,
                {
                    "request_id": row.get("request_id", ""),
                    "ts_ms": now_ms(),
                    "task_type": row.get("task_type", ""),
                    "provider": meta.get("provider", "ollama_local"),
                    "model_name": meta.get("model_name", ""),
                    "result_json": json.dumps(result),
                    "meta_json": json.dumps(meta),
                }
            )
            conn.commit()


async def persist_rejection_if_configured(db_url: str, row: Dict[str, Any], eval_row: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_local_fallback_rejections (
                    request_id,
                    ts_ms,
                    task_type,
                    reason_code,
                    payload_json
                ) VALUES (
                    %(request_id)s,
                    %(ts_ms)s,
                    %(task_type)s,
                    %(reason_code)s,
                    %(payload_json)s
                )
                """,
                {
                    "request_id": row.get("request_id", ""),
                    "ts_ms": now_ms(),
                    "task_type": row.get("task_type", ""),
                    "reason_code": eval_row["reason_code"],
                    "payload_json": json.dumps(row),
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
    provider = OllamaLocalFallbackProviderV30()
    gpu_lock = OllamaGpuLock(redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {INPUT_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                task_type = "unknown"
                try:
                    row = as_dict(payload)
                    task_type = str(row.get("task_type") or "unknown")
                    policy = await read_policy(r)
                    eval_row = evaluate_request(row, policy)

                    if eval_row["accepted"] != 1:
                        await persist_rejection_if_configured(db_url, row, eval_row)
                        await r.xadd(
                            REJECTIONS_STREAM,
                            {
                                "schema_version": 1,
                                "request_id": row.get("request_id", ""),
                                "task_type": task_type,
                                "reason_code": eval_row["reason_code"],
                                "payload_json": stable_json(row),
                                "ts_ms": str(now_ms()),
                            }, maxlen=MAXLEN,
                            approximate=True,
                        )
                        await r.xadd(
                            AUDIT_STREAM,
                            {
                                "event_type": "LOCAL_FALLBACK_REJECTED",
                                "request_id": row.get("request_id", ""),
                                "task_type": task_type,
                                "reason_code": eval_row["reason_code"],
                                "ts_ms": str(now_ms()),
                            }, maxlen=MAXLEN,
                            approximate=True,
                        )
                        if REJECTED:
                            REJECTED.labels(reason_code=eval_row["reason_code"], task_type=task_type).inc()
                        await r.xack(INPUT_STREAM, GROUP, msg_id)
                        continue

                    if not provider.is_available():
                        raise RuntimeError("local_fallback_provider_unavailable")

                    prompt = build_prompt(row)
                    schema = maybe_json(row.get("schema_json"), None)
                    async with gpu_lock.acquire(owner="local_fallback", timeout_sec=120):
                        result, meta = provider.analyze(task_type=task_type, prompt=prompt, schema=schema)
                    await persist_result_if_configured(db_url, row, result, meta)
                    out = {
                        "schema_version": 1,
                        "request_id": row.get("request_id", ""),
                        "task_type": task_type,
                        "provider": meta.get("provider", "ollama_local"),
                        "model_name": meta.get("model_name", ""),
                        "result_json": stable_json(result),
                        "meta_json": stable_json(meta),
                        "ts_ms": str(now_ms()),
                    }
                    await r.xadd(RESULTS_STREAM, out, maxlen=MAXLEN, approximate=True)
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "LOCAL_FALLBACK_COMPLETED",
                            "request_id": row.get("request_id", ""),
                            "task_type": task_type,
                            "provider": meta.get("provider", "ollama_local"),
                            "model_name": meta.get("model_name", ""),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "request_id": row.get("request_id", ""),
                            "task_type": task_type,
                            "provider": meta.get("provider", "ollama_local"),
                            "model_name": meta.get("model_name", ""),
                            "ts_ms": str(now_ms()),
                        }
                    )
                    if ACCEPTED:
                        ACCEPTED.labels(task_type=task_type).inc()
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "LOCAL_FALLBACK_FAILED",
                            "task_type": task_type,
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, task_type=task_type).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
