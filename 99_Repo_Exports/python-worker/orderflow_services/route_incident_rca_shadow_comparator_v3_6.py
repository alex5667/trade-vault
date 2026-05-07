from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, List, Tuple

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


APP_NAME = "route_incident_rca_shadow_comparator_v3_6"
HANDOFF_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_HANDOFF_SHADOW_STREAM",
    "stream:ml:vertex_local_handoff_shadow_requests",
)
LEGACY_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_LEGACY_SHADOW_STREAM",
    "stream:ml:route_incident_rca_legacy_shadow_requests",
)
RESULTS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_RESULTS_STREAM",
    "stream:ml:route_incident_rca_shadow_comparator_results",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_AUDIT_STREAM",
    "stream:ml:route_incident_rca_shadow_comparator_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_LAST_HASH",
    "metrics:ml:route_incident_rca_shadow_comparator:last",
)
PENDING_PREFIX = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_PENDING_PREFIX",
    "state:ml:route_incident_rca_shadow_comparator:pending:",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_PORT", "9922"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_MAXLEN", "20000"))
PENDING_TTL_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_PENDING_TTL_SEC", "86400"))
# scan_iter по всему keyspace (317K ключей) дорого — ограничиваем до 1 раза в 60 сек
METRICS_REFRESH_INTERVAL_SEC = float(os.getenv("ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_METRICS_INTERVAL_SEC", "60"))

# Database URL: project convention uses ANALYTICS_DB_DSN via PgBouncer
DB_URL = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_shadow_comparator_runs_total",
    "Route incident RCA shadow comparator runs",
    ("status", "side"),
)
LAT = _hist(
    "ml_route_incident_rca_shadow_comparator_latency_seconds",
    "Route incident RCA shadow comparator latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_shadow_comparator_up",
    "Route incident RCA shadow comparator up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_shadow_comparator_last_run_ts_seconds",
    "Route incident RCA shadow comparator last run timestamp",
)
COMPARES = _counter(
    "ml_route_incident_rca_shadow_comparisons_total",
    "Route incident RCA shadow comparisons",
    ("status",),
)
PENDING = _gauge(
    "ml_route_incident_rca_shadow_comparator_pending",
    "Estimated pending shadow rows in comparator state",
    ("side",),
)


_last_metrics_refresh: float = 0.0


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


def correlation_key(row: Dict[str, Any]) -> str:
    incident_id = str(row.get("incident_id") or "").strip()
    request_id = str(row.get("request_id") or "").strip()
    compact_hash = str(row.get("compact_hash") or "").strip()
    return incident_id or request_id or compact_hash


def pending_key(side: str, corr: str) -> str:
    return f"{PENDING_PREFIX}{side}:{corr}"


def payload_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = maybe_json(row.get("payload_json"), {})
    return payload if isinstance(payload, dict) else {}


def payload_keys(row: Dict[str, Any]) -> List[str]:
    return sorted(payload_dict(row).keys())


def primary_reason_codes(row: Dict[str, Any]) -> List[str]:
    payload = payload_dict(row)
    prc = payload.get("primary_reason_codes", [])
    if isinstance(prc, list):
        return sorted(str(x) for x in prc)
    fallback = maybe_json(row.get("primary_reason_codes_json"), [])
    if isinstance(fallback, list):
        return sorted(str(x) for x in fallback)
    return []


def compact_hash_matches(handoff: Dict[str, Any], legacy: Dict[str, Any]) -> bool:
    h = str(handoff.get("compact_hash") or "").strip()
    l = str(legacy.get("compact_hash") or "").strip()
    if not h or not l:
        return True
    return h == l


def compare_rows(handoff: Dict[str, Any], legacy: Dict[str, Any]) -> Dict[str, Any]:
    reason_codes: List[str] = []
    score = 1.0

    incident_eq = str(handoff.get("incident_id") or "") == str(legacy.get("incident_id") or "")
    task_type_eq = str(handoff.get("task_type") or "") == str(legacy.get("task_type") or "")
    severity_eq = str(handoff.get("severity") or "") == str(legacy.get("severity") or "")
    compact_eq = compact_hash_matches(handoff, legacy)

    handoff_keys = payload_keys(handoff)
    legacy_keys = payload_keys(legacy)
    only_handoff = sorted(set(handoff_keys) - set(legacy_keys))
    only_legacy = sorted(set(legacy_keys) - set(handoff_keys))

    handoff_prc = primary_reason_codes(handoff)
    legacy_prc = primary_reason_codes(legacy)
    prc_only_handoff = sorted(set(handoff_prc) - set(legacy_prc))
    prc_only_legacy = sorted(set(legacy_prc) - set(handoff_prc))

    if not incident_eq:
        score -= 0.35
        reason_codes.append("INCIDENT_ID_MISMATCH")
    if not task_type_eq:
        score -= 0.20
        reason_codes.append("TASK_TYPE_MISMATCH")
    if not severity_eq:
        score -= 0.10
        reason_codes.append("SEVERITY_MISMATCH")
    if not compact_eq:
        score -= 0.10
        reason_codes.append("COMPACT_HASH_MISMATCH")
    if only_handoff or only_legacy:
        score -= 0.15
        reason_codes.append("PAYLOAD_KEY_DRIFT")
    if prc_only_handoff or prc_only_legacy:
        score -= 0.10
        reason_codes.append("PRIMARY_REASON_CODES_DRIFT")

    score = max(0.0, round(score, 6))
    if "INCIDENT_ID_MISMATCH" in reason_codes or "TASK_TYPE_MISMATCH" in reason_codes:
        status = "MISMATCH"
    elif score >= 0.90:
        status = "MATCH"
    elif score >= 0.65:
        status = "DRIFT"
    else:
        status = "MISMATCH"

    if not reason_codes:
        reason_codes = ["OK"]

    return {
        "status": status,
        "score": score,
        "reason_codes": reason_codes,
        "incident_eq": incident_eq,
        "task_type_eq": task_type_eq,
        "severity_eq": severity_eq,
        "compact_hash_eq": compact_eq,
        "handoff_payload_keys": handoff_keys,
        "legacy_payload_keys": legacy_keys,
        "payload_keys_only_handoff": only_handoff,
        "payload_keys_only_legacy": only_legacy,
        "handoff_primary_reason_codes": handoff_prc,
        "legacy_primary_reason_codes": legacy_prc,
        "primary_reason_codes_only_handoff": prc_only_handoff,
        "primary_reason_codes_only_legacy": prc_only_legacy,
    }


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def store_pending(r: Any, side: str, row: Dict[str, Any]) -> None:
    corr = correlation_key(row)
    if not corr:
        return
    await r.set(pending_key(side, corr), stable_json(row), ex=PENDING_TTL_SEC)


async def read_pending(r: Any, side: str, corr: str) -> Dict[str, Any]:
    raw = await r.get(pending_key(side, corr))
    return maybe_json(raw, {}) if raw else {}


async def delete_pending_pair(r: Any, corr: str) -> None:
    await r.delete(pending_key("handoff", corr), pending_key("legacy", corr))


async def refresh_pending_metrics(r: Any) -> None:
    global _last_metrics_refresh
    now = time.monotonic()
    if now - _last_metrics_refresh < METRICS_REFRESH_INTERVAL_SEC:
        return
    _last_metrics_refresh = now
    try:
        handoff_n = 0
        async for _ in r.scan_iter(f"{PENDING_PREFIX}handoff:*", count=5000):
            handoff_n += 1
        legacy_n = 0
        async for _ in r.scan_iter(f"{PENDING_PREFIX}legacy:*", count=5000):
            legacy_n += 1
    except Exception:
        return
    if PENDING:
        PENDING.labels(side="handoff").set(handoff_n)
        PENDING.labels(side="legacy").set(legacy_n)


async def persist_if_configured(
    db_url: str,
    corr: str,
    handoff: Dict[str, Any],
    legacy: Dict[str, Any],
    comparison: Dict[str, Any],
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_shadow_comparisons (
                    correlation_key,
                    incident_id,
                    ts_ms,
                    status,
                    score,
                    reason_codes_json,
                    handoff_payload_json,
                    legacy_payload_json,
                    comparison_json
                ) VALUES (
                    %(correlation_key)s,
                    %(incident_id)s,
                    %(ts_ms)s,
                    %(status)s,
                    %(score)s,
                    %(reason_codes_json)s,
                    %(handoff_payload_json)s,
                    %(legacy_payload_json)s,
                    %(comparison_json)s
                )
                """,
                {
                    "correlation_key": corr,
                    "incident_id": handoff.get("incident_id") or legacy.get("incident_id") or "",
                    "ts_ms": now_ms(),
                    "status": comparison["status"],
                    "score": comparison["score"],
                    "reason_codes_json": json.dumps(comparison["reason_codes"]),
                    "handoff_payload_json": json.dumps(handoff),
                    "legacy_payload_json": json.dumps(legacy),
                    "comparison_json": json.dumps(comparison),
                }
            )
            conn.commit()


async def process_side(
    r: Any,
    db_url: str,
    side: str,
    row: Dict[str, Any],
) -> None:
    corr = correlation_key(row)
    if not corr:
        await r.xadd(
            AUDIT_STREAM,
            {
                "event_type": "ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_SKIPPED",
                "side": side,
                "reason_code": "CORRELATION_KEY_MISSING",
                "ts_ms": str(now_ms()),
            }, maxlen=MAXLEN,
            approximate=True,
        )
        return

    await store_pending(r, side, row)
    other_side = "legacy" if side == "handoff" else "handoff"
    other_row = await read_pending(r, other_side, corr)
    if not other_row:
        return

    handoff = row if side == "handoff" else other_row
    legacy = row if side == "legacy" else other_row
    comparison = compare_rows(handoff, legacy)
    await persist_if_configured(db_url, corr, handoff, legacy, comparison)

    out = {
        "schema_version": 1,
        "correlation_key": corr,
        "incident_id": handoff.get("incident_id") or legacy.get("incident_id") or "",
        "status": comparison["status"],
        "score": str(comparison["score"]),
        "reason_codes_json": stable_json(comparison["reason_codes"]),
        "comparison_json": stable_json(comparison),
        "ts_ms": str(now_ms()),
    }
    await r.xadd(RESULTS_STREAM, out, maxlen=MAXLEN, approximate=True)
    await r.xadd(
        AUDIT_STREAM,
        {
            "event_type": "ROUTE_INCIDENT_RCA_SHADOW_COMPARED",
            "correlation_key": corr,
            "status": comparison["status"],
            "score": str(comparison["score"]),
            "reason_codes_json": stable_json(comparison["reason_codes"]),
            "ts_ms": str(now_ms()),
        }, maxlen=MAXLEN,
        approximate=True,
    )
    await r.hset(
        LAST_HASH,
        mapping={
            "correlation_key": corr,
            "incident_id": handoff.get("incident_id") or legacy.get("incident_id") or "",
            "status": comparison["status"],
            "score": str(comparison["score"]),
            "reason_codes_json": stable_json(comparison["reason_codes"]),
            "ts_ms": str(now_ms()),
        }
    )
    if COMPARES:
        COMPARES.labels(status=comparison["status"]).inc()
    await delete_pending_pair(r, corr)


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    await ensure_group(r, HANDOFF_STREAM, GROUP)
    await ensure_group(r, LEGACY_STREAM, GROUP)
    db_url = DB_URL

    while True:
        rows = await r.xreadgroup(
            GROUP,
            CONSUMER,
            {HANDOFF_STREAM: ">", LEGACY_STREAM: ">"},
            count=64,
            block=5000,
        )
        if not rows:
            await refresh_pending_metrics(r)
            continue
        for stream_name_r, messages in rows:
            stream_name = stream_name_r.decode() if isinstance(stream_name_r, (bytes, bytearray)) else str(stream_name_r)
            side = "handoff" if "handoff" in stream_name else "legacy"
            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                try:
                    row = as_dict(payload)
                    await process_side(r, db_url, side, row)
                    await r.xack(stream_name, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_FAILED",
                            "side": side,
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(stream_name, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, side=side).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))
        await refresh_pending_metrics(r)


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
