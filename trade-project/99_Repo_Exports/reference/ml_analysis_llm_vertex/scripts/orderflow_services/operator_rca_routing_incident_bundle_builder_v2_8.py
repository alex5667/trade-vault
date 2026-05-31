from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:  # pragma: no cover - optional dependency in unit tests
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None

    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


APP_NAME = "operator_rca_routing_incident_bundle_builder_v2_8"

REQUESTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_REQUESTS_STREAM",
    "stream:ml:operator_rca_routing_incident_bundle_requests",
)
RESULTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_RESULTS_STREAM",
    "stream:ml:operator_rca_routing_incident_bundle_results",
)
AUDIT_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_APPLY_AUDIT_STREAM",
    "stream:ml:operator_rca_routing_apply_audit",
)
APPLY_RESULTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_APPLY_RESULTS_STREAM",
    "stream:ml:operator_rca_routing_apply_results",
)
VERIFY_RESULTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_VERIFY_RESULTS_STREAM",
    "stream:ml:operator_rca_routing_verify_results",
)
ROLLBACK_REQUESTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_ROLLBACK_REQUESTS_STREAM",
    "stream:ml:operator_rca_routing_rollback_requests",
)
ROLLBACK_RESULTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_ROLLBACK_RESULTS_STREAM",
    "stream:ml:operator_rca_routing_rollback_results",
)
ROLLBACK_JOURNAL_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_ROLLBACK_JOURNAL_STREAM",
    "stream:ml:operator_rca_routing_rollback_journal",
)
RETRY_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RETRY_REQUESTS_STREAM",
    "stream:ml:operator_rca_routing_retry_requests",
)
ESCALATION_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_ESCALATIONS_STREAM",
    "stream:ml:operator_rca_routing_escalations",
)
SLO_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_SLO_ROLLUPS_STREAM",
    "stream:ml:operator_rca_routing_slo_rollups",
)
METRICS_LAST_HASH = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_LAST_HASH",
    "metrics:ml:operator_rca_routing_incident_bundle:last",
)
METRICS_PER_ID_PREFIX = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_PER_ID_PREFIX",
    "metrics:ml:operator_rca_routing_incident_bundle:",
)
GROUP = os.getenv("ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
MAXLEN = int(os.getenv("ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_MAXLEN", "20000"))
LOOKBACK = int(os.getenv("ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_LOOKBACK", "200"))
PORT = int(os.getenv("ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_PORT", "9883"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_rca_routing_incident_bundle_runs_total",
    "Incident bundle runs",
    ("status", "severity"),
)
LAT = _hist(
    "ml_operator_rca_routing_incident_bundle_latency_seconds",
    "Incident bundle build latency seconds",
)
LAST_RUN_TS = _gauge(
    "ml_operator_rca_routing_incident_bundle_last_run_ts_seconds",
    "Last successful run timestamp",
)
UP = _gauge(
    "ml_operator_rca_routing_incident_bundle_up",
    "Incident bundle builder up",
)
TIMELINE_EVENTS = _gauge(
    "ml_operator_rca_routing_incident_bundle_timeline_events",
    "Timeline events in latest bundle",
    ("severity",),
)


def now_ms() -> int:
    return get_ny_time_millis()


def safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


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


def hash_bundle(obj: Any) -> str:
    return hashlib.sha256(stable_json(obj).encode("utf-8")).hexdigest()[:16]


def route_change_id_from_row(row: Dict[str, Any]) -> Optional[str]:
    for key in (
        "route_change_id",
        "routing_change_id",
        "apply_id",
        "recommendation_id",
        "request_id",
        "incident_id",
    ):
        val = row.get(key)
        if val:
            return str(val)
    return None


def summarize_route_diff(baseline: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    interesting_keys = (
        "provider",
        "model_name",
        "prompt_version",
        "policy_version",
        "advisory_only",
        "routing_mode",
    )
    diff: Dict[str, Dict[str, Any]] = {}
    for key in interesting_keys:
        if baseline.get(key) != current.get(key):
            diff[key] = {"before": baseline.get(key), "after": current.get(key)}
    return diff


def severity_from_reason_codes(reason_codes: Iterable[str]) -> str:
    codes = {str(x) for x in reason_codes if x}
    critical = {
        "ERROR_RATE_SPIKE",
        "PARSE_FAIL_RATE_HIGH",
        "LATENCY_P95_REGRESSION",
        "ROLLBACK_FAILED",
        "ROLLBACK_MTTR_SLO_BREACH",
        "ROLLBACK_SUCCESS_RATE_LOW",
        "ROUTING_POLICY_CORRUPTED",
    }
    warning = {
        "USEFULNESS_DROP",
        "LOW_EXPOSURE",
        "ROLLBACK_VERIFY_INCONCLUSIVE",
        "ROUTE_CHANGE_RETRYING",
        "ROUTE_CHANGE_ESCALATED",
    }
    if codes & critical:
        return "critical"
    if codes & warning:
        return "warning"
    return "info"


def primary_reason_codes(sections: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    out: List[str] = []
    seen = set()
    for rows in sections.values():
        for row in rows:
            value = row.get("reason_code") or row.get("reason_codes") or row.get("verification_reason_code")
            parsed = maybe_json(value)
            if isinstance(parsed, list):
                candidates = [str(x) for x in parsed]
            elif isinstance(value, str) and "," in value:
                candidates = [x.strip() for x in value.split(",") if x.strip()]
            elif value:
                candidates = [str(value)]
            else:
                candidates = []
            for c in candidates:
                if c not in seen:
                    seen.add(c)
                    out.append(c)
    return out[:12]


def build_timeline(sections: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    timeline: List[Dict[str, Any]] = []
    for section_name, rows in sections.items():
        for row in rows:
            ts = safe_int(
                row.get("ts_ms")
                or row.get("event_ts_ms")
                or row.get("verified_ts_ms")
                or row.get("created_at_ms"),
                0,
            )
            timeline.append(
                {
                    "ts_ms": ts,
                    "section": section_name,
                    "event_type": row.get("event_type") or row.get("status") or row.get("decision") or section_name,
                    "reason_code": row.get("reason_code") or row.get("verification_reason_code"),
                    "source": row.get("source") or row.get("provider") or section_name,
                }
            )
    timeline.sort(key=lambda x: (safe_int(x.get("ts_ms"), 0), str(x.get("section", ""))))
    return timeline


def compact_row(row: Dict[str, Any]) -> Dict[str, Any]:
    keys = (
        "route_change_id",
        "recommendation_id",
        "ts_ms",
        "event_ts_ms",
        "status",
        "decision",
        "reason_code",
        "reason_codes",
        "verification_reason_code",
        "provider",
        "model_name",
        "prompt_version",
        "policy_version",
        "executor_mode",
        "reviewer",
        "retry_attempt",
        "severity",
        "message",
        "baseline_route_json",
        "current_route_json",
        "restored_route_json",
    )
    return {k: row.get(k) for k in keys if k in row and row.get(k) not in (None, "", [])}


async def xr_recent(client: Any, stream_key: str, limit: int) -> List[Dict[str, Any]]:
    try:
        rows = await client.xrevrange(stream_key, count=limit)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


def filter_rows_by_route_change_id(rows: List[Dict[str, Any]], route_change_id: str) -> List[Dict[str, Any]]:
    out = []
    for row in rows:
        if route_change_id_from_row(row) == route_change_id:
            out.append(row)
    return out


async def fetch_sections(client: Any, route_change_id: str) -> Dict[str, List[Dict[str, Any]]]:
    source_map = {
        "apply_results": APPLY_RESULTS_STREAM,
        "verify_results": VERIFY_RESULTS_STREAM,
        "rollback_requests": ROLLBACK_REQUESTS_STREAM,
        "rollback_results": ROLLBACK_RESULTS_STREAM,
        "rollback_journal": ROLLBACK_JOURNAL_STREAM,
        "retry_requests": RETRY_STREAM,
        "escalations": ESCALATION_STREAM,
        "slo_rollups": SLO_STREAM,
        "audit": AUDIT_STREAM,
    }
    sections: Dict[str, List[Dict[str, Any]]] = {}
    for name, stream_key in source_map.items():
        rows = await xr_recent(client, stream_key, LOOKBACK)
        filtered = [
            compact_row(x) | {k: v for k, v in x.items() if k.endswith("_json")}
            for x in filter_rows_by_route_change_id(rows, route_change_id)
        ]
        sections[name] = filtered
    return sections


def latest_snapshot_from_section(rows: List[Dict[str, Any]], field: str) -> Dict[str, Any]:
    for row in rows:
        value = row.get(field)
        parsed = maybe_json(value, {})
        if isinstance(parsed, dict) and parsed:
            return parsed
    return {}


def build_bundle(route_change_id: str, request_row: Dict[str, Any], sections: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    baseline = latest_snapshot_from_section(sections.get("apply_results", []), "baseline_route_json")
    current = latest_snapshot_from_section(sections.get("verify_results", []), "current_route_json")
    if not current:
        current = latest_snapshot_from_section(sections.get("rollback_results", []), "restored_route_json")

    reason_codes = primary_reason_codes(sections)
    severity = severity_from_reason_codes(reason_codes)
    timeline = build_timeline(sections)
    summary = {
        "route_change_id": route_change_id,
        "severity": severity,
        "timeline_events_n": len(timeline),
        "apply_results_n": len(sections.get("apply_results", [])),
        "verify_results_n": len(sections.get("verify_results", [])),
        "rollback_results_n": len(sections.get("rollback_results", [])),
        "retry_requests_n": len(sections.get("retry_requests", [])),
        "escalations_n": len(sections.get("escalations", [])),
        "slo_rollups_n": len(sections.get("slo_rollups", [])),
    }
    bundle = {
        "schema_version": 1,
        "bundle_type": "operator_rca_routing_incident_bundle",
        "route_change_id": route_change_id,
        "requested_ts_ms": safe_int(request_row.get("ts_ms"), now_ms()),
        "built_ts_ms": now_ms(),
        "severity": severity,
        "primary_reason_codes": reason_codes,
        "summary": summary,
        "baseline_route_json": baseline,
        "current_route_json": current,
        "route_diff_json": summarize_route_diff(baseline, current),
        "timeline_json": timeline,
        "sections_json": sections,
    }
    bundle["bundle_hash"] = hash_bundle(bundle)
    return bundle


@dataclass
class BundlePersistResult:
    persisted: bool
    error: Optional[str] = None


async def persist_bundle_if_configured(db_url: str, bundle: Dict[str, Any]) -> BundlePersistResult:
    if not db_url or psycopg is None:
        return BundlePersistResult(False)
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO llm_operator_rca_routing_incident_bundles (
                        bundle_id,
                        route_change_id,
                        built_ts_ms,
                        severity,
                        bundle_hash,
                        primary_reason_codes_json,
                        summary_json,
                        bundle_json
                    ) VALUES (
                        %(bundle_id)s,
                        %(route_change_id)s,
                        %(built_ts_ms)s,
                        %(severity)s,
                        %(bundle_hash)s,
                        %(primary_reason_codes_json)s,
                        %(summary_json)s,
                        %(bundle_json)s
                    )
                    ON CONFLICT (bundle_id) DO UPDATE SET
                        built_ts_ms = EXCLUDED.built_ts_ms,
                        severity = EXCLUDED.severity,
                        bundle_hash = EXCLUDED.bundle_hash,
                        primary_reason_codes_json = EXCLUDED.primary_reason_codes_json,
                        summary_json = EXCLUDED.summary_json,
                        bundle_json = EXCLUDED.bundle_json
                    """,
                    {
                        "bundle_id": bundle["bundle_hash"],
                        "route_change_id": bundle["route_change_id"],
                        "built_ts_ms": bundle["built_ts_ms"],
                        "severity": bundle["severity"],
                        "bundle_hash": bundle["bundle_hash"],
                        "primary_reason_codes_json": json.dumps(bundle["primary_reason_codes"]),
                        "summary_json": json.dumps(bundle["summary"]),
                        "bundle_json": json.dumps(bundle),
                    },
                )
                conn.commit()
        return BundlePersistResult(True)
    except Exception as exc:  # pragma: no cover
        return BundlePersistResult(False, str(exc))


async def process_one(client: Any, request_fields: Dict[str, Any], db_url: str) -> Dict[str, Any]:
    route_change_id = route_change_id_from_row(request_fields)
    if not route_change_id:
        raise ValueError("missing route_change_id")
    sections = await fetch_sections(client, route_change_id)
    bundle = build_bundle(route_change_id, request_fields, sections)
    persist_result = await persist_bundle_if_configured(db_url, bundle)
    payload = {
        "schema_version": 1,
        "bundle_id": bundle["bundle_hash"],
        "route_change_id": route_change_id,
        "severity": bundle["severity"],
        "primary_reason_codes_json": stable_json(bundle["primary_reason_codes"]),
        "summary_json": stable_json(bundle["summary"]),
        "route_diff_json": stable_json(bundle["route_diff_json"]),
        "timeline_json": stable_json(bundle["timeline_json"]),
        "bundle_json": stable_json(bundle),
        "persisted": int(persist_result.persisted),
        "persist_error": persist_result.error or "",
        "ts_ms": now_ms(),
    }
    await client.xadd(RESULTS_STREAM, payload, maxlen=MAXLEN, approximate=True)
    await client.hset(
        METRICS_LAST_HASH,
        mapping={
            "route_change_id": route_change_id,
            "bundle_id": bundle["bundle_hash"],
            "severity": bundle["severity"],
            "timeline_events_n": len(bundle["timeline_json"]),
            "ts_ms": now_ms(),
        },
    )
    await client.hset(
        f"{METRICS_PER_ID_PREFIX}{route_change_id}",
        mapping={
            "bundle_id": bundle["bundle_hash"],
            "severity": bundle["severity"],
            "primary_reason_codes_json": stable_json(bundle["primary_reason_codes"]),
            "summary_json": stable_json(bundle["summary"]),
            "ts_ms": now_ms(),
        },
    )
    return bundle


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
    db_url = os.getenv("DATABASE_URL", "")
    block_ms = int(os.getenv("ML_OPERATOR_RCA_ROUTING_INCIDENT_BUNDLE_BLOCK_MS", "5000"))
    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {REQUESTS_STREAM: ">"}, count=32, block=block_ms)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                req = as_dict(payload)
                severity = "info"
                try:
                    bundle = await process_one(r, req, db_url)
                    severity = bundle["severity"]
                    if TIMELINE_EVENTS:
                        TIMELINE_EVENTS.labels(severity=severity).set(len(bundle["timeline_json"]))
                    if RUNS:
                        RUNS.labels(status="ok", severity=severity).inc()
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    if RUNS:
                        RUNS.labels(status="error", severity=severity).inc()
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTING_INCIDENT_BUNDLE_BUILD_FAILED",
                            "route_change_id": route_change_id_from_row(req) or "",
                            "error": str(exc),
                            "ts_ms": now_ms(),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                finally:
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
