from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundle_builder_v3_35"
APPLY_JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_journal",
)
ROLLBACK_JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ROLLBACK_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal",
)
ESCALATIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ESCALATIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_escalations",
)
VERIFICATION_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_verification_results",
)
RETRY_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry_results",
)
SLO_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_ROLLUPS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles:last",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_PORT", "9962"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_MAXLEN", "20000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_LOOKBACK_COUNT", "50"))
RECENT_WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_RECENT_WINDOW_MIN", "240"))
ONLY_SEVERITY = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_ONLY_SEVERITY", "warning,critical")
TRIGGER_ON_APPLY_DECISIONS = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_ON_APPLY_DECISIONS",
    "APPLY_PRIMARY_ARM_SHADOW,APPLY_SINGLE_ARM",
)


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_runs_total",
    "Winner-apply apply governance incident bundle builder runs",
    ("status", "trigger_type"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_latency_seconds",
    "Winner-apply apply governance incident bundle builder latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_up",
    "Winner-apply apply governance incident bundle builder up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_last_run_ts_seconds",
    "Winner-apply apply governance incident bundle builder last run ts",
)
BUNDLES = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_total",
    "Winner-apply apply governance incident bundles created",
    ("severity", "trigger_type"),
)


def now_ms() -> int:
    return get_ny_time_millis()


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_int(v: Any, default: int = 0) -> int:
    try:
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


def allowed_severities() -> set[str]:
    return {x.strip().lower() for x in ONLY_SEVERITY.split(",") if x.strip()}


def trigger_apply_decisions() -> set[str]:
    return {x.strip().upper() for x in TRIGGER_ON_APPLY_DECISIONS.split(",") if x.strip()}


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def xr_recent(client: Any, stream_key: str, count: int) -> List[Dict[str, Any]]:
    try:
        rows = await client.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


def within_recent_window(ts_ms: int) -> bool:
    return ts_ms >= now_ms() - RECENT_WINDOW_MIN * 60 * 1000


def normalize_trigger(source: str, row: Dict[str, Any]) -> Dict[str, Any]:
    ts_ms = parse_int(row.get("ts_ms"), 0)
    if source == "apply_journal":
        decision = str(row.get("decision") or "UNKNOWN").upper()
        severity = "warning"
        return {
            "trigger_type": "apply_transition",
            "transition_type": decision,
            "severity": severity,
            "reason_code": str(row.get("reason_code") or "UNKNOWN"),
            "ts_ms": ts_ms,
            "row": row,
        },
    if source == "rollback_journal":
        return {
            "trigger_type": "rollback",
            "transition_type": "ROLLBACK",
            "severity": "critical",
            "reason_code": str(row.get("reason_code") or "UNKNOWN"),
            "ts_ms": ts_ms,
            "row": row,
        },
    summary = maybe_json(row.get("summary_json"), {})
    severity = str(row.get("severity") or (summary or {}).get("severity") or "info").lower()
    reason_codes = (summary or {}).get("reason_codes", []) if isinstance(summary, dict) else []
    return {
        "trigger_type": "escalation",
        "transition_type": "NONE",
        "severity": severity,
        "reason_code": ",".join(reason_codes) if isinstance(reason_codes, list) else "UNKNOWN",
        "ts_ms": ts_ms,
        "row": row,
    },


def should_trigger_bundle(trigger: Dict[str, Any]) -> bool:
    if not within_recent_window(trigger["ts_ms"]):
        return False
    if trigger["trigger_type"] == "apply_transition":
        return trigger["transition_type"] in trigger_apply_decisions()
    if trigger["trigger_type"] == "rollback":
        return True
    return trigger["severity"] in allowed_severities()


def summarize_rows(rows: List[Dict[str, Any]], fields: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows[:LOOKBACK_COUNT]:
        item = {}
        for f in fields:
            if f in row:
                item[f] = row[f]
        out.append(item)
    return out


async def build_bundle(r: Any, trigger: Dict[str, Any]) -> Dict[str, Any]:
    apply_rows = await xr_recent(r, APPLY_JOURNAL_STREAM, LOOKBACK_COUNT)
    verification_rows = await xr_recent(r, VERIFICATION_STREAM, LOOKBACK_COUNT)
    retry_rows = await xr_recent(r, RETRY_STREAM, LOOKBACK_COUNT)
    rollback_rows = await xr_recent(r, ROLLBACK_JOURNAL_STREAM, LOOKBACK_COUNT)
    escalation_rows = await xr_recent(r, ESCALATIONS_STREAM, LOOKBACK_COUNT)
    slo_rows = await xr_recent(r, SLO_STREAM, LOOKBACK_COUNT)

    recent_apply = [r0 for r0 in apply_rows if within_recent_window(parse_int(r0.get("ts_ms"), 0))]
    recent_verification = [r0 for r0 in verification_rows if within_recent_window(parse_int(r0.get("ts_ms"), 0))]
    recent_retry = [r0 for r0 in retry_rows if within_recent_window(parse_int(r0.get("ts_ms"), 0))]
    recent_rollback = [r0 for r0 in rollback_rows if within_recent_window(parse_int(r0.get("ts_ms"), 0))]
    recent_escalation = [r0 for r0 in escalation_rows if within_recent_window(parse_int(r0.get("ts_ms"), 0))]
    recent_slo = [r0 for r0 in slo_rows if within_recent_window(parse_int(r0.get("ts_ms"), 0))]

    verification_reason_codes = sorted({
        str(r0.get("reason_code") or "")
        for r0 in recent_verification
        if str(r0.get("reason_code") or "")
    })
    retry_reason_codes = sorted({
        str(r0.get("reason_code") or "")
        for r0 in recent_retry
        if str(r0.get("reason_code") or "")
    })
    rollback_reason_codes = sorted({
        str(r0.get("reason_code") or "")
        for r0 in recent_rollback
        if str(r0.get("reason_code") or "")
    })
    escalation_severities = sorted({
        str(r0.get("severity") or "")
        for r0 in recent_escalation
        if str(r0.get("severity") or "")
    })
    apply_decisions = sorted({
        str(r0.get("decision") or "")
        for r0 in recent_apply
        if str(r0.get("decision") or "")
    })

    bundle_id = f"winner-apply-apply-governance-bundle:{trigger['trigger_type']}:{trigger['ts_ms']}"
    return {
        "schema_version": 1,
        "bundle_id": bundle_id,
        "contour": "route_incident_rca_mirror_rca_winner_apply_apply_governance",
        "trigger_type": trigger["trigger_type"],
        "trigger_severity": trigger["severity"],
        "trigger_reason_code": trigger["reason_code"],
        "trigger_ts_ms": trigger["ts_ms"],
        "summary": {
            "apply_events_n": len(recent_apply),
            "verification_events_n": len(recent_verification),
            "retry_events_n": len(recent_retry),
            "rollback_events_n": len(recent_rollback),
            "escalation_events_n": len(recent_escalation),
            "slo_rollups_n": len(recent_slo),
            "apply_decisions": apply_decisions,
            "verification_reason_codes": verification_reason_codes,
            "retry_reason_codes": retry_reason_codes,
            "rollback_reason_codes": rollback_reason_codes,
            "escalation_severities": escalation_severities,
        },
        "evidence": {
            "trigger": trigger["row"],
            "apply_recent": summarize_rows(
                recent_apply,
                ["decision", "reason_code", "mode_before", "primary_arm_before", "mode_after", "primary_arm_after", "ts_ms"],
            ),
            "verification_recent": summarize_rows(
                recent_verification,
                [
                    "decision",
                    "reason_code",
                    "current_mode",
                    "current_primary_arm",
                    "target_mode",
                    "target_primary_arm",
                    "rollback_mode",
                    "rollback_primary_arm",
                    "primary_match_rate",
                    "unexpected_primary_rate",
                    "shadow_rate",
                    "exposure_total",
                    "ts_ms",
                ],
            ),
            "retry_recent": summarize_rows(
                recent_retry,
                ["decision", "reason_code", "attempts", "rollback_mode", "rollback_primary_arm", "ts_ms"],
            ),
            "rollback_recent": summarize_rows(
                recent_rollback,
                ["reason_code", "mode_before", "primary_arm_before", "mode_after", "primary_arm_after", "ts_ms"],
            ),
            "escalations_recent": summarize_rows(
                recent_escalation,
                ["severity", "summary_json", "ts_ms"],
            ),
            "slo_recent": summarize_rows(
                recent_slo,
                ["payload_json", "reason_codes_json", "ts_ms"],
            ),
        },
    },


async def persist_if_configured(db_url: str, bundle: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """,
                INSERT INTO llm_governance_incident_bundles (
                    bundle_id,
                    ts_ms,
                    contour,
                    trigger_type,
                    trigger_severity,
                    trigger_reason_code,
                    bundle_json
                ) VALUES (
                    %(bundle_id)s,
                    %(ts_ms)s,
                    %(contour)s,
                    %(trigger_type)s,
                    %(trigger_severity)s,
                    %(trigger_reason_code)s,
                    %(bundle_json)s
                )
                """,
                {
                    "bundle_id": bundle["bundle_id"],
                    "ts_ms": now_ms(),
                    "contour": bundle["contour"],
                    "trigger_type": bundle["trigger_type"],
                    "trigger_severity": bundle["trigger_severity"],
                    "trigger_reason_code": bundle["trigger_reason_code"],
                    "bundle_json": json.dumps(bundle),
                },
            )
            conn.commit()


async def process_trigger(r: Any, db_url: str, source: str, row: Dict[str, Any]) -> Tuple[str, str]:
    trigger = normalize_trigger(source, row)
    if not should_trigger_bundle(trigger):
        return "SKIP", trigger["trigger_type"]

    bundle = await build_bundle(r, trigger)
    await persist_if_configured(db_url, bundle)
    await r.xadd(
        OUTPUT_STREAM,
        {
            "schema_version": 1,
            "bundle_id": bundle["bundle_id"],
            "contour": bundle["contour"],
            "trigger_type": bundle["trigger_type"],
            "trigger_severity": bundle["trigger_severity"],
            "trigger_reason_code": bundle["trigger_reason_code"],
            "bundle_json": stable_json(bundle),
            "ts_ms": str(now_ms()),
        },
        maxlen=MAXLEN,
        approximate=True,
    )
    await r.hset(
        LAST_HASH,
        mapping={
            "bundle_id": bundle["bundle_id"],
            "trigger_type": bundle["trigger_type"],
            "trigger_severity": bundle["trigger_severity"],
            "trigger_reason_code": bundle["trigger_reason_code"],
            "ts_ms": str(now_ms()),
        },
    )
    if BUNDLES:
        BUNDLES.labels(severity=bundle["trigger_severity"], trigger_type=bundle["trigger_type"]).inc()
    return "BUILT", bundle["trigger_type"]


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("DATABASE_URL", "")
    await ensure_group(r, APPLY_JOURNAL_STREAM, GROUP)
    await ensure_group(r, ROLLBACK_JOURNAL_STREAM, GROUP)
    await ensure_group(r, ESCALATIONS_STREAM, GROUP)

    while True:
        rows = await r.xreadgroup(
            GROUP,
            CONSUMER,
            {
                APPLY_JOURNAL_STREAM: ">",
                ROLLBACK_JOURNAL_STREAM: ">",
                ESCALATIONS_STREAM: ">",
            },
            count=32,
            block=5000,
        )
        if not rows:
            continue
        for stream_name, messages in rows:
            if stream_name == APPLY_JOURNAL_STREAM:
                source = "apply_journal"
            elif stream_name == ROLLBACK_JOURNAL_STREAM:
                source = "rollback_journal"
            else:
                source = "escalations"

            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                trigger_type = "none"
                try:
                    row = as_dict(payload)
                    decision, trigger_type = await process_trigger(r, db_url, source, row)
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLE_EVALUATED",
                            "source": source,
                            "decision": decision,
                            "trigger_type": trigger_type,
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(stream_name, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLE_FAILED",
                            "source": source,
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(stream_name, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, trigger_type=trigger_type).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
