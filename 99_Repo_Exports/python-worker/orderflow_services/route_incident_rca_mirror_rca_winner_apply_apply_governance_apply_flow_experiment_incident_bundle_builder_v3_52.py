from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
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
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundle_builder_v3_52"
VERIFICATION_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_results",
)
ROLLBACK_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_journal",
)
RETRY_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_results",
)
ESCALATIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ESCALATIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_escalations",
)
SLO_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_ROLLUPS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_rollups",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles:global",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_PORT", "9986"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_MAXLEN", "20000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_LOOKBACK_COUNT", "300"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_WINDOW_MIN", "10080"))

DEFAULT_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_MODE",
    "ENABLED",
).upper()
DEFAULT_ALLOW_SEVERITIES = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_ALLOW_SEVERITIES",
    "warning,critical",
)
DEFAULT_MIN_VERIFICATION_EVENTS = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_MIN_VERIFICATION_EVENTS",
    "1",
))
DEFAULT_VERIFY_KEEP_RATE_CRIT = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_VERIFY_KEEP_RATE_CRIT",
    "0.60",
))
DEFAULT_ROLLBACK_MTTR_P95_CRIT_SEC = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_ROLLBACK_MTTR_P95_CRIT_SEC",
    "900",
))
DEFAULT_ESCALATION_RATE_CRIT = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_ESCALATION_RATE_CRIT",
    "0.20",
))
DEFAULT_MAX_BUNDLE_BYTES = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_MAX_BUNDLE_BYTES",
    "196608",
))

ALLOWED_MODES = {"ENABLED", "DISABLED"}
SOURCE_STREAMS = (VERIFICATION_STREAM, ROLLBACK_STREAM, RETRY_STREAM, ESCALATIONS_STREAM, SLO_STREAM)


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_runs_total",
    "Apply-flow experiment incident bundle builder runs",
    ("status", "decision", "trigger_type"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_latency_seconds",
    "Apply-flow experiment incident bundle builder latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_up",
    "Apply-flow experiment incident bundle builder up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_last_run_ts_seconds",
    "Apply-flow experiment incident bundle builder last run timestamp",
)
BUNDLES = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_total",
    "Apply-flow experiment incident bundles total",
    ("trigger_type", "severity"),
)


def now_ms() -> int:
    return get_ny_time_millis()


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
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


def default_allow_severities() -> set[str]:
    return {x.strip().lower() for x in DEFAULT_ALLOW_SEVERITIES.split(",") if x.strip()}


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(raw.get("mode") or DEFAULT_MODE).upper()
    if mode not in ALLOWED_MODES:
        mode = DEFAULT_MODE
    allow_severities = maybe_json(raw.get("allow_severities_json"), list(default_allow_severities()))
    if not isinstance(allow_severities, list):
        allow_severities = list(default_allow_severities())
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "mode": mode,
        "allow_severities": {str(x).lower() for x in allow_severities},
        "min_verification_events": parse_int(raw.get("min_verification_events"), DEFAULT_MIN_VERIFICATION_EVENTS),
        "verify_keep_rate_crit": parse_float(raw.get("verify_keep_rate_crit"), DEFAULT_VERIFY_KEEP_RATE_CRIT),
        "rollback_mttr_p95_crit_sec": parse_float(raw.get("rollback_mttr_p95_crit_sec"), DEFAULT_ROLLBACK_MTTR_P95_CRIT_SEC),
        "escalation_rate_crit": parse_float(raw.get("escalation_rate_crit"), DEFAULT_ESCALATION_RATE_CRIT),
        "max_bundle_bytes": parse_int(raw.get("max_bundle_bytes"), DEFAULT_MAX_BUNDLE_BYTES),
    }


def recent(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cutoff = now_ms() - WINDOW_MIN * 60 * 1000
    return [r for r in rows if parse_int(r.get("ts_ms"), 0) >= cutoff]


def build_bundle_id(trigger_type: str, reason_code: str, severity: str, ts_ms: int) -> str:
    raw = f"{trigger_type}|{reason_code}|{severity}|{ts_ms // 60000}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"apply-flow-experiment-incident:{h}"


def latest_slo_payload(slo_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not slo_rows:
        return {}
    payload = maybe_json(slo_rows[0].get("rollup_json"), {})
    return payload if isinstance(payload, dict) else {}


def build_summary(
    verification_rows: List[Dict[str, Any]],
    rollback_rows: List[Dict[str, Any]],
    retry_rows: List[Dict[str, Any]],
    escalation_rows: List[Dict[str, Any]],
    slo_payload: Dict[str, Any],
) -> Dict[str, Any]:
    vr = recent(verification_rows)
    rr = recent(rollback_rows)
    tr = recent(retry_rows)
    er = recent(escalation_rows)
    verification_reason_codes = sorted({str(r.get("reason_code") or "") for r in vr if str(r.get("reason_code") or "")})
    rollback_reason_codes = sorted({str(r.get("reason_code") or "") for r in rr if str(r.get("reason_code") or "")})
    retry_reason_codes = sorted({str(r.get("reason_code") or "") for r in tr if str(r.get("reason_code") or "")})
    escalation_reason_codes = sorted({str(r.get("reason_code") or "") for r in er if str(r.get("reason_code") or "")})
    escalation_severities = sorted({str(r.get("severity") or "") for r in er if str(r.get("severity") or "")})
    return {
        "verification_events_n": len(vr),
        "rollback_events_n": len(rr),
        "retry_events_n": len(tr),
        "escalation_events_n": len(er),
        "verification_reason_codes": verification_reason_codes,
        "rollback_reason_codes": rollback_reason_codes,
        "retry_reason_codes": retry_reason_codes,
        "escalation_reason_codes": escalation_reason_codes,
        "escalation_severities": escalation_severities,
        "verify_keep_rate": parse_float(slo_payload.get("verify_keep_rate"), 0.0),
        "rollback_plan_rate": parse_float(slo_payload.get("rollback_plan_rate"), 0.0),
        "rollback_applied_rate": parse_float(slo_payload.get("rollback_applied_rate"), 0.0),
        "rollback_mttr_p95_sec": parse_float(slo_payload.get("rollback_mttr_p95_sec"), 0.0),
        "escalation_rate": parse_float(slo_payload.get("escalation_rate"), 0.0),
    }


def choose_trigger(trigger_stream: str, trigger_row: Dict[str, Any]) -> Dict[str, str]:
    if trigger_stream == VERIFICATION_STREAM:
        decision = str(trigger_row.get("decision") or "")
        reason = str(trigger_row.get("reason_code") or "")
        if decision == "ROLLBACK_PREVIOUS_PROFILE":
            return {"trigger_type": "verification", "reason_code": reason or "ROLLBACK_PREVIOUS_PROFILE"}
        return {"trigger_type": "verification", "reason_code": reason or decision or "VERIFICATION"}
    if trigger_stream == ROLLBACK_STREAM:
        return {"trigger_type": "rollback", "reason_code": str(trigger_row.get("reason_code") or "ROLLBACK")}
    if trigger_stream == RETRY_STREAM:
        return {"trigger_type": "retry", "reason_code": str(trigger_row.get("reason_code") or "RETRY")}
    if trigger_stream == ESCALATIONS_STREAM:
        return {"trigger_type": "escalation", "reason_code": str(trigger_row.get("reason_code") or "ESCALATION")}
    return {"trigger_type": "slo_rollup", "reason_code": "SLO"}


def choose_severity(trigger_stream: str, trigger_row: Dict[str, Any], summary: Dict[str, Any], policy: Dict[str, Any]) -> str:
    if trigger_stream == ESCALATIONS_STREAM:
        return str(trigger_row.get("severity") or "warning").lower()
    if trigger_stream == RETRY_STREAM:
        return "warning"
    if trigger_stream == ROLLBACK_STREAM:
        return "critical" if parse_int(trigger_row.get("applied"), 0) == 1 else "warning"
    if trigger_stream == VERIFICATION_STREAM and str(trigger_row.get("decision") or "") == "ROLLBACK_PREVIOUS_PROFILE":
        return "critical"
    if summary["verify_keep_rate"] < policy["verify_keep_rate_crit"]:
        return "warning"
    if summary["rollback_mttr_p95_sec"] > policy["rollback_mttr_p95_crit_sec"]:
        return "warning"
    if summary["escalation_rate"] > policy["escalation_rate_crit"]:
        return "critical"
    return "warning"


def evaluate_bundle(summary: Dict[str, Any], severity: str, policy: Dict[str, Any]) -> Dict[str, Any]:
    out = {"decision": "REJECT", "reason_code": "REJECTED", "severity": severity}
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
    if parse_int(summary.get("verification_events_n"), 0) < policy["min_verification_events"]:
        out["reason_code"] = "INSUFFICIENT_VERIFICATION_EVENTS"
        return out
    out["decision"] = "BUILD_BUNDLE"
    out["reason_code"] = "OK"
    return out


def build_bundle(
    trigger_stream: str,
    trigger_row: Dict[str, Any],
    verification_rows: List[Dict[str, Any]],
    rollback_rows: List[Dict[str, Any]],
    retry_rows: List[Dict[str, Any]],
    escalation_rows: List[Dict[str, Any]],
    slo_rows: List[Dict[str, Any]],
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    slo_payload = latest_slo_payload(slo_rows)
    summary = build_summary(verification_rows, rollback_rows, retry_rows, escalation_rows, slo_payload)
    trigger_meta = choose_trigger(trigger_stream, trigger_row)
    severity = choose_severity(trigger_stream, trigger_row, summary, policy)
    bundle_id = build_bundle_id(trigger_meta["trigger_type"], trigger_meta["reason_code"], severity, now_ms())
    bundle = {
        "schema_version": 1,
        "bundle_id": bundle_id,
        "bundle_family": "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident",
        "trigger_type": trigger_meta["trigger_type"],
        "trigger_reason_code": trigger_meta["reason_code"],
        "trigger_severity": severity,
        "source": APP_NAME,
        "summary": summary,
        "evidence": {
            "trigger": trigger_row,
            "latest_verification": verification_rows[0] if verification_rows else {},
            "latest_rollback": rollback_rows[0] if rollback_rows else {},
            "latest_retry": retry_rows[0] if retry_rows else {},
            "latest_escalation": escalation_rows[0] if escalation_rows else {},
            "latest_slo_rollup": slo_payload,
        },
        "forensics": {
            "recent_verification": recent(verification_rows)[:5],
            "recent_rollback": recent(rollback_rows)[:5],
            "recent_retry": recent(retry_rows)[:5],
            "recent_escalation": recent(escalation_rows)[:5],
        },
        "ts_ms": now_ms(),
    }
    return bundle


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def xr_recent(r: Any, stream_key: str, count: int) -> List[Dict[str, Any]]:
    try:
        rows = await r.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    return as_dict(await r.hgetall(key))


async def persist_if_configured(db_url: str, bundle: Dict[str, Any], trigger_stream: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles (
                    bundle_id, ts_ms, trigger_type, trigger_reason_code, trigger_severity, source_stream, bundle_json
                ) VALUES (
                    %(bundle_id)s, %(ts_ms)s, %(trigger_type)s, %(trigger_reason_code)s, %(trigger_severity)s, %(source_stream)s, %(bundle_json)s
                )
                """,
                {
                    "bundle_id": bundle["bundle_id"],
                    "ts_ms": parse_int(bundle.get("ts_ms"), now_ms()),
                    "trigger_type": bundle["trigger_type"],
                    "trigger_reason_code": bundle["trigger_reason_code"],
                    "trigger_severity": bundle["trigger_severity"],
                    "source_stream": trigger_stream,
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
    for s in SOURCE_STREAMS:
        await ensure_group(r, s, GROUP)
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        rows = await r.xreadgroup(
            GROUP,
            CONSUMER,
            {s: ">" for s in SOURCE_STREAMS},
            count=16,
            block=5000,
        )
        if not rows:
            continue
        for stream_name, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                decision_label = "REJECT"
                trigger_type = "unknown"
                try:
                    trigger_row = as_dict(payload)
                    policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    try:
                        exec_kill = await r.get('trade:exec_kill_switch')
                        if exec_kill and exec_kill.decode().strip() == '1':
                            policy['kill_switch'] = 1
                    except: pass
                    verification_rows = await xr_recent(r, VERIFICATION_STREAM, LOOKBACK_COUNT)
                    rollback_rows = await xr_recent(r, ROLLBACK_STREAM, LOOKBACK_COUNT)
                    retry_rows = await xr_recent(r, RETRY_STREAM, LOOKBACK_COUNT)
                    escalation_rows = await xr_recent(r, ESCALATIONS_STREAM, LOOKBACK_COUNT)
                    slo_rows = await xr_recent(r, SLO_STREAM, 10)
                    bundle = build_bundle(
                        trigger_stream=stream_name,
                        trigger_row=trigger_row,
                        verification_rows=verification_rows,
                        rollback_rows=rollback_rows,
                        retry_rows=retry_rows,
                        escalation_rows=escalation_rows,
                        slo_rows=slo_rows,
                        policy=policy,
                    )
                    trigger_type = str(bundle.get("trigger_type") or "unknown")
                    decision = evaluate_bundle(bundle["summary"], str(bundle["trigger_severity"]), policy)
                    decision_label = decision["decision"]

                    if decision["decision"] == "BUILD_BUNDLE":
                        bundle_json = stable_json(bundle)
                        if len(bundle_json.encode("utf-8")) > policy["max_bundle_bytes"]:
                            decision_label = "REJECT"
                            await r.xadd(
                                AUDIT_STREAM,
                                {
                                    "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLE_REJECTED",
                                    "decision": "REJECT",
                                    "reason_code": "BUNDLE_TOO_LARGE",
                                    "trigger_type": trigger_type,
                                    "ts_ms": str(now_ms()),
                                }, maxlen=MAXLEN,
                                approximate=True,
                            )
                        else:
                            await persist_if_configured(db_url, bundle, stream_name)
                            await r.xadd(
                                OUTPUT_STREAM,
                                {
                                    "bundle_id": bundle["bundle_id"],
                                    "trigger_type": bundle["trigger_type"],
                                    "trigger_reason_code": bundle["trigger_reason_code"],
                                    "trigger_severity": bundle["trigger_severity"],
                                    "bundle_json": bundle_json,
                                    "ts_ms": str(now_ms()),
                                }, maxlen=MAXLEN,
                                approximate=True,
                            )
                            await r.hset(
                                LAST_HASH,
                                mapping={
                                    "bundle_id": bundle["bundle_id"],
                                    "trigger_type": bundle["trigger_type"],
                                    "trigger_reason_code": bundle["trigger_reason_code"],
                                    "trigger_severity": bundle["trigger_severity"],
                                    "ts_ms": str(now_ms()),
                                }
                            )
                            if BUNDLES:
                                BUNDLES.labels(trigger_type=bundle["trigger_type"], severity=bundle["trigger_severity"]).inc()
                    else:
                        await r.xadd(
                            AUDIT_STREAM,
                            {
                                "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLE_REJECTED",
                                "decision": decision["decision"],
                                "reason_code": decision["reason_code"],
                                "trigger_type": trigger_type,
                                "severity": decision["severity"],
                                "ts_ms": str(now_ms()),
                            }, maxlen=MAXLEN,
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
                            "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLE_FAILED",
                            "error": str(exc),
                            "trigger_type": trigger_type,
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(stream_name, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, decision=decision_label, trigger_type=trigger_type).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
