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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_consumer_v3_45"
VERTEX_INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_VERTEX_RCA_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_vertex_rca_requests",
)
LOCAL_INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_LOCAL_RCA_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_local_rca_requests",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca:global",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_PORT", "9975"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_MAXLEN", "20000"))

DEFAULT_HANDLER_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_HANDLER_MODE",
    "DETERMINISTIC",
).upper()
DEFAULT_MAX_BUNDLE_BYTES = int(
    os.getenv(
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_MAX_BUNDLE_BYTES",
        "196608",
    )
)
DEFAULT_ALLOW_SEVERITIES = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_ALLOW_SEVERITIES",
    "warning,critical",
)
ALLOWED_HANDLER_MODES = {"DETERMINISTIC", "DISABLED"}


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_runs_total",
    "Apply-flow RCA consumer runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_latency_seconds",
    "Apply-flow RCA consumer latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_up",
    "Apply-flow RCA consumer up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_last_run_ts_seconds",
    "Apply-flow RCA consumer last run timestamp",
)
RESULTS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results_total",
    "Apply-flow RCA results",
    ("severity", "provider_mode"),
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


def default_allow_severities() -> set[str]:
    return {x.strip().lower() for x in DEFAULT_ALLOW_SEVERITIES.split(",") if x.strip()}


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(raw.get("handler_mode") or DEFAULT_HANDLER_MODE).upper()
    if mode not in ALLOWED_HANDLER_MODES:
        mode = DEFAULT_HANDLER_MODE
    allow_severities = maybe_json(raw.get("allow_severities_json"), list(default_allow_severities()))
    if not isinstance(allow_severities, list):
        allow_severities = list(default_allow_severities())
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "handler_mode": mode,
        "allow_severities": {str(x).lower() for x in allow_severities},
        "max_bundle_bytes": parse_int(raw.get("max_bundle_bytes"), DEFAULT_MAX_BUNDLE_BYTES),
    }


def evaluate_request(bundle: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
    severity = str(bundle.get("trigger_severity") or "").lower()
    out = {"decision": "REJECT", "reason_code": "REJECTED", "severity": severity}
    if policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if policy["handler_mode"] == "DISABLED":
        out["reason_code"] = "MODE_DISABLED"
        return out
    if severity not in policy["allow_severities"]:
        out["reason_code"] = "SEVERITY_NOT_ALLOWED"
        return out
    if len(stable_json(bundle).encode("utf-8")) > policy["max_bundle_bytes"]:
        out["reason_code"] = "BUNDLE_TOO_LARGE"
        return out
    out["decision"] = "BUILD_RESULT"
    out["reason_code"] = "OK"
    return out


def _summary(bundle: Dict[str, Any]) -> Dict[str, Any]:
    summary = bundle.get("summary", {})
    return summary if isinstance(summary, dict) else {}


def _trigger(bundle: Dict[str, Any]) -> Dict[str, Any]:
    evidence = bundle.get("evidence", {}) if isinstance(bundle.get("evidence"), dict) else {}
    trigger = evidence.get("trigger", {})
    return trigger if isinstance(trigger, dict) else {}


def build_result_payload(bundle: Dict[str, Any], provider_mode: str) -> Dict[str, Any]:
    summary = _summary(bundle)
    trigger = _trigger(bundle)
    verify_reasons = summary.get("verification_reason_codes", [])
    rollback_reasons = summary.get("rollback_reason_codes", [])
    retry_reasons = summary.get("retry_reason_codes", [])
    escalation_severities = summary.get("escalation_severities", [])
    if not isinstance(verify_reasons, list):
        verify_reasons = []
    if not isinstance(rollback_reasons, list):
        rollback_reasons = []
    if not isinstance(retry_reasons, list):
        retry_reasons = []
    if not isinstance(escalation_severities, list):
        escalation_severities = []

    dominant: List[str] = []
    hypotheses: List[str] = []
    next_actions: List[str] = []
    confidence = 0.58 if provider_mode == "VERTEX" else 0.52

    if str(bundle.get("trigger_type") or "") == "verification":
        dominant.append("Verification surfaced a post-apply governance failure in the dedicated apply-flow contour.")
        hypotheses.append("The applied policy did not converge cleanly to the intended live state after the winner-apply transition.")
        next_actions.append("Compare intended target policy, live experiment policy, and post-apply exposures for this contour.")
        confidence += 0.10
    if "PRIMARY_MATCH_RATE_TOO_LOW" in verify_reasons:
        dominant.append("Primary match rate after apply is too low for the promoted apply-flow RCA contour.")
        hypotheses.append("Primary traffic did not converge to the promoted arm or stale publishers kept emitting conflicting primary state.")
        next_actions.append("Inspect experiment primary arm assignment and post-apply exposure splits by arm.")
        confidence += 0.08
    if "UNEXPECTED_PRIMARY_RATE_TOO_HIGH" in verify_reasons:
        dominant.append("Unexpected primary rate remained too high after apply.")
        hypotheses.append("Non-target arms continued to emit primary traffic after the policy change.")
        next_actions.append("Audit lingering producers or delayed policy propagation on the apply-flow contour.")
        confidence += 0.07
    if "POLICY_MISMATCH_AFTER_APPLY" in verify_reasons:
        dominant.append("Live policy mismatched the intended target after apply.")
        hypotheses.append("The apply-controller decision was recorded, but live policy convergence failed or was overwritten.")
        next_actions.append("Check policy writes, controller journal, and subsequent mode switches after the apply event.")
        confidence += 0.09
    if "SHADOW_EXPOSURES_PRESENT_IN_SINGLE_ARM" in verify_reasons:
        dominant.append("Single-arm mode still emitted shadow exposures.")
        hypotheses.append("Single-arm enforcement is incomplete or shadow publishers were not fully quiesced.")
        next_actions.append("Verify shadow publishers and compare expected single-arm mode versus observed exposure records.")
        confidence += 0.08
    if "MAX_ATTEMPTS_REACHED" in retry_reasons or "EXHAUSTED" in [str(trigger.get("decision") or "").upper()]:
        dominant.append("Retry budget was exhausted while trying to converge the rollback target.")
        hypotheses.append("Rollback convergence is unstable or idempotent re-apply did not restore the expected experiment state.")
        next_actions.append("Freeze further promotions for this contour until retry exhaustion is explained.")
        confidence += 0.10
    if "ROLLBACK_MTTR_P95_HIGH" in retry_reasons or "ROLLBACK_MTTR_P95_HIGH" in rollback_reasons:
        dominant.append("Rollback MTTR p95 is elevated for the apply-flow contour.")
        hypotheses.append("Rollback execution or rollback verification is slower than the configured SLO envelope.")
        next_actions.append("Trace end-to-end rollback latency from verification decision to restored live experiment policy.")
        confidence += 0.06
    if "critical" in [str(x).lower() for x in escalation_severities]:
        dominant.append("Escalation severity reached critical for the apply-flow contour.")
        hypotheses.append("This contour currently needs bounded remediation and careful human review before more aggressive automation.")
        next_actions.append("Use this bundle to drive bounded RCA and pause additional contour promotion until quality recovers.")
        confidence += 0.05
    if str(bundle.get("trigger_type") or "") == "apply_decision":
        dominant.append("A winner recommendation triggered a bounded apply transition in the dedicated apply-flow contour.")
        hypotheses.append("The transition itself may be valid, but downstream verification and SLO drift determine whether it should persist.")
        next_actions.append("Correlate apply decision, journal entry, and the first verification outcomes after the transition.")
        confidence += 0.05
    if str(bundle.get("trigger_type") or "") == "escalation":
        dominant.append("An escalation summary flagged this apply-flow contour for RCA attention.")
        hypotheses.append("SLO pressure, rollback latency, retry exhaustion, or low verify-keep rate likely accumulated across recent events.")
        next_actions.append("Review the latest SLO rollups, retry state, and escalation reasons embedded in this bundle.")
        confidence += 0.04

    if not dominant:
        dominant.append("The apply-flow contour shows a bounded incident without a single dominant root cause yet.")
        hypotheses.append("Recent evidence is mixed and suggests more verification and sequence analysis is needed.")
        next_actions.append("Continue bounded observation and compare successive apply, verify, rollback, and retry events.")

    confidence = max(0.0, min(round(confidence, 3), 0.95))
    return {
        "schema_version": 1,
        "summary": " ".join(dominant[:3]),
        "dominant_findings": dominant[:5],
        "hypotheses": hypotheses[:5],
        "next_actions": next_actions[:5],
        "confidence": confidence,
        "quality_flags": {
            "provider_mode": provider_mode,
            "bundle_trigger_type": str(bundle.get("trigger_type") or ""),
            "bundle_trigger_severity": str(bundle.get("trigger_severity") or ""),
            "apply_decisions_n": parse_int(summary.get("apply_decisions_n"), 0),
            "verification_events_n": parse_int(summary.get("verification_events_n"), 0),
            "rollback_events_n": parse_int(summary.get("rollback_events_n"), 0),
            "retry_events_n": parse_int(summary.get("retry_events_n"), 0),
            "escalation_events_n": parse_int(summary.get("escalation_events_n"), 0),
        },
    }


def build_result_row(request: Dict[str, Any], bundle: Dict[str, Any], result_payload: Dict[str, Any], provider_mode: str) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "request_id": str(request.get("request_id") or bundle.get("bundle_id") or ""),
        "bundle_id": str(bundle.get("bundle_id") or ""),
        "task_type": "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_result",
        "severity": str(bundle.get("trigger_severity") or "warning"),
        "provider_mode": provider_mode,
        "result_json": stable_json(result_payload),
        "ts_ms": str(now_ms()),
    }


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    return as_dict(await r.hgetall(key))


async def persist_if_configured(db_url: str, request: Dict[str, Any], bundle: Dict[str, Any], result_row: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results (
                    request_id,
                    bundle_id,
                    ts_ms,
                    severity,
                    provider_mode,
                    result_json,
                    request_json,
                    bundle_json
                ) VALUES (
                    %(request_id)s,
                    %(bundle_id)s,
                    %(ts_ms)s,
                    %(severity)s,
                    %(provider_mode)s,
                    %(result_json)s,
                    %(request_json)s,
                    %(bundle_json)s
                )
                """,
                {
                    "request_id": result_row["request_id"],
                    "bundle_id": result_row["bundle_id"],
                    "ts_ms": now_ms(),
                    "severity": result_row["severity"],
                    "provider_mode": result_row["provider_mode"],
                    "result_json": json.dumps(maybe_json(result_row["result_json"], {})),
                    "request_json": json.dumps(request),
                    "bundle_json": json.dumps(bundle),
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
    await ensure_group(r, VERTEX_INPUT_STREAM, GROUP)
    await ensure_group(r, LOCAL_INPUT_STREAM, GROUP)
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        rows = await r.xreadgroup(
            GROUP,
            CONSUMER,
            {VERTEX_INPUT_STREAM: ">", LOCAL_INPUT_STREAM: ">"},
            count=32,
            block=5000,
        )
        if not rows:
            continue
        for stream_name, messages in rows:
            provider_mode = "VERTEX" if stream_name == VERTEX_INPUT_STREAM else "LOCAL"
            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                decision_label = "REJECT"
                try:
                    request = as_dict(payload)
                    bundle = maybe_json(request.get("bundle_json"), {})
                    if not isinstance(bundle, dict):
                        bundle = maybe_json(request.get("input_json"), {})
                    if not isinstance(bundle, dict):
                        bundle = {}
                    if not bundle and request.get("request_id"):
                        bundle = {
                            "bundle_id": request.get("request_id", ""),
                            "trigger_type": request.get("task_type", ""),
                            "trigger_severity": request.get("severity", ""),
                        }
                    policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    try:
                        exec_kill = await r.get('trade:exec_kill_switch')
                        if exec_kill and exec_kill.decode().strip() == '1':
                            policy['kill_switch'] = 1
                    except: pass
                    decision = evaluate_request(bundle, policy)
                    decision_label = decision["decision"]

                    if decision["decision"] == "BUILD_RESULT":
                        result_payload = build_result_payload(bundle, provider_mode)
                        result_row = build_result_row(request, bundle, result_payload, provider_mode)
                        await persist_if_configured(db_url, request, bundle, result_row)
                        await r.xadd(OUTPUT_STREAM, result_row, maxlen=MAXLEN, approximate=True)
                        if RESULTS:
                            RESULTS.labels(severity=decision["severity"] or "unknown", provider_mode=provider_mode).inc()
                        await r.hset(
                            LAST_HASH,
                            mapping={
                                "request_id": result_row["request_id"],
                                "bundle_id": result_row["bundle_id"],
                                "severity": result_row["severity"],
                                "provider_mode": provider_mode,
                                "decision": decision["decision"],
                                "ts_ms": str(now_ms()),
                            },
                        )
                    else:
                        await r.xadd(
                            AUDIT_STREAM,
                            {
                                "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_REJECTED",
                                "decision": decision["decision"],
                                "reason_code": decision["reason_code"],
                                "provider_mode": provider_mode,
                                "severity": decision["severity"] or "",
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
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_FAILED",
                            "error": str(exc),
                            "provider_mode": provider_mode,
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(stream_name, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, decision=decision_label).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
