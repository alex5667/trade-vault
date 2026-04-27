from __future__ import annotations

import asyncio
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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_result_consumer_v3_54"
VERTEX_INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_VERTEX_RCA_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_vertex_rca_requests",
)
LOCAL_INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_LOCAL_RCA_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_local_rca_requests",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results:global",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_PORT", "9988"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_MAXLEN", "20000"))

DEFAULT_HANDLER_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_HANDLER_MODE",
    "DETERMINISTIC",
).upper()
DEFAULT_ALLOW_SEVERITIES = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_ALLOW_SEVERITIES",
    "warning,critical",
)
DEFAULT_MAX_BUNDLE_BYTES = int(
    os.getenv(
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_MAX_BUNDLE_BYTES",
        "262144",
    )
)
ALLOWED_HANDLER_MODES = {"DETERMINISTIC", "DISABLED"}


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_runs_total",
    "Apply-flow experiment incident RCA result consumer runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_latency_seconds",
    "Apply-flow experiment incident RCA result consumer latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_up",
    "Apply-flow experiment incident RCA result consumer up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_last_run_ts_seconds",
    "Apply-flow experiment incident RCA result consumer last run timestamp",
)
RESULTS_TOTAL = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_total",
    "Apply-flow experiment incident RCA results total",
    ("provider_mode", "severity", "trigger_type"),
)


def now_ms() -> int:
    return int(time.time() * 1000)


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


def _default_allow_severities() -> set[str]:
    return {x.strip().lower() for x in DEFAULT_ALLOW_SEVERITIES.split(",") if x.strip()}


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(raw.get("handler_mode") or DEFAULT_HANDLER_MODE).upper()
    if mode not in ALLOWED_HANDLER_MODES:
        mode = DEFAULT_HANDLER_MODE
    allow_severities = maybe_json(raw.get("allow_severities_json"), list(_default_allow_severities()))
    if not isinstance(allow_severities, list):
        allow_severities = list(_default_allow_severities())
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
    val = bundle.get("summary", {})
    return val if isinstance(val, dict) else {}


def _evidence(bundle: Dict[str, Any]) -> Dict[str, Any]:
    val = bundle.get("evidence", {})
    return val if isinstance(val, dict) else {}


def build_result_payload(bundle: Dict[str, Any], provider_mode: str) -> Dict[str, Any]:
    trigger_type = str(bundle.get("trigger_type") or "")
    trigger_reason = str(bundle.get("trigger_reason_code") or "")
    summary = _summary(bundle)
    evidence = _evidence(bundle)
    latest_verification = evidence.get("latest_verification", {})
    latest_rollback = evidence.get("latest_rollback", {})
    latest_retry = evidence.get("latest_retry", {})
    latest_escalation = evidence.get("latest_escalation", {})
    latest_slo = evidence.get("latest_slo_rollup", {})

    dominant: list[str] = []
    hypotheses: list[str] = []
    next_actions: list[str] = []

    verify_keep_rate = parse_float(summary.get("verify_keep_rate"), 0.0)
    rollback_mttr_p95_sec = parse_float(summary.get("rollback_mttr_p95_sec"), 0.0)
    escalation_rate = parse_float(summary.get("escalation_rate"), 0.0)

    if trigger_type == "rollback":
        dominant.append("Verification concluded that the experiment contour should roll back to the previous profile.")
        hypotheses.append("The applied target profile did not translate cleanly into stable post-apply behavior.")
        next_actions.append("Compare applied target weights with observed post-apply exposure share by arm.")
    if trigger_reason == "TARGET_SHARE_TOO_LOW_AFTER_APPLY":
        dominant.append("Observed target share after apply remained too low.")
        hypotheses.append("The bridge kept routing too much traffic away from the intended incumbent arm.")
        next_actions.append("Inspect post-apply incident routing split and severity routing correctness.")
    if trigger_reason == "WEIGHTS_MISMATCH_AFTER_APPLY":
        dominant.append("Live experiment weights diverged from the intended apply target.")
        hypotheses.append("A config write or later automation step overrode the intended profile.")
        next_actions.append("Trace config writes, apply journal entries, and competing controllers.")
    if trigger_reason == "INCUMBENT_MISMATCH_AFTER_APPLY":
        dominant.append("Incumbent arm did not match the intended apply target.")
        hypotheses.append("Winner/incumbent metadata and live routing policy drifted apart after apply.")
        next_actions.append("Reconcile bridge policy, winner policy, and journal ordering.")
    if parse_int(summary.get("retry_events_n"), 0) > 0:
        dominant.append("The safety contour has already attempted bounded retry for this incident family.")
        hypotheses.append("The initial apply result may be unstable even after one bounded reapply attempt.")
        next_actions.append("Check retry attempts, attempt counters, and whether escalation thresholds were crossed.")
    if parse_int(summary.get("escalation_events_n"), 0) > 0:
        dominant.append("Escalation has already been triggered inside the dedicated experiment safety contour.")
        hypotheses.append("The issue is likely not a one-off verification blip and deserves operator attention.")
        next_actions.append("Use the escalation payload together with the RCA result before changing routing policy again.")
    if verify_keep_rate < 0.60:
        dominant.append("Verify-keep rate is degraded for this experiment contour.")
        hypotheses.append("Recent applies are not surviving the full post-apply verification loop.")
        next_actions.append("Review recent verification failures by reason_code and route profile.")
    if rollback_mttr_p95_sec > 900:
        dominant.append("Rollback MTTR p95 is above the desired envelope.")
        hypotheses.append("Rollback completion is slower than expected, increasing time spent in degraded state.")
        next_actions.append("Measure delay from verification failure to rollback completion.")
    if escalation_rate > 0.20:
        dominant.append("Escalation rate is elevated for this contour.")
        hypotheses.append("The current experiment routing strategy may be operationally noisy or fragile.")
        next_actions.append("Consider reducing aggressiveness before more live routing changes.")

    if provider_mode == "VERTEX":
        dominant = dominant[:5]
        hypotheses = hypotheses[:5]
        next_actions = next_actions[:5]
        confidence = 0.78
    else:
        dominant = dominant[:3]
        hypotheses = hypotheses[:3]
        next_actions = next_actions[:3]
        confidence = 0.67

    if not dominant:
        dominant.append("The dedicated experiment incident bundle is valid, but the dominant failure mechanism is still ambiguous.")
        hypotheses.append("Evidence is mixed and needs bounded inspection across verification and rollback records.")
        next_actions.append("Keep the contour isolated and compare usefulness of local versus vertex RCA outputs.")

    return {
        "schema_version": 1,
        "summary": " ".join(dominant[:3]),
        "dominant_findings": dominant,
        "hypotheses": hypotheses,
        "next_actions": next_actions,
        "confidence": confidence,
        "quality_flags": {
            "provider_mode": provider_mode,
            "trigger_type": trigger_type,
            "trigger_reason_code": trigger_reason,
            "trigger_severity": str(bundle.get("trigger_severity") or ""),
            "verification_events_n": parse_int(summary.get("verification_events_n"), 0),
            "rollback_events_n": parse_int(summary.get("rollback_events_n"), 0),
            "retry_events_n": parse_int(summary.get("retry_events_n"), 0),
            "escalation_events_n": parse_int(summary.get("escalation_events_n"), 0),
            "verify_keep_rate": verify_keep_rate,
            "rollback_mttr_p95_sec": rollback_mttr_p95_sec,
            "escalation_rate": escalation_rate,
            "latest_verification_reason": str(latest_verification.get("reason_code") or ""),
            "latest_rollback_reason": str(latest_rollback.get("reason_code") or ""),
            "latest_retry_reason": str(latest_retry.get("reason_code") or ""),
            "latest_escalation_reason": str(latest_escalation.get("reason_code") or ""),
            "latest_slo_snapshot_present": 1 if isinstance(latest_slo, dict) and len(latest_slo) > 0 else 0,
        },
    }


def build_result_row(request: Dict[str, Any], bundle: Dict[str, Any], result_payload: Dict[str, Any], provider_mode: str) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "request_id": str(request.get("request_id") or ""),
        "bundle_id": str(bundle.get("bundle_id") or ""),
        "provider_mode": provider_mode,
        "severity": str(bundle.get("trigger_severity") or "warning"),
        "trigger_type": str(bundle.get("trigger_type") or ""),
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
                INSERT INTO llm_p354_rca_use_results (
                    request_id, bundle_id, ts_ms, severity, trigger_type, provider_mode,
                    result_json, request_json, bundle_json
                ) VALUES (
                    %(request_id)s, %(bundle_id)s, %(ts_ms)s, %(severity)s, %(trigger_type)s, %(provider_mode)s,
                    %(result_json)s, %(request_json)s, %(bundle_json)s
                )
                """,
                {
                    "request_id": result_row["request_id"],
                    "bundle_id": result_row["bundle_id"],
                    "ts_ms": now_ms(),
                    "severity": result_row["severity"],
                    "trigger_type": result_row["trigger_type"],
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
                    if not bundle and request.get("bundle_id"):
                        bundle = {
                            "bundle_id": request.get("bundle_id", ""),
                            "trigger_type": request.get("trigger_type", ""),
                            "trigger_severity": request.get("severity", ""),
                        }
                    policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    decision = evaluate_request(bundle, policy)
                    decision_label = decision["decision"]

                    if decision["decision"] == "BUILD_RESULT":
                        result_payload = build_result_payload(bundle, provider_mode)
                        result_row = build_result_row(request, bundle, result_payload, provider_mode)
                        await persist_if_configured(db_url, request, bundle, result_row)
                        await r.xadd(OUTPUT_STREAM, result_row, maxlen=MAXLEN, approximate=True)
                        if RESULTS_TOTAL:
                            RESULTS_TOTAL.labels(
                                provider_mode=provider_mode,
                                severity=decision["severity"] or "unknown",
                                trigger_type=str(bundle.get("trigger_type") or "unknown"),
                            ).inc()
                        await r.hset(
                            LAST_HASH,
                            mapping={
                                "request_id": result_row["request_id"],
                                "bundle_id": result_row["bundle_id"],
                                "provider_mode": provider_mode,
                                "decision": decision["decision"],
                                "ts_ms": str(now_ms()),
                            },
                        )
                    else:
                        await r.xadd(
                            AUDIT_STREAM,
                            {
                                "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_REJECTED",
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
                            "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_FAILED",
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
