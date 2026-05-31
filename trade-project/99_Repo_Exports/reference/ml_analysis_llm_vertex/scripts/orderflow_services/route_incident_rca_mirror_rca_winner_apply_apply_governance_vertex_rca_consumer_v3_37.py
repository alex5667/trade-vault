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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_consumer_v3_37"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_requests",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_results",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca:global",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_PORT", "9964"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_MAXLEN", "20000"))

DEFAULT_HANDLER_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_HANDLER_MODE", "DETERMINISTIC").upper()
DEFAULT_MAX_BUNDLE_BYTES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_MAX_BUNDLE_BYTES", "131072"))
DEFAULT_ALLOW_SEVERITIES = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_ALLOW_SEVERITIES", "warning,critical")
ALLOWED_HANDLER_MODES = {"DETERMINISTIC", "DISABLED"}


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_runs_total",
    "Winner-apply apply governance vertex RCA consumer runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_latency_seconds",
    "Winner-apply apply governance vertex RCA consumer latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_up",
    "Winner-apply apply governance vertex RCA consumer up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_last_run_ts_seconds",
    "Winner-apply apply governance vertex RCA consumer last run timestamp",
)
RESULTS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_results_total",
    "Winner-apply apply governance vertex RCA results",
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


def _reason_lists(bundle: Dict[str, Any]) -> Tuple[List[str], List[str], List[str], List[str]]:
    summary = bundle.get("summary", {}) if isinstance(bundle.get("summary"), dict) else {}
    verification = summary.get("verification_reason_codes", [])
    retry = summary.get("retry_reason_codes", [])
    rollback = summary.get("rollback_reason_codes", [])
    escalation_severities = summary.get("escalation_severities", [])
    if not isinstance(verification, list):
        verification = []
    if not isinstance(retry, list):
        retry = []
    if not isinstance(rollback, list):
        rollback = []
    if not isinstance(escalation_severities, list):
        escalation_severities = []
    return [str(x) for x in verification], [str(x) for x in retry], [str(x) for x in rollback], [str(x) for x in escalation_severities]


def _slo_reason_codes(bundle: Dict[str, Any]) -> List[str]:
    evidence = bundle.get("evidence", {}) if isinstance(bundle.get("evidence"), dict) else {}
    slo_recent = evidence.get("slo_recent", [])
    out: List[str] = []
    if isinstance(slo_recent, list):
        for row in slo_recent[:5]:
            if not isinstance(row, dict):
                continue
            parsed = maybe_json(row.get("reason_codes_json"), [])
            if isinstance(parsed, list):
                out.extend(str(x) for x in parsed)
    return sorted({x for x in out if x})


def build_result_payload(bundle: Dict[str, Any]) -> Dict[str, Any]:
    severity = str(bundle.get("trigger_severity") or "warning").lower()
    verification_reasons, retry_reasons, rollback_reasons, escalation_severities = _reason_lists(bundle)
    slo_reasons = _slo_reason_codes(bundle)

    dominant: List[str] = []
    hypotheses: List[str] = []
    next_actions: List[str] = []
    confidence = 0.60

    if "POLICY_MISMATCH_AFTER_APPLY" in verification_reasons:
        dominant.append("The live experiment policy did not match the intended governance target after apply.")
        hypotheses.append("Controller intent was recorded, but the governance contour did not converge to the target live policy.")
        next_actions.append("Compare intended target policy versus live governance policy and inspect post-apply state propagation.")
        confidence += 0.10
    if "PRIMARY_MATCH_RATE_TOO_LOW" in verification_reasons:
        dominant.append("Post-apply exposures did not converge strongly enough to the target primary arm.")
        hypotheses.append("Primary routing after governance apply remains unstable or multiple producers still leak conflicting primary state.")
        next_actions.append("Inspect recent exposures and arm assignment after the governance apply decision.")
        confidence += 0.10
    if "UNEXPECTED_PRIMARY_RATE_TOO_HIGH" in verification_reasons:
        dominant.append("Unexpected primary exposures remained too high after governance apply.")
        hypotheses.append("Non-target arms continued to emit primary traffic after the new governance primary was promoted.")
        next_actions.append("Audit arm assignment rules and stale publishers emitting primary traffic.")
        confidence += 0.08
    if "SHADOW_EXPOSURES_PRESENT_IN_SINGLE_ARM" in verification_reasons:
        dominant.append("Single-arm governance mode still emitted shadow exposures.")
        hypotheses.append("Single-arm enforcement is incomplete or shadow publishers were not fully disabled.")
        next_actions.append("Verify single-arm gating and disable residual shadow publishers before another apply.")
        confidence += 0.09
    if "MAX_ATTEMPTS_REACHED" in retry_reasons or "RETRY_EXHAUSTED" in retry_reasons:
        dominant.append("Rollback re-apply retry budget was exhausted without converging to the rollback target.")
        hypotheses.append("The governance rollback path is unstable or idempotent rollback convergence is broken.")
        next_actions.append("Freeze further governance apply attempts and inspect retry plus rollback state.")
        confidence += 0.11
    if "ROLLBACK_MTTR_P95_HIGH" in slo_reasons or "ROLLBACK_MTTR_SLO_BREACH" in slo_reasons:
        dominant.append("Rollback MTTR p95 breached the configured governance SLO envelope.")
        hypotheses.append("Rollback completion or rollback confirmation is too slow under governance incidents.")
        next_actions.append("Instrument rollback latency end-to-end and review verification cadence versus rollback completion.")
        confidence += 0.06
    if "VERIFY_KEEP_RATE_LOW" in slo_reasons:
        dominant.append("Verify-keep rate is low, indicating weak post-apply governance stability.")
        hypotheses.append("Governance applies are accepted too early relative to contour stability.")
        next_actions.append("Raise governance apply thresholds or keep the contour longer in advisory mode before commit.")
        confidence += 0.07
    if "APPLY_RATE_LOW" in slo_reasons:
        dominant.append("Apply rate is below expected, indicating requested governance changes do not consistently become effective.")
        hypotheses.append("Controller decisions are produced, but live governance state is not consistently updated.")
        next_actions.append("Compare controller decisions versus journaled applies and inspect blocked governance applies.")
        confidence += 0.05
    if "critical" in [x.lower() for x in escalation_severities]:
        dominant.append("Escalation severity reached critical for the governance contour.")
        hypotheses.append("This governance contour should remain isolated from routine automation until quality recovers.")
        next_actions.append("Prefer bounded local fallback RCA for subsequent governance bundles if Vertex becomes degraded.")
        confidence += 0.05

    if not dominant:
        dominant.append("No single hard failure dominates; the governance contour appears conditionally stable.")
        hypotheses.append("Recent governance signals are mixed but not conclusively degraded.")
        next_actions.append("Continue bounded observation and collect more post-apply governance feedback before tightening automation.")

    confidence = max(0.0, min(round(confidence, 3), 0.95))
    return {
        "schema_version": 1,
        "summary": " ".join(dominant[:3]),
        "dominant_findings": dominant[:5],
        "hypotheses": hypotheses[:5],
        "next_actions": next_actions[:5],
        "confidence": confidence,
        "quality_flags": {
            "bundle_trigger_type": str(bundle.get("trigger_type") or ""),
            "bundle_trigger_severity": severity,
            "verification_reason_codes_n": len(verification_reasons),
            "retry_reason_codes_n": len(retry_reasons),
            "rollback_reason_codes_n": len(rollback_reasons),
            "slo_reason_codes_n": len(slo_reasons),
            "escalation_events_n": parse_int(bundle.get("summary", {}).get("escalation_events_n"), 0) if isinstance(bundle.get("summary"), dict) else 0,
        },
    }


def build_result_row(request: Dict[str, Any], bundle: Dict[str, Any], result_payload: Dict[str, Any], provider_mode: str) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "request_id": str(request.get("request_id") or bundle.get("bundle_id") or ""),
        "bundle_id": str(bundle.get("bundle_id") or ""),
        "task_type": "route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_result",
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
                INSERT INTO llm_governance_vertex_rca_results (
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
                    request = as_dict(payload)
                    bundle = maybe_json(request.get("bundle_json"), {})
                    if not isinstance(bundle, dict):
                        bundle = {}
                    if not bundle and request.get("bundle_id"):
                        bundle = {
                            "bundle_id": request.get("bundle_id", ""),
                            "trigger_type": request.get("trigger_type", ""),
                            "trigger_severity": request.get("trigger_severity", ""),
                        }
                    policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    decision = evaluate_request(bundle, policy)
                    decision_label = decision["decision"]

                    if decision["decision"] == "BUILD_RESULT":
                        provider_mode = policy["handler_mode"]
                        result_payload = build_result_payload(bundle)
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
                                "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_REJECTED",
                                "decision": decision["decision"],
                                "reason_code": decision["reason_code"],
                                "severity": decision["severity"] or "",
                                "ts_ms": str(now_ms()),
                            },
                            maxlen=MAXLEN,
                            approximate=True,
                        )

                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_FAILED",
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
    asyncio.run(main())
