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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_escalation_controller_v3_51"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_results",
)
RETRY_RESULTS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_results",
)
ESCALATIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ESCALATIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_escalations",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry:global",
)
EXPERIMENT_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:global",
)
WINNER_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:global",
)
STATE_KEY_PREFIX = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_STATE_KEY_PREFIX",
    "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_PORT", "9985"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_MAXLEN", "20000"))

DEFAULT_ADVISORY_ONLY = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_ADVISORY_ONLY",
    "1",
))
DEFAULT_EXECUTOR_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_EXECUTOR_MODE",
    "DRY_RUN",
).upper()
DEFAULT_ALLOW_COMMIT = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_ALLOW_COMMIT",
    "0",
))
DEFAULT_MAX_ATTEMPTS = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_MAX_ATTEMPTS",
    "2",
))
DEFAULT_ALLOWED_RETRY_REASONS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_ALLOWED_REASONS_JSON",
    '["TARGET_SHARE_TOO_LOW_AFTER_APPLY"]',
)
DEFAULT_WARNING_REASONS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_WARNING_REASONS_JSON",
    '["TARGET_SHARE_TOO_LOW_AFTER_APPLY"]',
)


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_runs_total",
    "Apply-flow experiment retry runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_latency_seconds",
    "Apply-flow experiment retry latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_up",
    "Apply-flow experiment retry up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_last_run_ts_seconds",
    "Apply-flow experiment retry last run timestamp",
)
RETRIES_TOTAL = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retries_total",
    "Apply-flow experiment retries total",
    ("decision", "applied"),
)
ESCALATIONS_TOTAL = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_escalations_total",
    "Apply-flow experiment escalations total",
    ("severity", "reason_code"),
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


def normalize_weights(raw: Any) -> Dict[str, int]:
    obj = maybe_json(raw, {})
    if not isinstance(obj, dict):
        obj = {}
    return {
        "vertex_primary_weight": parse_int(obj.get("vertex_primary_weight"), 0),
        "vertex_compact_weight": parse_int(obj.get("vertex_compact_weight"), 0),
        "local_candidate_weight": parse_int(obj.get("local_candidate_weight"), 0),
    }


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    allowed_retry = maybe_json(raw.get("allowed_retry_reasons_json"), maybe_json(DEFAULT_ALLOWED_RETRY_REASONS_JSON, []))
    warning_reasons = maybe_json(raw.get("warning_reasons_json"), maybe_json(DEFAULT_WARNING_REASONS_JSON, []))
    if not isinstance(allowed_retry, list):
        allowed_retry = []
    if not isinstance(warning_reasons, list):
        warning_reasons = []
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
        "allow_commit": parse_int(raw.get("allow_commit"), DEFAULT_ALLOW_COMMIT),
        "max_attempts": parse_int(raw.get("max_attempts"), DEFAULT_MAX_ATTEMPTS),
        "allowed_retry_reasons": {str(x) for x in allowed_retry},
        "warning_reasons": {str(x) for x in warning_reasons},
    }


def build_state_key(target_profile: str, target_incumbent_arm: str, reason_code: str) -> str:
    return f"{STATE_KEY_PREFIX}:{target_profile}:{target_incumbent_arm}:{reason_code}"


def evaluate_action(verification_row: Dict[str, Any], attempts: int, policy: Dict[str, Any]) -> Dict[str, Any]:
    reason_code = str(verification_row.get("reason_code") or "")
    decision = str(verification_row.get("decision") or "")
    out = {
        "decision": "HOLD",
        "reason_code": "NO_ACTION",
        "severity": "info",
        "target_profile": str(verification_row.get("target_profile") or ""),
        "target_incumbent_arm": str(verification_row.get("target_incumbent_arm") or ""),
        "target_weights": normalize_weights(verification_row.get("target_weights_json")),
    }
    if policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if decision != "ROLLBACK_PREVIOUS_PROFILE":
        out["reason_code"] = "VERIFICATION_NOT_ACTIONABLE"
        return out
    if reason_code in policy["allowed_retry_reasons"] and attempts < policy["max_attempts"]:
        out["decision"] = "RETRY_REAPPLY_TARGET_PROFILE"
        out["reason_code"] = reason_code
        out["severity"] = "warning"
        return out
    out["decision"] = "ESCALATE"
    out["reason_code"] = reason_code or "UNKNOWN_REASON"
    out["severity"] = "warning" if reason_code in policy["warning_reasons"] else "critical"
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def persist_if_configured(db_url: str, verification_row: Dict[str, Any], action_out: Dict[str, Any], attempts: int, applied: int) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            if action_out["decision"] == "RETRY_REAPPLY_TARGET_PROFILE":
                cur.execute(
                    """
                    INSERT INTO ml_route_rca_experiment_retry_results_v51 (
                        ts_ms, decision, reason_code, target_profile, target_incumbent_arm,
                        target_weights_json, attempts, applied, result_json
                    ) VALUES (
                        %(ts_ms)s, %(decision)s, %(reason_code)s, %(target_profile)s, %(target_incumbent_arm)s,
                        %(target_weights_json)s, %(attempts)s, %(applied)s, %(result_json)s
                    )
                    """,
                    {
                        "ts_ms": now_ms(),
                        "decision": action_out["decision"],
                        "reason_code": action_out["reason_code"],
                        "target_profile": action_out["target_profile"],
                        "target_incumbent_arm": action_out["target_incumbent_arm"],
                        "target_weights_json": json.dumps(action_out["target_weights"]),
                        "attempts": attempts,
                        "applied": applied,
                        "result_json": json.dumps({"verification_row": verification_row, "action_out": action_out}),
                    },
                )
            if action_out["decision"] == "ESCALATE":
                cur.execute(
                    """
                    INSERT INTO ml_route_rca_experiment_escalations_v51 (
                        ts_ms, severity, reason_code, target_profile, target_incumbent_arm, escalation_json
                    ) VALUES (
                        %(ts_ms)s, %(severity)s, %(reason_code)s, %(target_profile)s, %(target_incumbent_arm)s, %(escalation_json)s
                    )
                    """,
                    {
                        "ts_ms": now_ms(),
                        "severity": action_out["severity"],
                        "reason_code": action_out["reason_code"],
                        "target_profile": action_out["target_profile"],
                        "target_incumbent_arm": action_out["target_incumbent_arm"],
                        "escalation_json": json.dumps({"verification_row": verification_row, "action_out": action_out, "attempts": attempts}),
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
                decision_label = "HOLD"
                try:
                    verification_row = as_dict(payload)
                    policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
                    state_key = build_state_key(
                        str(verification_row.get("target_profile") or ""),
                        str(verification_row.get("target_incumbent_arm") or ""),
                        str(verification_row.get("reason_code") or ""),
                    )
                    attempts = parse_int(await r.get(state_key), 0)
                    action_out = evaluate_action(verification_row, attempts, policy)
                    decision_label = action_out["decision"]

                    applied = 0
                    if action_out["decision"] == "RETRY_REAPPLY_TARGET_PROFILE":
                        attempts += 1
                        await r.set(state_key, str(attempts))
                        if (
                            policy["advisory_only"] == 0
                            and policy["executor_mode"] == "COMMIT"
                            and policy["allow_commit"] == 1
                        ):
                            weights = action_out["target_weights"]
                            await r.hset(
                                EXPERIMENT_POLICY_KEY,
                                mapping={
                                    "mode": "SHADOW",
                                    "vertex_primary_weight": str(weights["vertex_primary_weight"]),
                                    "vertex_compact_weight": str(weights["vertex_compact_weight"]),
                                    "local_candidate_weight": str(weights["local_candidate_weight"]),
                                    "last_weight_rebalance_ts_ms": str(now_ms()),
                                    "last_weight_rebalance_source": APP_NAME,
                                    "last_weight_rebalance_reason_code": action_out["reason_code"],
                                    "last_weight_rebalance_profile": action_out["target_profile"],
                                },
                            )
                            await r.hset(
                                WINNER_POLICY_KEY,
                                mapping={
                                    "incumbent_arm": action_out["target_incumbent_arm"],
                                    "last_incumbent_apply_ts_ms": str(now_ms()),
                                    "last_incumbent_apply_source": APP_NAME,
                                    "last_incumbent_apply_reason_code": action_out["reason_code"],
                                },
                            )
                            applied = 1
                        await r.xadd(
                            RETRY_RESULTS_STREAM,
                            {
                                "schema_version": 1,
                                "decision": action_out["decision"],
                                "reason_code": action_out["reason_code"],
                                "target_profile": action_out["target_profile"],
                                "target_incumbent_arm": action_out["target_incumbent_arm"],
                                "target_weights_json": stable_json(action_out["target_weights"]),
                                "attempts": str(attempts),
                                "applied": str(applied),
                                "source_verification_ts_ms": str(parse_int(verification_row.get("ts_ms"), 0)),
                                "ts_ms": str(now_ms()),
                            },
                            maxlen=MAXLEN,
                            approximate=True,
                        )
                        if RETRIES_TOTAL:
                            RETRIES_TOTAL.labels(decision=action_out["decision"], applied=str(applied)).inc()
                    elif action_out["decision"] == "ESCALATE":
                        await r.xadd(
                            ESCALATIONS_STREAM,
                            {
                                "schema_version": 1,
                                "severity": action_out["severity"],
                                "reason_code": action_out["reason_code"],
                                "target_profile": action_out["target_profile"],
                                "target_incumbent_arm": action_out["target_incumbent_arm"],
                                "attempts": str(attempts),
                                "source_verification_ts_ms": str(parse_int(verification_row.get("ts_ms"), 0)),
                                "ts_ms": str(now_ms()),
                            },
                            maxlen=MAXLEN,
                            approximate=True,
                        )
                        if ESCALATIONS_TOTAL:
                            ESCALATIONS_TOTAL.labels(severity=action_out["severity"], reason_code=action_out["reason_code"]).inc()

                    await persist_if_configured(db_url, verification_row, action_out, attempts, applied)
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "decision": action_out["decision"],
                            "reason_code": action_out["reason_code"],
                            "severity": action_out["severity"],
                            "target_profile": action_out["target_profile"],
                            "attempts": str(attempts),
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        },
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "APPLY_FLOW_EXPERIMENT_RETRY_ESCALATION",
                            "decision": action_out["decision"],
                            "reason_code": action_out["reason_code"],
                            "severity": action_out["severity"],
                            "attempts": str(attempts),
                            "applied": str(applied),
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
                        {"event_type": "APPLY_FLOW_EXPERIMENT_RETRY_FAILED", "error": str(exc), "ts_ms": str(now_ms())},
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
