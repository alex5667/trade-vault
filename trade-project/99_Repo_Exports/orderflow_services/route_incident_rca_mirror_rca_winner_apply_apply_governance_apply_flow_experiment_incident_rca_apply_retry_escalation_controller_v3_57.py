from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_escalation_controller_v3_57"
PROM_PREFIX = "ml_phase3_57_retry_escalation"

BASE_LABELS = {
    "pipeline": "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply",
    "version": "v3"
}
LABEL_NAMES = list(BASE_LABELS.keys())

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
METRICS_PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_PORT", "9994"))
POLL_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_POLL_SEC", "30"))
ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_ADVISORY_ONLY", "1")) != 0
ALLOW_COMMIT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_ALLOW_COMMIT", "0")) != 0
MAX_ATTEMPTS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_MAX_ATTEMPTS", "2"))
BASE_BACKOFF_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_BASE_BACKOFF_SEC", "120"))
COOLDOWN_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_COOLDOWN_SEC", "900"))
GROUP = "cg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_escalation_v3_57"
CONSUMER = os.getenv("HOSTNAME", "incident-rca-apply-retry-v3-57-1")

ALLOWED_REASONS = set(json.loads(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_ALLOWED_REASONS_JSON",
    '["BRIDGE_MODE_MISMATCH_AFTER_APPLY","VERTEX_ONLY_UNDERPERFORMS_AFTER_APPLY","VERTEX_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY","LOCAL_ONLY_UNDERPERFORMS_AFTER_APPLY","LOCAL_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY"]',
)))
WARNING_REASONS = set(json.loads(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_RETRY_WARNING_REASONS_JSON",
    '["BRIDGE_MODE_MISMATCH_AFTER_APPLY"]',
)))

ROLLBACK_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback_journal"
RETRY_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results"
ESCALATION_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations"
RETRY_AUDIT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_audit"
METRICS_HASH = "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry:last"
BRIDGE_POLICY_KEY = "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge:global"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(APP_NAME)


RUNS = Counter(f"{PROM_PREFIX}_runs_total", "Retry/escalation controller runs", ["status", "decision"] + LABEL_NAMES) if Counter else None
LATENCY = Histogram(f"{PROM_PREFIX}_latency_seconds", "Retry/escalation loop latency seconds", LABEL_NAMES) if Histogram else None
UP = Gauge(f"{PROM_PREFIX}_up", "Retry/escalation controller up", LABEL_NAMES) if Gauge else None
LAST_RUN = Gauge(f"{PROM_PREFIX}_last_run_ts_seconds", "Retry/escalation last run ts seconds", LABEL_NAMES) if Gauge else None
RETRIES = Counter(f"{PROM_PREFIX}_retries_total", "Retries total", ["applied", "reason_code"] + LABEL_NAMES) if Counter else None
ESCALATIONS = Counter(f"{PROM_PREFIX}_escalations_total", "Escalations total", ["severity", "reason_code"] + LABEL_NAMES) if Counter else None


def now_ms() -> int:
    return int(time.time() * 1000)


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
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except Exception:
            return default
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default if default is not None else value


def i64(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = maybe_json(row.get("payload"), None)
    if isinstance(payload, dict):
        merged = dict(payload)
        merged.setdefault("_raw", row)
        return merged
    return row


def state_attempts_key(row: Dict[str, Any]) -> str:
    return ":".join([
        "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_attempts",
        str(row.get("rollback_mode") or "UNKNOWN"),
        str(row.get("failed_target_mode") or "UNKNOWN"),
        str(row.get("reason_code") or "UNKNOWN"),
    ])


def state_not_before_key(row: Dict[str, Any]) -> str:
    return ":".join([
        "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_not_before_ms",
        str(row.get("rollback_mode") or "UNKNOWN"),
        str(row.get("failed_target_mode") or "UNKNOWN"),
        str(row.get("reason_code") or "UNKNOWN"),
    ])


def evaluate_action(row: Dict[str, Any], attempts: int, max_attempts: int) -> Dict[str, Any]:
    if str(row.get("decision") or "") != "ROLLBACK_TO_PREVIOUS_MODE":
        return {
            "decision": "HOLD",
            "reason_code": "ROLLBACK_NOT_ACTIONABLE",
            "severity": "info",
        }
    if i64(row.get("applied")) == 1:
        return {
            "decision": "HOLD",
            "reason_code": "ALREADY_ROLLED_BACK",
            "severity": "info",
        }

    reason_code = str(row.get("reason_code") or "UNKNOWN_REASON")
    severity = "warning" if reason_code in WARNING_REASONS else "critical"
    if reason_code in ALLOWED_REASONS and attempts < max_attempts:
        return {
            "decision": "RETRY_ROLLBACK_TO_PREVIOUS_MODE",
            "reason_code": reason_code,
            "severity": severity,
        }
    return {
        "decision": "ESCALATE",
        "reason_code": reason_code,
        "severity": severity,
    }


async def ensure_group(r: "redis.Redis") -> None:
    try:
        await r.xgroup_create(ROLLBACK_STREAM, GROUP, id="0-0", mkstream=True)
    except Exception:
        pass


def backoff_sec(attempts: int) -> int:
    return BASE_BACKOFF_SEC * (2 ** max(0, attempts))


async def maybe_commit_retry(r: "redis.Redis", row: Dict[str, Any], decision: Dict[str, Any]) -> int:
    if ADVISORY_ONLY or not ALLOW_COMMIT:
        return 0
    rollback_mode = str(row.get("rollback_mode") or "").strip()
    if not rollback_mode:
        return 0
    await r.hset(
        BRIDGE_POLICY_KEY,
        mapping={
            "mode": rollback_mode,
            "last_mode_switch_source": APP_NAME,
            "last_mode_switch_reason_code": decision["reason_code"],
            "last_mode_switch_ts_ms": str(now_ms()),
        },
    )
    return 1


def stringify_mapping(obj: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in obj.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False, sort_keys=True)
        else:
            out[k] = str(v)
    return out


async def process_message(r: "redis.Redis", msg_id: Any, row: Dict[str, Any]) -> str:
    attempt_key = state_attempts_key(row)
    not_before_key = state_not_before_key(row)

    attempts = i64(await r.get(attempt_key), 0)
    not_before_ms = i64(await r.get(not_before_key), 0)

    if now_ms() < not_before_ms:
        payload = {
            "schema_version": 1,
            "app_name": APP_NAME,
            "ts_ms": now_ms(),
            "source_rollback_id": row.get("_id", ""),
            "source_rollback_ts_ms": i64(row.get("ts_ms")),
            "rollback_mode": str(row.get("rollback_mode") or ""),
            "failed_target_mode": str(row.get("failed_target_mode") or ""),
            "decision": "HOLD",
            "reason_code": "COOLDOWN_ACTIVE",
            "severity": "info",
            "attempts": attempts,
            "applied": 0,
            "advisory_only": int(ADVISORY_ONLY),
        }
        await r.xadd(RETRY_AUDIT_STREAM, {"payload": json.dumps(payload, ensure_ascii=False, sort_keys=True)}, maxlen=20000, approximate=True)
        await r.hset(METRICS_HASH, mapping=stringify_mapping(payload))
        return "HOLD"

    decision = evaluate_action(row=row, attempts=attempts, max_attempts=MAX_ATTEMPTS)
    applied = 0
    payload = {
        "schema_version": 1,
        "app_name": APP_NAME,
        "ts_ms": now_ms(),
        "source_rollback_id": row.get("_id", ""),
        "source_rollback_ts_ms": i64(row.get("ts_ms")),
        "source_verification_ts_ms": i64(row.get("source_verification_ts_ms") or row.get("verification_ts_ms")),
        "rollback_mode": str(row.get("rollback_mode") or ""),
        "failed_target_mode": str(row.get("failed_target_mode") or ""),
        "decision": decision["decision"],
        "reason_code": decision["reason_code"],
        "severity": decision["severity"],
        "attempts": attempts,
        "applied": 0,
        "advisory_only": int(ADVISORY_ONLY),
    }

    if decision["decision"] == "RETRY_ROLLBACK_TO_PREVIOUS_MODE":
        applied = await maybe_commit_retry(r, row, decision)
        next_attempts = attempts + 1
        payload["attempts"] = next_attempts
        payload["applied"] = applied

        await r.set(attempt_key, str(next_attempts), ex=COOLDOWN_SEC)
        await r.set(not_before_key, str(now_ms() + backoff_sec(attempts) * 1000), ex=COOLDOWN_SEC)

        await r.xadd(RETRY_STREAM, stringify_mapping(payload), maxlen=20000, approximate=True)
        await r.xadd(RETRY_AUDIT_STREAM, {"payload": json.dumps(payload, ensure_ascii=False, sort_keys=True)}, maxlen=20000, approximate=True)
        await r.hset(METRICS_HASH, mapping=stringify_mapping(payload))
        if RETRIES:
            RETRIES.labels(applied=str(applied), reason_code=decision["reason_code"], **BASE_LABELS).inc()
        log.warning(
            "retry_result decision=%s reason_code=%s rollback_mode=%s failed_target_mode=%s attempts=%s applied=%s",
            payload["decision"],
            payload["reason_code"],
            payload["rollback_mode"],
            payload["failed_target_mode"],
            payload["attempts"],
            payload["applied"],
        )
        return payload["decision"]

    if decision["decision"] == "ESCALATE":
        await r.xadd(ESCALATION_STREAM, stringify_mapping(payload), maxlen=20000, approximate=True)
        await r.xadd(RETRY_AUDIT_STREAM, {"payload": json.dumps(payload, ensure_ascii=False, sort_keys=True)}, maxlen=20000, approximate=True)
        await r.hset(METRICS_HASH, mapping=stringify_mapping(payload))
        if ESCALATIONS:
            ESCALATIONS.labels(severity=decision["severity"], reason_code=decision["reason_code"], **BASE_LABELS).inc()
        log.error(
            "escalation reason_code=%s rollback_mode=%s failed_target_mode=%s attempts=%s",
            payload["reason_code"],
            payload["rollback_mode"],
            payload["failed_target_mode"],
            payload["attempts"],
        )
        return payload["decision"]

    await r.xadd(RETRY_AUDIT_STREAM, {"payload": json.dumps(payload, ensure_ascii=False, sort_keys=True)}, maxlen=20000, approximate=True)
    await r.hset(METRICS_HASH, mapping=stringify_mapping(payload))
    return payload["decision"]


async def loop() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")

    start_http_server(METRICS_PORT)
    if UP:
        UP.labels(**BASE_LABELS).set(1)

    r = redis.from_url(REDIS_URL, decode_responses=False)
    await ensure_group(r)

    try:
        while True:
            t0 = time.time()
            status = "ok"
            decision_label = "none"
            try:
                rows = await r.xreadgroup(
                    groupname=GROUP,
                    consumername=CONSUMER,
                    streams={ROLLBACK_STREAM: ">"},
                    count=50,
                    block=POLL_SEC * 1000,
                )
                if not rows:
                    if LAST_RUN:
                        LAST_RUN.labels(**BASE_LABELS).set(time.time())
                    continue

                for _, messages in rows:
                    for msg_id, fields in messages:
                        raw = as_dict(fields)
                        raw["_id"] = msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
                        row = normalize_row(raw)
                        row.setdefault("_id", raw["_id"])
                        decision_label = await process_message(r, msg_id, row)
                        await r.xack(ROLLBACK_STREAM, GROUP, msg_id)
                        if LAST_RUN:
                            LAST_RUN.labels(**BASE_LABELS).set(time.time())
            except Exception:
                status = "error"
                log.exception("retry/escalation loop failed")
            finally:
                if RUNS:
                    RUNS.labels(status=status, decision=decision_label, **BASE_LABELS).inc()
                if LATENCY:
                    LATENCY.labels(**BASE_LABELS).observe(max(0.0, time.time() - t0))
            await asyncio.sleep(0.01)
    finally:
        if UP:
            UP.labels(**BASE_LABELS).set(0)
        await r.close()


def main() -> None:
    asyncio.run(loop())


if __name__ == "__main__":
    main()
