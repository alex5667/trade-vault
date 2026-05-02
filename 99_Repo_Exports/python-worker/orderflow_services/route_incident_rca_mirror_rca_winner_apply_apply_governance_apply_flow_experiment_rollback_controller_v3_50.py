from __future__ import annotations
from utils.time_utils import get_ny_time_millis

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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_controller_v3_50"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_results",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_journal",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback:global",
)
EXPERIMENT_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:global",
)
WINNER_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:global",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_PORT", "9983"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_MAXLEN", "20000"))

DEFAULT_ADVISORY_ONLY = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_ADVISORY_ONLY",
    "1",
))
DEFAULT_EXECUTOR_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_EXECUTOR_MODE",
    "DRY_RUN",
).upper()
DEFAULT_ALLOW_COMMIT = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_ALLOW_COMMIT",
    "0",
))
DEFAULT_ALLOWED_REASONS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_ALLOWED_REASONS_JSON",
    '["WEIGHTS_MISMATCH_AFTER_APPLY","INCUMBENT_MISMATCH_AFTER_APPLY","TARGET_SHARE_TOO_LOW_AFTER_APPLY"]',
)


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_runs_total",
    "Apply-flow experiment rollback runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_latency_seconds",
    "Apply-flow experiment rollback latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_up",
    "Apply-flow experiment rollback up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_last_run_ts_seconds",
    "Apply-flow experiment rollback last run timestamp",
)
ROLLBACKS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollbacks_total",
    "Apply-flow experiment rollbacks total",
    ("applied", "reason_code"),
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
    allowed_reasons = maybe_json(raw.get("allowed_reasons_json"), maybe_json(DEFAULT_ALLOWED_REASONS_JSON, []))
    if not isinstance(allowed_reasons, list):
        allowed_reasons = []
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
        "allow_commit": parse_int(raw.get("allow_commit"), DEFAULT_ALLOW_COMMIT),
        "allowed_reasons": {str(x) for x in allowed_reasons},
    }


def evaluate_rollback(verification_row: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
    decision = str(verification_row.get("decision") or "")
    reason_code = str(verification_row.get("reason_code") or "")
    rollback_profile = str(verification_row.get("rollback_profile") or "unknown_profile")
    rollback_incumbent_arm = str(verification_row.get("rollback_incumbent_arm") or "vertex_primary")
    rollback_weights = normalize_weights(verification_row.get("rollback_weights_json"))

    out = {
        "decision": "HOLD",
        "reason_code": "NO_ROLLBACK",
        "rollback_profile": rollback_profile,
        "rollback_incumbent_arm": rollback_incumbent_arm,
        "rollback_weights": rollback_weights,
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
    if reason_code not in policy["allowed_reasons"]:
        out["reason_code"] = "REASON_NOT_ALLOWED"
        return out
    out["decision"] = "ROLLBACK_TO_PREVIOUS_PROFILE"
    out["reason_code"] = reason_code
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def persist_if_configured(db_url: str, rollback_out: Dict[str, Any], verification_row: Dict[str, Any], applied: int) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_rr_winner_apply_gov_exp_rollback_journal (
                    ts_ms, decision, reason_code, rollback_profile, rollback_incumbent_arm,
                    rollback_weights_json, applied, journal_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(rollback_profile)s, %(rollback_incumbent_arm)s,
                    %(rollback_weights_json)s, %(applied)s, %(journal_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": rollback_out["decision"],
                    "reason_code": rollback_out["reason_code"],
                    "rollback_profile": rollback_out["rollback_profile"],
                    "rollback_incumbent_arm": rollback_out["rollback_incumbent_arm"],
                    "rollback_weights_json": json.dumps(rollback_out["rollback_weights"]),
                    "applied": applied,
                    "journal_json": json.dumps({"rollback_out": rollback_out, "verification_row": verification_row}),
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
                    rollback_policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
                    rollback_out = evaluate_rollback(verification_row, rollback_policy)
                    decision_label = rollback_out["decision"]

                    applied = 0
                    if (
                        rollback_out["decision"] == "ROLLBACK_TO_PREVIOUS_PROFILE"
                        and rollback_policy["advisory_only"] == 0
                        and rollback_policy["executor_mode"] == "COMMIT"
                        and rollback_policy["allow_commit"] == 1
                    ):
                        await r.hset(
                            EXPERIMENT_POLICY_KEY,
                            mapping={
                                "mode": "SHADOW",
                                "vertex_primary_weight": str(rollback_out["rollback_weights"]["vertex_primary_weight"]),
                                "vertex_compact_weight": str(rollback_out["rollback_weights"]["vertex_compact_weight"]),
                                "local_candidate_weight": str(rollback_out["rollback_weights"]["local_candidate_weight"]),
                                "last_weight_rebalance_ts_ms": str(now_ms()),
                                "last_weight_rebalance_source": APP_NAME,
                                "last_weight_rebalance_reason_code": rollback_out["reason_code"],
                                "last_weight_rebalance_profile": rollback_out["rollback_profile"],
                            }
                        )
                        await r.hset(
                            WINNER_POLICY_KEY,
                            mapping={
                                "incumbent_arm": rollback_out["rollback_incumbent_arm"],
                                "last_incumbent_apply_ts_ms": str(now_ms()),
                                "last_incumbent_apply_source": APP_NAME,
                                "last_incumbent_apply_reason_code": rollback_out["reason_code"],
                            }
                        )
                        applied = 1

                    await persist_if_configured(db_url, rollback_out, verification_row, applied)
                    await r.xadd(
                        OUTPUT_STREAM,
                        {
                            "schema_version": 1,
                            "decision": rollback_out["decision"],
                            "reason_code": rollback_out["reason_code"],
                            "rollback_profile": rollback_out["rollback_profile"],
                            "rollback_incumbent_arm": rollback_out["rollback_incumbent_arm"],
                            "rollback_weights_json": stable_json(rollback_out["rollback_weights"]),
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "decision": rollback_out["decision"],
                            "reason_code": rollback_out["reason_code"],
                            "rollback_profile": rollback_out["rollback_profile"],
                            "rollback_incumbent_arm": rollback_out["rollback_incumbent_arm"],
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        }
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_DECIDED",
                            "decision": rollback_out["decision"],
                            "reason_code": rollback_out["reason_code"],
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    if ROLLBACKS:
                        ROLLBACKS.labels(applied=str(applied), reason_code=rollback_out["reason_code"]).inc()
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_FAILED",
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
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
