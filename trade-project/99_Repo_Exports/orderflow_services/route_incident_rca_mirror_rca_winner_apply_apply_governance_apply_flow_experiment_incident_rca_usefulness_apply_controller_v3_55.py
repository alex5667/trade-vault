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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_apply_controller_v3_55"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_decisions",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_decisions",
)
JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_journal",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller:global",
)
BRIDGE_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_BRIDGE_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge:global",
)
ROLLBACK_STATE_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_ROLLBACK_STATE_KEY",
    "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller:rollback_ready",
)
ACTION_STATE_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_ACTION_STATE_KEY",
    "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller:last_action",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_PORT", "9990"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_MAXLEN", "20000"))

DEFAULT_ADVISORY_ONLY = int(
    os.getenv(
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_ADVISORY_ONLY",
        "1",
    )
)
DEFAULT_EXECUTOR_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_EXECUTOR_MODE",
    "DRY_RUN",
).upper()
DEFAULT_ALLOW_COMMIT = int(
    os.getenv(
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_ALLOW_COMMIT",
        "0",
    )
)
DEFAULT_ALLOWED_TARGET_MODES_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_ALLOWED_TARGET_MODES_JSON",
    '["AUTO","VERTEX_ONLY","LOCAL_ONLY"]',
)
DEFAULT_COOLDOWN_SEC = int(
    os.getenv(
        "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_COOLDOWN_SEC",
        "21600",
    )
)


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_runs_total",
    "Apply-flow experiment incident RCA apply-controller runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_latency_seconds",
    "Apply-flow experiment incident RCA apply-controller latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_up",
    "Apply-flow experiment incident RCA apply-controller up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_last_run_ts_seconds",
    "Apply-flow experiment incident RCA apply-controller last run timestamp",
)
APPLIES_TOTAL = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_applies_total",
    "Apply-flow experiment incident RCA bridge-mode applies total",
    ("target_mode", "applied"),
)
CURRENT_BRIDGE_MODE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_current_bridge_mode",
    "Apply-flow experiment incident RCA apply-controller current bridge mode",
    ("mode",),
)


def now_ms() -> int:
    return int(time.time() * 1000)


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


def normalize_mode(mode: str) -> str:
    value = str(mode or "AUTO").upper()
    return value if value in {"AUTO", "VERTEX_ONLY", "LOCAL_ONLY", "DISABLED"} else "AUTO"


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    allowed_target_modes = maybe_json(raw.get("allowed_target_modes_json"), maybe_json(DEFAULT_ALLOWED_TARGET_MODES_JSON, []))
    if not isinstance(allowed_target_modes, list):
        allowed_target_modes = ["AUTO", "VERTEX_ONLY", "LOCAL_ONLY"]
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
        "allow_commit": parse_int(raw.get("allow_commit"), DEFAULT_ALLOW_COMMIT),
        "cooldown_sec": parse_int(raw.get("cooldown_sec"), DEFAULT_COOLDOWN_SEC),
        "allowed_target_modes": {normalize_mode(str(x)) for x in allowed_target_modes},
    }


def target_mode_from_usefulness_decision(decision: str, current_bridge_mode: str) -> str:
    mapping = {
        "PREFER_VERTEX_ONLY": "VERTEX_ONLY",
        "PREFER_LOCAL_ONLY": "LOCAL_ONLY",
        "RETURN_TO_AUTO": "AUTO",
        "KEEP_AUTO": "AUTO",
        "KEEP_VERTEX_ONLY": "VERTEX_ONLY",
        "KEEP_LOCAL_ONLY": "LOCAL_ONLY",
    }
    return mapping.get(decision, current_bridge_mode)


def evaluate_apply(decision_row: Dict[str, Any], current_bridge_mode: str, policy: Dict[str, Any], cooldown_active: bool) -> Dict[str, Any]:
    usefulness_decision = str(decision_row.get("decision") or "")
    reason_code = str(decision_row.get("reason_code") or "")
    target_bridge_mode = target_mode_from_usefulness_decision(usefulness_decision, current_bridge_mode)

    out = {
        "decision": "HOLD",
        "reason_code": "NO_CHANGE",
        "usefulness_decision": usefulness_decision,
        "usefulness_reason_code": reason_code,
        "current_bridge_mode": current_bridge_mode,
        "target_bridge_mode": target_bridge_mode,
        "cooldown_active": 1 if cooldown_active else 0,
    }

    if policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if usefulness_decision in {"HOLD", ""}:
        out["reason_code"] = "USEFULNESS_HOLD"
        return out
    if target_bridge_mode not in policy["allowed_target_modes"]:
        out["reason_code"] = "TARGET_MODE_NOT_ALLOWED"
        return out
    if cooldown_active:
        out["reason_code"] = "COOLDOWN_ACTIVE"
        return out
    if target_bridge_mode == current_bridge_mode:
        out["decision"] = "KEEP_CURRENT_MODE"
        out["reason_code"] = "TARGET_ALREADY_ACTIVE"
        return out
    if target_bridge_mode == "VERTEX_ONLY":
        out["decision"] = "APPLY_VERTEX_ONLY"
        out["reason_code"] = reason_code or "VERTEX_BETTER_THAN_LOCAL"
        return out
    if target_bridge_mode == "LOCAL_ONLY":
        out["decision"] = "APPLY_LOCAL_ONLY"
        out["reason_code"] = reason_code or "LOCAL_BETTER_THAN_VERTEX"
        return out
    if target_bridge_mode == "AUTO":
        out["decision"] = "APPLY_AUTO"
        out["reason_code"] = reason_code or "RETURN_TO_AUTO"
        return out
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    return as_dict(await r.hgetall(key))


async def persist_if_configured(db_url: str, decision_out: Dict[str, Any], usefulness_row: Dict[str, Any], applied: int) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_p355_rca_ctrl_decs (
                    ts_ms, usefulness_decision, usefulness_reason_code, decision, reason_code,
                    current_bridge_mode, target_bridge_mode, applied, decision_json
                ) VALUES (
                    %(ts_ms)s, %(usefulness_decision)s, %(usefulness_reason_code)s, %(decision)s, %(reason_code)s,
                    %(current_bridge_mode)s, %(target_bridge_mode)s, %(applied)s, %(decision_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "usefulness_decision": decision_out["usefulness_decision"],
                    "usefulness_reason_code": decision_out["usefulness_reason_code"],
                    "decision": decision_out["decision"],
                    "reason_code": decision_out["reason_code"],
                    "current_bridge_mode": decision_out["current_bridge_mode"],
                    "target_bridge_mode": decision_out["target_bridge_mode"],
                    "applied": applied,
                    "decision_json": json.dumps({"decision_out": decision_out, "usefulness_row": usefulness_row}),
                },
            )
            cur.execute(
                """
                INSERT INTO llm_p355_rca_ctrl_journal (
                    ts_ms, decision, reason_code, current_bridge_mode, target_bridge_mode,
                    rollback_ready_json, applied, journal_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(current_bridge_mode)s, %(target_bridge_mode)s,
                    %(rollback_ready_json)s, %(applied)s, %(journal_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": decision_out["decision"],
                    "reason_code": decision_out["reason_code"],
                    "current_bridge_mode": decision_out["current_bridge_mode"],
                    "target_bridge_mode": decision_out["target_bridge_mode"],
                    "rollback_ready_json": json.dumps(
                        {
                            "previous_mode": decision_out["current_bridge_mode"],
                            "target_mode": decision_out["target_bridge_mode"],
                        }
                    ),
                    "applied": applied,
                    "journal_json": json.dumps({"decision_out": decision_out, "usefulness_row": usefulness_row}),
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
                    usefulness_row = as_dict(payload)
                    apply_policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    bridge_policy = await read_hash(r, BRIDGE_POLICY_KEY)
                    current_mode = normalize_mode(bridge_policy.get("mode", "AUTO"))
                    action_state = await read_hash(r, ACTION_STATE_KEY)
                    last_action_ts_ms = parse_int(action_state.get("last_action_ts_ms"), 0)
                    cooldown_active = last_action_ts_ms > 0 and (now_ms() - last_action_ts_ms) < apply_policy["cooldown_sec"] * 1000

                    decision_out = evaluate_apply(usefulness_row, current_mode, apply_policy, cooldown_active)
                    decision_label = decision_out["decision"]

                    applied = 0
                    if (
                        decision_out["decision"] in {"APPLY_VERTEX_ONLY", "APPLY_LOCAL_ONLY", "APPLY_AUTO"}
                        and apply_policy["advisory_only"] == 0
                        and apply_policy["executor_mode"] == "COMMIT"
                        and apply_policy["allow_commit"] == 1
                    ):
                        previous_mode = current_mode
                        target_mode = decision_out["target_bridge_mode"]
                        await r.hset(
                            BRIDGE_POLICY_KEY,
                            mapping={
                                "mode": target_mode,
                                "last_mode_switch_source": APP_NAME,
                                "last_mode_switch_reason_code": decision_out["reason_code"],
                                "last_mode_switch_ts_ms": str(now_ms()),
                            },
                        )
                        await r.hset(
                            ROLLBACK_STATE_KEY,
                            mapping={
                                "previous_mode": previous_mode,
                                "current_mode_before_apply": previous_mode,
                                "target_mode": target_mode,
                                "applied_by": APP_NAME,
                                "applied_reason_code": decision_out["reason_code"],
                                "applied_ts_ms": str(now_ms()),
                            },
                        )
                        await r.hset(
                            ACTION_STATE_KEY,
                            mapping={
                                "last_action_ts_ms": str(now_ms()),
                                "last_decision": decision_out["decision"],
                                "last_reason_code": decision_out["reason_code"],
                            },
                        )
                        applied = 1

                    await persist_if_configured(db_url, decision_out, usefulness_row, applied)
                    await r.xadd(
                        DECISIONS_STREAM,
                        {
                            "schema_version": 1,
                            "usefulness_decision": decision_out["usefulness_decision"],
                            "usefulness_reason_code": decision_out["usefulness_reason_code"],
                            "decision": decision_out["decision"],
                            "reason_code": decision_out["reason_code"],
                            "current_bridge_mode": decision_out["current_bridge_mode"],
                            "target_bridge_mode": decision_out["target_bridge_mode"],
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xadd(
                        JOURNAL_STREAM,
                        {
                            "schema_version": 1,
                            "decision": decision_out["decision"],
                            "reason_code": decision_out["reason_code"],
                            "current_bridge_mode": decision_out["current_bridge_mode"],
                            "target_bridge_mode": decision_out["target_bridge_mode"],
                            "rollback_ready_json": stable_json(
                                {
                                    "previous_mode": decision_out["current_bridge_mode"],
                                    "target_mode": decision_out["target_bridge_mode"],
                                }
                            ),
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "decision": decision_out["decision"],
                            "reason_code": decision_out["reason_code"],
                            "current_bridge_mode": decision_out["current_bridge_mode"],
                            "target_bridge_mode": decision_out["target_bridge_mode"],
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        },
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_DECIDED",
                            "decision": decision_out["decision"],
                            "reason_code": decision_out["reason_code"],
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    if APPLIES_TOTAL:
                        APPLIES_TOTAL.labels(target_mode=decision_out["target_bridge_mode"], applied=str(applied)).inc()
                    if CURRENT_BRIDGE_MODE:
                        for mode in ("AUTO", "VERTEX_ONLY", "LOCAL_ONLY", "DISABLED"):
                            CURRENT_BRIDGE_MODE.labels(mode=mode).set(1 if current_mode == mode else 0)
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_FAILED",
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
