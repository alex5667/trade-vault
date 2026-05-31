from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from core.redis_keys import RedisKeyPrefixes as RK
from utils.time_utils import get_ny_time_millis

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


APP_NAME = "route_incident_rca_mirror_rollout_controller_v3_9"
GOVERNOR_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_governor_decisions",
)
VERIFICATION_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_verification_results",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rollout_decisions",
)
JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rollout_journal",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rollout_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rollout:last",
)
SHADOW_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_shadow_handoff:global",
)
ROLLOUT_STATE_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_STATE_KEY",
    "state:ml:route_incident_rca_mirror_rollout:state",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rollout:global",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_PORT", "9925"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_MAXLEN", "20000"))

DEFAULT_ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_ADVISORY_ONLY", "1"))
DEFAULT_EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_EXECUTOR_MODE", "DRY_RUN").upper()
DEFAULT_PROMOTION_COOLDOWN_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_PROMOTION_COOLDOWN_SEC", "21600"))
DEFAULT_ALLOW_GOVERNOR_PROMOTION = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_ALLOW_GOVERNOR_PROMOTION", "1"))
DEFAULT_ALLOW_VERIFICATION_ROLLBACK = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_ALLOW_VERIFICATION_ROLLBACK", "1"))

# Database mapping convention
DB_URL = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

ROLLOUT_STATES = {
    "AUDIT_ONLY_STABLE",
    "PROMOTION_APPLIED",
    "MIRROR_ACTIVE",
    "ROLLBACK_APPLIED",
    "UNKNOWN",
},


def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rollout_runs_total",
    "Route incident RCA mirror rollout controller runs",
    ("status", "decision", "source"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rollout_latency_seconds",
    "Route incident RCA mirror rollout controller latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rollout_up",
    "Route incident RCA mirror rollout controller up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rollout_last_run_ts_seconds",
    "Route incident RCA mirror rollout controller last run timestamp",
)
TRANSITIONS = _counter(
    "ml_route_incident_rca_mirror_rollout_transitions_total",
    "Route incident RCA mirror rollout transitions",
    ("transition_type", "mode"),
)
CURRENT_MODE = _gauge(
    "ml_route_incident_rca_mirror_rollout_current_mode",
    "Route incident RCA mirror rollout current mode",
    ("mode",),
)
CURRENT_STATE = _gauge(
    "ml_route_incident_rca_mirror_rollout_current_state",
    "Route incident RCA mirror rollout current state",
    ("state",),
)


def now_ms() -> int:
    return get_ny_time_millis()


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def as_dict(fields: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
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


def policy_from_hash(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
        "promotion_cooldown_sec": parse_int(raw.get("promotion_cooldown_sec"), DEFAULT_PROMOTION_COOLDOWN_SEC),
        "allow_governor_promotion": parse_int(raw.get("allow_governor_promotion"), DEFAULT_ALLOW_GOVERNOR_PROMOTION),
        "allow_verification_rollback": parse_int(raw.get("allow_verification_rollback"), DEFAULT_ALLOW_VERIFICATION_ROLLBACK),
    }


def shadow_mode_from_hash(raw: dict[str, Any]) -> str:
    return (raw.get("mode") or "AUDIT_ONLY").upper()


def rollout_state_from_hash(raw: dict[str, Any], current_mode: str) -> str:
    state = (raw.get("rollout_state") or "").upper()
    if state in ROLLOUT_STATES:
        return state
    if current_mode == "AUDIT_ONLY":
        return "AUDIT_ONLY_STABLE"
    if current_mode == "MIRROR":
        return "MIRROR_ACTIVE"
    return "UNKNOWN"


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


def normalize_event(source: str, row: dict[str, Any]) -> dict[str, Any]:
    decision = (row.get("decision") or "").upper()
    reason_code = (row.get("reason_code") or "UNKNOWN")
    current_mode = (row.get("current_mode") or "").upper()
    target_mode = str(row.get("target_mode") or current_mode).upper()
    return {
        "source": source,
        "decision": decision,
        "reason_code": reason_code,
        "current_mode": current_mode,
        "target_mode": target_mode,
        "row": row,
    }


def evaluate_event(
    *,
    event: dict[str, Any],
    current_mode: str,
    rollout_state: str,
    last_transition_ts_ms: int,
    policy: dict[str, Any],
    now_ts_ms: int,
) -> dict[str, Any]:
    source = event["source"]
    decision = event["decision"]
    reason_code = event["reason_code"]
    promotion_cooldown_active = (
        last_transition_ts_ms > 0 and (now_ts_ms - last_transition_ts_ms) < policy["promotion_cooldown_sec"] * 1000
    )

    out = {
        "controller_decision": "HOLD",
        "controller_reason_code": "NO_CHANGE",
        "source": source,
        "current_mode": current_mode,
        "current_rollout_state": rollout_state,
        "target_mode": current_mode,
        "target_rollout_state": rollout_state,
        "transition_type": "NONE",
        "cooldown_active": 1 if promotion_cooldown_active else 0,
    }

    if source == "governor":
        if policy["allow_governor_promotion"] != 1:
            out["controller_reason_code"] = "GOVERNOR_PROMOTION_DISABLED"
            return out
        if decision != "PROMOTE_TO_MIRROR":
            out["controller_reason_code"] = "GOVERNOR_NO_PROMOTION"
            return out
        if current_mode != "AUDIT_ONLY":
            out["controller_reason_code"] = "PROMOTION_NOT_FROM_AUDIT"
            return out
        if promotion_cooldown_active:
            out["controller_reason_code"] = "PROMOTION_COOLDOWN_ACTIVE"
            return out
        out["controller_decision"] = "PROMOTE"
        out["controller_reason_code"] = reason_code
        out["target_mode"] = "MIRROR"
        out["target_rollout_state"] = "PROMOTION_APPLIED"
        out["transition_type"] = "AUDIT_TO_MIRROR"
        return out

    if source == "verification":
        if policy["allow_verification_rollback"] != 1:
            out["controller_reason_code"] = "VERIFICATION_ROLLBACK_DISABLED"
            return out
        if decision != "ROLLBACK_TO_AUDIT":
            out["controller_reason_code"] = "VERIFICATION_NO_ROLLBACK"
            return out
        if current_mode != "MIRROR":
            out["controller_reason_code"] = "ROLLBACK_NOT_FROM_MIRROR"
            return out
        out["controller_decision"] = "ROLLBACK"
        out["controller_reason_code"] = reason_code
        out["target_mode"] = "AUDIT_ONLY"
        out["target_rollout_state"] = "ROLLBACK_APPLIED"
        out["transition_type"] = "MIRROR_TO_AUDIT"
        return out

    out["controller_reason_code"] = "UNKNOWN_SOURCE"
    return out


async def apply_transition(
    r: Any,
    evaluation: dict[str, Any],
) -> None:
    await r.hset(
        SHADOW_POLICY_KEY,
        mapping={
            "mode": evaluation["target_mode"],
            "last_mode_switch_ts_ms": str(now_ms()),
            "last_mode_switch_reason_code": evaluation["controller_reason_code"],
            "last_mode_switch_source": APP_NAME,
        }
    )
    await r.hset(
        ROLLOUT_STATE_KEY,
        mapping={
            "rollout_state": evaluation["target_rollout_state"],
            "last_transition_ts_ms": str(now_ms()),
            "last_transition_type": evaluation["transition_type"],
            "last_transition_reason_code": evaluation["controller_reason_code"],
            "last_transition_source": evaluation["source"],
        }
    )


async def persist_if_configured(
    db_url: str,
    event: dict[str, Any],
    evaluation: dict[str, Any],
    snapshot: dict[str, Any],
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rollout_decisions (
                    ts_ms,
                    source,
                    event_decision,
                    event_reason_code,
                    controller_decision,
                    controller_reason_code,
                    current_mode,
                    target_mode,
                    current_rollout_state,
                    target_rollout_state,
                    advisory_only,
                    executor_mode,
                    snapshot_json
                ) VALUES (
                    %(ts_ms)s,
                    %(source)s,
                    %(event_decision)s,
                    %(event_reason_code)s,
                    %(controller_decision)s,
                    %(controller_reason_code)s,
                    %(current_mode)s,
                    %(target_mode)s,
                    %(current_rollout_state)s,
                    %(target_rollout_state)s,
                    %(advisory_only)s,
                    %(executor_mode)s,
                    %(snapshot_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "source": event["source"],
                    "event_decision": event["decision"],
                    "event_reason_code": event["reason_code"],
                    "controller_decision": evaluation["controller_decision"],
                    "controller_reason_code": evaluation["controller_reason_code"],
                    "current_mode": evaluation["current_mode"],
                    "target_mode": evaluation["target_mode"],
                    "current_rollout_state": evaluation["current_rollout_state"],
                    "target_rollout_state": evaluation["target_rollout_state"],
                    "advisory_only": snapshot["policy"]["advisory_only"],
                    "executor_mode": snapshot["policy"]["executor_mode"],
                    "snapshot_json": json.dumps(snapshot),
                }
            )
            if evaluation["transition_type"] != "NONE":
                cur.execute(
                    """

                    INSERT INTO llm_route_incident_rca_mirror_rollout_journal (
                        ts_ms,
                        transition_type,
                        source,
                        reason_code,
                        mode_before,
                        mode_after,
                        state_before,
                        state_after,
                        snapshot_json
                    ) VALUES (
                        %(ts_ms)s,
                        %(transition_type)s,
                        %(source)s,
                        %(reason_code)s,
                        %(mode_before)s,
                        %(mode_after)s,
                        %(state_before)s,
                        %(state_after)s,
                        %(snapshot_json)s
                    )
                    """,
                    {
                        "ts_ms": now_ms(),
                        "transition_type": evaluation["transition_type"],
                        "source": event["source"],
                        "reason_code": evaluation["controller_reason_code"],
                        "mode_before": evaluation["current_mode"],
                        "mode_after": evaluation["target_mode"],
                        "state_before": evaluation["current_rollout_state"],
                        "state_after": evaluation["target_rollout_state"],
                        "snapshot_json": json.dumps(snapshot),
                    }
                )
            conn.commit()


async def update_current_labels(current_mode: str, rollout_state: str) -> None:
    if CURRENT_MODE:
        for mode in ("AUDIT_ONLY", "MIRROR"):
            CURRENT_MODE.labels(mode=mode).set(1 if current_mode == mode else 0)
    if CURRENT_STATE:
        for state in ("AUDIT_ONLY_STABLE", "PROMOTION_APPLIED", "MIRROR_ACTIVE", "ROLLBACK_APPLIED", "UNKNOWN"):
            CURRENT_STATE.labels(state=state).set(1 if rollout_state == state else 0)


async def process_event(
    r: Any,
    db_url: str,
    source: str,
    row: dict[str, Any],
) -> None:
    shadow_policy = as_dict(await r.hgetall(SHADOW_POLICY_KEY))
    rollout_state_raw = as_dict(await r.hgetall(ROLLOUT_STATE_KEY))
    policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
    try:
        exec_kill = await r.get(RK.EXEC_KILL_SWITCH)
        if exec_kill and exec_kill.decode().strip() == '1':
            policy['kill_switch'] = 1
    except Exception: pass
    current_mode = shadow_mode_from_hash(shadow_policy)
    rollout_state = rollout_state_from_hash(rollout_state_raw, current_mode)
    last_transition_ts_ms = parse_int(rollout_state_raw.get("last_transition_ts_ms"), 0)

    event = normalize_event(source, row)
    evaluation = evaluate_event(
        event=event,
        current_mode=current_mode,
        rollout_state=rollout_state,
        last_transition_ts_ms=last_transition_ts_ms,
        policy=policy,
        now_ts_ms=now_ms(),
    )

    snapshot = {
        "event": event,
        "evaluation": evaluation,
        "policy": policy,
        "shadow_policy_before": shadow_policy,
        "rollout_state_before": rollout_state_raw,
    }

    if (
        evaluation["transition_type"] != "NONE"
        and policy["advisory_only"] == 0
        and policy["executor_mode"] == "COMMIT"
    ):
        await apply_transition(r, evaluation)
        if TRANSITIONS:
            TRANSITIONS.labels(
                transition_type=evaluation["transition_type"],
                mode=evaluation["target_mode"],
            ).inc()

    await persist_if_configured(db_url, event, evaluation, snapshot)

    out = {
        "schema_version": 1,
        "source": event["source"],
        "event_decision": event["decision"],
        "controller_decision": evaluation["controller_decision"],
        "controller_reason_code": evaluation["controller_reason_code"],
        "current_mode": evaluation["current_mode"],
        "target_mode": evaluation["target_mode"],
        "current_rollout_state": evaluation["current_rollout_state"],
        "target_rollout_state": evaluation["target_rollout_state"],
        "transition_type": evaluation["transition_type"],
        "snapshot_json": stable_json(snapshot),
        "ts_ms": str(now_ms()),
    }
    await r.xadd(DECISIONS_STREAM, out, maxlen=MAXLEN, approximate=True)
    await r.xadd(
        JOURNAL_STREAM,
        {
            "schema_version": 1,
            "source": event["source"],
            "transition_type": evaluation["transition_type"],
            "mode_before": evaluation["current_mode"],
            "mode_after": evaluation["target_mode"],
            "state_before": evaluation["current_rollout_state"],
            "state_after": evaluation["target_rollout_state"],
            "reason_code": evaluation["controller_reason_code"],
            "ts_ms": str(now_ms()),
        }, maxlen=MAXLEN,
        approximate=True,
    )
    await r.xadd(
        AUDIT_STREAM,
        {
            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_CONTROLLER_DECIDED",
            **out,
        }, maxlen=MAXLEN,
        approximate=True,
    )
    await r.hset(
        LAST_HASH,
        mapping={
            "source": event["source"],
            "event_decision": event["decision"],
            "controller_decision": evaluation["controller_decision"],
            "controller_reason_code": evaluation["controller_reason_code"],
            "current_mode": evaluation["current_mode"],
            "target_mode": evaluation["target_mode"],
            "current_rollout_state": evaluation["current_rollout_state"],
            "target_rollout_state": evaluation["target_rollout_state"],
            "transition_type": evaluation["transition_type"],
            "ts_ms": str(now_ms()),
        }
    )
    await update_current_labels(evaluation["target_mode"], evaluation["target_rollout_state"])


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = DB_URL
    await ensure_group(r, GOVERNOR_STREAM, GROUP)
    await ensure_group(r, VERIFICATION_STREAM, GROUP)

    while True:
        rows = await r.xreadgroup(
            GROUP,
            CONSUMER,
            {GOVERNOR_STREAM: ">", VERIFICATION_STREAM: ">"},
            count=64,
            block=5000,
        )
        if not rows:
            continue
        for stream_name, messages in rows:
            source = "governor" if stream_name == GOVERNOR_STREAM else "verification"
            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                decision_label = "HOLD"
                try:
                    row = as_dict(payload)
                    await process_event(r, db_url, source, row)
                    await r.xack(stream_name, GROUP, msg_id)
                    last_hash = as_dict(await r.hgetall(LAST_HASH))
                    decision_label = (last_hash.get("controller_decision") or "HOLD")
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_CONTROLLER_FAILED",
                            "source": source,
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(stream_name, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, decision=decision_label, source=source).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
