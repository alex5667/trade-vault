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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_apply_controller_v3_32"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions",
)
JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_journal",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller:global",
)
EXPERIMENT_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment:global",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_PORT", "9957"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_MAXLEN", "20000"))

DEFAULT_ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_ADVISORY_ONLY", "1"))
DEFAULT_EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_EXECUTOR_MODE", "DRY_RUN").upper()
DEFAULT_APPLY_STRATEGY = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_STRATEGY", "SHADOW_PRIMARY").upper()
DEFAULT_COOLDOWN_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_COOLDOWN_SEC", "21600"))
DEFAULT_MIN_WINNER_SCORE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_MIN_WINNER_SCORE", "0.0"))
DEFAULT_ALLOW_ARMS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_ALLOW_ARMS_JSON",
    '["vertex_candidate","local_fallback_candidate"]',
)

ALLOWED_APPLY_STRATEGIES = {"SHADOW_PRIMARY", "SINGLE_ARM"}
PROMOTE_DECISION_TO_ARM = {
    "PROMOTE_VERTEX_CANDIDATE": "vertex_candidate",
    "PROMOTE_LOCAL_FALLBACK_CANDIDATE": "local_fallback_candidate",
},
ALL_ARMS = {"deterministic", "vertex_candidate", "local_fallback_candidate"}


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_controller_runs_total",
    "Winner-apply apply controller runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_controller_latency_seconds",
    "Winner-apply apply controller latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_controller_up",
    "Winner-apply apply controller up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_controller_last_run_ts_seconds",
    "Winner-apply apply controller last run timestamp",
)
TRANSITIONS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_controller_transitions_total",
    "Winner-apply apply controller transitions",
    ("apply_strategy", "winner_arm"),
)
CURRENT_MODE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_controller_current_mode",
    "Winner-apply apply controller current experiment mode",
    ("mode",),
)
CURRENT_PRIMARY_ARM = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_controller_current_primary_arm",
    "Winner-apply apply controller current primary arm",
    ("arm",),
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


def default_allow_arms() -> list[str]:
    parsed = maybe_json(DEFAULT_ALLOW_ARMS_JSON, [])
    if not isinstance(parsed, list):
        parsed = []
    arms = [str(x) for x in parsed if str(x) in ALL_ARMS]
    return arms or ["vertex_candidate", "local_fallback_candidate"]


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    apply_strategy = str(raw.get("apply_strategy") or DEFAULT_APPLY_STRATEGY).upper()
    if apply_strategy not in ALLOWED_APPLY_STRATEGIES:
        apply_strategy = DEFAULT_APPLY_STRATEGY
    allow_arms = maybe_json(raw.get("allow_arms_json"), default_allow_arms())
    if not isinstance(allow_arms, list):
        allow_arms = default_allow_arms()
    allow_arms_out = [str(x) for x in allow_arms if str(x) in ALL_ARMS]
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
        "apply_strategy": apply_strategy,
        "cooldown_sec": parse_int(raw.get("cooldown_sec"), DEFAULT_COOLDOWN_SEC),
        "min_winner_score": parse_float(raw.get("min_winner_score"), DEFAULT_MIN_WINNER_SCORE),
        "allow_arms": allow_arms_out,
    }


def experiment_policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(raw.get("mode") or "SHADOW").upper()
    primary_arm = str(raw.get("primary_arm") or "deterministic")
    shadow_arms = maybe_json(raw.get("shadow_arms_json"), ["vertex_candidate", "local_fallback_candidate"])
    if not isinstance(shadow_arms, list):
        shadow_arms = ["vertex_candidate", "local_fallback_candidate"]
    last_switch_ts_ms = parse_int(raw.get("last_mode_switch_ts_ms"), 0)
    return {
        "mode": mode,
        "primary_arm": primary_arm if primary_arm in ALL_ARMS else "deterministic",
        "shadow_arms": [str(x) for x in shadow_arms if str(x) in ALL_ARMS],
        "last_mode_switch_ts_ms": last_switch_ts_ms,
    }


def evaluate_apply(
    *,
    recommendation: Dict[str, Any],
    controller_policy: Dict[str, Any],
    experiment_policy: Dict[str, Any],
    now_ts_ms: int,
) -> Dict[str, Any]:
    decision = str(recommendation.get("decision") or "")
    winner_arm = str(recommendation.get("winner_arm") or "")
    winner_score = parse_float(recommendation.get("winner_score"), 0.0)
    current_mode = experiment_policy["mode"]
    current_primary_arm = experiment_policy["primary_arm"]
    cooldown_active = (
        experiment_policy["last_mode_switch_ts_ms"] > 0
        and (now_ts_ms - experiment_policy["last_mode_switch_ts_ms"]) < controller_policy["cooldown_sec"] * 1000
    )

    out = {
        "decision": "HOLD",
        "reason_code": "NO_CHANGE",
        "current_mode": current_mode,
        "current_primary_arm": current_primary_arm,
        "target_mode": current_mode,
        "target_primary_arm": current_primary_arm,
        "target_shadow_arms": experiment_policy["shadow_arms"],
        "apply_strategy": controller_policy["apply_strategy"],
        "winner_arm": winner_arm,
        "winner_score": winner_score,
        "cooldown_active": 1 if cooldown_active else 0,
    }

    if controller_policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if controller_policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if decision not in PROMOTE_DECISION_TO_ARM:
        out["reason_code"] = "RECOMMENDATION_NOT_PROMOTION"
        return out
    if winner_arm not in PROMOTE_DECISION_TO_ARM.values():
        out["reason_code"] = "WINNER_ARM_NOT_ALLOWED_KIND"
        return out
    if winner_arm not in controller_policy["allow_arms"]:
        out["reason_code"] = "WINNER_ARM_NOT_ALLOWED"
        return out
    if winner_score < controller_policy["min_winner_score"]:
        out["reason_code"] = "WINNER_SCORE_TOO_LOW"
        return out
    if cooldown_active:
        out["reason_code"] = "COOLDOWN_ACTIVE"
        return out

    if current_mode not in {"SHADOW", "SINGLE_ARM"}:
        out["reason_code"] = "CURRENT_MODE_NOT_SUPPORTED"
        return out

    if controller_policy["apply_strategy"] == "SHADOW_PRIMARY":
        if current_mode == "SHADOW" and current_primary_arm == winner_arm:
            out["reason_code"] = "PRIMARY_ALREADY_ACTIVE"
            return out
        out["decision"] = "APPLY_PRIMARY_ARM_SHADOW"
        out["reason_code"] = "PROMOTE_WINNER_TO_SHADOW_PRIMARY"
        out["target_mode"] = "SHADOW"
        out["target_primary_arm"] = winner_arm
        out["target_shadow_arms"] = [arm for arm in ["deterministic", "vertex_candidate", "local_fallback_candidate"] if arm != winner_arm]
        return out

    if controller_policy["apply_strategy"] == "SINGLE_ARM":
        if current_mode == "SINGLE_ARM" and current_primary_arm == winner_arm:
            out["reason_code"] = "SINGLE_ARM_ALREADY_ACTIVE"
            return out
        out["decision"] = "APPLY_SINGLE_ARM"
        out["reason_code"] = "PROMOTE_WINNER_TO_SINGLE_ARM"
        out["target_mode"] = "SINGLE_ARM"
        out["target_primary_arm"] = winner_arm
        out["target_shadow_arms"] = []
        return out

    out["reason_code"] = "UNKNOWN_APPLY_STRATEGY"
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    return as_dict(await r.hgetall(key))


async def update_mode_metrics(mode: str, primary_arm: str) -> None:
    if CURRENT_MODE:
        for m in ("SHADOW", "SINGLE_ARM", "MULTI_ARM", "DISABLED"):
            CURRENT_MODE.labels(mode=m).set(1 if mode == m else 0)
    if CURRENT_PRIMARY_ARM:
        for arm in ("deterministic", "vertex_candidate", "local_fallback_candidate"):
            CURRENT_PRIMARY_ARM.labels(arm=arm).set(1 if primary_arm == arm else 0)


async def apply_change(r: Any, evaluation: Dict[str, Any]) -> None:
    mapping = {
        "mode": evaluation["target_mode"],
        "primary_arm": evaluation["target_primary_arm"],
        "shadow_arms_json": stable_json(evaluation["target_shadow_arms"]),
        "last_mode_switch_ts_ms": str(now_ms()),
        "last_mode_switch_source": APP_NAME,
        "last_mode_switch_reason_code": evaluation["reason_code"],
    }
    await r.hset(EXPERIMENT_POLICY_KEY, mapping=mapping)


async def persist_if_configured(
    db_url: str,
    recommendation: Dict[str, Any],
    evaluation: Dict[str, Any],
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions (
                    ts_ms,
                    decision,
                    reason_code,
                    current_mode,
                    current_primary_arm,
                    target_mode,
                    target_primary_arm,
                    apply_strategy,
                    winner_arm,
                    winner_score,
                    recommendation_json,
                    evaluation_json
                ) VALUES (
                    %(ts_ms)s,
                    %(decision)s,
                    %(reason_code)s,
                    %(current_mode)s,
                    %(current_primary_arm)s,
                    %(target_mode)s,
                    %(target_primary_arm)s,
                    %(apply_strategy)s,
                    %(winner_arm)s,
                    %(winner_score)s,
                    %(recommendation_json)s,
                    %(evaluation_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": evaluation["decision"],
                    "reason_code": evaluation["reason_code"],
                    "current_mode": evaluation["current_mode"],
                    "current_primary_arm": evaluation["current_primary_arm"],
                    "target_mode": evaluation["target_mode"],
                    "target_primary_arm": evaluation["target_primary_arm"],
                    "apply_strategy": evaluation["apply_strategy"],
                    "winner_arm": evaluation["winner_arm"],
                    "winner_score": evaluation["winner_score"],
                    "recommendation_json": json.dumps(recommendation),
                    "evaluation_json": json.dumps(evaluation),
                }
            )
            if evaluation["decision"] in {"APPLY_PRIMARY_ARM_SHADOW", "APPLY_SINGLE_ARM"}:
                cur.execute(
                    """

                    INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_controller_journal (
                        ts_ms,
                        decision,
                        reason_code,
                        mode_before,
                        primary_arm_before,
                        mode_after,
                        primary_arm_after,
                        journal_json
                    ) VALUES (
                        %(ts_ms)s,
                        %(decision)s,
                        %(reason_code)s,
                        %(mode_before)s,
                        %(primary_arm_before)s,
                        %(mode_after)s,
                        %(primary_arm_after)s,
                        %(journal_json)s
                    )
                    """,
                    {
                        "ts_ms": now_ms(),
                        "decision": evaluation["decision"],
                        "reason_code": evaluation["reason_code"],
                        "mode_before": evaluation["current_mode"],
                        "primary_arm_before": evaluation["current_primary_arm"],
                        "mode_after": evaluation["target_mode"],
                        "primary_arm_after": evaluation["target_primary_arm"],
                        "journal_json": json.dumps(evaluation),
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
                    recommendation = as_dict(payload)
                    controller_policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    try:
                        exec_kill = await r.get('trade:exec_kill_switch')
                        if exec_kill and exec_kill.decode().strip() == '1':
                            controller_policy['kill_switch'] = 1
                    except: pass
                    experiment_policy = experiment_policy_from_hash(await read_hash(r, EXPERIMENT_POLICY_KEY))
                    evaluation = evaluate_apply(
                        recommendation=recommendation,
                        controller_policy=controller_policy,
                        experiment_policy=experiment_policy,
                        now_ts_ms=now_ms(),
                    )
                    decision_label = evaluation["decision"]

                    if (
                        evaluation["decision"] in {"APPLY_PRIMARY_ARM_SHADOW", "APPLY_SINGLE_ARM"}
                        and controller_policy["advisory_only"] == 0
                        and controller_policy["executor_mode"] == "COMMIT"
                    ):
                        await apply_change(r, evaluation)
                        if TRANSITIONS:
                            TRANSITIONS.labels(
                                apply_strategy=evaluation["apply_strategy"],
                                winner_arm=evaluation["winner_arm"],
                            ).inc()

                    await persist_if_configured(db_url, recommendation, evaluation)

                    out = {
                        "schema_version": 1,
                        "decision": evaluation["decision"],
                        "reason_code": evaluation["reason_code"],
                        "current_mode": evaluation["current_mode"],
                        "current_primary_arm": evaluation["current_primary_arm"],
                        "target_mode": evaluation["target_mode"],
                        "target_primary_arm": evaluation["target_primary_arm"],
                        "apply_strategy": evaluation["apply_strategy"],
                        "winner_arm": evaluation["winner_arm"],
                        "winner_score": str(evaluation["winner_score"]),
                        "cooldown_active": str(evaluation["cooldown_active"]),
                        "ts_ms": str(now_ms()),
                    }
                    await r.xadd(DECISIONS_STREAM, out, maxlen=MAXLEN, approximate=True)
                    await r.xadd(
                        JOURNAL_STREAM,
                        {
                            "schema_version": 1,
                            "decision": evaluation["decision"],
                            "reason_code": evaluation["reason_code"],
                            "mode_before": evaluation["current_mode"],
                            "primary_arm_before": evaluation["current_primary_arm"],
                            "mode_after": evaluation["target_mode"],
                            "primary_arm_after": evaluation["target_primary_arm"],
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_DECIDED",
                            **out,
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "decision": evaluation["decision"],
                            "reason_code": evaluation["reason_code"],
                            "current_mode": evaluation["current_mode"],
                            "current_primary_arm": evaluation["current_primary_arm"],
                            "target_mode": evaluation["target_mode"],
                            "target_primary_arm": evaluation["target_primary_arm"],
                            "winner_arm": evaluation["winner_arm"],
                            "winner_score": str(evaluation["winner_score"]),
                            "apply_strategy": evaluation["apply_strategy"],
                            "ts_ms": str(now_ms()),
                        }
                    )
                    await update_mode_metrics(evaluation["target_mode"], evaluation["target_primary_arm"])
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_FAILED",
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
