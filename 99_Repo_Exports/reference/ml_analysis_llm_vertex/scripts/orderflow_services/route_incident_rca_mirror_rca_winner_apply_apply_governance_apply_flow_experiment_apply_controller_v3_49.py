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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_v3_49"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_decisions",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_decisions",
)
JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_journal",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller:global",
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
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_PORT", "9981"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_MAXLEN", "20000"))

DEFAULT_ADVISORY_ONLY = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_ADVISORY_ONLY",
    "1",
))
DEFAULT_EXECUTOR_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_EXECUTOR_MODE",
    "DRY_RUN",
).upper()
DEFAULT_ALLOW_COMMIT = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_ALLOW_COMMIT",
    "0",
))
DEFAULT_COOLDOWN_SEC = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_COOLDOWN_SEC",
    "21600",
))
DEFAULT_MIN_SCORE_MARGIN = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_MIN_SCORE_MARGIN",
    "0.05",
))
DEFAULT_ALLOW_WINNER_ARMS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_ALLOW_WINNER_ARMS_JSON",
    '["vertex_compact_candidate","local_candidate"]',
)
DEFAULT_PROFILE_VERTEX_PRIMARY_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_PROFILE_VERTEX_PRIMARY_JSON",
    '{"vertex_primary_weight":50,"vertex_compact_weight":30,"local_candidate_weight":20}',
)
DEFAULT_PROFILE_VERTEX_COMPACT_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_PROFILE_VERTEX_COMPACT_JSON",
    '{"vertex_primary_weight":30,"vertex_compact_weight":50,"local_candidate_weight":20}',
)
DEFAULT_PROFILE_LOCAL_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_PROFILE_LOCAL_JSON",
    '{"vertex_primary_weight":25,"vertex_compact_weight":25,"local_candidate_weight":50}',
)

ARMS = {"vertex_primary", "vertex_compact_candidate", "local_candidate"}
ACTIONABLE_DECISIONS = {"PROMOTE_VERTEX_COMPACT_CANDIDATE", "PROMOTE_LOCAL_CANDIDATE"}


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_runs_total",
    "Apply-flow experiment apply-controller runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_latency_seconds",
    "Apply-flow experiment apply-controller latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_up",
    "Apply-flow experiment apply-controller up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_last_run_ts_seconds",
    "Apply-flow experiment apply-controller last run timestamp",
)
APPLIES = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_applies_total",
    "Apply-flow experiment apply-controller applies",
    ("target_profile", "winner_arm", "applied"),
)
CURRENT_PROFILE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_current_profile",
    "Apply-flow experiment current profile",
    ("profile",),
)
CURRENT_INCUMBENT = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_current_incumbent_arm",
    "Apply-flow experiment current incumbent arm",
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


def normalize_profile(raw: Any, default: Dict[str, int]) -> Dict[str, int]:
    val = maybe_json(raw, default)
    if not isinstance(val, dict):
        val = default
    return {
        "vertex_primary_weight": parse_int(val.get("vertex_primary_weight"), default["vertex_primary_weight"]),
        "vertex_compact_weight": parse_int(val.get("vertex_compact_weight"), default["vertex_compact_weight"]),
        "local_candidate_weight": parse_int(val.get("local_candidate_weight"), default["local_candidate_weight"]),
    }


def default_profiles() -> Dict[str, Dict[str, int]]:
    return {
        "vertex_primary_profile": normalize_profile(DEFAULT_PROFILE_VERTEX_PRIMARY_JSON, {"vertex_primary_weight": 50, "vertex_compact_weight": 30, "local_candidate_weight": 20}),
        "vertex_compact_profile": normalize_profile(DEFAULT_PROFILE_VERTEX_COMPACT_JSON, {"vertex_primary_weight": 30, "vertex_compact_weight": 50, "local_candidate_weight": 20}),
        "local_profile": normalize_profile(DEFAULT_PROFILE_LOCAL_JSON, {"vertex_primary_weight": 25, "vertex_compact_weight": 25, "local_candidate_weight": 50}),
    }


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    allow_winner_arms = maybe_json(raw.get("allow_winner_arms_json"), maybe_json(DEFAULT_ALLOW_WINNER_ARMS_JSON, []))
    if not isinstance(allow_winner_arms, list):
        allow_winner_arms = ["vertex_compact_candidate", "local_candidate"]
    profiles = default_profiles()
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
        "allow_commit": parse_int(raw.get("allow_commit"), DEFAULT_ALLOW_COMMIT),
        "cooldown_sec": parse_int(raw.get("cooldown_sec"), DEFAULT_COOLDOWN_SEC),
        "min_score_margin": parse_float(raw.get("min_score_margin"), DEFAULT_MIN_SCORE_MARGIN),
        "allow_winner_arms": [str(x) for x in allow_winner_arms if str(x) in ARMS],
        "profiles": {
            "vertex_primary_profile": normalize_profile(raw.get("profile_vertex_primary_json"), profiles["vertex_primary_profile"]),
            "vertex_compact_profile": normalize_profile(raw.get("profile_vertex_compact_json"), profiles["vertex_compact_profile"]),
            "local_profile": normalize_profile(raw.get("profile_local_json"), profiles["local_profile"]),
        },
    }


def experiment_policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "mode": str(raw.get("mode") or "SHADOW").upper(),
        "vertex_primary_weight": parse_int(raw.get("vertex_primary_weight"), 50),
        "vertex_compact_weight": parse_int(raw.get("vertex_compact_weight"), 30),
        "local_candidate_weight": parse_int(raw.get("local_candidate_weight"), 20),
        "last_weight_rebalance_ts_ms": parse_int(raw.get("last_weight_rebalance_ts_ms"), 0),
    }


def winner_policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    incumbent = str(raw.get("incumbent_arm") or "vertex_primary")
    return {
        "incumbent_arm": incumbent if incumbent in ARMS else "vertex_primary",
    }


def weights_dict(exp_policy: Dict[str, Any]) -> Dict[str, int]:
    return {
        "vertex_primary_weight": parse_int(exp_policy.get("vertex_primary_weight"), 50),
        "vertex_compact_weight": parse_int(exp_policy.get("vertex_compact_weight"), 30),
        "local_candidate_weight": parse_int(exp_policy.get("local_candidate_weight"), 20),
    }


def infer_profile_name(weights: Dict[str, int], profiles: Dict[str, Dict[str, int]]) -> str:
    for name, profile in profiles.items():
        if profile == weights:
            return name
    return "custom_profile"


def extract_score_margin(scorecards_json: str, incumbent_arm: str, winner_arm: str) -> float:
    data = maybe_json(scorecards_json, {})
    if not isinstance(data, dict):
        return 0.0
    incumbent = data.get(incumbent_arm, {})
    winner = data.get(winner_arm, {})
    if not isinstance(incumbent, dict) or not isinstance(winner, dict):
        return 0.0
    return round(parse_float(winner.get("score"), 0.0) - parse_float(incumbent.get("score"), 0.0), 6)


def map_decision_to_profile(decision: str) -> str:
    if decision == "PROMOTE_VERTEX_COMPACT_CANDIDATE":
        return "vertex_compact_profile"
    if decision == "PROMOTE_LOCAL_CANDIDATE":
        return "local_profile"
    return "vertex_primary_profile"


def evaluate_apply(
    winner_decision_row: Dict[str, Any],
    exp_policy: Dict[str, Any],
    winner_policy: Dict[str, Any],
    controller_policy: Dict[str, Any],
) -> Dict[str, Any]:
    current_weights = weights_dict(exp_policy)
    current_profile = infer_profile_name(current_weights, controller_policy["profiles"])
    current_incumbent = winner_policy["incumbent_arm"]
    decision = str(winner_decision_row.get("decision") or "")
    winner_arm = str(winner_decision_row.get("winner_arm") or "")
    score_margin = extract_score_margin(str(winner_decision_row.get("scorecards_json") or "{}"), current_incumbent, winner_arm)
    cooldown_active = (
        exp_policy["last_weight_rebalance_ts_ms"] > 0
        and (now_ms() - exp_policy["last_weight_rebalance_ts_ms"]) < controller_policy["cooldown_sec"] * 1000
    )
    out = {
        "decision": "HOLD",
        "reason_code": "NO_CHANGE",
        "winner_decision": decision,
        "winner_arm": winner_arm,
        "score_margin": score_margin,
        "current_profile": current_profile,
        "current_incumbent_arm": current_incumbent,
        "target_profile": current_profile,
        "target_incumbent_arm": current_incumbent,
        "current_weights": current_weights,
        "target_weights": current_weights,
        "cooldown_active": 1 if cooldown_active else 0,
    }
    if controller_policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if controller_policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if decision == "KEEP_VERTEX_PRIMARY":
        out["decision"] = "KEEP_CURRENT_WEIGHTS"
        out["reason_code"] = "INCUMBENT_STILL_BEST"
        return out
    if decision not in ACTIONABLE_DECISIONS:
        out["reason_code"] = "UNKNOWN_WINNER_DECISION"
        return out
    if winner_arm not in controller_policy["allow_winner_arms"]:
        out["reason_code"] = "WINNER_ARM_NOT_ALLOWED"
        return out
    if score_margin < controller_policy["min_score_margin"]:
        out["reason_code"] = "SCORE_MARGIN_TOO_SMALL"
        return out
    if cooldown_active:
        out["reason_code"] = "COOLDOWN_ACTIVE"
        return out
    target_profile = map_decision_to_profile(decision)
    target_weights = controller_policy["profiles"][target_profile]
    if current_weights == target_weights and current_incumbent == winner_arm:
        out["decision"] = "KEEP_CURRENT_WEIGHTS"
        out["reason_code"] = "TARGET_ALREADY_ACTIVE"
        return out
    out["target_profile"] = target_profile
    out["target_weights"] = target_weights
    out["target_incumbent_arm"] = winner_arm
    if decision == "PROMOTE_VERTEX_COMPACT_CANDIDATE":
        out["decision"] = "APPLY_VERTEX_COMPACT_PROFILE"
        out["reason_code"] = "WINNER_SCORE_ABOVE_INCUMBENT"
        return out
    if decision == "PROMOTE_LOCAL_CANDIDATE":
        out["decision"] = "APPLY_LOCAL_PROFILE"
        out["reason_code"] = "WINNER_SCORE_ABOVE_INCUMBENT"
        return out
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    return as_dict(await r.hgetall(key))


async def persist_if_configured(
    db_url: str,
    decision_out: Dict[str, Any],
    winner_row: Dict[str, Any],
    applied: int,
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_rr_winner_apply_gov_exp_apply_ctrl_decisions (
                    ts_ms, decision, reason_code, winner_decision, winner_arm, score_margin,
                    current_profile, target_profile, current_incumbent_arm, target_incumbent_arm,
                    applied, decision_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(winner_decision)s, %(winner_arm)s, %(score_margin)s,
                    %(current_profile)s, %(target_profile)s, %(current_incumbent_arm)s, %(target_incumbent_arm)s,
                    %(applied)s, %(decision_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": decision_out["decision"],
                    "reason_code": decision_out["reason_code"],
                    "winner_decision": decision_out["winner_decision"],
                    "winner_arm": decision_out["winner_arm"],
                    "score_margin": decision_out["score_margin"],
                    "current_profile": decision_out["current_profile"],
                    "target_profile": decision_out["target_profile"],
                    "current_incumbent_arm": decision_out["current_incumbent_arm"],
                    "target_incumbent_arm": decision_out["target_incumbent_arm"],
                    "applied": applied,
                    "decision_json": json.dumps({"decision_out": decision_out, "winner_row": winner_row}),
                },
            )
            cur.execute(
                """
                INSERT INTO llm_rr_winner_apply_gov_exp_apply_ctrl_journal (
                    ts_ms, decision, reason_code, winner_arm, current_profile, target_profile,
                    current_weights_json, target_weights_json, applied, journal_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(winner_arm)s, %(current_profile)s, %(target_profile)s,
                    %(current_weights_json)s, %(target_weights_json)s, %(applied)s, %(journal_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": decision_out["decision"],
                    "reason_code": decision_out["reason_code"],
                    "winner_arm": decision_out["winner_arm"],
                    "current_profile": decision_out["current_profile"],
                    "target_profile": decision_out["target_profile"],
                    "current_weights_json": json.dumps(decision_out["current_weights"]),
                    "target_weights_json": json.dumps(decision_out["target_weights"]),
                    "applied": applied,
                    "journal_json": json.dumps({"decision_out": decision_out, "winner_row": winner_row}),
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
                    winner_row = as_dict(payload)
                    controller_policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    exp_policy = experiment_policy_from_hash(await read_hash(r, EXPERIMENT_POLICY_KEY))
                    winner_policy = winner_policy_from_hash(await read_hash(r, WINNER_POLICY_KEY))
                    decision_out = evaluate_apply(winner_row, exp_policy, winner_policy, controller_policy)
                    decision_label = decision_out["decision"]

                    should_apply = (
                        decision_out["decision"] in {"APPLY_VERTEX_COMPACT_PROFILE", "APPLY_LOCAL_PROFILE"}
                        and controller_policy["advisory_only"] == 0
                        and controller_policy["executor_mode"] == "COMMIT"
                        and controller_policy["allow_commit"] == 1
                    )
                    applied = 0
                    if should_apply:
                        target_weights = decision_out["target_weights"]
                        await r.hset(
                            EXPERIMENT_POLICY_KEY,
                            mapping={
                                "mode": "SHADOW",
                                "vertex_primary_weight": str(target_weights["vertex_primary_weight"]),
                                "vertex_compact_weight": str(target_weights["vertex_compact_weight"]),
                                "local_candidate_weight": str(target_weights["local_candidate_weight"]),
                                "last_weight_rebalance_ts_ms": str(now_ms()),
                                "last_weight_rebalance_source": APP_NAME,
                                "last_weight_rebalance_reason_code": decision_out["reason_code"],
                                "last_weight_rebalance_profile": decision_out["target_profile"],
                            },
                        )
                        await r.hset(
                            WINNER_POLICY_KEY,
                            mapping={
                                "incumbent_arm": decision_out["target_incumbent_arm"],
                                "last_incumbent_apply_ts_ms": str(now_ms()),
                                "last_incumbent_apply_source": APP_NAME,
                                "last_incumbent_apply_reason_code": decision_out["reason_code"],
                            },
                        )
                        applied = 1

                    await persist_if_configured(db_url, decision_out, winner_row, applied)
                    await r.xadd(
                        DECISIONS_STREAM,
                        {
                            "schema_version": 1,
                            "decision": decision_out["decision"],
                            "reason_code": decision_out["reason_code"],
                            "winner_decision": decision_out["winner_decision"],
                            "winner_arm": decision_out["winner_arm"],
                            "score_margin": str(decision_out["score_margin"]),
                            "current_profile": decision_out["current_profile"],
                            "target_profile": decision_out["target_profile"],
                            "current_incumbent_arm": decision_out["current_incumbent_arm"],
                            "target_incumbent_arm": decision_out["target_incumbent_arm"],
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
                            "winner_arm": decision_out["winner_arm"],
                            "current_profile": decision_out["current_profile"],
                            "target_profile": decision_out["target_profile"],
                            "current_weights_json": stable_json(decision_out["current_weights"]),
                            "target_weights_json": stable_json(decision_out["target_weights"]),
                            "applied": str(applied),
                            "executor_mode": controller_policy["executor_mode"],
                            "allow_commit": str(controller_policy["allow_commit"]),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_DECIDED",
                            "decision": decision_out["decision"],
                            "reason_code": decision_out["reason_code"],
                            "winner_arm": decision_out["winner_arm"],
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
                            "winner_arm": decision_out["winner_arm"],
                            "current_profile": decision_out["current_profile"],
                            "target_profile": decision_out["target_profile"],
                            "applied": str(applied),
                            "ts_ms": str(now_ms()),
                        },
                    )
                    if APPLIES:
                        APPLIES.labels(
                            target_profile=decision_out["target_profile"],
                            winner_arm=decision_out["winner_arm"] or "none",
                            applied=str(applied),
                        ).inc()
                    if CURRENT_PROFILE:
                        profiles = controller_policy["profiles"]
                        inferred = infer_profile_name(weights_dict(exp_policy), profiles)
                        for profile_name in list(profiles.keys()) + ["custom_profile"]:
                            CURRENT_PROFILE.labels(profile=profile_name).set(1 if inferred == profile_name else 0)
                    if CURRENT_INCUMBENT:
                        for arm in sorted(ARMS):
                            CURRENT_INCUMBENT.labels(arm=arm).set(1 if winner_policy["incumbent_arm"] == arm else 0)
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_FAILED",
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
