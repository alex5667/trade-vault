from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import hashlib
import json
import os
from core.redis_keys import RedisKeyPrefixes as RK
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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_experiment_harness_v3_30"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles",
)
EXPOSURES_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPOSURES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment_exposures",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment:global",
)
DETERMINISTIC_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_DETERMINISTIC_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rca_requests",
)
VERTEX_CANDIDATE_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_VERTEX_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_requests",
)
LOCAL_CANDIDATE_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_LOCAL_STREAM",
    "stream:ml:local_fallback_requests",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_PORT", "9955"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_MAXLEN", "20000"))

DEFAULT_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_MODE", "SHADOW").upper()
DEFAULT_HASH_SALT = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_HASH_SALT", "route_incident_rca_mirror_rca_winner_apply_apply_v3_30")
DEFAULT_ALLOW_SEVERITIES = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_ALLOW_SEVERITIES", "warning,critical")
DEFAULT_MAX_BUNDLE_BYTES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_MAX_BUNDLE_BYTES", "131072"))
DEFAULT_ARM_WEIGHTS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_ARM_WEIGHTS_JSON",
    '{"deterministic":70,"vertex_candidate":20,"local_fallback_candidate":10}',
)
DEFAULT_PRIMARY_ARM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_PRIMARY_ARM", "deterministic")
DEFAULT_SHADOW_ARMS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_SHADOW_ARMS_JSON",
    '["vertex_candidate","local_fallback_candidate"]',
)

ALLOWED_MODES = {"DISABLED", "SHADOW", "SINGLE_ARM", "MULTI_ARM"}
ALLOWED_ARMS = {"deterministic", "vertex_candidate", "local_fallback_candidate"}


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_experiment_runs_total",
    "Winner-apply apply RCA experiment harness runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_experiment_latency_seconds",
    "Winner-apply apply RCA experiment harness latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_experiment_up",
    "Winner-apply apply RCA experiment harness up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_experiment_last_run_ts_seconds",
    "Winner-apply apply RCA experiment harness last run timestamp",
)
EXPOSURES = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_exposures_total",
    "Winner-apply apply RCA experiment exposures",
    ("arm", "severity", "mode"),
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


def default_arm_weights() -> Dict[str, int]:
    parsed = maybe_json(DEFAULT_ARM_WEIGHTS_JSON, {})
    if not isinstance(parsed, dict):
        parsed = {}
    out: Dict[str, int] = {}
    for k, v in parsed.items():
        kk = str(k)
        if kk in ALLOWED_ARMS:
            out[kk] = max(parse_int(v, 0), 0)
    return out or {"deterministic": 100}


def default_shadow_arms() -> List[str]:
    parsed = maybe_json(DEFAULT_SHADOW_ARMS_JSON, [])
    if not isinstance(parsed, list):
        parsed = []
    out = [str(x) for x in parsed if str(x) in ALLOWED_ARMS and str(x) != DEFAULT_PRIMARY_ARM]
    return out


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(raw.get("mode") or DEFAULT_MODE).upper()
    if mode not in ALLOWED_MODES:
        mode = DEFAULT_MODE
    allow_severities = maybe_json(raw.get("allow_severities_json"), list(default_allow_severities()))
    if not isinstance(allow_severities, list):
        allow_severities = list(default_allow_severities())
    arm_weights = maybe_json(raw.get("arm_weights_json"), default_arm_weights())
    if not isinstance(arm_weights, dict):
        arm_weights = default_arm_weights()
    weights: Dict[str, int] = {}
    for arm, weight in arm_weights.items():
        arm_s = str(arm)
        if arm_s in ALLOWED_ARMS:
            weights[arm_s] = max(parse_int(weight, 0), 0)
    if sum(weights.values()) <= 0:
        weights = {"deterministic": 100}
    primary_arm = str(raw.get("primary_arm") or DEFAULT_PRIMARY_ARM)
    if primary_arm not in ALLOWED_ARMS:
        primary_arm = "deterministic"
    shadow_arms = maybe_json(raw.get("shadow_arms_json"), default_shadow_arms())
    if not isinstance(shadow_arms, list):
        shadow_arms = default_shadow_arms()
    shadow_out = [str(x) for x in shadow_arms if str(x) in ALLOWED_ARMS and str(x) != primary_arm]
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "mode": mode,
        "hash_salt": str(raw.get("hash_salt") or DEFAULT_HASH_SALT),
        "allow_severities": {str(x).lower() for x in allow_severities},
        "max_bundle_bytes": parse_int(raw.get("max_bundle_bytes"), DEFAULT_MAX_BUNDLE_BYTES),
        "arm_weights": weights,
        "primary_arm": primary_arm,
        "shadow_arms": shadow_out,
    }


def choose_arm(bundle_id: str, salt: str, weights: Dict[str, int]) -> str:
    total = sum(weights.values())
    if total <= 0:
        return "deterministic"
    digest = hashlib.sha256(f"{salt}|{bundle_id}".encode("utf-8")).hexdigest()
    value = int(digest[:16], 16) % total
    cursor = 0
    for arm in sorted(weights.keys()):
        cursor += weights[arm]
        if value < cursor:
            return arm
    return sorted(weights.keys())[0]


def evaluate_bundle(bundle: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
    severity = str(bundle.get("trigger_severity") or "").lower()
    bundle_id = str(bundle.get("bundle_id") or "")
    out = {
        "decision": "REJECT",
        "reason_code": "REJECTED",
        "severity": severity,
        "primary_arm": "",
        "shadow_arms": [],
    }
    if policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if policy["mode"] == "DISABLED":
        out["reason_code"] = "MODE_DISABLED"
        return out
    if not bundle_id:
        out["reason_code"] = "BUNDLE_ID_MISSING"
        return out
    if severity not in policy["allow_severities"]:
        out["reason_code"] = "SEVERITY_NOT_ALLOWED"
        return out
    if len(stable_json(bundle).encode("utf-8")) > policy["max_bundle_bytes"]:
        out["reason_code"] = "BUNDLE_TOO_LARGE"
        return out

    if policy["mode"] == "SINGLE_ARM":
        out["decision"] = "EXPOSE"
        out["reason_code"] = "MODE_SINGLE_ARM"
        out["primary_arm"] = policy["primary_arm"]
        out["shadow_arms"] = []
        return out

    if policy["mode"] == "SHADOW":
        out["decision"] = "EXPOSE"
        out["reason_code"] = "MODE_SHADOW"
        out["primary_arm"] = policy["primary_arm"]
        out["shadow_arms"] = policy["shadow_arms"]
        return out

    assigned = choose_arm(bundle_id, policy["hash_salt"], policy["arm_weights"])
    out["decision"] = "EXPOSE"
    out["reason_code"] = "MODE_MULTI_ARM"
    out["primary_arm"] = assigned
    out["shadow_arms"] = []
    return out


def exposure_row(bundle: Dict[str, Any], arm: str, is_primary: bool, mode: str) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "bundle_id": str(bundle.get("bundle_id") or ""),
        "trigger_type": str(bundle.get("trigger_type") or ""),
        "trigger_severity": str(bundle.get("trigger_severity") or ""),
        "arm": arm,
        "is_primary": "1" if is_primary else "0",
        "mode": mode,
        "ts_ms": str(now_ms()),
    }


def build_arm_request(bundle: Dict[str, Any], arm: str, is_primary: bool) -> Dict[str, Any]:
    base = {
        "schema_version": 1,
        "request_id": f"{bundle.get('bundle_id','')}:{arm}",
        "bundle_id": str(bundle.get("bundle_id") or ""),
        "task_family": "route_incident_rca_mirror_rca_winner_apply_apply_experiment",
        "task_type": "route_incident_rca_mirror_rca_winner_apply_apply_experiment",
        "arm": arm,
        "is_primary": "1" if is_primary else "0",
        "source": APP_NAME,
        "bundle_json": stable_json(bundle),
        "severity": str(bundle.get("trigger_severity") or "warning"),
        "ts_ms": str(now_ms()),
    }
    if arm == "deterministic":
        base["provider_mode"] = "DETERMINISTIC"
    elif arm == "vertex_candidate":
        base["provider_mode"] = "VERTEX_CANDIDATE"
    elif arm == "local_fallback_candidate":
        base["provider_mode"] = "LOCAL_FALLBACK_CANDIDATE"
        base["task_type"] = "vertex_unavailable_fallback"
        base["force_local"] = "1"
        base["vertex_unavailable"] = "1"
        base["input_json"] = stable_json(bundle)
    return base


def arm_destination_stream(arm: str) -> str:
    if arm == "deterministic":
        return DETERMINISTIC_STREAM
    if arm == "vertex_candidate":
        return VERTEX_CANDIDATE_STREAM
    if arm == "local_fallback_candidate":
        return LOCAL_CANDIDATE_STREAM
    return ""


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    return as_dict(await r.hgetall(key))


async def persist_if_configured(
    db_url: str,
    bundle: Dict[str, Any],
    decision: Dict[str, Any],
    exposures: List[Dict[str, Any]],
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_experiment_decisions (
                    bundle_id,
                    ts_ms,
                    trigger_type,
                    trigger_severity,
                    decision,
                    reason_code,
                    primary_arm,
                    shadow_arms_json,
                    bundle_json
                ) VALUES (
                    %(bundle_id)s,
                    %(ts_ms)s,
                    %(trigger_type)s,
                    %(trigger_severity)s,
                    %(decision)s,
                    %(reason_code)s,
                    %(primary_arm)s,
                    %(shadow_arms_json)s,
                    %(bundle_json)s
                )
                """,
                {
                    "bundle_id": bundle.get("bundle_id", ""),
                    "ts_ms": now_ms(),
                    "trigger_type": bundle.get("trigger_type", ""),
                    "trigger_severity": bundle.get("trigger_severity", ""),
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "primary_arm": decision["primary_arm"],
                    "shadow_arms_json": json.dumps(decision["shadow_arms"]),
                    "bundle_json": json.dumps(bundle),
                }
            )
            for exp in exposures:
                cur.execute(
                    """

                    INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_experiment_exposures (
                        bundle_id, ts_ms, trigger_type, trigger_severity, arm, is_primary, mode, exposure_json
                    ) VALUES (
                        %(bundle_id)s, %(ts_ms)s, %(trigger_type)s, %(trigger_severity)s, %(arm)s, %(is_primary)s, %(mode)s, %(exposure_json)s
                    )
                    """,
                    {
                        "bundle_id": exp["bundle_id"],
                        "ts_ms": parse_int(exp["ts_ms"], now_ms()),
                        "trigger_type": exp["trigger_type"],
                        "trigger_severity": exp["trigger_severity"],
                        "arm": exp["arm"],
                        "is_primary": parse_int(exp["is_primary"], 0),
                        "mode": exp["mode"],
                        "exposure_json": json.dumps(exp),
                    }
                )
            conn.commit()


async def route_exposures(r: Any, bundle: Dict[str, Any], decision: Dict[str, Any], mode: str) -> List[Dict[str, Any]]:
    exposures: List[Dict[str, Any]] = []
    primary_arm = decision["primary_arm"]
    shadow_arms = decision["shadow_arms"]
    arms = [(primary_arm, True)] + [(a, False) for a in shadow_arms]
    for arm, is_primary in arms:
        exp = exposure_row(bundle, arm, is_primary, mode)
        exposures.append(exp)
        await r.xadd(EXPOSURES_STREAM, exp, maxlen=MAXLEN, approximate=True)
        dest = arm_destination_stream(arm)
        if dest:
            req = build_arm_request(bundle, arm, is_primary)
            await r.xadd(dest, req, maxlen=MAXLEN, approximate=True)
        if EXPOSURES:
            EXPOSURES.labels(arm=arm, severity=exp["trigger_severity"] or "unknown", mode=mode).inc()
    return exposures


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
                    row = as_dict(payload)
                    bundle = maybe_json(row.get("bundle_json"), {})
                    if not isinstance(bundle, dict):
                        bundle = {}
                    if not bundle and row.get("bundle_id"):
                        bundle = {
                            "bundle_id": row.get("bundle_id", ""),
                            "trigger_type": row.get("trigger_type", ""),
                            "trigger_severity": row.get("trigger_severity", ""),
                        }
                    policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    try:
                        exec_kill = await r.get(RK.EXEC_KILL_SWITCH)
                        if exec_kill and exec_kill.decode().strip() == '1':
                            policy['kill_switch'] = 1
                    except: pass
                    decision = evaluate_bundle(bundle, policy)
                    decision_label = decision["decision"]
                    exposures: List[Dict[str, Any]] = []
                    if decision["decision"] == "EXPOSE":
                        exposures = await route_exposures(r, bundle, decision, policy["mode"])
                    await persist_if_configured(db_url, bundle, decision, exposures)

                    out = {
                        "schema_version": 1,
                        "bundle_id": str(bundle.get("bundle_id") or ""),
                        "trigger_type": str(bundle.get("trigger_type") or ""),
                        "trigger_severity": str(bundle.get("trigger_severity") or ""),
                        "decision": decision["decision"],
                        "reason_code": decision["reason_code"],
                        "primary_arm": decision["primary_arm"],
                        "shadow_arms_json": stable_json(decision["shadow_arms"]),
                        "mode": policy["mode"],
                        "ts_ms": str(now_ms()),
                    }
                    await r.xadd(DECISIONS_STREAM, out, maxlen=MAXLEN, approximate=True)
                    await r.xadd(
                        AUDIT_STREAM,
                        {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_DECIDED", **out},
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "bundle_id": str(bundle.get("bundle_id") or ""),
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "primary_arm": decision["primary_arm"],
                            "mode": policy["mode"],
                            "ts_ms": str(now_ms()),
                        }
                    )
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_FAILED",
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
