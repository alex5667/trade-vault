from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import hashlib
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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_harness_v3_47"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles",
)
VERTEX_EXPERIMENT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERTEX_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_vertex_requests",
)
LOCAL_EXPERIMENT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_LOCAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_local_requests",
)
EXPOSURES_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_EXPOSURES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_exposures",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:global",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_PORT", "9978"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_MAXLEN", "20000"))

DEFAULT_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_MODE",
    "SHADOW",
).upper()
DEFAULT_ALLOW_SEVERITIES = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ALLOW_SEVERITIES",
    "warning,critical",
)
DEFAULT_VERTEX_PRIMARY_WEIGHT = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERTEX_PRIMARY_WEIGHT",
    "50",
))
DEFAULT_VERTEX_COMPACT_WEIGHT = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERTEX_COMPACT_WEIGHT",
    "30",
))
DEFAULT_LOCAL_CANDIDATE_WEIGHT = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_LOCAL_CANDIDATE_WEIGHT",
    "20",
))
DEFAULT_MAX_BUNDLE_BYTES = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_MAX_BUNDLE_BYTES",
    "196608",
))
ALLOWED_MODES = {"SHADOW", "DISABLED"}
ARMS = ("vertex_primary", "vertex_compact_candidate", "local_candidate")


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_runs_total",
    "Apply-flow experiment harness runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_latency_seconds",
    "Apply-flow experiment harness latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_up",
    "Apply-flow experiment harness up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_last_run_ts_seconds",
    "Apply-flow experiment harness last run timestamp",
)
ARM_EXPOSURES = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_exposures_total",
    "Apply-flow experiment harness exposures",
    ("arm", "severity"),
)
CURRENT_MODE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_current_mode",
    "Apply-flow experiment current mode",
    ("mode",),
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


def _default_allow_severities() -> set[str]:
    return {x.strip().lower() for x in DEFAULT_ALLOW_SEVERITIES.split(",") if x.strip()}


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(raw.get("mode") or DEFAULT_MODE).upper()
    if mode not in ALLOWED_MODES:
        mode = DEFAULT_MODE
    allow_severities = maybe_json(raw.get("allow_severities_json"), list(_default_allow_severities()))
    if not isinstance(allow_severities, list):
        allow_severities = list(_default_allow_severities())
    vertex_primary_weight = parse_int(raw.get("vertex_primary_weight"), DEFAULT_VERTEX_PRIMARY_WEIGHT)
    vertex_compact_weight = parse_int(raw.get("vertex_compact_weight"), DEFAULT_VERTEX_COMPACT_WEIGHT)
    local_candidate_weight = parse_int(raw.get("local_candidate_weight"), DEFAULT_LOCAL_CANDIDATE_WEIGHT)
    total = max(vertex_primary_weight + vertex_compact_weight + local_candidate_weight, 1)
    return {
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "mode": mode,
        "allow_severities": {str(x).lower() for x in allow_severities},
        "vertex_primary_weight": vertex_primary_weight,
        "vertex_compact_weight": vertex_compact_weight,
        "local_candidate_weight": local_candidate_weight,
        "total_weight": total,
        "max_bundle_bytes": parse_int(raw.get("max_bundle_bytes"), DEFAULT_MAX_BUNDLE_BYTES),
    },


def deterministic_bucket(bundle_id: str, salt: str = "apply-flow-exp-v3-47") -> int:
    key = f"{salt}:{bundle_id}".encode("utf-8")
    digest = hashlib.sha256(key).hexdigest()
    return int(digest[:8], 16) % 100


def choose_arm(bundle_id: str, policy: Dict[str, Any]) -> str:
    bucket = deterministic_bucket(bundle_id)
    v1 = policy["vertex_primary_weight"]
    v2 = v1 + policy["vertex_compact_weight"]
    if bucket < v1:
        return "vertex_primary"
    if bucket < v2:
        return "vertex_compact_candidate"
    return "local_candidate"


def build_prompt(bundle: Dict[str, Any], arm: str) -> str:
    if arm == "vertex_primary":
        return (
            "Analyze this route_incident_rca mirror RCA winner-apply apply governance apply-flow incident bundle. "
            "Focus on apply-controller intent, live policy mismatch, verification outcomes, rollback causes, "
            "retry exhaustion, SLO/MTTR pressure, escalation severity, and bounded next actions."
        )
    if arm == "vertex_compact_candidate":
        return (
            "Give a compact bounded RCA for this apply-flow governance incident bundle. "
            "Return dominant failure mechanism, 2-4 hypotheses, and the highest-value next checks only."
        )
    return (
        "Vertex path is not used for this experimental arm. "
        "Produce a bounded local-style RCA summary for this apply-flow governance incident bundle, "
        "with short hypotheses and concrete containment checks."
    )


def build_request(bundle: Dict[str, Any], arm: str) -> Tuple[str, Dict[str, Any]]:
    bundle_id = str(bundle.get("bundle_id") or "")
    request_id = f"{bundle_id}:{arm}"
    base = {
        "schema_version": 1,
        "request_id": request_id,
        "bundle_id": bundle_id,
        "experiment_arm": arm,
        "task_family": "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rca",
        "severity": str(bundle.get("trigger_severity") or "warning"),
        "source": APP_NAME,
        "prompt": build_prompt(bundle, arm),
        "ts_ms": str(now_ms()),
    },
    if arm in {"vertex_primary", "vertex_compact_candidate"}:
        base["task_type"] = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_vertex_rca"
        base["bundle_json"] = stable_json(bundle)
        return VERTEX_EXPERIMENT_STREAM, base
    base["task_type"] = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_local_rca"
    base["force_local"] = "1"
    base["input_json"] = stable_json(bundle)
    return LOCAL_EXPERIMENT_STREAM, base


def evaluate_bundle(bundle: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
    severity = str(bundle.get("trigger_severity") or "").lower()
    out = {
        "decision": "REJECT",
        "reason_code": "REJECTED",
        "arm": "",
        "severity": severity,
    },
    if policy["kill_switch"] == 1:
        out["reason_code"] = "KILL_SWITCH"
        return out
    if policy["enabled"] != 1:
        out["reason_code"] = "DISABLED"
        return out
    if policy["mode"] == "DISABLED":
        out["reason_code"] = "MODE_DISABLED"
        return out
    if severity not in policy["allow_severities"]:
        out["reason_code"] = "SEVERITY_NOT_ALLOWED"
        return out
    if len(stable_json(bundle).encode("utf-8")) > policy["max_bundle_bytes"]:
        out["reason_code"] = "BUNDLE_TOO_LARGE"
        return out
    arm = choose_arm(str(bundle.get("bundle_id") or ""), policy)
    out["decision"] = "EXPOSE_AND_ROUTE"
    out["reason_code"] = "OK"
    out["arm"] = arm
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    return as_dict(await r.hgetall(key))


async def persist_if_configured(db_url: str, bundle: Dict[str, Any], decision: Dict[str, Any], destination_stream: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """,
                INSERT INTO llm_rca_governance_apply_flow_exp_exposures (
                    bundle_id, ts_ms, arm, severity, destination_stream, exposure_json
                ) VALUES (
                    %(bundle_id)s, %(ts_ms)s, %(arm)s, %(severity)s, %(destination_stream)s, %(exposure_json)s
                )
                """,
                {
                    "bundle_id": bundle.get("bundle_id", ""),
                    "ts_ms": now_ms(),
                    "arm": decision.get("arm", ""),
                    "severity": decision.get("severity", ""),
                    "destination_stream": destination_stream,
                    "exposure_json": json.dumps({"bundle": bundle, "decision": decision}),
                },
            )
            cur.execute(
                """,
                INSERT INTO llm_rca_governance_apply_flow_exp_decisions (
                    bundle_id, ts_ms, severity, decision, reason_code, arm, destination_stream, decision_json
                ) VALUES (
                    %(bundle_id)s, %(ts_ms)s, %(severity)s, %(decision)s, %(reason_code)s, %(arm)s, %(destination_stream)s, %(decision_json)s
                )
                """,
                {
                    "bundle_id": bundle.get("bundle_id", ""),
                    "ts_ms": now_ms(),
                    "severity": decision.get("severity", ""),
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "arm": decision.get("arm", ""),
                    "destination_stream": destination_stream,
                    "decision_json": json.dumps(decision),
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
                    row = as_dict(payload)
                    bundle = maybe_json(row.get("bundle_json"), {})
                    if not isinstance(bundle, dict):
                        bundle = {}
                    if not bundle and row.get("bundle_id"):
                        bundle = {
                            "bundle_id": row.get("bundle_id", ""),
                            "trigger_type": row.get("trigger_type", ""),
                            "trigger_severity": row.get("trigger_severity", ""),
                        },
                    policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    try:
                        exec_kill = await r.get('trade:exec_kill_switch')
                        if exec_kill and exec_kill.decode().strip() == '1':
                            policy['kill_switch'] = 1
                    except: pass
                    decision = evaluate_bundle(bundle, policy)
                    decision_label = decision["decision"]
                    destination_stream = ""

                    if decision["decision"] == "EXPOSE_AND_ROUTE":
                        destination_stream, request_row = build_request(bundle, decision["arm"])
                        await r.xadd(destination_stream, request_row, maxlen=MAXLEN, approximate=True)
                        await r.xadd(
                            EXPOSURES_STREAM,
                            {
                                "schema_version": 1,
                                "bundle_id": str(bundle.get("bundle_id") or ""),
                                "request_id": str(request_row.get("request_id") or ""),
                                "arm": decision["arm"],
                                "severity": decision["severity"],
                                "destination_stream": destination_stream,
                                "ts_ms": str(now_ms()),
                            },
                            maxlen=MAXLEN,
                            approximate=True,
                        )
                        if ARM_EXPOSURES:
                            ARM_EXPOSURES.labels(arm=decision["arm"], severity=decision["severity"] or "unknown").inc()

                    await persist_if_configured(db_url, bundle, decision, destination_stream)
                    await r.xadd(
                        DECISIONS_STREAM,
                        {
                            "schema_version": 1,
                            "bundle_id": str(bundle.get("bundle_id") or ""),
                            "severity": decision["severity"],
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "arm": decision["arm"],
                            "destination_stream": destination_stream,
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_DECIDED",
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "arm": decision["arm"],
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "bundle_id": str(bundle.get("bundle_id") or ""),
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "arm": decision["arm"],
                            "destination_stream": destination_stream,
                            "ts_ms": str(now_ms()),
                        },
                    )
                    if CURRENT_MODE:
                        for mode in ("SHADOW", "DISABLED"):
                            CURRENT_MODE.labels(mode=mode).set(1 if policy["mode"] == mode else 0)
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_FAILED",
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
