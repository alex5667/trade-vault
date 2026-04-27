from __future__ import annotations

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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_governor_v3_54"
FEEDBACK_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_FEEDBACK_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_feedback",
)
RESULTS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results",
)
ROLLUPS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_ROLLUPS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_rollups",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness:global",
)
BRIDGE_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_BRIDGE_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge:global",
)
STATE_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_STATE_KEY",
    "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness:last_action",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_PORT", "9989"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MAXLEN", "20000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_LOOKBACK_COUNT", "500"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_WINDOW_MIN", "10080"))

DEFAULT_MIN_VERTEX_SAMPLES = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_VERTEX_SAMPLES",
    "5",
))
DEFAULT_MIN_LOCAL_SAMPLES = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_LOCAL_SAMPLES",
    "5",
))
DEFAULT_MIN_USEFULNESS = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_USEFULNESS",
    "0.60",
))
DEFAULT_MIN_ACCEPTED_RATE = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_ACCEPTED_RATE",
    "0.60",
))
DEFAULT_MIN_QUALITY = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_QUALITY",
    "0.55",
))
DEFAULT_MIN_DELTA_USEFULNESS = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_DELTA_USEFULNESS",
    "0.05",
))
DEFAULT_MIN_DELTA_ACCEPTED = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_DELTA_ACCEPTED",
    "0.05",
))
DEFAULT_COOLDOWN_SEC = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_COOLDOWN_SEC",
    "21600",
))
DEFAULT_ADVISORY_ONLY = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_ADVISORY_ONLY",
    "1",
))
DEFAULT_EXECUTOR_MODE = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_EXECUTOR_MODE",
    "DRY_RUN",
).upper()


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_runs_total",
    "Apply-flow experiment incident RCA usefulness runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_latency_seconds",
    "Apply-flow experiment incident RCA usefulness latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_up",
    "Apply-flow experiment incident RCA usefulness up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_last_run_ts_seconds",
    "Apply-flow experiment incident RCA usefulness last run timestamp",
)
VERTEX_AVG_USEFULNESS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_vertex_avg_usefulness",
    "Apply-flow experiment incident RCA vertex avg usefulness",
)
LOCAL_AVG_USEFULNESS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_local_avg_usefulness",
    "Apply-flow experiment incident RCA local avg usefulness",
)
CURRENT_BRIDGE_MODE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge_mode",
    "Apply-flow experiment incident RCA bridge mode",
    ("mode",),
)


def now_ms() -> int:
    return int(time.time() * 1000)


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


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "min_vertex_samples": parse_int(raw.get("min_vertex_samples"), DEFAULT_MIN_VERTEX_SAMPLES),
        "min_local_samples": parse_int(raw.get("min_local_samples"), DEFAULT_MIN_LOCAL_SAMPLES),
        "min_usefulness": parse_float(raw.get("min_usefulness"), DEFAULT_MIN_USEFULNESS),
        "min_accepted_rate": parse_float(raw.get("min_accepted_rate"), DEFAULT_MIN_ACCEPTED_RATE),
        "min_quality": parse_float(raw.get("min_quality"), DEFAULT_MIN_QUALITY),
        "min_delta_usefulness": parse_float(raw.get("min_delta_usefulness"), DEFAULT_MIN_DELTA_USEFULNESS),
        "min_delta_accepted": parse_float(raw.get("min_delta_accepted"), DEFAULT_MIN_DELTA_ACCEPTED),
        "cooldown_sec": parse_int(raw.get("cooldown_sec"), DEFAULT_COOLDOWN_SEC),
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
    }


def current_bridge_mode(raw: Dict[str, Any]) -> str:
    mode = str(raw.get("mode") or "AUTO").upper()
    return mode if mode in {"AUTO", "VERTEX_ONLY", "LOCAL_ONLY", "DISABLED"} else "AUTO"


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def xr_recent(client: Any, stream_key: str, count: int) -> List[Dict[str, Any]]:
    try:
        rows = await client.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


def join_feedback_with_results(feedback_rows: List[Dict[str, Any]], result_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cutoff = now_ms() - WINDOW_MIN * 60 * 1000
    result_map: Dict[str, Dict[str, Any]] = {}
    for row in result_rows:
        ts_ms = parse_int(row.get("ts_ms"), 0)
        if ts_ms < cutoff:
            continue
        rid = str(row.get("request_id") or "")
        if rid:
            result_map[rid] = row

    joined: List[Dict[str, Any]] = []
    for fb in feedback_rows:
        ts_ms = parse_int(fb.get("ts_ms"), 0)
        if ts_ms < cutoff:
            continue
        rid = str(fb.get("request_id") or "")
        result = result_map.get(rid)
        if not result:
            continue
        provider_mode = str(result.get("provider_mode") or "").upper()
        if provider_mode not in {"VERTEX", "LOCAL"}:
            continue
        joined.append(
            {
                "request_id": rid,
                "bundle_id": str(fb.get("bundle_id") or ""),
                "provider_mode": provider_mode,
                "quality_score": parse_float(fb.get("quality_score"), 0.0),
                "usefulness_score": parse_float(fb.get("usefulness_score"), 0.0),
                "accepted": parse_int(fb.get("accepted"), 0),
                "reason_code": str(fb.get("reason_code") or ""),
                "ts_ms": ts_ms,
            }
        )
    return joined


def provider_rollup(joined: List[Dict[str, Any]], provider_mode: str) -> Dict[str, Any]:
    rows = [r for r in joined if r["provider_mode"] == provider_mode]
    n = len(rows)
    if n == 0:
        return {
            "provider_mode": provider_mode,
            "n": 0,
            "avg_quality": 0.0,
            "avg_usefulness": 0.0,
            "accepted_rate": 0.0,
        }
    return {
        "provider_mode": provider_mode,
        "n": n,
        "avg_quality": round(sum(r["quality_score"] for r in rows) / n, 6),
        "avg_usefulness": round(sum(r["usefulness_score"] for r in rows) / n, 6),
        "accepted_rate": round(sum(r["accepted"] for r in rows) / n, 6),
    }


def evaluate_usefulness(vertex: Dict[str, Any], local: Dict[str, Any], bridge_mode: str, policy: Dict[str, Any], cooldown_active: bool) -> Dict[str, Any]:
    base = {
        "decision": "HOLD",
        "reason_code": "NO_CHANGE",
        "target_bridge_mode": bridge_mode,
        "current_bridge_mode": bridge_mode,
        "cooldown_active": 1 if cooldown_active else 0,
    }

    if vertex["n"] < policy["min_vertex_samples"] or local["n"] < policy["min_local_samples"]:
        base["reason_code"] = "INSUFFICIENT_SAMPLES"
        return base

    vertex_good = (
        vertex["avg_usefulness"] >= policy["min_usefulness"]
        and vertex["accepted_rate"] >= policy["min_accepted_rate"]
        and vertex["avg_quality"] >= policy["min_quality"]
    )
    local_good = (
        local["avg_usefulness"] >= policy["min_usefulness"]
        and local["accepted_rate"] >= policy["min_accepted_rate"]
        and local["avg_quality"] >= policy["min_quality"]
    )
    vertex_better = (
        (vertex["avg_usefulness"] - local["avg_usefulness"]) >= policy["min_delta_usefulness"]
        and (vertex["accepted_rate"] - local["accepted_rate"]) >= policy["min_delta_accepted"]
    )
    local_better = (
        (local["avg_usefulness"] - vertex["avg_usefulness"]) >= policy["min_delta_usefulness"]
        and (local["accepted_rate"] - vertex["accepted_rate"]) >= policy["min_delta_accepted"]
    )

    if bridge_mode == "AUTO":
        if vertex_good and vertex_better and not cooldown_active:
            base["decision"] = "PREFER_VERTEX_ONLY"
            base["reason_code"] = "VERTEX_BETTER_THAN_LOCAL"
            base["target_bridge_mode"] = "VERTEX_ONLY"
            return base
        if local_good and local_better and not cooldown_active:
            base["decision"] = "PREFER_LOCAL_ONLY"
            base["reason_code"] = "LOCAL_BETTER_THAN_VERTEX"
            base["target_bridge_mode"] = "LOCAL_ONLY"
            return base
        base["decision"] = "KEEP_AUTO"
        base["reason_code"] = "AUTO_STILL_BALANCED"
        return base

    if bridge_mode == "VERTEX_ONLY":
        if local_good and local_better and not cooldown_active:
            base["decision"] = "RETURN_TO_AUTO"
            base["reason_code"] = "VERTEX_ONLY_UNDERPERFORMS"
            base["target_bridge_mode"] = "AUTO"
            return base
        base["decision"] = "KEEP_VERTEX_ONLY"
        base["reason_code"] = "VERTEX_ONLY_STILL_VALID"
        return base

    if bridge_mode == "LOCAL_ONLY":
        if vertex_good and vertex_better and not cooldown_active:
            base["decision"] = "RETURN_TO_AUTO"
            base["reason_code"] = "LOCAL_ONLY_UNDERPERFORMS"
            base["target_bridge_mode"] = "AUTO"
            return base
        base["decision"] = "KEEP_LOCAL_ONLY"
        base["reason_code"] = "LOCAL_ONLY_STILL_VALID"
        return base

    base["reason_code"] = "BRIDGE_MODE_DISABLED"
    return base


async def persist_if_configured(db_url: str, vertex: Dict[str, Any], local: Dict[str, Any], decision: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_p354_rca_use_rollups (
                    ts_ms, window_min,
                    vertex_n, vertex_avg_quality, vertex_avg_usefulness, vertex_accepted_rate,
                    local_n, local_avg_quality, local_avg_usefulness, local_accepted_rate,
                    rollup_json
                ) VALUES (
                    %(ts_ms)s, %(window_min)s,
                    %(vertex_n)s, %(vertex_avg_quality)s, %(vertex_avg_usefulness)s, %(vertex_accepted_rate)s,
                    %(local_n)s, %(local_avg_quality)s, %(local_avg_usefulness)s, %(local_accepted_rate)s,
                    %(rollup_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "window_min": WINDOW_MIN,
                    "vertex_n": vertex["n"],
                    "vertex_avg_quality": vertex["avg_quality"],
                    "vertex_avg_usefulness": vertex["avg_usefulness"],
                    "vertex_accepted_rate": vertex["accepted_rate"],
                    "local_n": local["n"],
                    "local_avg_quality": local["avg_quality"],
                    "local_avg_usefulness": local["avg_usefulness"],
                    "local_accepted_rate": local["accepted_rate"],
                    "rollup_json": json.dumps({"vertex": vertex, "local": local}),
                },
            )
            cur.execute(
                """
                INSERT INTO llm_p354_rca_use_decs (
                    ts_ms, current_bridge_mode, target_bridge_mode, decision, reason_code, decision_json
                ) VALUES (
                    %(ts_ms)s, %(current_bridge_mode)s, %(target_bridge_mode)s, %(decision)s, %(reason_code)s, %(decision_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "current_bridge_mode": decision["current_bridge_mode"],
                    "target_bridge_mode": decision["target_bridge_mode"],
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "decision_json": json.dumps({"vertex": vertex, "local": local, "decision": decision}),
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
    await ensure_group(r, FEEDBACK_STREAM, GROUP)
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {FEEDBACK_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, _payload in messages:
                started = time.perf_counter()
                status = "ok"
                decision_label = "HOLD"
                try:
                    feedback_rows = await xr_recent(r, FEEDBACK_STREAM, LOOKBACK_COUNT)
                    result_rows = await xr_recent(r, RESULTS_STREAM, LOOKBACK_COUNT)
                    joined = join_feedback_with_results(feedback_rows, result_rows)
                    vertex = provider_rollup(joined, "VERTEX")
                    local = provider_rollup(joined, "LOCAL")

                    policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
                    bridge_raw = as_dict(await r.hgetall(BRIDGE_POLICY_KEY))
                    bridge_mode = current_bridge_mode(bridge_raw)
                    state_raw = as_dict(await r.hgetall(STATE_KEY))
                    last_action_ts = parse_int(state_raw.get("last_action_ts_ms"), 0)
                    cooldown_active = last_action_ts > 0 and (now_ms() - last_action_ts) < policy["cooldown_sec"] * 1000

                    decision = evaluate_usefulness(vertex, local, bridge_mode, policy, cooldown_active)
                    decision_label = decision["decision"]

                    if (
                        decision["decision"] in {"PREFER_VERTEX_ONLY", "PREFER_LOCAL_ONLY", "RETURN_TO_AUTO"}
                        and policy["advisory_only"] == 0
                        and policy["executor_mode"] == "COMMIT"
                    ):
                        await r.hset(
                            BRIDGE_POLICY_KEY,
                            mapping={
                                "mode": decision["target_bridge_mode"],
                                "last_mode_switch_source": APP_NAME,
                                "last_mode_switch_reason_code": decision["reason_code"],
                            },
                        )
                        await r.hset(
                            STATE_KEY,
                            mapping={
                                "last_action_ts_ms": str(now_ms()),
                                "last_decision": decision["decision"],
                                "last_reason_code": decision["reason_code"],
                            },
                        )

                    await persist_if_configured(db_url, vertex, local, decision)
                    await r.xadd(
                        ROLLUPS_STREAM,
                        {
                            "schema_version": 1,
                            "vertex_rollup_json": stable_json(vertex),
                            "local_rollup_json": stable_json(local),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xadd(
                        DECISIONS_STREAM,
                        {
                            "schema_version": 1,
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "current_bridge_mode": decision["current_bridge_mode"],
                            "target_bridge_mode": decision["target_bridge_mode"],
                            "vertex_rollup_json": stable_json(vertex),
                            "local_rollup_json": stable_json(local),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "current_bridge_mode": decision["current_bridge_mode"],
                            "target_bridge_mode": decision["target_bridge_mode"],
                            "vertex_avg_usefulness": str(vertex["avg_usefulness"]),
                            "local_avg_usefulness": str(local["avg_usefulness"]),
                            "vertex_n": str(vertex["n"]),
                            "local_n": str(local["n"]),
                            "ts_ms": str(now_ms()),
                        },
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_DECIDED",
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    if VERTEX_AVG_USEFULNESS:
                        VERTEX_AVG_USEFULNESS.set(vertex["avg_usefulness"])
                    if LOCAL_AVG_USEFULNESS:
                        LOCAL_AVG_USEFULNESS.set(local["avg_usefulness"])
                    if CURRENT_BRIDGE_MODE:
                        for mode in ("AUTO", "VERTEX_ONLY", "LOCAL_ONLY", "DISABLED"):
                            CURRENT_BRIDGE_MODE.labels(mode=mode).set(1 if bridge_mode == mode else 0)
                    await r.xack(FEEDBACK_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_FAILED",
                            "error": str(exc),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(FEEDBACK_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, decision=decision_label).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
