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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_post_apply_verifier_v3_56"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_journal",
)
USEFULNESS_ROLLUPS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_ROLLUPS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_rollups",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_results",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification:global",
)
BRIDGE_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_BRIDGE_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge:global",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_PORT", "9991"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_MAXLEN", "20000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_LOOKBACK_COUNT", "100"))

DEFAULT_VERIFY_DELAY_SEC = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_DELAY_SEC",
    "900",
))
DEFAULT_MIN_PROVIDER_SAMPLES = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_MIN_PROVIDER_SAMPLES",
    "5",
))
DEFAULT_MIN_PROVIDER_USEFULNESS = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_MIN_PROVIDER_USEFULNESS",
    "0.60",
))
DEFAULT_MIN_PROVIDER_ACCEPTED = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_MIN_PROVIDER_ACCEPTED",
    "0.60",
))
DEFAULT_REQUIRE_MODE_MATCH = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_REQUIRE_MODE_MATCH",
    "1",
))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_runs_total",
    "Incident RCA bridge-mode apply verification runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_latency_seconds",
    "Incident RCA bridge-mode apply verification latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_up",
    "Incident RCA bridge-mode apply verification up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_last_run_ts_seconds",
    "Incident RCA bridge-mode apply verification last run timestamp",
)
OBSERVED_VERTEX_USEFULNESS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_observed_vertex_usefulness",
    "Incident RCA bridge-mode apply verification observed vertex usefulness",
)
OBSERVED_LOCAL_USEFULNESS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_observed_local_usefulness",
    "Incident RCA bridge-mode apply verification observed local usefulness",
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
    return {
        "verify_delay_sec": parse_int(raw.get("verify_delay_sec"), DEFAULT_VERIFY_DELAY_SEC),
        "min_provider_samples": parse_int(raw.get("min_provider_samples"), DEFAULT_MIN_PROVIDER_SAMPLES),
        "min_provider_usefulness": parse_float(raw.get("min_provider_usefulness"), DEFAULT_MIN_PROVIDER_USEFULNESS),
        "min_provider_accepted": parse_float(raw.get("min_provider_accepted"), DEFAULT_MIN_PROVIDER_ACCEPTED),
        "require_mode_match": parse_int(raw.get("require_mode_match"), DEFAULT_REQUIRE_MODE_MATCH),
    }


def bridge_mode_from_hash(raw: Dict[str, Any]) -> str:
    return normalize_mode(raw.get("mode", "AUTO"))


def decode_rollup(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    vertex = maybe_json(row.get("vertex_rollup_json"), {})
    local = maybe_json(row.get("local_rollup_json"), {})
    if not isinstance(vertex, dict):
        vertex = {}
    if not isinstance(local, dict):
        local = {}
    return vertex, local


def evaluate_post_apply(
    journal_row: Dict[str, Any],
    current_mode: str,
    latest_vertex_rollup: Dict[str, Any],
    latest_local_rollup: Dict[str, Any],
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    target_mode = normalize_mode(journal_row.get("target_bridge_mode", "AUTO"))
    previous_mode = normalize_mode(journal_row.get("current_bridge_mode", "AUTO"))
    apply_ts_ms = parse_int(journal_row.get("ts_ms"), 0)

    out = {
        "decision": "HOLD",
        "reason_code": "PENDING_SETTLE",
        "current_mode": current_mode,
        "target_mode": target_mode,
        "rollback_mode": previous_mode,
        "observed_vertex_avg_usefulness": parse_float(latest_vertex_rollup.get("avg_usefulness"), 0.0),
        "observed_local_avg_usefulness": parse_float(latest_local_rollup.get("avg_usefulness"), 0.0),
        "observed_vertex_accepted_rate": parse_float(latest_vertex_rollup.get("accepted_rate"), 0.0),
        "observed_local_accepted_rate": parse_float(latest_local_rollup.get("accepted_rate"), 0.0),
        "observed_vertex_n": parse_int(latest_vertex_rollup.get("n"), 0),
        "observed_local_n": parse_int(latest_local_rollup.get("n"), 0),
    }

    if parse_int(journal_row.get("applied"), 0) != 1:
        out["reason_code"] = "NOT_APPLIED"
        return out
    if (now_ms() - apply_ts_ms) < policy["verify_delay_sec"] * 1000:
        out["reason_code"] = "VERIFY_DELAY_NOT_ELAPSED"
        return out
    if policy["require_mode_match"] == 1 and current_mode != target_mode:
        out["decision"] = "ROLLBACK_PREVIOUS_MODE"
        out["reason_code"] = "BRIDGE_MODE_MISMATCH_AFTER_APPLY"
        return out

    if target_mode == "VERTEX_ONLY":
        if out["observed_vertex_n"] < policy["min_provider_samples"]:
            out["reason_code"] = "INSUFFICIENT_VERTEX_SAMPLES"
            return out
        if out["observed_vertex_avg_usefulness"] < policy["min_provider_usefulness"]:
            out["decision"] = "ROLLBACK_PREVIOUS_MODE"
            out["reason_code"] = "VERTEX_ONLY_UNDERPERFORMS_AFTER_APPLY"
            return out
        if out["observed_vertex_accepted_rate"] < policy["min_provider_accepted"]:
            out["decision"] = "ROLLBACK_PREVIOUS_MODE"
            out["reason_code"] = "VERTEX_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY"
            return out
        out["decision"] = "VERIFIED"
        out["reason_code"] = "POST_APPLY_OK"
        return out

    if target_mode == "LOCAL_ONLY":
        if out["observed_local_n"] < policy["min_provider_samples"]:
            out["reason_code"] = "INSUFFICIENT_LOCAL_SAMPLES"
            return out
        if out["observed_local_avg_usefulness"] < policy["min_provider_usefulness"]:
            out["decision"] = "ROLLBACK_PREVIOUS_MODE"
            out["reason_code"] = "LOCAL_ONLY_UNDERPERFORMS_AFTER_APPLY"
            return out
        if out["observed_local_accepted_rate"] < policy["min_provider_accepted"]:
            out["decision"] = "ROLLBACK_PREVIOUS_MODE"
            out["reason_code"] = "LOCAL_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY"
            return out
        out["decision"] = "VERIFIED"
        out["reason_code"] = "POST_APPLY_OK"
        return out

    if target_mode == "AUTO":
        out["decision"] = "VERIFIED"
        out["reason_code"] = "POST_APPLY_OK"
        return out

    out["reason_code"] = "UNKNOWN_TARGET_MODE"
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def xr_recent(r: Any, stream_key: str, count: int) -> List[Dict[str, Any]]:
    try:
        rows = await r.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


async def persist_if_configured(db_url: str, journal_row: Dict[str, Any], verification: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_results (
                    ts_ms, decision, reason_code, current_mode, target_mode, rollback_mode,
                    observed_vertex_avg_usefulness, observed_local_avg_usefulness,
                    observed_vertex_accepted_rate, observed_local_accepted_rate,
                    observed_vertex_n, observed_local_n, verification_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(current_mode)s, %(target_mode)s, %(rollback_mode)s,
                    %(observed_vertex_avg_usefulness)s, %(observed_local_avg_usefulness)s,
                    %(observed_vertex_accepted_rate)s, %(observed_local_accepted_rate)s,
                    %(observed_vertex_n)s, %(observed_local_n)s, %(verification_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": verification["decision"],
                    "reason_code": verification["reason_code"],
                    "current_mode": verification["current_mode"],
                    "target_mode": verification["target_mode"],
                    "rollback_mode": verification["rollback_mode"],
                    "observed_vertex_avg_usefulness": verification["observed_vertex_avg_usefulness"],
                    "observed_local_avg_usefulness": verification["observed_local_avg_usefulness"],
                    "observed_vertex_accepted_rate": verification["observed_vertex_accepted_rate"],
                    "observed_local_accepted_rate": verification["observed_local_accepted_rate"],
                    "observed_vertex_n": verification["observed_vertex_n"],
                    "observed_local_n": verification["observed_local_n"],
                    "verification_json": json.dumps({"journal_row": journal_row, "verification": verification}),
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
                    journal_row = as_dict(payload)
                    verify_policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
                    current_mode = bridge_mode_from_hash(as_dict(await r.hgetall(BRIDGE_POLICY_KEY)))
                    rollups = await xr_recent(r, USEFULNESS_ROLLUPS_STREAM, LOOKBACK_COUNT)
                    latest_rollup = rollups[0] if rollups else {}
                    latest_vertex_rollup, latest_local_rollup = decode_rollup(latest_rollup)
                    verification = evaluate_post_apply(
                        journal_row=journal_row,
                        current_mode=current_mode,
                        latest_vertex_rollup=latest_vertex_rollup,
                        latest_local_rollup=latest_local_rollup,
                        policy=verify_policy,
                    )
                    decision_label = verification["decision"]

                    await persist_if_configured(db_url, journal_row, verification)
                    await r.xadd(
                        OUTPUT_STREAM,
                        {
                            "schema_version": 1,
                            "decision": verification["decision"],
                            "reason_code": verification["reason_code"],
                            "current_mode": verification["current_mode"],
                            "target_mode": verification["target_mode"],
                            "rollback_mode": verification["rollback_mode"],
                            "observed_vertex_avg_usefulness": str(verification["observed_vertex_avg_usefulness"]),
                            "observed_local_avg_usefulness": str(verification["observed_local_avg_usefulness"]),
                            "observed_vertex_accepted_rate": str(verification["observed_vertex_accepted_rate"]),
                            "observed_local_accepted_rate": str(verification["observed_local_accepted_rate"]),
                            "observed_vertex_n": str(verification["observed_vertex_n"]),
                            "observed_local_n": str(verification["observed_local_n"]),
                            "source_journal_ts_ms": str(parse_int(journal_row.get("ts_ms"), 0)),
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "decision": verification["decision"],
                            "reason_code": verification["reason_code"],
                            "current_mode": verification["current_mode"],
                            "target_mode": verification["target_mode"],
                            "rollback_mode": verification["rollback_mode"],
                            "ts_ms": str(now_ms()),
                        },
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFIED",
                            "decision": verification["decision"],
                            "reason_code": verification["reason_code"],
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    if OBSERVED_VERTEX_USEFULNESS:
                        OBSERVED_VERTEX_USEFULNESS.set(verification["observed_vertex_avg_usefulness"])
                    if OBSERVED_LOCAL_USEFULNESS:
                        OBSERVED_LOCAL_USEFULNESS.set(verification["observed_local_avg_usefulness"])
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_FAILED",
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
