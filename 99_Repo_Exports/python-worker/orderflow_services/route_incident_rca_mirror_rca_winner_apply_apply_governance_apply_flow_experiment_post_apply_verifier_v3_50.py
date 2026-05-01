from __future__ import annotations
from utils.time_utils import get_ny_time_millis

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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_post_apply_verifier_v3_50"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_journal",
)
EXPOSURES_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_EXPOSURES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_exposures",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_results",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification:global",
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
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_PORT", "9982"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_MAXLEN", "20000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_LOOKBACK_COUNT", "1000"))

DEFAULT_VERIFY_DELAY_SEC = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_DELAY_SEC",
    "900",
))
DEFAULT_MIN_POST_APPLY_EXPOSURES = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_MIN_POST_APPLY_EXPOSURES",
    "5",
))
DEFAULT_MIN_TARGET_SHARE_FLOOR = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_MIN_TARGET_SHARE_FLOOR",
    "0.25",
))
DEFAULT_SHARE_TOLERANCE = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_SHARE_TOLERANCE",
    "0.20",
))
DEFAULT_REQUIRE_INCUMBENT_MATCH = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_REQUIRE_INCUMBENT_MATCH",
    "1",
))
DEFAULT_MAX_WEIGHT_DELTA_SUM = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_MAX_WEIGHT_DELTA_SUM",
    "0",
))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_runs_total",
    "Apply-flow experiment verification runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_latency_seconds",
    "Apply-flow experiment verification latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_up",
    "Apply-flow experiment verification up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_last_run_ts_seconds",
    "Apply-flow experiment verification last run timestamp",
)
OBSERVED_TARGET_SHARE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_observed_target_share",
    "Apply-flow experiment verification observed target share",
)
POST_APPLY_EXPOSURES = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_post_apply_exposures",
    "Apply-flow experiment verification post apply exposures",
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


def normalize_weights(raw: Any) -> Dict[str, int]:
    obj = maybe_json(raw, {})
    if not isinstance(obj, dict):
        obj = {}
    return {
        "vertex_primary_weight": parse_int(obj.get("vertex_primary_weight"), 0),
        "vertex_compact_weight": parse_int(obj.get("vertex_compact_weight"), 0),
        "local_candidate_weight": parse_int(obj.get("local_candidate_weight"), 0),
    },


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "verify_delay_sec": parse_int(raw.get("verify_delay_sec"), DEFAULT_VERIFY_DELAY_SEC),
        "min_post_apply_exposures": parse_int(raw.get("min_post_apply_exposures"), DEFAULT_MIN_POST_APPLY_EXPOSURES),
        "min_target_share_floor": parse_float(raw.get("min_target_share_floor"), DEFAULT_MIN_TARGET_SHARE_FLOOR),
        "share_tolerance": parse_float(raw.get("share_tolerance"), DEFAULT_SHARE_TOLERANCE),
        "require_incumbent_match": parse_int(raw.get("require_incumbent_match"), DEFAULT_REQUIRE_INCUMBENT_MATCH),
        "max_weight_delta_sum": parse_int(raw.get("max_weight_delta_sum"), DEFAULT_MAX_WEIGHT_DELTA_SUM),
    },


def experiment_policy_from_hash(raw: Dict[str, Any]) -> Dict[str, int]:
    return {
        "vertex_primary_weight": parse_int(raw.get("vertex_primary_weight"), 0),
        "vertex_compact_weight": parse_int(raw.get("vertex_compact_weight"), 0),
        "local_candidate_weight": parse_int(raw.get("local_candidate_weight"), 0),
    },


def winner_policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "incumbent_arm": str(raw.get("incumbent_arm") or "vertex_primary"),
    },


def weights_delta_sum(a: Dict[str, int], b: Dict[str, int]) -> int:
    return (
        abs(parse_int(a.get("vertex_primary_weight"), 0) - parse_int(b.get("vertex_primary_weight"), 0))
        + abs(parse_int(a.get("vertex_compact_weight"), 0) - parse_int(b.get("vertex_compact_weight"), 0))
        + abs(parse_int(a.get("local_candidate_weight"), 0) - parse_int(b.get("local_candidate_weight"), 0))
    )


def expected_target_share(target_weights: Dict[str, int], target_incumbent_arm: str) -> float:
    total = max(
        parse_int(target_weights.get("vertex_primary_weight"), 0)
        + parse_int(target_weights.get("vertex_compact_weight"), 0)
        + parse_int(target_weights.get("local_candidate_weight"), 0),
        1,
    )
    key = (
        "vertex_primary_weight"
        if target_incumbent_arm == "vertex_primary"
        else "vertex_compact_weight"
        if target_incumbent_arm == "vertex_compact_candidate"
        else "local_candidate_weight"
    )
    return round(parse_int(target_weights.get(key), 0) / total, 6)


def observed_target_share(exposure_rows: List[Dict[str, Any]], apply_ts_ms: int, target_incumbent_arm: str) -> Tuple[int, float]:
    filtered = [r for r in exposure_rows if parse_int(r.get("ts_ms"), 0) >= apply_ts_ms]
    total = len(filtered)
    if total == 0:
        return 0, 0.0
    target_n = sum(1 for r in filtered if str(r.get("arm") or "") == target_incumbent_arm)
    return total, round(target_n / total, 6)


def evaluate_post_apply(
    journal_row: Dict[str, Any],
    current_weights: Dict[str, int],
    current_incumbent_arm: str,
    exposure_rows: List[Dict[str, Any]],
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    apply_ts_ms = parse_int(journal_row.get("ts_ms"), 0)
    target_weights = normalize_weights(journal_row.get("target_weights_json"))
    rollback_weights = normalize_weights(journal_row.get("current_weights_json"))
    target_profile = str(journal_row.get("target_profile") or "unknown_profile")
    target_incumbent_arm = str(journal_row.get("winner_arm") or "")
    rollback_incumbent_arm = str(journal_row.get("winner_arm") or "")
    current_profile = str(journal_row.get("current_profile") or "unknown_profile")
    current_incumbent_before = str(journal_row.get("winner_arm") or "")

    out = {
        "decision": "HOLD",
        "reason_code": "PENDING_SETTLE",
        "target_profile": target_profile,
        "current_profile": current_profile,
        "target_incumbent_arm": target_incumbent_arm,
        "rollback_profile": current_profile,
        "rollback_incumbent_arm": current_incumbent_before,
        "target_weights": target_weights,
        "rollback_weights": rollback_weights,
        "post_apply_exposure_n": 0,
        "observed_target_share": 0.0,
        "expected_target_share": expected_target_share(target_weights, target_incumbent_arm),
    },

    applied = parse_int(journal_row.get("applied"), 0)
    if applied != 1:
        out["reason_code"] = "NOT_APPLIED"
        return out

    if (now_ms() - apply_ts_ms) < policy["verify_delay_sec"] * 1000:
        out["reason_code"] = "VERIFY_DELAY_NOT_ELAPSED"
        return out

    delta = weights_delta_sum(current_weights, target_weights)
    if delta > policy["max_weight_delta_sum"]:
        out["decision"] = "ROLLBACK_PREVIOUS_PROFILE"
        out["reason_code"] = "WEIGHTS_MISMATCH_AFTER_APPLY"
        return out

    if policy["require_incumbent_match"] == 1 and current_incumbent_arm != target_incumbent_arm:
        out["decision"] = "ROLLBACK_PREVIOUS_PROFILE"
        out["reason_code"] = "INCUMBENT_MISMATCH_AFTER_APPLY"
        return out

    total_n, observed_share = observed_target_share(exposure_rows, apply_ts_ms, target_incumbent_arm)
    out["post_apply_exposure_n"] = total_n
    out["observed_target_share"] = observed_share

    if total_n < policy["min_post_apply_exposures"]:
        out["reason_code"] = "INSUFFICIENT_POST_APPLY_EXPOSURES"
        return out

    exp_share = out["expected_target_share"]
    floor = max(policy["min_target_share_floor"], exp_share - policy["share_tolerance"])
    if observed_share < floor:
        out["decision"] = "ROLLBACK_PREVIOUS_PROFILE"
        out["reason_code"] = "TARGET_SHARE_TOO_LOW_AFTER_APPLY"
        return out

    out["decision"] = "VERIFIED"
    out["reason_code"] = "POST_APPLY_OK"
    return out


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def read_hash(r: Any, key: str) -> Dict[str, Any]:
    return as_dict(await r.hgetall(key))


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
                """,
                INSERT INTO llm_rr_winner_apply_gov_exp_verification_results (
                    ts_ms, decision, reason_code, target_profile, rollback_profile, target_incumbent_arm, rollback_incumbent_arm,
                    post_apply_exposure_n, observed_target_share, expected_target_share, verification_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(target_profile)s, %(rollback_profile)s, %(target_incumbent_arm)s, %(rollback_incumbent_arm)s,
                    %(post_apply_exposure_n)s, %(observed_target_share)s, %(expected_target_share)s, %(verification_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": verification["decision"],
                    "reason_code": verification["reason_code"],
                    "target_profile": verification["target_profile"],
                    "rollback_profile": verification["rollback_profile"],
                    "target_incumbent_arm": verification["target_incumbent_arm"],
                    "rollback_incumbent_arm": verification["rollback_incumbent_arm"],
                    "post_apply_exposure_n": verification["post_apply_exposure_n"],
                    "observed_target_share": verification["observed_target_share"],
                    "expected_target_share": verification["expected_target_share"],
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
                    verify_policy = policy_from_hash(await read_hash(r, GLOBAL_POLICY_KEY))
                    current_weights = experiment_policy_from_hash(await read_hash(r, EXPERIMENT_POLICY_KEY))
                    current_incumbent_arm = winner_policy_from_hash(await read_hash(r, WINNER_POLICY_KEY))["incumbent_arm"]
                    exposure_rows = await xr_recent(r, EXPOSURES_STREAM, LOOKBACK_COUNT)
                    verification = evaluate_post_apply(
                        journal_row=journal_row,
                        current_weights=current_weights,
                        current_incumbent_arm=current_incumbent_arm,
                        exposure_rows=exposure_rows,
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
                            "target_profile": verification["target_profile"],
                            "rollback_profile": verification["rollback_profile"],
                            "target_incumbent_arm": verification["target_incumbent_arm"],
                            "rollback_incumbent_arm": verification["rollback_incumbent_arm"],
                            "target_weights_json": stable_json(verification["target_weights"]),
                            "rollback_weights_json": stable_json(verification["rollback_weights"]),
                            "post_apply_exposure_n": str(verification["post_apply_exposure_n"]),
                            "observed_target_share": str(verification["observed_target_share"]),
                            "expected_target_share": str(verification["expected_target_share"]),
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
                            "target_profile": verification["target_profile"],
                            "rollback_profile": verification["rollback_profile"],
                            "post_apply_exposure_n": str(verification["post_apply_exposure_n"]),
                            "observed_target_share": str(verification["observed_target_share"]),
                            "expected_target_share": str(verification["expected_target_share"]),
                            "ts_ms": str(now_ms()),
                        },
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFIED",
                            "decision": verification["decision"],
                            "reason_code": verification["reason_code"],
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    if OBSERVED_TARGET_SHARE:
                        OBSERVED_TARGET_SHARE.set(verification["observed_target_share"])
                    if POST_APPLY_EXPOSURES:
                        POST_APPLY_EXPOSURES.set(verification["post_apply_exposure_n"])
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_FAILED",
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
