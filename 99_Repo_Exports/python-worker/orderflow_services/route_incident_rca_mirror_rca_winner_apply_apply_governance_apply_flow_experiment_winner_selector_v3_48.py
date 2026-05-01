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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_selector_v3_48"
EXPOSURES_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_EXPOSURES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_exposures",
)
RESULTS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_results",
)
FEEDBACK_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_FEEDBACK_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_feedback",
)
SCORECARDS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SCORECARDS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_scorecards",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:global",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_PORT", "9980"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MAXLEN", "20000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_LOOKBACK_COUNT", "1000"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_WINDOW_MIN", "10080"))

DEFAULT_MIN_EXPOSURES = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_EXPOSURES",
    "5",
))
DEFAULT_MIN_FEEDBACK = int(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_FEEDBACK",
    "3",
))
DEFAULT_MIN_RESULT_COVERAGE = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_RESULT_COVERAGE",
    "0.50",
))
DEFAULT_MIN_FEEDBACK_COVERAGE = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_FEEDBACK_COVERAGE",
    "0.30",
))
DEFAULT_MIN_QUALITY = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_QUALITY",
    "0.55",
))
DEFAULT_MIN_USEFULNESS = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_USEFULNESS",
    "0.60",
))
DEFAULT_MIN_ACCEPTED_RATE = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_ACCEPTED_RATE",
    "0.60",
))
DEFAULT_MIN_SCORE_MARGIN = float(os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_SCORE_MARGIN",
    "0.05",
))
DEFAULT_INCUMBENT_ARM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_INCUMBENT_ARM",
    "vertex_primary",
)
ARMS = ("vertex_primary", "vertex_compact_candidate", "local_candidate")


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_runs_total",
    "Apply-flow experiment winner selector runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_latency_seconds",
    "Apply-flow experiment winner selector latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_up",
    "Apply-flow experiment winner selector up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_last_run_ts_seconds",
    "Apply-flow experiment winner selector last run timestamp",
)
ARM_SCORE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_arm_score",
    "Apply-flow experiment arm score",
    ("arm",),
)
ARM_FEEDBACK_N = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_arm_feedback_n",
    "Apply-flow experiment arm feedback count",
    ("arm",),
)
ARM_ACCEPTED_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_arm_accepted_rate",
    "Apply-flow experiment arm accepted rate",
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


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    incumbent = str(raw.get("incumbent_arm") or DEFAULT_INCUMBENT_ARM)
    if incumbent not in ARMS:
        incumbent = DEFAULT_INCUMBENT_ARM
    return {
        "min_exposures": parse_int(raw.get("min_exposures"), DEFAULT_MIN_EXPOSURES),
        "min_feedback": parse_int(raw.get("min_feedback"), DEFAULT_MIN_FEEDBACK),
        "min_result_coverage": parse_float(raw.get("min_result_coverage"), DEFAULT_MIN_RESULT_COVERAGE),
        "min_feedback_coverage": parse_float(raw.get("min_feedback_coverage"), DEFAULT_MIN_FEEDBACK_COVERAGE),
        "min_quality": parse_float(raw.get("min_quality"), DEFAULT_MIN_QUALITY),
        "min_usefulness": parse_float(raw.get("min_usefulness"), DEFAULT_MIN_USEFULNESS),
        "min_accepted_rate": parse_float(raw.get("min_accepted_rate"), DEFAULT_MIN_ACCEPTED_RATE),
        "min_score_margin": parse_float(raw.get("min_score_margin"), DEFAULT_MIN_SCORE_MARGIN),
        "incumbent_arm": incumbent,
    },


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


def _recent(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cutoff = now_ms() - WINDOW_MIN * 60 * 1000
    return [r for r in rows if parse_int(r.get("ts_ms"), 0) >= cutoff]


def build_scorecards(exposures: List[Dict[str, Any]], results: List[Dict[str, Any]], feedback: List[Dict[str, Any]], policy: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    exp_recent = _recent(exposures)
    res_recent = _recent(results)
    fb_recent = _recent(feedback)

    exp_map: Dict[str, str] = {}
    for row in exp_recent:
        rid = str(row.get("request_id") or "")
        arm = str(row.get("arm") or "")
        if rid and arm in ARMS:
            exp_map[rid] = arm

    res_map: Dict[str, Dict[str, Any]] = {}
    for row in res_recent:
        rid = str(row.get("request_id") or "")
        if rid:
            res_map[rid] = row

    arm_feedback: Dict[str, List[Dict[str, Any]]] = {arm: [] for arm in ARMS}
    arm_exposures_n: Dict[str, int] = {arm: 0 for arm in ARMS}
    arm_results_n: Dict[str, int] = {arm: 0 for arm in ARMS}

    for rid, arm in exp_map.items():
        arm_exposures_n[arm] += 1
        if rid in res_map:
            arm_results_n[arm] += 1

    for row in fb_recent:
        rid = str(row.get("request_id") or "")
        arm = exp_map.get(rid, "")
        if arm in ARMS:
            arm_feedback[arm].append(row)

    scorecards: Dict[str, Dict[str, Any]] = {}
    for arm in ARMS:
        frows = arm_feedback[arm]
        exposure_n = arm_exposures_n[arm]
        result_n = arm_results_n[arm]
        feedback_n = len(frows)
        avg_quality = round(sum(parse_float(r.get("quality_score"), 0.0) for r in frows) / feedback_n, 6) if feedback_n else 0.0
        avg_usefulness = round(sum(parse_float(r.get("usefulness_score"), 0.0) for r in frows) / feedback_n, 6) if feedback_n else 0.0
        accepted_rate = round(sum(parse_int(r.get("accepted"), 0) for r in frows) / feedback_n, 6) if feedback_n else 0.0
        result_coverage = round((result_n / exposure_n), 6) if exposure_n else 0.0
        feedback_coverage = round((feedback_n / exposure_n), 6) if exposure_n else 0.0
        coverage_multiplier = round(min(result_coverage, 1.0) * min(feedback_coverage, 1.0), 6)
        score_raw = round((0.4 * avg_usefulness) + (0.3 * avg_quality) + (0.3 * accepted_rate), 6)
        score = round(score_raw * coverage_multiplier, 6)
        eligible = int(
            exposure_n >= policy["min_exposures"]
            and feedback_n >= policy["min_feedback"]
            and result_coverage >= policy["min_result_coverage"]
            and feedback_coverage >= policy["min_feedback_coverage"]
            and avg_quality >= policy["min_quality"]
            and avg_usefulness >= policy["min_usefulness"]
            and accepted_rate >= policy["min_accepted_rate"]
        )
        scorecards[arm] = {
            "arm": arm,
            "exposure_n": exposure_n,
            "result_n": result_n,
            "feedback_n": feedback_n,
            "avg_quality": avg_quality,
            "avg_usefulness": avg_usefulness,
            "accepted_rate": accepted_rate,
            "result_coverage": result_coverage,
            "feedback_coverage": feedback_coverage,
            "coverage_multiplier": coverage_multiplier,
            "score_raw": score_raw,
            "score": score,
            "eligible": eligible,
        },
    return scorecards


def select_winner(scorecards: Dict[str, Dict[str, Any]], policy: Dict[str, Any]) -> Dict[str, Any]:
    incumbent_arm = policy["incumbent_arm"]
    incumbent = scorecards.get(incumbent_arm, {})
    best_arm = incumbent_arm
    best_score = parse_float(incumbent.get("score"), 0.0)
    best_eligible = parse_int(incumbent.get("eligible"), 0)

    for arm, scorecard in scorecards.items():
        if arm == incumbent_arm:
            continue
        if parse_int(scorecard["eligible"], 0) != 1:
            continue
        if parse_float(scorecard["score"], 0.0) > best_score:
            best_arm = arm
            best_score = parse_float(scorecard["score"], 0.0)
            best_eligible = 1

    if not incumbent or parse_int(incumbent.get("eligible"), 0) != 1:
        if best_arm != incumbent_arm and best_eligible == 1:
            if best_arm == "vertex_compact_candidate":
                return {"decision": "PROMOTE_VERTEX_COMPACT_CANDIDATE", "winner_arm": best_arm, "reason_code": "INCUMBENT_NOT_ELIGIBLE"}
            if best_arm == "local_candidate":
                return {"decision": "PROMOTE_LOCAL_CANDIDATE", "winner_arm": best_arm, "reason_code": "INCUMBENT_NOT_ELIGIBLE"}
        return {"decision": "KEEP_VERTEX_PRIMARY", "winner_arm": incumbent_arm, "reason_code": "INSUFFICIENT_ELIGIBLE_DATA"}

    if best_arm == incumbent_arm:
        return {"decision": "KEEP_VERTEX_PRIMARY", "winner_arm": incumbent_arm, "reason_code": "INCUMBENT_STILL_BEST"}

    margin = parse_float(scorecards[best_arm]["score"], 0.0) - parse_float(incumbent.get("score"), 0.0)
    if margin < policy["min_score_margin"]:
        return {"decision": "KEEP_VERTEX_PRIMARY", "winner_arm": incumbent_arm, "reason_code": "MARGIN_TOO_SMALL"}

    if best_arm == "vertex_compact_candidate":
        return {"decision": "PROMOTE_VERTEX_COMPACT_CANDIDATE", "winner_arm": best_arm, "reason_code": "WINNER_SCORE_ABOVE_INCUMBENT"}
    if best_arm == "local_candidate":
        return {"decision": "PROMOTE_LOCAL_CANDIDATE", "winner_arm": best_arm, "reason_code": "WINNER_SCORE_ABOVE_INCUMBENT"}
    return {"decision": "KEEP_VERTEX_PRIMARY", "winner_arm": incumbent_arm, "reason_code": "UNKNOWN_WINNER"}


async def persist_if_configured(db_url: str, feedback_row: Dict[str, Any] | None, scorecards: Dict[str, Dict[str, Any]], decision: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            if feedback_row is not None:
                cur.execute(
                    """,
                    INSERT INTO llm_rca_gov_apply_flow_exp_feedback (
                        request_id, bundle_id, ts_ms, quality_score, usefulness_score, accepted, reason_code, feedback_json
                    ) VALUES (
                        %(request_id)s, %(bundle_id)s, %(ts_ms)s, %(quality_score)s, %(usefulness_score)s, %(accepted)s, %(reason_code)s, %(feedback_json)s
                    )
                    """,
                    {
                        "request_id": feedback_row.get("request_id", ""),
                        "bundle_id": feedback_row.get("bundle_id", ""),
                        "ts_ms": parse_int(feedback_row.get("ts_ms"), now_ms()),
                        "quality_score": parse_float(feedback_row.get("quality_score"), 0.0),
                        "usefulness_score": parse_float(feedback_row.get("usefulness_score"), 0.0),
                        "accepted": parse_int(feedback_row.get("accepted"), 0),
                        "reason_code": feedback_row.get("reason_code", ""),
                        "feedback_json": json.dumps(feedback_row),
                    },
                )
            for arm in ARMS:
                sc = scorecards[arm]
                cur.execute(
                    """,
                    INSERT INTO llm_rca_gov_apply_flow_exp_scorec (
                        ts_ms, arm, exposure_n, result_n, feedback_n, avg_quality, avg_usefulness, accepted_rate,
                        result_coverage, feedback_coverage, coverage_multiplier, score_raw, score, eligible, scorecard_json
                    ) VALUES (
                        %(ts_ms)s, %(arm)s, %(exposure_n)s, %(result_n)s, %(feedback_n)s, %(avg_quality)s, %(avg_usefulness)s, %(accepted_rate)s,
                        %(result_coverage)s, %(feedback_coverage)s, %(coverage_multiplier)s, %(score_raw)s, %(score)s, %(eligible)s, %(scorecard_json)s
                    )
                    """,
                    {
                        "ts_ms": now_ms(),
                        **sc,
                        "scorecard_json": json.dumps(sc),
                    },
                )
            cur.execute(
                """,
                INSERT INTO llm_rca_gov_apply_flow_exp_win_dec (
                    ts_ms, decision, reason_code, winner_arm, decision_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(winner_arm)s, %(decision_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "winner_arm": decision["winner_arm"],
                    "decision_json": json.dumps({"scorecards": scorecards, "decision": decision}),
                },
            )
            conn.commit()


async def ensure_feedback_group(r: Any) -> None:
    await ensure_group(r, FEEDBACK_STREAM, GROUP)


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    await ensure_feedback_group(r)
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {FEEDBACK_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                decision_label = "KEEP_VERTEX_PRIMARY"
                try:
                    feedback_row = as_dict(payload)
                    exposures = await xr_recent(r, EXPOSURES_STREAM, LOOKBACK_COUNT)
                    results = await xr_recent(r, RESULTS_STREAM, LOOKBACK_COUNT)
                    feedback = await xr_recent(r, FEEDBACK_STREAM, LOOKBACK_COUNT)
                    scorecards = build_scorecards(exposures, results, feedback + [feedback_row], policy_from_hash({}))
                    policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
                    try:
                        exec_kill = await r.get('trade:exec_kill_switch')
                        if exec_kill and exec_kill.decode().strip() == '1':
                            policy['kill_switch'] = 1
                    except: pass
                    scorecards = build_scorecards(exposures, results, feedback + [feedback_row], policy)
                    decision = select_winner(scorecards, policy)
                    decision_label = decision["decision"]

                    await persist_if_configured(db_url, feedback_row, scorecards, decision)
                    for arm in ARMS:
                        sc = scorecards[arm]
                        await r.xadd(
                            SCORECARDS_STREAM,
                            {
                                "schema_version": 1,
                                "arm": arm,
                                "scorecard_json": stable_json(sc),
                                "ts_ms": str(now_ms()),
                            },
                            maxlen=MAXLEN,
                            approximate=True,
                        )
                        if ARM_SCORE:
                            ARM_SCORE.labels(arm=arm).set(sc["score"])
                        if ARM_FEEDBACK_N:
                            ARM_FEEDBACK_N.labels(arm=arm).set(sc["feedback_n"])
                        if ARM_ACCEPTED_RATE:
                            ARM_ACCEPTED_RATE.labels(arm=arm).set(sc["accepted_rate"])

                    await r.xadd(
                        DECISIONS_STREAM,
                        {
                            "schema_version": 1,
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "winner_arm": decision["winner_arm"],
                            "scorecards_json": stable_json(scorecards),
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
                            "winner_arm": decision["winner_arm"],
                            "ts_ms": str(now_ms()),
                        },
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_DECIDED",
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "winner_arm": decision["winner_arm"],
                            "ts_ms": str(now_ms()),
                        },
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(FEEDBACK_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_FAILED",
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
