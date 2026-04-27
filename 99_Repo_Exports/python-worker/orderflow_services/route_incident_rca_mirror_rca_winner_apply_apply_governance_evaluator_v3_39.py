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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_v3_39"
EXPOSURES_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EXPOSURES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment_exposures",
)
RESULT_STREAMS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_RESULT_STREAMS_JSON",
    '["stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_results","stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_candidate_results","stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_local_rca_candidate_results"]',
)
FEEDBACK_STREAMS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_FEEDBACK_STREAMS_JSON",
    '["stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_feedback","stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_candidate_feedback","stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_local_rca_candidate_feedback"]',
)
SCORECARDS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_SCORECARDS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_scorecards",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator:global",
)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_PORT", "9967"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MAXLEN", "20000"))
RUN_EVERY_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_RUN_EVERY_SEC", "300"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_LOOKBACK_COUNT", "500"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_WINDOW_MIN", "10080"))

DEFAULT_MIN_EXPOSURES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MIN_EXPOSURES", "10"))
DEFAULT_MIN_FEEDBACK = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MIN_FEEDBACK", "5"))
DEFAULT_MIN_RESULT_COVERAGE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MIN_RESULT_COVERAGE", "0.30"))
DEFAULT_MIN_FEEDBACK_COVERAGE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MIN_FEEDBACK_COVERAGE", "0.20"))
DEFAULT_MIN_QUALITY = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MIN_QUALITY", "0.55"))
DEFAULT_MIN_USEFULNESS = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MIN_USEFULNESS", "0.60"))
DEFAULT_MIN_ACCEPTED_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MIN_ACCEPTED_RATE", "0.60"))
DEFAULT_MIN_SCORE_MARGIN = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_MIN_SCORE_MARGIN", "0.05"))
DEFAULT_INCUMBENT_ARM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_INCUMBENT_ARM", "deterministic")
DEFAULT_ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_ADVISORY_ONLY", "1"))
DEFAULT_EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_EXECUTOR_MODE", "DRY_RUN").upper()

ARMS = ("deterministic", "vertex_candidate", "local_fallback_candidate")


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_runs_total",
    "Winner-apply apply governance evaluator runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_latency_seconds",
    "Winner-apply apply governance evaluator latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_up",
    "Winner-apply apply governance evaluator up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_last_run_ts_seconds",
    "Winner-apply apply governance evaluator last run timestamp",
)
ARM_SCORE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_arm_score",
    "Winner-apply apply governance evaluator arm score",
    ("arm",),
)
ARM_FEEDBACK_N = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_arm_feedback_n",
    "Winner-apply apply governance evaluator arm feedback count",
    ("arm",),
)
ARM_ACCEPTED_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_evaluator_arm_accepted_rate",
    "Winner-apply apply governance evaluator arm accepted rate",
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


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    incumbent_arm = str(raw.get("incumbent_arm") or DEFAULT_INCUMBENT_ARM)
    if incumbent_arm not in ARMS:
        incumbent_arm = "deterministic"
    return {
        "min_exposures": parse_int(raw.get("min_exposures"), DEFAULT_MIN_EXPOSURES),
        "min_feedback": parse_int(raw.get("min_feedback"), DEFAULT_MIN_FEEDBACK),
        "min_result_coverage": parse_float(raw.get("min_result_coverage"), DEFAULT_MIN_RESULT_COVERAGE),
        "min_feedback_coverage": parse_float(raw.get("min_feedback_coverage"), DEFAULT_MIN_FEEDBACK_COVERAGE),
        "min_quality": parse_float(raw.get("min_quality"), DEFAULT_MIN_QUALITY),
        "min_usefulness": parse_float(raw.get("min_usefulness"), DEFAULT_MIN_USEFULNESS),
        "min_accepted_rate": parse_float(raw.get("min_accepted_rate"), DEFAULT_MIN_ACCEPTED_RATE),
        "min_score_margin": parse_float(raw.get("min_score_margin"), DEFAULT_MIN_SCORE_MARGIN),
        "incumbent_arm": incumbent_arm,
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
    }


def arm_from_request_id(request_id: str, explicit_arm: str = "") -> str:
    if explicit_arm in ARMS:
        return explicit_arm
    rid = str(request_id or "")
    suffix = rid.rsplit(":", 1)[-1] if ":" in rid else ""
    if suffix in ARMS:
        return suffix
    return ""


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


def window_filter(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cutoff = now_ms() - WINDOW_MIN * 60 * 1000
    return [r for r in rows if parse_int(r.get("ts_ms"), 0) >= cutoff]


def empty_scorecard(arm: str) -> Dict[str, Any]:
    return {
        "arm": arm,
        "exposure_n": 0,
        "result_n": 0,
        "feedback_n": 0,
        "avg_quality": 0.0,
        "avg_usefulness": 0.0,
        "accepted_rate": 0.0,
        "result_coverage": 0.0,
        "feedback_coverage": 0.0,
        "coverage_multiplier": 0.0,
        "score_raw": 0.0,
        "score": 0.0,
        "eligible": 0,
        "reason_codes": [],
    }


def scorecards_from_rows(
    exposures: List[Dict[str, Any]],
    results: List[Dict[str, Any]],
    feedback: List[Dict[str, Any]],
    policy: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    cards = {arm: empty_scorecard(arm) for arm in ARMS}

    for row in exposures:
        arm = str(row.get("arm") or "")
        if arm in cards:
            cards[arm]["exposure_n"] += 1

    for row in results:
        arm = arm_from_request_id(str(row.get("request_id") or ""), str(row.get("arm") or ""))
        if arm in cards:
            cards[arm]["result_n"] += 1

    fb_values: Dict[str, Dict[str, List[float]]] = {arm: {"q": [], "u": [], "a": []} for arm in ARMS}
    for row in feedback:
        arm = arm_from_request_id(str(row.get("request_id") or ""), str(row.get("arm") or ""))
        if arm in cards:
            cards[arm]["feedback_n"] += 1
            fb_values[arm]["q"].append(parse_float(row.get("quality_score"), 0.0))
            fb_values[arm]["u"].append(parse_float(row.get("usefulness_score"), 0.0))
            fb_values[arm]["a"].append(float(parse_int(row.get("accepted"), 0)))

    for arm, card in cards.items():
        exposure_n = max(card["exposure_n"], 0)
        result_n = max(card["result_n"], 0)
        feedback_n = max(card["feedback_n"], 0)
        q = fb_values[arm]["q"]
        u = fb_values[arm]["u"]
        a = fb_values[arm]["a"]
        card["avg_quality"] = round(sum(q) / len(q), 6) if q else 0.0
        card["avg_usefulness"] = round(sum(u) / len(u), 6) if u else 0.0
        card["accepted_rate"] = round(sum(a) / len(a), 6) if a else 0.0
        card["result_coverage"] = round((result_n / exposure_n), 6) if exposure_n > 0 else 0.0
        card["feedback_coverage"] = round((feedback_n / exposure_n), 6) if exposure_n > 0 else 0.0

        result_cov_norm = min(1.0, card["result_coverage"] / max(policy["min_result_coverage"], 1e-9))
        feedback_cov_norm = min(1.0, card["feedback_coverage"] / max(policy["min_feedback_coverage"], 1e-9))
        card["coverage_multiplier"] = round(0.4 * result_cov_norm + 0.6 * feedback_cov_norm, 6)
        card["score_raw"] = round(
            0.40 * card["avg_usefulness"] + 0.35 * card["avg_quality"] + 0.25 * card["accepted_rate"],
            6,
        )
        card["score"] = round(card["score_raw"] * card["coverage_multiplier"], 6)

        reasons: List[str] = []
        if exposure_n < policy["min_exposures"]:
            reasons.append("EXPOSURES_TOO_LOW")
        if feedback_n < policy["min_feedback"]:
            reasons.append("FEEDBACK_TOO_LOW")
        if card["result_coverage"] < policy["min_result_coverage"]:
            reasons.append("RESULT_COVERAGE_TOO_LOW")
        if card["feedback_coverage"] < policy["min_feedback_coverage"]:
            reasons.append("FEEDBACK_COVERAGE_TOO_LOW")
        if q and card["avg_quality"] < policy["min_quality"]:
            reasons.append("QUALITY_TOO_LOW")
        if u and card["avg_usefulness"] < policy["min_usefulness"]:
            reasons.append("USEFULNESS_TOO_LOW")
        if a and card["accepted_rate"] < policy["min_accepted_rate"]:
            reasons.append("ACCEPTED_RATE_TOO_LOW")

        eligible = (
            exposure_n >= policy["min_exposures"]
            and feedback_n >= policy["min_feedback"]
            and card["result_coverage"] >= policy["min_result_coverage"]
            and card["feedback_coverage"] >= policy["min_feedback_coverage"]
            and card["avg_quality"] >= policy["min_quality"]
            and card["avg_usefulness"] >= policy["min_usefulness"]
            and card["accepted_rate"] >= policy["min_accepted_rate"]
        )
        card["eligible"] = 1 if eligible else 0
        card["reason_codes"] = reasons or ["OK"]

    return cards


def recommend(cards: Dict[str, Dict[str, Any]], policy: Dict[str, Any]) -> Dict[str, Any]:
    incumbent = policy["incumbent_arm"]
    incumbent_card = cards.get(incumbent, empty_scorecard(incumbent))

    candidates = [cards[a] for a in ARMS if a != incumbent and cards[a]["eligible"] == 1]
    if not candidates:
        return {
            "decision": "KEEP_DETERMINISTIC" if incumbent == "deterministic" else f"KEEP_{incumbent.upper()}",
            "reason_code": "NO_ELIGIBLE_CANDIDATE",
            "winner_arm": incumbent,
            "incumbent_arm": incumbent,
            "winner_score": incumbent_card["score"],
            "incumbent_score": incumbent_card["score"],
        }

    best = max(candidates, key=lambda c: (c["score"], c["avg_usefulness"], c["avg_quality"]))
    margin = round(best["score"] - incumbent_card["score"], 6)

    if margin < policy["min_score_margin"]:
        return {
            "decision": "KEEP_DETERMINISTIC" if incumbent == "deterministic" else f"KEEP_{incumbent.upper()}",
            "reason_code": "INCUMBENT_STILL_BEST",
            "winner_arm": incumbent,
            "incumbent_arm": incumbent,
            "winner_score": incumbent_card["score"],
            "incumbent_score": incumbent_card["score"],
        }

    if best["arm"] == "vertex_candidate":
        decision = "PROMOTE_VERTEX_CANDIDATE"
    elif best["arm"] == "local_fallback_candidate":
        decision = "PROMOTE_LOCAL_FALLBACK_CANDIDATE"
    else:
        decision = "KEEP_DETERMINISTIC"

    return {
        "decision": decision,
        "reason_code": "CANDIDATE_OUTPERFORMS_INCUMBENT",
        "winner_arm": best["arm"],
        "incumbent_arm": incumbent,
        "winner_score": best["score"],
        "incumbent_score": incumbent_card["score"],
    }


async def persist_if_configured(db_url: str, scorecards: Dict[str, Dict[str, Any]], recommendation: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            for arm, card in scorecards.items():
                cur.execute(
                    """
                    INSERT INTO llm_governance_scorecards (
                        ts_ms, arm, exposure_n, result_n, feedback_n, avg_quality,
                        avg_usefulness, accepted_rate, result_coverage, feedback_coverage,
                        coverage_multiplier, score_raw, score, eligible, reason_codes_json
                    ) VALUES (
                        %(ts_ms)s, %(arm)s, %(exposure_n)s, %(result_n)s, %(feedback_n)s, %(avg_quality)s,
                        %(avg_usefulness)s, %(accepted_rate)s, %(result_coverage)s, %(feedback_coverage)s,
                        %(coverage_multiplier)s, %(score_raw)s, %(score)s, %(eligible)s, %(reason_codes_json)s
                    )
                    """,
                    {
                        "ts_ms": now_ms(),
                        "arm": arm,
                        **{k: v for k, v in card.items() if k not in {"arm", "reason_codes"}},
                        "reason_codes_json": json.dumps(card["reason_codes"]),
                    },
                )
            cur.execute(
                """
                INSERT INTO llm_governance_evaluator_decisions (
                    ts_ms, decision, reason_code, winner_arm, incumbent_arm,
                    winner_score, incumbent_score, decision_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(winner_arm)s, %(incumbent_arm)s,
                    %(winner_score)s, %(incumbent_score)s, %(decision_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    **recommendation,
                    "decision_json": json.dumps(recommendation),
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
    db_url = os.getenv("DATABASE_URL", "")

    result_streams = maybe_json(RESULT_STREAMS_JSON, [])
    if not isinstance(result_streams, list):
        result_streams = []
    feedback_streams = maybe_json(FEEDBACK_STREAMS_JSON, [])
    if not isinstance(feedback_streams, list):
        feedback_streams = []

    while True:
        started = time.perf_counter()
        status = "ok"
        decision_label = "KEEP_DETERMINISTIC"
        try:
            policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
            try:
                exec_kill = await r.get('trade:exec_kill_switch')
                if exec_kill and exec_kill.decode().strip() == '1':
                    policy['kill_switch'] = 1
            except: pass
            exposures = window_filter(await xr_recent(r, EXPOSURES_STREAM, LOOKBACK_COUNT))

            results: List[Dict[str, Any]] = []
            for s in result_streams:
                if isinstance(s, str) and s:
                    results.extend(window_filter(await xr_recent(r, s, LOOKBACK_COUNT)))

            feedback: List[Dict[str, Any]] = []
            for s in feedback_streams:
                if isinstance(s, str) and s:
                    feedback.extend(window_filter(await xr_recent(r, s, LOOKBACK_COUNT)))

            scorecards = scorecards_from_rows(exposures, results, feedback, policy)
            recommendation = recommend(scorecards, policy)
            decision_label = recommendation["decision"]

            await persist_if_configured(db_url, scorecards, recommendation)

            for arm, card in scorecards.items():
                await r.xadd(
                    SCORECARDS_STREAM,
                    {
                        "schema_version": 1,
                        "arm": arm,
                        "scorecard_json": stable_json(card),
                        "ts_ms": str(now_ms()),
                    },
                    maxlen=MAXLEN,
                    approximate=True,
                )
                if ARM_SCORE:
                    ARM_SCORE.labels(arm=arm).set(card["score"])
                if ARM_FEEDBACK_N:
                    ARM_FEEDBACK_N.labels(arm=arm).set(card["feedback_n"])
                if ARM_ACCEPTED_RATE:
                    ARM_ACCEPTED_RATE.labels(arm=arm).set(card["accepted_rate"])

            await r.xadd(
                DECISIONS_STREAM,
                {
                    "schema_version": 1,
                    "decision": recommendation["decision"],
                    "reason_code": recommendation["reason_code"],
                    "winner_arm": recommendation["winner_arm"],
                    "incumbent_arm": recommendation["incumbent_arm"],
                    "winner_score": str(recommendation["winner_score"]),
                    "incumbent_score": str(recommendation["incumbent_score"]),
                    "scorecards_json": stable_json(scorecards),
                    "ts_ms": str(now_ms()),
                },
                maxlen=MAXLEN,
                approximate=True,
            )
            await r.hset(
                LAST_HASH,
                mapping={
                    "decision": recommendation["decision"],
                    "reason_code": recommendation["reason_code"],
                    "winner_arm": recommendation["winner_arm"],
                    "incumbent_arm": recommendation["incumbent_arm"],
                    "winner_score": str(recommendation["winner_score"]),
                    "incumbent_score": str(recommendation["incumbent_score"]),
                    "ts_ms": str(now_ms()),
                },
            )
            await r.xadd(
                AUDIT_STREAM,
                {
                    "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_DECIDED",
                    "decision": recommendation["decision"],
                    "reason_code": recommendation["reason_code"],
                    "winner_arm": recommendation["winner_arm"],
                    "ts_ms": str(now_ms()),
                },
                maxlen=MAXLEN,
                approximate=True,
            )
            if LAST_RUN_TS:
                LAST_RUN_TS.set(time.time())
        except Exception as exc:
            status = "error"
            await r.xadd(
                AUDIT_STREAM,
                {
                    "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EVALUATOR_FAILED",
                    "error": str(exc),
                    "ts_ms": str(now_ms()),
                },
                maxlen=MAXLEN,
                approximate=True,
            )
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_label).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
            await asyncio.sleep(max(RUN_EVERY_SEC, 5))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
