from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from collections import defaultdict
from typing import Any, Dict, Tuple, List, Optional

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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_evaluator_v3_23"

EXPOSURES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_EXPOSURES_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_experiment_exposures")
RESULTS_STREAMS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_RESULTS_STREAMS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_results").split(",")
FEEDBACK_STREAMS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_FEEDBACK_STREAMS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_feedback").split(",")

SCORECARDS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_SCORECARDS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_scorecards")
DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_evaluator_decisions")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_evaluator_audit")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_evaluator:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_PORT", "9945"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MAXLEN", "2000"))

MIN_EXPOSURES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_EXPOSURES", "10"))
MIN_FEEDBACK = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_FEEDBACK", "5"))
MIN_RESULT_COVERAGE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_RESULT_COVERAGE", "0.30"))
MIN_FEEDBACK_COVERAGE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_FEEDBACK_COVERAGE", "0.20"))
MIN_QUALITY = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_QUALITY", "0.55"))
MIN_USEFULNESS = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_USEFULNESS", "0.60"))
MIN_ACCEPTED_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_ACCEPTED_RATE", "0.60"))
MIN_SCORE_MARGIN = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_SCORE_MARGIN", "0.05"))
INCUMBENT_ARM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_INCUMBENT_ARM", "deterministic")

POLL_INTERVAL_SEC = 10.0
LOOKBACK_COUNT = 1000

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_evaluator_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_evaluator_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_evaluator_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_evaluator_last_run_ts_seconds", "Last run")

ARM_SCORE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_evaluator_arm_score", "Arm Score", ("arm",))
ARM_FDBK = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_evaluator_arm_feedback_n", "Arm Feedback N", ("arm",))
ARM_ACC = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_evaluator_arm_accepted_rate", "Arm Accepted", ("arm",))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default

def parse_arm_from_request_id(request_id: str) -> str:
    # Example format: req_1775497536507_0_vertex_candidate 
    # Mapped by consumer. For simplicity if arm suffix is present we split, else we return incumbent
    if "vertex_candidate" in request_id:
        return "vertex_candidate"
    if "local_fallback_candidate" in request_id:
        return "local_fallback_candidate"
    if "deterministic" in request_id:
        return "deterministic"
    return INCUMBENT_ARM

def build_scorecards(exposures: List[Dict[str, Any]], results: List[Dict[str, Any]], feedbacks: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    arms: Dict[str, Dict[str, float]] = defaultdict(lambda: {"exposure_n": 0, "result_n": 0, "feedback_n": 0, "q_sum": 0.0, "u_sum": 0.0, "acc_sum": 0.0})
    
    # 1. Count exposures
    for e in exposures:
        arm = e.get("arm", INCUMBENT_ARM)
        arms[arm]["exposure_n"] += 1
        
    # 2. Count results
    for r in results:
        arm = parse_arm_from_request_id(r.get("request_id", ""))
        arms[arm]["result_n"] += 1
        
    # 3. Aggregate feedback
    for f in feedbacks:
        arm = parse_arm_from_request_id(f.get("request_id", ""))
        arms[arm]["feedback_n"] += 1
        arms[arm]["q_sum"] += safe_float(f.get("quality_score", "0"))
        arms[arm]["u_sum"] += safe_float(f.get("usefulness_score", "0"))
        arms[arm]["acc_sum"] += safe_float(f.get("accepted", "0"))
        
    # 4. Finalize scorecards
    scorecards = {}
    for arm, data in arms.items():
        n_e = max(data["exposure_n"], 1) # Prevent div by 0 for coverage
        n_f = max(data["feedback_n"], 1)
        
        avg_q = data["q_sum"] / n_f if data["feedback_n"] > 0 else 0.0
        avg_u = data["u_sum"] / n_f if data["feedback_n"] > 0 else 0.0
        acc_r = data["acc_sum"] / n_f if data["feedback_n"] > 0 else 0.0
        
        res_cov = data["result_n"] / n_e
        fdbk_cov = data["feedback_n"] / n_e
        
        cov_multiplier = (res_cov * fdbk_cov) ** 0.5 # Geometric mean penalizes heavily if one is very low
        
        score_raw = (avg_q * 0.3 + avg_u * 0.4 + acc_r * 0.3)
        score = score_raw * cov_multiplier
        
        eligible = (
            data["exposure_n"] >= MIN_EXPOSURES and
            data["feedback_n"] >= MIN_FEEDBACK and
            res_cov >= MIN_RESULT_COVERAGE and
            fdbk_cov >= MIN_FEEDBACK_COVERAGE and
            avg_q >= MIN_QUALITY and
            avg_u >= MIN_USEFULNESS and
            acc_r >= MIN_ACCEPTED_RATE
        )
        
        scorecards[arm] = {
            "exposure_n": data["exposure_n"],
            "result_n": data["result_n"],
            "feedback_n": data["feedback_n"],
            "avg_quality": avg_q,
            "avg_usefulness": avg_u,
            "accepted_rate": acc_r,
            "result_coverage": res_cov,
            "feedback_coverage": fdbk_cov,
            "coverage_multiplier": cov_multiplier,
            "score_raw": score_raw,
            "score": score,
            "eligible": 1 if eligible else 0
        }
    return scorecards

def evaluate_winner(scorecards: Dict[str, Dict[str, float]]) -> Tuple[str, str]:
    incumbent_score = 0.0
    if INCUMBENT_ARM in scorecards:
        incumbent_score = scorecards[INCUMBENT_ARM]["score"]
        
    best_candidate = INCUMBENT_ARM
    best_score = incumbent_score
    
    for arm, sc in scorecards.items():
        if arm == INCUMBENT_ARM:
            continue
            
        if sc["eligible"] == 1:
            margin = sc["score"] - incumbent_score
            if margin >= MIN_SCORE_MARGIN and sc["score"] > best_score:
                best_score = sc["score"]
                best_candidate = arm
                
    if best_candidate == INCUMBENT_ARM:
        return "KEEP_DETERMINISTIC", INCUMBENT_ARM
        
    return f"PROMOTE_{best_candidate.upper()}", best_candidate

async def persist_scorecards(db_url: str, decision_id: str, scorecards: Dict[str, Dict[str, float]]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            for arm, sc in scorecards.items():
                cur.execute(
                    """
                    INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_scorecards (
                        decision_id, arm, exposure_n, result_n, feedback_n, 
                        avg_quality, avg_usefulness, accepted_rate, result_coverage, feedback_coverage, 
                        score, eligible, ts_ms
                    ) VALUES (
                        %(decision_id)s, %(arm)s, %(exposure_n)s, %(result_n)s, %(feedback_n)s,
                        %(avg_quality)s, %(avg_usefulness)s, %(accepted_rate)s, %(result_coverage)s, %(feedback_coverage)s,
                        %(score)s, %(eligible)s, %(ts_ms)s
                    )
                    """,
                    {
                        "decision_id": decision_id,
                        "arm": arm,
                        "exposure_n": sc["exposure_n"],
                        "result_n": sc["result_n"],
                        "feedback_n": sc["feedback_n"],
                        "avg_quality": sc["avg_quality"],
                        "avg_usefulness": sc["avg_usefulness"],
                        "accepted_rate": sc["accepted_rate"],
                        "result_coverage": sc["result_coverage"],
                        "feedback_coverage": sc["feedback_coverage"],
                        "score": sc["score"],
                        "eligible": sc["eligible"],
                        "ts_ms": now_ms(),
                    },
                )
            conn.commit()

async def persist_decision(db_url: str, decision_id: str, recommendation: str, winner_arm: str, incumbent_arm: str, margin: float) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_evaluator_decisions (
                    decision_id, recommendation, winner_arm, incumbent_arm, score_margin, ts_ms
                ) VALUES (
                    %(decision_id)s, %(recommendation)s, %(winner_arm)s, %(incumbent_arm)s, %(margin)s, %(ts_ms)s
                )
                """,
                {
                    "decision_id": decision_id,
                    "recommendation": recommendation,
                    "winner_arm": winner_arm,
                    "incumbent_arm": incumbent_arm,
                    "margin": margin,
                    "ts_ms": now_ms(),
                },
            )
            conn.commit()

async def fetch_recent(r: Any, stream: str, count: int) -> List[Dict[str, Any]]:
    try:
        hist = await r.xrevrange(stream, max="+", min="-", count=count)
        return [decode_dict(fields) for _, fields in hist] if hist else []
    except Exception:
        return []

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
        
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")
    
    while True:
        started = time.perf_counter()
        status = "ok"
        recommendation = "none"
        
        try:
            exposures = await fetch_recent(r, EXPOSURES_STREAM, LOOKBACK_COUNT)
            
            results = []
            for rs in RESULTS_STREAMS:
                if rs:
                    res = await fetch_recent(r, rs, LOOKBACK_COUNT)
                    results.extend(res)
                    
            feedbacks = []
            for fs in FEEDBACK_STREAMS:
                if fs:
                    fs_res = await fetch_recent(r, fs, LOOKBACK_COUNT)
                    feedbacks.extend(fs_res)

            scorecards = build_scorecards(exposures, results, feedbacks)
            recommendation, winner = evaluate_winner(scorecards)
            
            decision_id = f"eval_{int(time.time())}"
            margin = 0.0
            
            if winner in scorecards and INCUMBENT_ARM in scorecards:
                margin = scorecards[winner]["score"] - scorecards[INCUMBENT_ARM]["score"]
                
            for arm, sc in scorecards.items():
                if ARM_SCORE: ARM_SCORE.labels(arm=arm).set(sc["score"])
                if ARM_FDBK: ARM_FDBK.labels(arm=arm).set(sc["feedback_n"])
                if ARM_ACC: ARM_ACC.labels(arm=arm).set(sc["accepted_rate"])
                
                await r.xadd(SCORECARDS_STREAM, {
                    "decision_id": decision_id,
                    "arm": arm,
                    "score": str(sc["score"]),
                    "eligible": str(sc["eligible"]),
                    "exposure_n": str(sc["exposure_n"]),
                    "ts_ms": str(now_ms())
                }, maxlen=MAXLEN, approximate=True)
                
            await r.xadd(DECISIONS_STREAM, {
                "decision_id": decision_id,
                "recommendation": recommendation,
                "winner_arm": winner,
                "score_margin": str(margin),
                "ts_ms": str(now_ms())
            }, maxlen=MAXLEN, approximate=True)

            await persist_scorecards(db_url, decision_id, scorecards)
            await persist_decision(db_url, decision_id, recommendation, winner, INCUMBENT_ARM, margin)
            
            await r.hset(LAST_METRIC, "decision_id", decision_id)
            await r.hset(LAST_METRIC, "recommendation", recommendation)
            await r.hset(LAST_METRIC, "winner_arm", winner)
            await r.hset(LAST_METRIC, "ts_ms", str(now_ms()))
                
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=recommendation).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
