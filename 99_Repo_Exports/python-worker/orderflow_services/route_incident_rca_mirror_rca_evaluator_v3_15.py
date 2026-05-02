from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from typing import Any, Dict, Tuple, List

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

APP_NAME = "route_incident_rca_mirror_rca_evaluator_v3_15"
EXPOSURES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_EXPOSURES_STREAM", "stream:ml:route_incident_rca_mirror_rca_experiment_exposures")

# result streams
RESULTS_STREAMS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_RESULTS_STREAMS_JSON", 
    '{"deterministic":"stream:ml:route_incident_rca_mirror_rca_results","vertex_candidate":"stream:ml:route_incident_rca_mirror_vertex_rca_results","local_fallback_candidate":"stream:ml:local_fallback_results"}'
)

# feedback streams
FEEDBACK_STREAMS_JSON = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_FEEDBACK_STREAMS_JSON",
    '{"deterministic":"stream:ml:route_incident_rca_mirror_rca_feedback","vertex_candidate":"stream:ml:route_incident_rca_mirror_vertex_rca_feedback","local_fallback_candidate":"stream:ml:local_fallback_feedback"}'
)

SCORECARDS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_SCORECARDS_STREAM", "stream:ml:route_incident_rca_mirror_rca_scorecards")
DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_evaluator_decisions")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_evaluator_audit")
LAST_HASH = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_LAST_HASH", "metrics:ml:route_incident_rca_mirror_rca_evaluator:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_PORT", "9934"))

# Thresholds
MIN_EXPOSURES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_EXPOSURES", "10"))
MIN_FEEDBACK = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_FEEDBACK", "5"))
MIN_RESULT_COVERAGE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_RESULT_COVERAGE", "0.30"))
MIN_FEEDBACK_COVERAGE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_FEEDBACK_COVERAGE", "0.20"))
MIN_QUALITY = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_QUALITY", "0.55"))
MIN_USEFULNESS = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_USEFULNESS", "0.60"))
MIN_ACCEPTED_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_ACCEPTED_RATE", "0.60"))
MIN_SCORE_MARGIN = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_SCORE_MARGIN", "0.05"))
INCUMBENT_ARM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_INCUMBENT_ARM", "deterministic")

MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MAXLEN", "1000"))
POLL_INTERVAL_SEC = 5.0
EVAL_PERIOD_MS = 60000 * 5  # 5 min logic

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_evaluator_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_evaluator_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_evaluator_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_evaluator_last_run_ts_seconds", "Last run")

ARM_SCORE = _gauge("ml_route_incident_rca_mirror_rca_evaluator_arm_score", "Score", ("arm",))
ARM_FDBK_N = _gauge("ml_route_incident_rca_mirror_rca_evaluator_arm_feedback_n", "Feedback count", ("arm",))
ARM_ACC_RATE = _gauge("ml_route_incident_rca_mirror_rca_evaluator_arm_accepted_rate", "Accepted rate", ("arm",))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

def parse_arm_from_request_id(req_id: str) -> str:
    # expect format: bundle_id:arm_name
    parts = req_id.split(":")
    if len(parts) > 1:
        return parts[-1]
    return "deterministic"

class ArmMetrics:
    def __init__(self, arm: str):
        self.arm = arm
        self.exposure_n = 0
        self.result_n = 0
        self.feedback_n = 0
        self.sum_quality = 0.0
        self.sum_usefulness = 0.0
        self.sum_accepted = 0.0
        
    def add_exposure(self): self.exposure_n += 1
    def add_result(self): self.result_n += 1
    def add_feedback(self, q: float, u: float, accepted: float):
        self.feedback_n += 1
        self.sum_quality += q
        self.sum_usefulness += u
        self.sum_accepted += accepted

    def compute_scorecard(self) -> Dict[str, Any]:
        avg_quality = self.sum_quality / self.feedback_n if self.feedback_n > 0 else 0.0
        avg_usefulness = self.sum_usefulness / self.feedback_n if self.feedback_n > 0 else 0.0
        accepted_rate = self.sum_accepted / self.feedback_n if self.feedback_n > 0 else 0.0
        
        result_coverage = self.result_n / self.exposure_n if self.exposure_n > 0 else 0.0
        feedback_coverage = self.feedback_n / self.exposure_n if self.exposure_n > 0 else 0.0
        
        score_raw = (avg_quality + avg_usefulness + accepted_rate) / 3.0
        coverage_multiplier = min(1.0, feedback_coverage * 2.0)
        score = score_raw * coverage_multiplier
        
        eligible = (
            self.exposure_n >= MIN_EXPOSURES and
            self.feedback_n >= MIN_FEEDBACK and
            result_coverage >= MIN_RESULT_COVERAGE and
            feedback_coverage >= MIN_FEEDBACK_COVERAGE and
            avg_quality >= MIN_QUALITY and
            avg_usefulness >= MIN_USEFULNESS and
            accepted_rate >= MIN_ACCEPTED_RATE
        )
        
        return {
            "arm": self.arm,
            "exposure_n": self.exposure_n,
            "result_n": self.result_n,
            "feedback_n": self.feedback_n,
            "avg_quality": avg_quality,
            "avg_usefulness": avg_usefulness,
            "accepted_rate": accepted_rate,
            "result_coverage": result_coverage,
            "feedback_coverage": feedback_coverage,
            "coverage_multiplier": coverage_multiplier,
            "score_raw": score_raw,
            "score": score,
            "eligible": eligible
        }

def select_winner(scorecards: Dict[str, Dict[str, Any]], incumbent: str, min_margin: float) -> str:
    best_arm = incumbent
    incumbent_score = scorecards.get(incumbent, {}).get("score", 0.0)
    
    best_candidate_score = -1.0
    best_candidate_arm = None
    
    for arm, sc in scorecards.items():
        if arm == incumbent:
            continue
        if sc["eligible"] and sc["score"] > best_candidate_score:
            best_candidate_score = sc["score"]
            best_candidate_arm = arm
            
    if best_candidate_arm and (best_candidate_score > incumbent_score + min_margin):
        return best_candidate_arm
        
    return incumbent

async def read_stream_batch(r: Any, stream_name: str, count: int) -> List[Dict[str, Any]]:
    # Simple tail reading
    try:
        results = await r.xrevrange(stream_name, max="+", min="-", count=count)
        data = []
        if results:
            for msg_id, fields in results:
                decoded = decode_dict(fields)
                data.append(decoded)
        return data
    except Exception:
        return []

async def persist_scorecard(db_url: str, sc: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_scorecards (
                    arm, exposure_n, result_n, feedback_n, 
                    avg_quality, avg_usefulness, accepted_rate, 
                    score, eligible, ts_ms
                ) VALUES (
                    %(arm)s, %(exposure_n)s, %(result_n)s, %(feedback_n)s,
                    %(avg_quality)s, %(avg_usefulness)s, %(accepted_rate)s,
                    %(score)s, %(eligible)s, %(ts_ms)s
                )
                """,
                {
                    "arm": sc["arm"],
                    "exposure_n": sc["exposure_n"],
                    "result_n": sc["result_n"],
                    "feedback_n": sc["feedback_n"],
                    "avg_quality": sc["avg_quality"],
                    "avg_usefulness": sc["avg_usefulness"],
                    "accepted_rate": sc["accepted_rate"],
                    "score": sc["score"],
                    "eligible": sc["eligible"],
                    "ts_ms": now_ms(),
                }
            )
            conn.commit()

async def persist_decision(db_url: str, decision: str, scorecards: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_evaluator_decisions (
                    decision, scorecards_json, ts_ms
                ) VALUES (
                    %(decision)s, %(scorecards)s, %(ts_ms)s
                )
                """,
                {
                    "decision": decision,
                    "scorecards": json.dumps(scorecards),
                    "ts_ms": now_ms(),
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
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")
    
    result_streams = {}
    feedback_streams = {}
    try:
        result_streams = json.loads(RESULTS_STREAMS_JSON)
        feedback_streams = json.loads(FEEDBACK_STREAMS_JSON)
    except Exception:
        pass

    last_eval_ms = 0
    
    while True:
        started = time.perf_counter()
        status = "ok"
        decision_label = "none"
        
        try:
            curr_ms = now_ms()
            if curr_ms - last_eval_ms >= EVAL_PERIOD_MS:
                exposures = await read_stream_batch(r, EXPOSURES_STREAM, 2000)
                
                arm_stats = {}
                for exp in exposures:
                    arm = exp.get("arm", "unknown")
                    if arm not in arm_stats:
                        arm_stats[arm] = ArmMetrics(arm)
                    arm_stats[arm].add_exposure()
                    
                for arm, st in result_streams.items():
                    res_data = await read_stream_batch(r, st, 1000)
                    for item in res_data:
                        actual_arm = parse_arm_from_request_id(item.get("request_id", ""))
                        if actual_arm not in arm_stats:
                            arm_stats[actual_arm] = ArmMetrics(actual_arm)
                        arm_stats[actual_arm].add_result()
                        
                for arm, st in feedback_streams.items():
                    fdbk_data = await read_stream_batch(r, st, 1000)
                    for item in fdbk_data:
                        actual_arm = parse_arm_from_request_id(item.get("request_id", ""))
                        if actual_arm not in arm_stats:
                            arm_stats[actual_arm] = ArmMetrics(actual_arm)
                            
                        q = float(item.get("quality_score", 0.0))
                        u = float(item.get("usefulness_score", 0.0))
                        acc = float(item.get("accepted", 0.0))
                        arm_stats[actual_arm].add_feedback(q, u, acc)
                        
                scorecards = {}
                for arm, metrics in arm_stats.items():
                    sc = metrics.compute_scorecard()
                    scorecards[arm] = sc
                    
                    await r.xadd(SCORECARDS_STREAM, {k: str(v) for k,v in sc.items()}, maxlen=MAXLEN, approximate=True)
                    await persist_scorecard(db_url, sc)
                    
                    if ARM_SCORE:
                        ARM_SCORE.labels(arm=arm).set(sc["score"])
                        ARM_FDBK_N.labels(arm=arm).set(sc["feedback_n"])
                        ARM_ACC_RATE.labels(arm=arm).set(sc["accepted_rate"])
                        
                winner = select_winner(scorecards, INCUMBENT_ARM, MIN_SCORE_MARGIN)
                decision_text = f"KEEP_{INCUMBENT_ARM.upper()}"
                if winner != INCUMBENT_ARM:
                    decision_text = f"PROMOTE_{winner.upper()}"
                    
                await r.xadd(DECISIONS_STREAM, {"decision": decision_text, "winner": winner, "ts_ms": str(curr_ms)}, maxlen=MAXLEN, approximate=True)
                await persist_decision(db_url, decision_text, scorecards)
                
                await r.hset(LAST_HASH, mapping={"decision": decision_text, "winner": winner, "ts_ms": str(curr_ms)})
                
                decision_label = "evaluated"
                last_eval_ms = curr_ms
                
            await asyncio.sleep(POLL_INTERVAL_SEC)
                            
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "EVALUATION_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
            await asyncio.sleep(POLL_INTERVAL_SEC)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_label).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
