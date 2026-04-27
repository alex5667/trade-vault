from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import os
import time
from typing import Any, Dict, List, Tuple

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        pass

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_vertex_feedback_governor_v3_29"

IN_FEEDBACK = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_FEEDBACK", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_feedback")

OUT_ROLLUPS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_FEEDBACK_ROLLUPS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_feedback_rollups")
OUT_DECISIONS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_GOVERNANCE_DECISIONS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_governance_decisions")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_GOVERNANCE_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_governance:last")

CG_NAME = "cg_apply_vertex_feedback_gov_v3_29"
CONS_NAME = f"cons_{os.getpid()}"

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_GOVERNANCE_PORT", "9954"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_MAXLEN", "2000"))

POLL_INTERVAL_SEC = 30.0

MIN_SAMPLES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_GOVERNANCE_MIN_SAMPLES", "10"))
MIN_AVG_QUALITY = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_GOVERNANCE_MIN_AVG_QUALITY", "0.55"))
MIN_AVG_USEFULNESS = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_GOVERNANCE_MIN_AVG_USEFULNESS", "0.60"))
MIN_ACCEPTED_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_GOVERNANCE_MIN_ACCEPTED_RATE", "0.60"))
MAX_LOW_QUALITY_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_GOVERNANCE_MAX_LOW_QUALITY_RATE", "0.35"))

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_governance_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_governance_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_governance_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_governance_last_run_ts_seconds", "Last run")

AVG_Q = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_governance_avg_quality", "Quality")
AVG_U = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_governance_avg_usefulness", "Usefulness")
ACC_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_governance_accepted_rate", "Accepted Rate")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def fetch_recent_feedback(r: Any, limit: int = 100) -> List[Dict[str, Any]]:
    try:
        data = await r.xrevrange(IN_FEEDBACK, max="+", min="-", count=limit)
        return [{"msg_id": mid.decode() if isinstance(mid, bytes) else mid, **decode_dict(f)} for mid, f in data] if data else []
    except Exception:
        return []

def calculate_rollups(feedbacks: List[Dict[str, Any]]) -> Tuple[float, float, float, float]:
    if not feedbacks:
        return 0.0, 0.0, 0.0, 0.0
        
    q_sum = 0.0
    u_sum = 0.0
    acc_count = 0
    low_q_count = 0
    
    total = len(feedbacks)
    for f in feedbacks:
        q = float(f.get("quality_score", 0.0))
        u = float(f.get("usefulness_score", 0.0))
        acc = int(f.get("accepted", 0))
        
        q_sum += q
        u_sum += u
        if acc > 0:
            acc_count += 1
        if q < 0.4:
            low_q_count += 1
            
    return q_sum/total, u_sum/total, acc_count/total, low_q_count/total

def evaluate_governance(samples: int, avg_q: float, avg_u: float, acc_r: float, low_q_r: float) -> str:
    if samples < MIN_SAMPLES:
        return "HOLD"
        
    if avg_q < MIN_AVG_QUALITY or avg_u < MIN_AVG_USEFULNESS or acc_r < MIN_ACCEPTED_RATE or low_q_r > MAX_LOW_QUALITY_RATE:
        return "PREFER_LOCAL_ONLY"
        
    return "KEEP_AUTO"

async def process_batch(r: Any) -> None:
    started = time.perf_counter()
    status = "ok"
    decision = "HOLD"
    
    try:
        fbs = await fetch_recent_feedback(r, 100)
        samples = len(fbs)
        
        avg_q, avg_u, acc_r, low_q_r = calculate_rollups(fbs)
        
        decision = evaluate_governance(samples, avg_q, avg_u, acc_r, low_q_r)
        
        if AVG_Q: AVG_Q.set(avg_q)
        if AVG_U: AVG_U.set(avg_u)
        if ACC_RATE: ACC_RATE.set(acc_r)
        
        await r.xadd(OUT_ROLLUPS, {
            "samples": str(samples),
            "avg_quality": str(avg_q),
            "avg_usefulness": str(avg_u),
            "accepted_rate": str(acc_r),
            "low_quality_rate": str(low_q_r),
            "ts_ms": str(now_ms()),
        }, maxlen=MAXLEN, approximate=True)
            
        await r.xadd(OUT_DECISIONS, {
            "decision": decision,
            "samples": str(samples),
            "avg_usefulness": str(avg_u),
            "ts_ms": str(now_ms()),
        }, maxlen=MAXLEN, approximate=True)
        
        await r.hset(LAST_METRIC, "decision", decision)
        await r.hset(LAST_METRIC, "avg_quality", str(avg_q))
        await r.hset(LAST_METRIC, "avg_usefulness", str(avg_u))
        await r.hset(LAST_METRIC, "accepted_rate", str(acc_r))
        await r.hset(LAST_METRIC, "ts_ms", str(now_ms()))
        
    except Exception as exc:
        status = "error"
    finally:
        if RUNS:
            RUNS.labels(status=status, decision=decision).inc()
        if LAT:
            LAT.observe(max(time.perf_counter() - started, 0.0))


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)

    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    while True:
        try:
            await process_batch(r)
            if LAST_RUN:
                LAST_RUN.set(time.time())
        except Exception:
            pass
        finally:
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
