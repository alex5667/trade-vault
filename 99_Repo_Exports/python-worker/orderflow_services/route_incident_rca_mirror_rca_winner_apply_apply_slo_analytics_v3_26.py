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
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_slo_analytics_v3_26"

CTRL_DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_decisions")
CTRL_JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_journal")
VERIFY_RESULTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_RESULTS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results")
ROLLBACK_JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_JOURNAL", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal")

SLO_ROLLUPS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_ROLLUPS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_PORT", "9948"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_MAXLEN", "2000"))

POLL_INTERVAL_SEC = 60.0 # Calculate every minute for the last period

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_slo_runs_total", "Runs", ("status",))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_slo_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_slo_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_slo_last_run_ts_seconds", "Last run")

APPLY_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_rate", "Apply Rate")
VERIFY_KEEP_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_verify_keep_rate", "Verify Keep Rate")
ROLLBACK_MTTR_P50 = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_rollback_mttr_p50_seconds", "Rollback MTTR p50")
ROLLBACK_MTTR_P95 = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_rollback_mttr_p95_seconds", "Rollback MTTR p95")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def fetch_recent(r: Any, stream: str, count: int) -> List[Dict[str, Any]]:
    try:
        hist = await r.xrevrange(stream, max="+", min="-", count=count)
        return [{"msg_id": msg_id.decode() if isinstance(msg_id, bytes) else msg_id, **decode_dict(fields)} for msg_id, fields in hist] if hist else []
    except Exception:
        return []

async def persist_slo(db_url: str, apply_rate: float, verify_keep_rate: float, 
                      rollback_mttr_p50_sec: float, rollback_mttr_p95_sec: float) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups (
                    apply_rate, verify_keep_rate, rollback_mttr_p50_sec, rollback_mttr_p95_sec, ts_ms
                ) VALUES (
                    %(apply_rate)s, %(verify_keep_rate)s, %(rollback_mttr_p50_sec)s, %(rollback_mttr_p95_sec)s, %(ts_ms)s
                )
                """,
                {
                    "apply_rate": apply_rate,
                    "verify_keep_rate": verify_keep_rate,
                    "rollback_mttr_p50_sec": rollback_mttr_p50_sec,
                    "rollback_mttr_p95_sec": rollback_mttr_p95_sec,
                    "ts_ms": now_ms(),
                }
            )
            conn.commit()

def calculate_analytics(decisions: List[Dict[str, Any]], journal: List[Dict[str, Any]], 
                        verify_results: List[Dict[str, Any]], rollback_journal: List[Dict[str, Any]]) -> Tuple[float, float, float, float]:
    # 1. Apply rate: HOW many "APPLY" decisions turned into actual journal entries
    apply_rate = 1.0
    apply_decisions = [d for d in decisions if d.get("decision", "").startswith("APPLY")]
    if apply_decisions:
        applied_ids = set()
        for j in journal:
            applied_ids.add(j.get("apply_id"))
        successes = sum(1 for d in apply_decisions if d.get("apply_id") in applied_ids)
        apply_rate = successes / len(apply_decisions)
        
    # 2. Verify Keep Rate: HOW many recent verifications returned KEEP_APPLIED
    verify_keep_rate = 1.0
    if verify_results:
        keeps = sum(1 for v in verify_results if v.get("decision") == "KEEP_APPLIED")
        verify_keep_rate = keeps / len(verify_results)
        
    # 3. Rollback MTTR: time between verification failure (deciding ROLLBACK) and actual rollback_journal entry
    rollback_mttr_p50_sec = 0.0
    rollback_mttr_p95_sec = 0.0
    
    mttrs_sec = []
    # Match Rollback journal to verify results
    for rb in rollback_journal:
        rb_apply_id = rb.get("apply_id")
        rb_ts = int(rb.get("ts_ms", 0))
        # Find earliest verification failing for this apply_id that is <= rb_ts
        relevant_fails = [v for v in verify_results if v.get("apply_id") == rb_apply_id and v.get("decision") == "ROLLBACK_PREVIOUS_POLICY" and int(v.get("ts_ms", 0)) <= rb_ts]
        if relevant_fails:
            earliest_fail_ts = min(int(v.get("ts_ms", 0)) for v in relevant_fails)
            mttr_ms = rb_ts - earliest_fail_ts
            if mttr_ms >= 0:
                mttrs_sec.append(mttr_ms / 1000.0)
                
    if mttrs_sec:
        mttrs_sec.sort()
        idx_50 = int(len(mttrs_sec) * 0.5)
        idx_95 = int(len(mttrs_sec) * 0.95)
        rollback_mttr_p50_sec = mttrs_sec[idx_50]
        rollback_mttr_p95_sec = mttrs_sec[idx_95]
        
    return apply_rate, verify_keep_rate, rollback_mttr_p50_sec, rollback_mttr_p95_sec

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
        
        try:
            # We look at last 100 for slo computation
            decisions = await fetch_recent(r, CTRL_DECISIONS_STREAM, 100)
            journal = await fetch_recent(r, CTRL_JOURNAL_STREAM, 100)
            verify_results = await fetch_recent(r, VERIFY_RESULTS_STREAM, 300)
            rollback_journal = await fetch_recent(r, ROLLBACK_JOURNAL_STREAM, 100)
            
            ar, vkr, rb50, rb95 = calculate_analytics(decisions, journal, verify_results, rollback_journal)
            
            if APPLY_RATE: APPLY_RATE.set(ar)
            if VERIFY_KEEP_RATE: VERIFY_KEEP_RATE.set(vkr)
            if ROLLBACK_MTTR_P50: ROLLBACK_MTTR_P50.set(rb50)
            if ROLLBACK_MTTR_P95: ROLLBACK_MTTR_P95.set(rb95)
            
            await r.xadd(SLO_ROLLUPS_STREAM, {
                "apply_rate": str(ar),
                "verify_keep_rate": str(vkr),
                "rollback_mttr_p50_sec": str(rb50),
                "rollback_mttr_p95_sec": str(rb95),
                "ts_ms": str(now_ms())
            }, maxlen=MAXLEN, approximate=True)
            
            await persist_slo(db_url, ar, vkr, rb50, rb95)
            
            await r.hset(LAST_METRIC, "apply_rate", str(ar))
            await r.hset(LAST_METRIC, "verify_keep_rate", str(vkr))
            await r.hset(LAST_METRIC, "rollback_mttr_p95_sec", str(rb95))
            
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
