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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_retry_controller_v3_26"

ROLLBACK_JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_JOURNAL", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal")
RETRY_RESULTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_RESULTS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry_results")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry:last")

CFG_HARNESS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_CFG", "cfg:ml:route_incident_rca_mirror_rca_winner_apply_experiment:global")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_PORT", "9949"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_MAXLEN", "2000"))

MAX_ATTEMPTS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_MAX_ATTEMPTS", "2"))
BACKOFF_SEC = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_BACKOFF_SEC", "120"))

POLL_INTERVAL_SEC = 20.0 

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_retry_runs_total", "Runs", ("status", "action"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_retry_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_retry_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_retry_last_run_ts_seconds", "Last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    },

async def fetch_recent(r: Any, stream: str, count: int) -> List[Dict[str, Any]]:
    try:
        hist = await r.xrevrange(stream, max="+", min="-", count=count)
        return [{"msg_id": msg_id.decode() if isinstance(msg_id, bytes) else msg_id, **decode_dict(fields)} for msg_id, fields in hist] if hist else []
    except Exception:
        return []

async def persist_retry(db_url: str, apply_id: str, attempt: int, status: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """,
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_retry_results (
                    apply_id, attempt, status, ts_ms
                ) VALUES (
                    %(apply_id)s, %(attempt)s, %(status)s, %(ts_ms)s
                )
                """,
                {
                    "apply_id": apply_id,
                    "attempt": attempt,
                    "status": status,
                    "ts_ms": now_ms(),
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
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    # simple state memory: apply_id -> attempts
    retry_state_attempts: Dict[str, int] = {}
    last_retry_ts: Dict[str, int] = {}

    while True:
        started = time.perf_counter()
        status = "ok"
        action = "none"
        
        try:
            rb_journal = await fetch_recent(r, ROLLBACK_JOURNAL_STREAM, 20)
            
            if rb_journal:
                latest_rb = rb_journal[0]
                apply_id = latest_rb.get("apply_id", "")
                rb_ts = int(latest_rb.get("ts_ms", 0))
                
                # Check live policy
                harness = decode_dict(await r.hgetall(CFG_HARNESS) or {})
                live_mode = harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE", "UNKNOWN")
                live_arm = harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM", "UNKNOWN")
                
                target_mode = latest_rb.get("restored_mode", "")
                target_arm = latest_rb.get("restored_primary_arm", "")
                
                # if live != rollback target, it means the rollback from 3.25 failed or someone messed up config
                # We do NOT re-apply the winner apply, we re-apply the ROLLBACK TARGET.
                
                if target_mode and target_arm and (live_mode != target_mode or live_arm != target_arm):
                    # Need retry
                    attempts = retry_state_attempts.get(apply_id, 0)
                    last_ts = last_retry_ts.get(apply_id, 0)
                    
                    if attempts < MAX_ATTEMPTS:
                        if (now_ms() - last_ts) / 1000.0 >= BACKOFF_SEC:
                            action = "reapply_rb"
                            
                            new_harness = dict(harness)
                            new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE"] = target_mode
                            new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM"] = target_arm
                            if target_mode == "SHADOW":
                                shadows = ["vertex_candidate", "local_fallback_candidate", "deterministic"]
                                shadows = [s for s in shadows if s != target_arm]
                                new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"] = json.dumps(shadows)
                            else:
                                new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"] = "[]"
                            
                            await r.hset(CFG_HARNESS, mapping=new_harness)
                            
                            attempts += 1
                            retry_state_attempts[apply_id] = attempts
                            last_retry_ts[apply_id] = now_ms()
                            
                            await r.xadd(RETRY_RESULTS_STREAM, {
                                "apply_id": apply_id,
                                "attempt": str(attempts),
                                "status": "executed",
                                "ts_ms": str(now_ms())
                            }, maxlen=MAXLEN, approximate=True)
                            
                            await persist_retry(db_url, apply_id, attempts, "executed")
                            
                            await r.hset(LAST_METRIC, "apply_id", apply_id)
                            await r.hset(LAST_METRIC, "attempt", str(attempts))
                            await r.hset(LAST_METRIC, "status", "executed")
                    else:
                        action = "exhausted"
                        await r.xadd(RETRY_RESULTS_STREAM, {
                            "apply_id": apply_id,
                            "attempt": str(attempts),
                            "status": "exhausted",
                            "ts_ms": str(now_ms())
                        }, maxlen=MAXLEN, approximate=True)
                        await r.hset(LAST_METRIC, "status", "exhausted")

            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, action=action).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
