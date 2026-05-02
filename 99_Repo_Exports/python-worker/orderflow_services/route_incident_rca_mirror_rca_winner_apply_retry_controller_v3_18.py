from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_retry_controller_v3_18"
VERIFICATION_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_RESULTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results")
ROLLBACK_JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal")
EXPERIMENT_CFG = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_CFG", "cfg:ml:route_incident_rca_mirror_rca_experiment:global")

RESULTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RETRY_RESULTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_retry_results")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RETRY_PORT", "9938"))

MAX_ATTEMPTS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RETRY_MAX_ATTEMPTS", "2"))
BACKOFF_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RETRY_BACKOFF_SEC", "120"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RETRY_MAXLEN", "1000"))

POLL_INTERVAL_SEC = 10.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_retry_runs_total", "Runs", ("status",))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_retry_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_retry_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_retry_last_run_ts_seconds", "Last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def load_last_rollback(r: Any) -> Optional[Dict[str, Any]]:
    res = await r.xrevrange(ROLLBACK_JOURNAL_STREAM, max="+", min="-", count=1)
    if not res:
        return None
    msg_id, fields = res[0]
    return decode_dict(fields)

async def load_live_cfg(r: Any) -> Dict[str, Any]:
    res = await r.hgetall(EXPERIMENT_CFG)
    return decode_dict(res) if res else {}

def is_cfg_match(live: Dict[str, Any], rb_target: Dict[str, Any]) -> bool:
    mode_match = live.get("mode") == rb_target.get("mode")
    primary_match = live.get("primary_arm") == rb_target.get("primary_arm")
    shadows_match = live.get("shadow_arms") == rb_target.get("shadow_arms")
    
    return mode_match and primary_match and shadows_match

async def perform_retry_reapply(r: Any, db_url: str, rb_target: Dict[str, Any]) -> None:
    if rb_target:
        await r.hset(EXPERIMENT_CFG, mapping=rb_target)
        # Note: In a real system, tracking attempts would be stateful
        # For this model we assume stateless bounded single reapply per period
        
        await r.xadd(RESULTS_STREAM, {
            "action": "REAPPLY_ROLLBACK_TARGET",
            "target": json.dumps(rb_target),
            "ts_ms": str(now_ms())
        }, maxlen=MAXLEN, approximate=True)
        
        if not db_url or psycopg is None:
            return
        with psycopg.connect(db_url) as conn:  # pragma: no cover
            with conn.cursor() as cur:
                cur.execute(
                    """

                    INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_retry_results (
                        action, target_json, ts_ms
                    ) VALUES (
                        %(action)s, %(target_json)s, %(ts_ms)s
                    )
                    """,
                    {
                        "action": "REAPPLY_ROLLBACK_TARGET",
                        "target_json": json.dumps(rb_target),
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

    while True:
        started = time.perf_counter()
        status = "ok"
        
        try:
            rb_entry = await load_last_rollback(r)
            if rb_entry:
                rb_ts = int(rb_entry.get("ts_ms", "0"))
                curr_ms = now_ms()
                
                # If rollback is recent but we haven't synced
                if (curr_ms - rb_ts) > BACKOFF_SEC * 1000 and (curr_ms - rb_ts) < (BACKOFF_SEC * 1000 * 5):
                    target_cfg_str = rb_entry.get("new_config", "{}")
                    target_cfg = json.loads(target_cfg_str)
                    
                    live_cfg = await load_live_cfg(r)
                    if not is_cfg_match(live_cfg, target_cfg):
                        await perform_retry_reapply(r, db_url, target_cfg)
                        
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
