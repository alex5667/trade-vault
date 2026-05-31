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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_auto_escalation_summarizer_v3_26"

LAST_SLO_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo:last")
LAST_RETRY_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry:last")

ESCALATIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ESCALATIONS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_escalations")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ESCALATIONS_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_escalations:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ESCALATIONS_PORT", "9950"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ESCALATIONS_MAXLEN", "2000"))

ROLLBACK_MTTR_SLO_SEC = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ROLLBACK_MTTR_SLO_SEC", "120"))

POLL_INTERVAL_SEC = 30.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_escalations_runs_total", "Runs", ("status", "severity"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_escalations_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_escalations_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_escalations_last_run_ts_seconds", "Last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def persist_escalation(db_url: str, severity: str, message: str) -> None:
    if not db_url or psycopg is None:
        return
    import json as _json
    summary = {"severity": severity, "message": message}
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_escalations (
                    severity, summary_json, ts_ms
                ) VALUES (
                    %(severity)s, %(summary_json)s, %(ts_ms)s
                )
                """,
                {
                    "severity": severity,
                    "summary_json": _json.dumps(summary),
                    "ts_ms": now_ms(),
                },
            )
            conn.commit()

def calculate_severity(slo_data: Dict[str, str], retry_data: Dict[str, str]) -> Tuple[str, str]:
    verify_keep_rate = float(slo_data.get("verify_keep_rate", "1.0"))
    rollback_mttr_95 = float(slo_data.get("rollback_mttr_p95_sec", "0.0"))
    retry_status = retry_data.get("status", "ok")
    
    if retry_status == "exhausted":
        return "critical", "Retry attempts for rollback target exhausted. Manual intervention required."
        
    if verify_keep_rate < 0.80:
        return "warning", f"Verify keep rate is alarmingly low ({verify_keep_rate:.2f}). Check verification rules."
        
    if rollback_mttr_95 > ROLLBACK_MTTR_SLO_SEC:
        return "warning", f"Rollback MTTR p95 ({rollback_mttr_95:.1f}s) exceeded SLO ({ROLLBACK_MTTR_SLO_SEC}s)."
        
    return "info", "All SLA/SLO clear."

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
        severity = "info"
        
        try:
            slo_data = decode_dict(await r.hgetall(LAST_SLO_METRIC) or {})
            retry_data = decode_dict(await r.hgetall(LAST_RETRY_METRIC) or {})
            
            severity, msg = calculate_severity(slo_data, retry_data)
            
            await r.xadd(ESCALATIONS_STREAM, {
                "severity": severity,
                "message": msg,
                "ts_ms": str(now_ms())
            }, maxlen=MAXLEN, approximate=True)
            
            await persist_escalation(db_url, severity, msg)
            
            await r.hset(LAST_METRIC, "severity", severity)
            await r.hset(LAST_METRIC, "message", msg)
            
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, severity=severity).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
