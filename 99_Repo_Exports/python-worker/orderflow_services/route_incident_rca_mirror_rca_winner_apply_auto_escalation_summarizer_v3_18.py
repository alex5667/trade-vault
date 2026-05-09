from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis

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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_auto_escalation_summarizer_v3_18"

ESCALATIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ESCALATIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_escalations")
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ESCALATIONS_PORT", "9939"))

ROLLBACK_MTTR_SLO_SEC = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_MTTR_SLO_SEC", "120.0"))

MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ESCALATIONS_MAXLEN", "1000"))
POLL_INTERVAL_SEC = 20.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_escalations_runs_total", "Runs", ("status",))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_escalations_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_escalations_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_escalations_last_run_ts_seconds", "Last run")

def now_ms() -> int:
    return get_ny_time_millis()

def calculate_severity(apply_rate: float, verify_keep_rate: float, rollback_mttr_p95: float, recent_retries: int) -> str:
    # critical if mttr is exceptionally high OR verify keep rate is super low (meaning we constantly rollback) OR we tried to retry many times
    if recent_retries > 5 or rollback_mttr_p95 > ROLLBACK_MTTR_SLO_SEC * 3 or (verify_keep_rate < 0.2 and apply_rate > 0.0):
        return "critical"

    if rollback_mttr_p95 > ROLLBACK_MTTR_SLO_SEC or verify_keep_rate < 0.5:
        return "warning"

    return "info"

async def read_latest_slo(r: Any) -> dict[str, float]:
    res = await r.xrevrange("stream:ml:route_incident_rca_mirror_rca_winner_apply_slo_rollups", max="+", min="-", count=1)
    if not res:
        return {"apply_rate": 0.0, "verify_keep_rate": 1.0, "rollback_mttr_p95_sec": 0.0}

    msg_id, fields = res[0]
    out = {}
    for k,v in fields.items():
        k_str = k.decode() if isinstance(k, bytes) else k
        v_str = v.decode() if isinstance(v, bytes) else v
        if k_str in ["apply_rate", "verify_keep_rate", "rollback_mttr_p95_sec"]:
            try:
                out[k_str] = float(v_str)
            except Exception:
                out[k_str] = 0.0
    return out

async def read_recent_retries(r: Any) -> int:
    # count retries in last hour
    hour_ago = now_ms() - 3600*1000
    res = await r.xrange("stream:ml:route_incident_rca_mirror_rca_winner_apply_retry_results", min=f"{hour_ago}-0", max="+")
    return len(res)

async def escalate(db_url: str, severity: str, metrics: dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_escalations (
                    severity, metrics_json, ts_ms
                ) VALUES (
                    %(severity)s, %(metrics_json)s, %(ts_ms)s
                )
                """,
                {
                    "severity": severity,
                    "metrics_json": json.dumps(metrics),
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
            slo_data = await read_latest_slo(r)
            retries = await read_recent_retries(r)

            severity = calculate_severity(
                slo_data.get("apply_rate", 0.0),
                slo_data.get("verify_keep_rate", 1.0),
                slo_data.get("rollback_mttr_p95_sec", 0.0),
                retries
            )

            if severity in ["warning", "critical"]:
                await r.xadd(ESCALATIONS_STREAM, {
                    "severity": severity,
                    "metrics": json.dumps(slo_data),
                    "recent_retries": str(retries),
                    "ts_ms": str(now_ms())
                }, maxlen=MAXLEN, approximate=True)

                await escalate(db_url, severity, slo_data)

            if LAST_RUN:
                LAST_RUN.set(time.time())

        except Exception:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
