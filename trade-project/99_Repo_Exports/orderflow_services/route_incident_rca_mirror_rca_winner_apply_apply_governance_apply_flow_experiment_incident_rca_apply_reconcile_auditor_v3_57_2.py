from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Tuple

try:
    import redis.asyncio as redis
except Exception:
    redis = None

try:
    import psycopg
except Exception:
    psycopg = None

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None

from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_persister_v3_57_1 import normalize as normalize_slo
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results_persister_v3_57_1 import normalize as normalize_retry
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations_persister_v3_57_1 import normalize as normalize_escalation

APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_reconcile_auditor_v3_57_2"
PROM = APP_NAME
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PG_DSN = os.getenv("TIMESCALE_DSN", os.getenv("DATABASE_URL", ""))
METRICS_PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_RECONCILE_AUDITOR_PORT", "9999"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_RECONCILE_WINDOW_MIN", "1440"))
POLL_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_RECONCILE_POLL_SEC", "300"))
SAMPLE_N = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_RECONCILE_SAMPLE_N", "20"))

REPORT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_reconcile_reports"
AUDIT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_reconcile_audit"
METRICS_HASH = "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_reconcile:last"

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
log = logging.getLogger(APP_NAME)

RUNS = Counter(f"{PROM}_runs_total", "runs", ["status", "alias"]) if Counter else None
UP = Gauge(f"{PROM}_up", "up") if Gauge else None
LAST_RUN = Gauge(f"{PROM}_last_run_ts_seconds", "last run ts") if Gauge else None
GAP_N = Gauge(f"{PROM}_gap_n", "gap count", ["alias"]) if Gauge else None
MISSING_SAMPLE_N = Gauge(f"{PROM}_missing_sample_n", "missing sample count", ["alias"]) if Gauge else None
LAT = Histogram(f"{PROM}_loop_seconds", "loop seconds") if Histogram else None

SPECS = {
    "slo": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups",
        "normalize": normalize_slo,
        "count_sql": "SELECT count(*) FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups WHERE ts_ms >= %s AND ts_ms <= %s",
        "exists_sql": "SELECT 1 FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups WHERE ts_ms = %s LIMIT 1",
        "key_fn": lambda r: (int(r['ts_ms']),),
    },
    "retry": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results",
        "normalize": normalize_retry,
        "count_sql": "SELECT count(*) FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results WHERE ts_ms >= %s AND ts_ms <= %s",
        "exists_sql": "SELECT 1 FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results WHERE ts_ms=%s AND rollback_mode=%s AND failed_target_mode=%s AND reason_code=%s LIMIT 1",
        "key_fn": lambda r: (int(r['ts_ms']), str(r['rollback_mode']), str(r['failed_target_mode']), str(r['reason_code'])),
    },
    "escalation": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations",
        "normalize": normalize_escalation,
        "count_sql": "SELECT count(*) FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations WHERE ts_ms >= %s AND ts_ms <= %s",
        "exists_sql": "SELECT 1 FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations WHERE ts_ms=%s AND rollback_mode=%s AND failed_target_mode=%s AND reason_code=%s LIMIT 1",
        "key_fn": lambda r: (int(r['ts_ms']), str(r['rollback_mode']), str(r['failed_target_mode']), str(r['reason_code'])),
    },
}

def now_ms() -> int:
    return int(time.time() * 1000)

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

async def redis_rows_in_window(r: "redis.Redis", stream: str, min_id: str, max_id: str, count: int = 5000) -> List[Tuple[str, Dict[str, Any]]]:
    rows = await r.xrange(stream, min=min_id, max=max_id, count=count)
    out: List[Tuple[str, Dict[str, Any]]] = []
    for sid, fields in rows:
        out.append((sid.decode() if isinstance(sid, (bytes, bytearray)) else str(sid), as_dict(fields)))
    return out

async def pg_count(conn: "psycopg.AsyncConnection", sql: str, ts_min: int, ts_max: int) -> int:
    async with conn.cursor() as cur:
        await cur.execute(sql, (ts_min, ts_max))
        row = await cur.fetchone()
    return int(row[0] if row else 0)

async def pg_exists(conn: "psycopg.AsyncConnection", sql: str, params: Tuple[Any, ...]) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        row = await cur.fetchone()
    return row is not None

async def audit_alias(r: "redis.Redis", conn: "psycopg.AsyncConnection", alias: str) -> Dict[str, Any]:
    spec = SPECS[alias]
    ts_max = now_ms()
    ts_min = ts_max - WINDOW_MIN * 60 * 1000
    min_id = f"{ts_min}-0"
    max_id = f"{ts_max}-999999"

    raw_rows = await redis_rows_in_window(r, spec["stream"], min_id, max_id)
    redis_n = len(raw_rows)
    pg_n = await pg_count(conn, spec["count_sql"], ts_min, ts_max)
    gap_n = redis_n - pg_n

    sample_rows = raw_rows[:SAMPLE_N]
    missing_sample_n = 0
    for _, raw in sample_rows:
        row = spec["normalize"](raw)
        exists = await pg_exists(conn, spec["exists_sql"], spec["key_fn"](row))
        if not exists:
            missing_sample_n += 1

    status = "OK"
    if gap_n != 0 or missing_sample_n > 0:
        status = "GAP"

    report = {
        "schema_version": 1,
        "app_name": APP_NAME,
        "alias": alias,
        "window_min": WINDOW_MIN,
        "ts_ms": ts_max,
        "window_start_ts_ms": ts_min,
        "window_end_ts_ms": ts_max,
        "redis_n": redis_n,
        "pg_n": pg_n,
        "gap_n": gap_n,
        "sample_n": len(sample_rows),
        "missing_sample_n": missing_sample_n,
        "status": status,
    }
    await r.xadd(REPORT_STREAM, {"payload": json.dumps(report, ensure_ascii=False, sort_keys=True)}, maxlen=50000, approximate=True)
    await r.xadd(AUDIT_STREAM, {"payload": json.dumps({"event": "reconcile_complete", **report}, ensure_ascii=False, sort_keys=True)}, maxlen=50000, approximate=True)
    await r.hset(METRICS_HASH, mapping={f"{alias}:{k}": str(v) for k, v in report.items() if not isinstance(v, (list, dict))})
    if GAP_N:
        GAP_N.labels(alias=alias).set(gap_n)
    if MISSING_SAMPLE_N:
        MISSING_SAMPLE_N.labels(alias=alias).set(missing_sample_n)
    return report

async def main_loop() -> None:
    if redis is None or psycopg is None:
        raise RuntimeError("redis.asyncio and psycopg are required")
    start_http_server(METRICS_PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(REDIS_URL, decode_responses=False)
    conn = await psycopg.AsyncConnection.connect(PG_DSN)
    try:
        while True:
            t0 = time.time()
            for alias in ("slo", "retry", "escalation"):
                status = "ok"
                try:
                    report = await audit_alias(r, conn, alias)
                    log.info("reconcile alias=%s status=%s redis_n=%s pg_n=%s gap_n=%s missing_sample_n=%s", alias, report["status"], report["redis_n"], report["pg_n"], report["gap_n"], report["missing_sample_n"])
                except Exception:
                    status = "error"
                    log.exception("reconcile failed alias=%s", alias)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, alias=alias).inc()
            if LAST_RUN:
                LAST_RUN.set(time.time())
            if LAT:
                LAT.observe(max(0.0, time.time() - t0))
            await asyncio.sleep(POLL_SEC)
    finally:
        if UP:
            UP.set(0)
        await conn.close()
        await r.close()

def main() -> None:
    asyncio.run(main_loop())

if __name__ == "__main__":
    main()
