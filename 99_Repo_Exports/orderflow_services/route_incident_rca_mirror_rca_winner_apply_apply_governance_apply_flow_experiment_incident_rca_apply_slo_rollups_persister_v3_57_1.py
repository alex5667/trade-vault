from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional

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

APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_persister_v3_57_1"
PROM = APP_NAME
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PG_DSN = os.getenv("TIMESCALE_DSN", os.getenv("DATABASE_URL", ""))
METRICS_PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_SLO_PERSISTER_PORT", "9995"))
POLL_BLOCK_MS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_SLO_PERSISTER_BLOCK_MS", "15000"))
STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups"
DLQ_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_dlq"
AUDIT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_persist_audit"
GROUP = "cg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_persister_v3_57_1"
CONSUMER = os.getenv("HOSTNAME", "incident-rca-apply-slo-persister-v3-57-1")
TABLE = "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups"

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
log = logging.getLogger(APP_NAME)

RUNS = Counter(f"{PROM}_runs_total", "runs", ["status"]) if Counter else None
ROWS_WRITTEN = Counter(f"{PROM}_rows_written_total", "rows written") if Counter else None
ROWS_SKIPPED = Counter(f"{PROM}_rows_skipped_total", "rows skipped", ["reason"]) if Counter else None
DLQ_TOTAL = Counter(f"{PROM}_dlq_total", "dlq total", ["reason"]) if Counter else None
UP = Gauge(f"{PROM}_up", "up") if Gauge else None
LAST_RUN = Gauge(f"{PROM}_last_run_ts_seconds", "last run ts") if Gauge else None
LAG_MS = Gauge(f"{PROM}_stream_lag_ms", "stream lag ms") if Gauge else None
LAT = Histogram(f"{PROM}_loop_seconds", "loop seconds") if Histogram else None

UPSERT_SQL = f"""
INSERT INTO {TABLE} (
    ts_ms, window_min, verification_n, verified_n, rollback_planned_n,
    rollback_applied_n, retry_n, escalation_n, verify_keep_rate,
    rollback_plan_rate, rollback_applied_rate, rollback_mttr_p95_sec,
    retry_rate, escalation_rate, mttr_slo_sec, rollup_json
) VALUES (
    %(ts_ms)s, %(window_min)s, %(verification_n)s, %(verified_n)s, %(rollback_planned_n)s,
    %(rollback_applied_n)s, %(retry_n)s, %(escalation_n)s, %(verify_keep_rate)s,
    %(rollback_plan_rate)s, %(rollback_applied_rate)s, %(rollback_mttr_p95_sec)s,
    %(retry_rate)s, %(escalation_rate)s, %(mttr_slo_sec)s, %(rollup_json)s
)
ON CONFLICT (ts_ms)
DO UPDATE SET
    window_min = EXCLUDED.window_min,
    verification_n = EXCLUDED.verification_n,
    verified_n = EXCLUDED.verified_n,
    rollback_planned_n = EXCLUDED.rollback_planned_n,
    rollback_applied_n = EXCLUDED.rollback_applied_n,
    retry_n = EXCLUDED.retry_n,
    escalation_n = EXCLUDED.escalation_n,
    verify_keep_rate = EXCLUDED.verify_keep_rate,
    rollback_plan_rate = EXCLUDED.rollback_plan_rate,
    rollback_applied_rate = EXCLUDED.rollback_applied_rate,
    rollback_mttr_p95_sec = EXCLUDED.rollback_mttr_p95_sec,
    retry_rate = EXCLUDED.retry_rate,
    escalation_rate = EXCLUDED.escalation_rate,
    mttr_slo_sec = EXCLUDED.mttr_slo_sec,
    rollup_json = EXCLUDED.rollup_json
"""

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

def maybe_json(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode()
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}

def i64(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default

def f64(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default

def normalize(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = maybe_json(row.get("payload"))
    base = payload if payload else row
    return {
        "ts_ms": i64(base.get("ts_ms")),
        "window_min": i64(base.get("window_min")),
        "verification_n": i64(base.get("verification_n")),
        "verified_n": i64(base.get("verified_n")),
        "rollback_planned_n": i64(base.get("rollback_planned_n")),
        "rollback_applied_n": i64(base.get("rollback_applied_n")),
        "retry_n": i64(base.get("retry_n")),
        "escalation_n": i64(base.get("escalation_n")),
        "verify_keep_rate": f64(base.get("verify_keep_rate")),
        "rollback_plan_rate": f64(base.get("rollback_plan_rate")),
        "rollback_applied_rate": f64(base.get("rollback_applied_rate")),
        "rollback_mttr_p95_sec": f64(base.get("rollback_mttr_p95_sec")),
        "retry_rate": f64(base.get("retry_rate")),
        "escalation_rate": f64(base.get("escalation_rate")),
        "mttr_slo_sec": f64(base.get("mttr_slo_sec")),
        "rollup_json": json.dumps(base, ensure_ascii=False, sort_keys=True),
    }

async def ensure_group(r: "redis.Redis") -> None:
    try:
        await r.xgroup_create(STREAM, GROUP, id="0-0", mkstream=True)
    except Exception:
        pass

async def write_row(conn: "psycopg.AsyncConnection", row: Dict[str, Any]) -> None:
    async with conn.cursor() as cur:
        await cur.execute(UPSERT_SQL, row)
    await conn.commit()

async def main_loop() -> None:
    if redis is None or psycopg is None:
        raise RuntimeError("redis.asyncio and psycopg are required")
    start_http_server(METRICS_PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(REDIS_URL, decode_responses=False)
    conn = await psycopg.AsyncConnection.connect(PG_DSN)
    await ensure_group(r)
    try:
        while True:
            t0 = time.time()
            status = "ok"
            try:
                rows = await r.xreadgroup(
                    groupname=GROUP,
                    consumername=CONSUMER,
                    streams={STREAM: ">"},
                    count=100,
                    block=POLL_BLOCK_MS,
                )
                if not rows:
                    if LAST_RUN:
                        LAST_RUN.set(time.time())
                    continue
                for _, items in rows:
                    for msg_id, fields in items:
                        raw = as_dict(fields)
                        parsed = normalize(raw)
                        try:
                            if parsed["ts_ms"] <= 0:
                                raise ValueError("bad_ts_ms")
                            await write_row(conn, parsed)
                            if ROWS_WRITTEN:
                                ROWS_WRITTEN.inc()
                            if LAG_MS:
                                LAG_MS.set(max(0, now_ms() - parsed["ts_ms"]))
                            await r.xadd(
                                AUDIT_STREAM,
                                {
                                    "event": "persisted",
                                    "table": TABLE,
                                    "source_stream": STREAM,
                                    "source_id": msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id),
                                    "ts_ms": str(parsed["ts_ms"]),
                                },
                                maxlen=20000,
                                approximate=True,
                            )
                            await r.xack(STREAM, GROUP, msg_id)
                        except Exception as e:
                            if DLQ_TOTAL:
                                DLQ_TOTAL.labels(reason=type(e).__name__).inc()
                            await r.xadd(
                                DLQ_STREAM,
                                {
                                    "source_id": msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id),
                                    "error": type(e).__name__,
                                    "payload": json.dumps(raw, ensure_ascii=False, sort_keys=True),
                                },
                                maxlen=20000,
                                approximate=True,
                            )
                            await r.xack(STREAM, GROUP, msg_id)
                if LAST_RUN:
                    LAST_RUN.set(time.time())
            except Exception:
                status = "error"
                log.exception("loop failed")
            finally:
                if RUNS:
                    RUNS.labels(status=status).inc()
                if LAT:
                    LAT.observe(max(0.0, time.time() - t0))
    finally:
        if UP:
            UP.set(0)
        await conn.close()
        await r.close()

def main() -> None:
    asyncio.run(main_loop())

if __name__ == "__main__":
    main()
