from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
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

APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_replayer_v3_57_2"
PROM = APP_NAME

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PG_DSN = os.getenv("TIMESCALE_DSN", os.getenv("DATABASE_URL", ""))
METRICS_PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_REPLAYER_PORT", "9998"))
MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_MODE", "DRY_RUN").upper()  # DRY_RUN|COMMIT
ALIASES = [x.strip() for x in os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_ALIASES", "slo,retry,escalation").split(",") if x.strip()]
BATCH_SIZE = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_BATCH_SIZE", "500"))
START_ID = os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_START_ID", "0-0")
END_ID = os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_END_ID", "+")
RESUME = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_RESUME", "1")) != 0

BACKFILL_RUNS_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_runs"
BACKFILL_AUDIT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_audit"
BACKFILL_DLQ_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_dlq"
STATE_HASH = "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill:last"

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
log = logging.getLogger(APP_NAME)

RUNS = Counter(f"{PROM}_runs_total", "runs", ["status", "mode"]) if Counter else None
ROWS_SCANNED = Counter(f"{PROM}_rows_scanned_total", "rows scanned", ["alias"]) if Counter else None
ROWS_WRITTEN = Counter(f"{PROM}_rows_written_total", "rows written", ["alias"]) if Counter else None
DLQ_TOTAL = Counter(f"{PROM}_dlq_total", "dlq total", ["alias", "reason"]) if Counter else None
UP = Gauge(f"{PROM}_up", "up") if Gauge else None
LAST_RUN = Gauge(f"{PROM}_last_run_ts_seconds", "last run ts") if Gauge else None
LAST_CURSOR_TS_MS = Gauge(f"{PROM}_last_cursor_ts_ms", "last cursor ts ms", ["alias"]) if Gauge else None
LAT = Histogram(f"{PROM}_loop_seconds", "loop seconds") if Histogram else None

SPECS = {
    "slo": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups",
        "normalize": normalize_slo,
        "upsert_sql": """
            INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups (
                ts_ms, window_min, verification_n, verified_n, rollback_planned_n, rollback_applied_n,
                retry_n, escalation_n, verify_keep_rate, rollback_plan_rate, rollback_applied_rate,
                rollback_mttr_p95_sec, retry_rate, escalation_rate, mttr_slo_sec, rollup_json
            ) VALUES (
                %(ts_ms)s, %(window_min)s, %(verification_n)s, %(verified_n)s, %(rollback_planned_n)s, %(rollback_applied_n)s,
                %(retry_n)s, %(escalation_n)s, %(verify_keep_rate)s, %(rollback_plan_rate)s, %(rollback_applied_rate)s,
                %(rollback_mttr_p95_sec)s, %(retry_rate)s, %(escalation_rate)s, %(mttr_slo_sec)s, %(rollup_json)s
            )
            ON CONFLICT (ts_ms) DO UPDATE SET
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
        """,
    },
    "retry": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results",
        "normalize": normalize_retry,
        "upsert_sql": """
            INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results (
                ts_ms, source_rollback_ts_ms, source_verification_ts_ms, rollback_mode, failed_target_mode,
                decision, reason_code, severity, attempts, applied, result_json
            ) VALUES (
                %(ts_ms)s, %(source_rollback_ts_ms)s, %(source_verification_ts_ms)s, %(rollback_mode)s, %(failed_target_mode)s,
                %(decision)s, %(reason_code)s, %(severity)s, %(attempts)s, %(applied)s, %(result_json)s
            )
            ON CONFLICT (ts_ms, rollback_mode, failed_target_mode, reason_code) DO UPDATE SET
                source_rollback_ts_ms = EXCLUDED.source_rollback_ts_ms,
                source_verification_ts_ms = EXCLUDED.source_verification_ts_ms,
                decision = EXCLUDED.decision,
                severity = EXCLUDED.severity,
                attempts = EXCLUDED.attempts,
                applied = EXCLUDED.applied,
                result_json = EXCLUDED.result_json
        """,
    },
    "escalation": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations",
        "normalize": normalize_escalation,
        "upsert_sql": """
            INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations (
                ts_ms, source_rollback_ts_ms, source_verification_ts_ms, rollback_mode, failed_target_mode,
                decision, reason_code, severity, escalation_json
            ) VALUES (
                %(ts_ms)s, %(source_rollback_ts_ms)s, %(source_verification_ts_ms)s, %(rollback_mode)s, %(failed_target_mode)s,
                %(decision)s, %(reason_code)s, %(severity)s, %(escalation_json)s
            )
            ON CONFLICT (ts_ms, rollback_mode, failed_target_mode, reason_code) DO UPDATE SET
                source_rollback_ts_ms = EXCLUDED.source_rollback_ts_ms,
                source_verification_ts_ms = EXCLUDED.source_verification_ts_ms,
                decision = EXCLUDED.decision,
                severity = EXCLUDED.severity,
                escalation_json = EXCLUDED.escalation_json
        """,
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

def cursor_key(alias: str) -> str:
    return f"state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_cursor:{alias}"

async def fetch_batch(r: "redis.Redis", stream: str, last_id: str, end_id: str, count: int) -> List[Tuple[str, Dict[str, Any]]]:
    rows = await r.xrange(stream, min=last_id, max=end_id, count=count)
    out: List[Tuple[str, Dict[str, Any]]] = []
    for msg_id, fields in rows:
        sid = msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
        if sid == last_id:
            continue
        out.append((sid, as_dict(fields)))
    return out

async def upsert_many(conn: "psycopg.AsyncConnection", sql: str, rows: List[Dict[str, Any]]) -> int:
    async with conn.cursor() as cur:
        for row in rows:
            await cur.execute(sql, row)
    await conn.commit()
    return len(rows)

async def process_alias(r: "redis.Redis", conn: "psycopg.AsyncConnection", run_id: str, alias: str) -> Dict[str, Any]:
    spec = SPECS[alias]
    normalize = spec["normalize"]
    stream = spec["stream"]
    upsert_sql = spec["upsert_sql"]

    last_id = START_ID
    if RESUME:
        saved = await r.get(cursor_key(alias))
        if saved:
            last_id = saved.decode() if isinstance(saved, (bytes, bytearray)) else str(saved)

    scanned_n = 0
    written_n = 0
    dlq_n = 0
    t0 = now_ms()

    while True:
        batch = await fetch_batch(r, stream, last_id, END_ID, BATCH_SIZE)
        if not batch:
            break

        prepared: List[Dict[str, Any]] = []
        for sid, raw in batch:
            scanned_n += 1
            if ROWS_SCANNED:
                ROWS_SCANNED.labels(alias=alias).inc()
            try:
                row = normalize(raw)
                if int(row.get("ts_ms", 0)) <= 0:
                    raise ValueError("bad_ts_ms")
                prepared.append(row)
                last_id = sid
                if LAST_CURSOR_TS_MS:
                    LAST_CURSOR_TS_MS.labels(alias=alias).set(int(row["ts_ms"]))
            except Exception as e:
                dlq_n += 1
                if DLQ_TOTAL:
                    DLQ_TOTAL.labels(alias=alias, reason=type(e).__name__).inc()
                await r.xadd(
                    BACKFILL_DLQ_STREAM,
                    {
                        "run_id": run_id,
                        "alias": alias,
                        "source_stream": stream,
                        "source_id": sid,
                        "error": type(e).__name__,
                        "payload": json.dumps(raw, ensure_ascii=False, sort_keys=True),
                    },
                    maxlen=50000,
                    approximate=True,
                )

        if MODE == "COMMIT" and prepared:
            written = await upsert_many(conn, upsert_sql, prepared)
            written_n += written
            if ROWS_WRITTEN:
                ROWS_WRITTEN.labels(alias=alias).inc(written)
        else:
            written_n += len(prepared)

        await r.set(cursor_key(alias), last_id)

    report = {
        "schema_version": 1,
        "app_name": APP_NAME,
        "run_id": run_id,
        "alias": alias,
        "mode": MODE,
        "start_id": START_ID,
        "end_id": END_ID,
        "last_stream_id": last_id,
        "scanned_n": scanned_n,
        "written_n": written_n,
        "dlq_n": dlq_n,
        "duration_ms": max(0, now_ms() - t0),
        "ts_ms": now_ms(),
    }
    await r.xadd(BACKFILL_RUNS_STREAM, {"payload": json.dumps(report, ensure_ascii=False, sort_keys=True)}, maxlen=50000, approximate=True)
    await r.xadd(BACKFILL_AUDIT_STREAM, {"payload": json.dumps({"event": "alias_complete", **report}, ensure_ascii=False, sort_keys=True)}, maxlen=50000, approximate=True)
    await r.hset(STATE_HASH, mapping={f"{alias}:{k}": str(v) for k, v in report.items() if not isinstance(v, (dict, list))})
    return report

async def main_loop() -> None:
    if redis is None or psycopg is None:
        raise RuntimeError("redis.asyncio and psycopg are required")
    start_http_server(METRICS_PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(REDIS_URL, decode_responses=False)
    conn = await psycopg.AsyncConnection.connect(PG_DSN)
    status = "ok"
    t0 = time.time()
    try:
        run_id = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"
        for alias in ALIASES:
            if alias not in SPECS:
                raise ValueError(f"unknown_alias:{alias}")
            report = await process_alias(r, conn, run_id, alias)
            log.info("backfill alias=%s mode=%s scanned=%s written=%s dlq=%s last_id=%s", alias, MODE, report["scanned_n"], report["written_n"], report["dlq_n"], report["last_stream_id"])
        if LAST_RUN:
            LAST_RUN.set(time.time())
    except Exception:
        status = "error"
        log.exception("backfill failed")
        raise
    finally:
        if RUNS:
            RUNS.labels(status=status, mode=MODE).inc()
        if LAT:
            LAT.observe(max(0.0, time.time() - t0))
        if UP:
            UP.set(0)
        await conn.close()
        await r.close()

def main() -> None:
    asyncio.run(main_loop())

if __name__ == "__main__":
    main()
