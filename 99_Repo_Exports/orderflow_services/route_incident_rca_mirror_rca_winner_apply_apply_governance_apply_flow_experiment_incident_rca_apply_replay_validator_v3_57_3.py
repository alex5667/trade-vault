from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Tuple

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

APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validator_v3_57_3"
PROM = APP_NAME

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PG_DSN = os.getenv("TIMESCALE_DSN", os.getenv("DATABASE_URL", ""))
METRICS_PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_VALIDATOR_PORT", "10000"))
WINDOW_START_TS_MS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_WINDOW_START_TS_MS", "0"))
WINDOW_END_TS_MS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_WINDOW_END_TS_MS", "0"))
ALIASES = [x.strip() for x in os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_ALIASES", "slo,retry,escalation").split(",") if x.strip()]
MAX_ROWS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_MAX_ROWS", "5000"))

REPORT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation_reports"
AUDIT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation_audit"
METRICS_HASH = "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation:last"

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
log = logging.getLogger(APP_NAME)

RUNS = Counter(f"{PROM}_runs_total", "runs", ["status", "alias"]) if Counter else None
UP = Gauge(f"{PROM}_up", "up") if Gauge else None
LAST_RUN = Gauge(f"{PROM}_last_run_ts_seconds", "last run ts") if Gauge else None
STREAM_ROW_COUNT = Gauge(f"{PROM}_stream_row_count", "stream row count", ["alias"]) if Gauge else None
PG_ROW_COUNT = Gauge(f"{PROM}_pg_row_count", "pg row count", ["alias"]) if Gauge else None
KEY_COVERAGE = Gauge(f"{PROM}_key_coverage_ratio", "key coverage ratio", ["alias"]) if Gauge else None
HASH_MATCH = Gauge(f"{PROM}_hash_match", "hash match 0/1", ["alias"]) if Gauge else None
MISSING_IN_PG = Gauge(f"{PROM}_missing_in_pg_n", "missing in pg", ["alias"]) if Gauge else None
EXTRA_IN_PG = Gauge(f"{PROM}_extra_in_pg_n", "extra in pg", ["alias"]) if Gauge else None
LAT = Histogram(f"{PROM}_loop_seconds", "loop seconds") if Histogram else None

SPECS = {
    "slo": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups",
        "normalize": normalize_slo,
        "stream_key": lambda r: (int(r["ts_ms"]),),
        "pg_select": """
            SELECT ts_ms, window_min, verification_n, verified_n, rollback_planned_n, rollback_applied_n,
                   retry_n, escalation_n, verify_keep_rate, rollback_plan_rate, rollback_applied_rate,
                   rollback_mttr_p95_sec, retry_rate, escalation_rate, mttr_slo_sec
            FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups
            WHERE ts_ms >= %s AND ts_ms <= %s
            ORDER BY ts_ms ASC
            LIMIT %s
        """,
        "subset_fields": [
            "ts_ms", "window_min", "verification_n", "verified_n", "rollback_planned_n",
            "rollback_applied_n", "retry_n", "escalation_n", "verify_keep_rate",
            "rollback_plan_rate", "rollback_applied_rate", "rollback_mttr_p95_sec",
            "retry_rate", "escalation_rate", "mttr_slo_sec",
        ],
    },
    "retry": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results",
        "normalize": normalize_retry,
        "stream_key": lambda r: (int(r["ts_ms"]), str(r["rollback_mode"]), str(r["failed_target_mode"]), str(r["reason_code"])),
        "pg_select": """
            SELECT ts_ms, source_rollback_ts_ms, source_verification_ts_ms, rollback_mode, failed_target_mode,
                   decision, reason_code, severity, attempts, applied
            FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results
            WHERE ts_ms >= %s AND ts_ms <= %s
            ORDER BY ts_ms ASC, rollback_mode ASC, failed_target_mode ASC, reason_code ASC
            LIMIT %s
        """,
        "subset_fields": [
            "ts_ms", "source_rollback_ts_ms", "source_verification_ts_ms", "rollback_mode",
            "failed_target_mode", "decision", "reason_code", "severity", "attempts", "applied",
        ],
    },
    "escalation": {
        "stream": "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations",
        "table": "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations",
        "normalize": normalize_escalation,
        "stream_key": lambda r: (int(r["ts_ms"]), str(r["rollback_mode"]), str(r["failed_target_mode"]), str(r["reason_code"])),
        "pg_select": """
            SELECT ts_ms, source_rollback_ts_ms, source_verification_ts_ms, rollback_mode, failed_target_mode,
                   decision, reason_code, severity
            FROM llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations
            WHERE ts_ms >= %s AND ts_ms <= %s
            ORDER BY ts_ms ASC, rollback_mode ASC, failed_target_mode ASC, reason_code ASC
            LIMIT %s
        """,
        "subset_fields": [
            "ts_ms", "source_rollback_ts_ms", "source_verification_ts_ms", "rollback_mode",
            "failed_target_mode", "decision", "reason_code", "severity",
        ],
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

def canonical_subset(row: Dict[str, Any], fields: Iterable[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for f in fields:
        v = row.get(f)
        if isinstance(v, float):
            out[f] = round(v, 10)
        else:
            out[f] = v
    return out

def stable_hash(rows: List[Dict[str, Any]], fields: Iterable[str]) -> str:
    items = [canonical_subset(r, fields) for r in rows]
    blob = json.dumps(items, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

async def read_stream_window(r: "redis.Redis", stream: str, ts_min: int, ts_max: int, limit: int) -> List[Dict[str, Any]]:
    rows = await r.xrange(stream, min=f"{ts_min}-0", max=f"{ts_max}-999999", count=limit)
    out: List[Dict[str, Any]] = []
    for _, fields in rows:
        out.append(as_dict(fields))
    return out

async def read_pg_window(conn: "psycopg.AsyncConnection", sql: str, ts_min: int, ts_max: int, limit: int) -> List[Dict[str, Any]]:
    async with conn.cursor() as cur:
        await cur.execute(sql, (ts_min, ts_max, limit))
        cols = [d.name for d in cur.description]
        rows = await cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]

def status_from_metrics(stream_n: int, pg_n: int, missing_in_pg_n: int, extra_in_pg_n: int, hash_match: bool) -> str:
    if stream_n != pg_n:
        return "COUNT_MISMATCH"
    if missing_in_pg_n > 0 or extra_in_pg_n > 0:
        return "KEY_GAP"
    if not hash_match:
        return "HASH_MISMATCH"
    return "PASS"

async def validate_alias(r: "redis.Redis", conn: "psycopg.AsyncConnection", alias: str, ts_min: int, ts_max: int) -> Dict[str, Any]:
    spec = SPECS[alias]
    raw_stream_rows = await read_stream_window(r, spec["stream"], ts_min, ts_max, MAX_ROWS)
    stream_rows = [spec["normalize"](row) for row in raw_stream_rows]
    pg_rows = await read_pg_window(conn, spec["pg_select"], ts_min, ts_max, MAX_ROWS)

    stream_rows_sorted = sorted(stream_rows, key=spec["stream_key"])
    pg_rows_sorted = sorted(pg_rows, key=spec["stream_key"])

    stream_keys = {spec["stream_key"](r) for r in stream_rows_sorted}
    pg_keys = {spec["stream_key"](r) for r in pg_rows_sorted}

    missing_in_pg = sorted(stream_keys - pg_keys)
    extra_in_pg = sorted(pg_keys - stream_keys)

    stream_hash = stable_hash(stream_rows_sorted, spec["subset_fields"])
    pg_hash = stable_hash(pg_rows_sorted, spec["subset_fields"])
    hash_match = stream_hash == pg_hash
    coverage_ratio = (len(stream_keys & pg_keys) / len(stream_keys)) if stream_keys else 1.0
    status = status_from_metrics(len(stream_rows_sorted), len(pg_rows_sorted), len(missing_in_pg), len(extra_in_pg), hash_match)

    report = {
        "schema_version": 1,
        "app_name": APP_NAME,
        "alias": alias,
        "ts_ms": now_ms(),
        "window_start_ts_ms": ts_min,
        "window_end_ts_ms": ts_max,
        "stream_row_count": len(stream_rows_sorted),
        "pg_row_count": len(pg_rows_sorted),
        "key_coverage_ratio": round(coverage_ratio, 6),
        "missing_in_pg_n": len(missing_in_pg),
        "extra_in_pg_n": len(extra_in_pg),
        "stream_subset_hash": stream_hash,
        "pg_subset_hash": pg_hash,
        "hash_match": int(hash_match),
        "status": status,
        "missing_in_pg_sample": missing_in_pg[:10],
        "extra_in_pg_sample": extra_in_pg[:10],
    }
    return report

async def main_loop() -> None:
    if redis is None or psycopg is None:
        raise RuntimeError("redis.asyncio and psycopg are required")
    if WINDOW_START_TS_MS <= 0 or WINDOW_END_TS_MS <= 0 or WINDOW_END_TS_MS < WINDOW_START_TS_MS:
        raise ValueError("invalid replay validation window")

    start_http_server(METRICS_PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(REDIS_URL, decode_responses=False)
    conn = await psycopg.AsyncConnection.connect(PG_DSN)
    try:
        t0 = time.time()
        for alias in ALIASES:
            status = "ok"
            try:
                report = await validate_alias(r, conn, alias, WINDOW_START_TS_MS, WINDOW_END_TS_MS)
                payload = json.dumps(report, ensure_ascii=False, sort_keys=True)
                await r.xadd(REPORT_STREAM, {"payload": payload}, maxlen=50000, approximate=True)
                await r.xadd(AUDIT_STREAM, {"payload": payload, "event": "replay_validation_complete"}, maxlen=50000, approximate=True)
                await r.hset(METRICS_HASH, mapping={f"{alias}:{k}": json.dumps(v, ensure_ascii=False, sort_keys=True) if isinstance(v, (list, dict)) else str(v) for k, v in report.items()})

                if STREAM_ROW_COUNT:
                    STREAM_ROW_COUNT.labels(alias=alias).set(report["stream_row_count"])
                if PG_ROW_COUNT:
                    PG_ROW_COUNT.labels(alias=alias).set(report["pg_row_count"])
                if KEY_COVERAGE:
                    KEY_COVERAGE.labels(alias=alias).set(report["key_coverage_ratio"])
                if HASH_MATCH:
                    HASH_MATCH.labels(alias=alias).set(report["hash_match"])
                if MISSING_IN_PG:
                    MISSING_IN_PG.labels(alias=alias).set(report["missing_in_pg_n"])
                if EXTRA_IN_PG:
                    EXTRA_IN_PG.labels(alias=alias).set(report["extra_in_pg_n"])

                log.info("replay_validate alias=%s status=%s stream_n=%s pg_n=%s key_cov=%.4f hash_match=%s",
                         alias, report["status"], report["stream_row_count"], report["pg_row_count"],
                         report["key_coverage_ratio"], report["hash_match"])
            except Exception:
                status = "error"
                log.exception("replay validation failed alias=%s", alias)
                raise
            finally:
                if RUNS:
                    RUNS.labels(status=status, alias=alias).inc()
        if LAST_RUN:
            LAST_RUN.set(time.time())
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
