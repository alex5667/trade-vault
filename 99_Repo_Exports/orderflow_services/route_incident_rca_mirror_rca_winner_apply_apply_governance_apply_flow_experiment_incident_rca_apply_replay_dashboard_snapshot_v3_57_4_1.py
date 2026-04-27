from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List

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

APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_dashboard_snapshot_v3_57_4_1"
PROM = APP_NAME
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PG_DSN = os.getenv("TIMESCALE_DSN", os.getenv("DATABASE_URL", ""))
METRICS_PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_DASHBOARD_PORT", "10003"))
POLL_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_DASHBOARD_POLL_SEC", "60"))
LOOKBACK_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_DASHBOARD_LOOKBACK_MIN", "1440"))
REQUIRED_ALIASES = [x.strip() for x in os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_DASHBOARD_REQUIRED_ALIASES", "slo,retry,escalation").split(",") if x.strip()]

VALIDATION_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation_reports"
GATE_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_decisions"
SNAPSHOT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_dashboard_snapshots"
AUDIT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_dashboard_audit"
METRICS_HASH = "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_dashboard:last"

TABLE = "llm_route_incident_rca_apply_replay_dashboard_snapshots"

UPSERT_SQL = f"""
INSERT INTO {TABLE} (
    ts_ms, window_start_ts_ms, window_end_ts_ms, gate_decision, aliases_ok, aliases_required,
    snapshot_status, snapshot_json
) VALUES (
    %(ts_ms)s, %(window_start_ts_ms)s, %(window_end_ts_ms)s, %(gate_decision)s, %(aliases_ok)s, %(aliases_required)s,
    %(snapshot_status)s, %(snapshot_json)s
)
ON CONFLICT (ts_ms)
DO UPDATE SET
    window_start_ts_ms = EXCLUDED.window_start_ts_ms,
    window_end_ts_ms = EXCLUDED.window_end_ts_ms,
    gate_decision = EXCLUDED.gate_decision,
    aliases_ok = EXCLUDED.aliases_ok,
    aliases_required = EXCLUDED.aliases_required,
    snapshot_status = EXCLUDED.snapshot_status,
    snapshot_json = EXCLUDED.snapshot_json
"""

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
log = logging.getLogger(APP_NAME)

RUNS = Counter(f"{PROM}_runs_total", "runs", ["status"]) if Counter else None
UP = Gauge(f"{PROM}_up", "up") if Gauge else None
LAST_RUN = Gauge(f"{PROM}_last_run_ts_seconds", "last run ts") if Gauge else None
GATE_PASS = Gauge(f"{PROM}_gate_pass", "gate pass 0/1") if Gauge else None
ALIAS_REPORTS_OK = Gauge(f"{PROM}_alias_reports_ok", "alias reports ok") if Gauge else None
LAT = Histogram(f"{PROM}_loop_seconds", "loop seconds") if Histogram else None

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
            x = json.loads(v)
            if isinstance(x, dict):
                return x
        except Exception:
            return {}
    return {}

def normalize(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = maybe_json(row.get("payload"))
    return payload if payload else row

async def latest_gate(r: "redis.Redis") -> Dict[str, Any]:
    rows = await r.xrevrange(GATE_STREAM, "+", "-", count=20)
    for _, fields in rows:
        gate = normalize(as_dict(fields))
        if str(gate.get("decision") or "") in ("PASS", "BLOCK"):
            return gate
    return {}

async def reports_for_window(r: "redis.Redis", w0: int, w1: int) -> Dict[str, Dict[str, Any]]:
    rows = await r.xrevrange(VALIDATION_STREAM, "+", "-", count=500)
    out: Dict[str, Dict[str, Any]] = {}
    for _, fields in rows:
        rep = normalize(as_dict(fields))
        alias = str(rep.get("alias") or "")
        if alias == "" or alias in out:
            continue
        if int(rep.get("window_start_ts_ms") or 0) == w0 and int(rep.get("window_end_ts_ms") or 0) == w1:
            out[alias] = rep
        if len(out) >= len(REQUIRED_ALIASES):
            break
    return out

def build_snapshot(gate: Dict[str, Any], reports: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ts_ms = now_ms()
    w0 = int(gate.get("window_start_ts_ms") or 0)
    w1 = int(gate.get("window_end_ts_ms") or 0)
    aliases_ok = int(gate.get("aliases_ok") or 0)
    aliases_required = int(gate.get("aliases_required") or len(REQUIRED_ALIASES))
    alias_views: Dict[str, Any] = {}
    ok_reports = 0
    for alias in REQUIRED_ALIASES:
        rep = reports.get(alias, {})
        alias_views[alias] = {
            "status": str(rep.get("status") or "MISSING"),
            "key_coverage_ratio": float(rep.get("key_coverage_ratio") or 0.0),
            "hash_match": int(rep.get("hash_match") or 0),
            "stream_row_count": int(rep.get("stream_row_count") or 0),
            "pg_row_count": int(rep.get("pg_row_count") or 0),
            "missing_in_pg_n": int(rep.get("missing_in_pg_n") or 0),
            "extra_in_pg_n": int(rep.get("extra_in_pg_n") or 0),
            "report_age_sec": max(0, (ts_ms - int(rep.get("ts_ms") or ts_ms)) // 1000),
        }
        if alias_views[alias]["status"] == "PASS":
            ok_reports += 1

    snapshot_status = "OK" if str(gate.get("decision") or "") == "PASS" and ok_reports == len(REQUIRED_ALIASES) else "ATTENTION"
    return {
        "schema_version": 1,
        "app_name": APP_NAME,
        "ts_ms": ts_ms,
        "window_start_ts_ms": w0,
        "window_end_ts_ms": w1,
        "gate_decision": str(gate.get("decision") or "UNKNOWN"),
        "gate_reasons": gate.get("gate_reasons", []),
        "aliases_ok": aliases_ok,
        "aliases_required": aliases_required,
        "snapshot_status": snapshot_status,
        "alias_views": alias_views,
    }

async def upsert_snapshot(conn: "psycopg.AsyncConnection", snapshot: Dict[str, Any]) -> None:
    row = {
        "ts_ms": int(snapshot["ts_ms"]),
        "window_start_ts_ms": int(snapshot["window_start_ts_ms"]),
        "window_end_ts_ms": int(snapshot["window_end_ts_ms"]),
        "gate_decision": str(snapshot["gate_decision"]),
        "aliases_ok": int(snapshot["aliases_ok"]),
        "aliases_required": int(snapshot["aliases_required"]),
        "snapshot_status": str(snapshot["snapshot_status"]),
        "snapshot_json": json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
    }
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
    try:
        while True:
            t0 = time.time()
            status = "ok"
            try:
                gate = await latest_gate(r)
                if not gate:
                    raise ValueError("missing_gate_decision")
                w0 = int(gate.get("window_start_ts_ms") or 0)
                w1 = int(gate.get("window_end_ts_ms") or 0)
                if w0 <= 0 or w1 <= 0:
                    raise ValueError("bad_gate_window")
                reports = await reports_for_window(r, w0, w1)
                snapshot = build_snapshot(gate, reports)
                blob = json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
                await r.xadd(SNAPSHOT_STREAM, {"payload": blob}, maxlen=50000, approximate=True)
                await r.xadd(AUDIT_STREAM, {"payload": blob, "event": "replay_dashboard_snapshot"}, maxlen=50000, approximate=True)
                await r.hset(METRICS_HASH, mapping={k: json.dumps(v, ensure_ascii=False, sort_keys=True) if isinstance(v, (list, dict)) else str(v) for k, v in snapshot.items()})
                await upsert_snapshot(conn, snapshot)
                if GATE_PASS:
                    GATE_PASS.set(1 if snapshot["gate_decision"] == "PASS" else 0)
                if ALIAS_REPORTS_OK:
                    ALIAS_REPORTS_OK.set(sum(1 for a in snapshot["alias_views"].values() if a["status"] == "PASS"))
                if LAST_RUN:
                    LAST_RUN.set(time.time())
            except Exception:
                status = "error"
                log.exception("dashboard snapshot loop failed")
            finally:
                if RUNS:
                    RUNS.labels(status=status).inc()
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
