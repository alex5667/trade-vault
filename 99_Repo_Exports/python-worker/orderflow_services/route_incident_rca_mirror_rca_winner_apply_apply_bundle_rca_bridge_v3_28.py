from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

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
        pass

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_bundle_rca_bridge_v3_28"

IN_BUNDLES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles")

OUT_VERTEX_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERTEX_RCA_REQUESTS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_requests")
OUT_LOCAL_STREAM = os.getenv("ML_LOCAL_FALLBACK_REQUESTS", "stream:ml:local_fallback_requests")

OUT_DECISIONS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_DECISIONS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_decisions")
OUT_AUDIT = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_AUDIT", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_audit")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge:last")

VERTEX_HEALTH = os.getenv("ML_VERTEX_HEALTH", "metrics:ml:vertex_health:last")
CG_NAME = "cg_rca_bridge_v3_28"
CONS_NAME = f"cons_{os.getpid()}"

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_PORT", "9952"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_MAXLEN", "2000"))

MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_MODE", "AUTO").upper()
REQ_VERTEX_DEGRADED_LOCAL = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL", "1") == "1"
MAX_BUNDLE_BYTES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_MAX_BUNDLE_BYTES", "131072"))
MAX_PROMPT_CHARS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RCA_BRIDGE_MAX_PROMPT_CHARS", "12000"))
POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_last_run_ts_seconds", "Last run")

ROUTED = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_routed_total", "Routed", ("route", "severity"))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: dict[Any, Any]) -> dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def fetch_vertex_health(r: Any) -> bool:
    try:
        vh = await r.hgetall(VERTEX_HEALTH)
        if not vh:
            return False
        d = decode_dict(vh)
        # status can be ok/degraded/down
        if d.get("status", "down") == "ok":
            if time.time() - float(d.get("ts_seconds", 0)) < 300:
                return True
        return False
    except Exception:
        return False

def route_decision(mode: str, vertex_healthy: bool, req_degraded: bool, bundle_bytes: int, severity: str) -> tuple[str, str]:
    if mode == "DISABLED":
        return "REJECT", "disabled"

    if bundle_bytes > MAX_BUNDLE_BYTES:
        return "REJECT", "bundle_too_large"

    if severity not in ("warning", "critical"):
        return "REJECT", "severity_too_low"

    if mode == "VERTEX_ONLY":
        return "ROUTE_VERTEX", "vertex_only"

    if mode == "LOCAL_ONLY":
        return "ROUTE_LOCAL", "local_only"

    if mode == "AUTO":
        if vertex_healthy:
            return "ROUTE_VERTEX", "vertex_healthy"
        elif req_degraded:
            return "ROUTE_LOCAL", "vertex_degraded"
        else:
            return "REJECT", "vertex_degraded_local_not_allowed"

    return "REJECT", "unknown_mode"

def prepare_vertex_payload(bundle_id: str, bundle_json: str) -> dict[str, str]:
    return {
        "apply_id": bundle_id, # Reusing apply_id field for bundle_id for simplicity as primary key in RCA streams
        "task_family": "route_incident_rca_mirror_rca_winner_apply_apply_rca",
        "task_type": "route_incident_rca_mirror_rca_winner_apply_apply_rca",
        "bundle_json": bundle_json[:MAX_PROMPT_CHARS] if len(bundle_json) > MAX_PROMPT_CHARS else bundle_json,
        "ts_ms": str(now_ms())
    }

def prepare_local_payload(bundle_id: str, bundle_json: str) -> dict[str, str]:
    return {
        "ticket_id": f"vw_app_rca_{bundle_id}_{now_ms()}",
        "task_family": "route_incident_rca_mirror_rca_winner_apply_apply_rca",
        "task_type": "vertex_unavailable_fallback",
        "source": APP_NAME,
        "input_json": bundle_json[:MAX_PROMPT_CHARS] if len(bundle_json) > MAX_PROMPT_CHARS else bundle_json,
        "ts_ms": str(now_ms())
    }

async def persist_decision(db_url: str, bundle_id: str, decision: str, reason: str, severity: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_decisions (
                    bundle_id, decision, reason, severity, ts_ms
                ) VALUES (
                    %(bundle_id)s, %(decision)s, %(reason)s, %(severity)s, %(ts_ms)s
                )
                """,
                {
                    "bundle_id": bundle_id,
                    "decision": decision,
                    "reason": reason,
                    "severity": severity,
                    "ts_ms": now_ms(),
                }
            )
            conn.commit()

async def process_msg(r: Any, db_url: str, msg_id: str, fields: dict[str, Any]) -> None:
    started = time.perf_counter()
    status = "ok"
    decision = "REJECT"
    route_reason = "unknown"
    severity = fields.get("severity", "info")

    try:
        bundle_id = fields.get("bundle_id", "")
        bundle_json = fields.get("payload_json", "")
        if not bundle_id or not bundle_json:
            decision = "REJECT"
            route_reason = "missing_data"
            return

        b_bytes = len(bundle_json.encode('utf-8'))
        v_ok = await fetch_vertex_health(r)

        decision, route_reason = route_decision(MODE, v_ok, REQ_VERTEX_DEGRADED_LOCAL, b_bytes, severity)

        if decision == "ROUTE_VERTEX":
            await r.xadd(OUT_VERTEX_STREAM, prepare_vertex_payload(bundle_id, bundle_json), maxlen=MAXLEN, approximate=True)
            if ROUTED: ROUTED.labels(route="vertex", severity=severity).inc()
        elif decision == "ROUTE_LOCAL":
            await r.xadd(OUT_LOCAL_STREAM, prepare_local_payload(bundle_id, bundle_json), maxlen=MAXLEN, approximate=True)
            if ROUTED: ROUTED.labels(route="local", severity=severity).inc()

        await r.xadd(OUT_DECISIONS, {
            "bundle_id": bundle_id,
            "decision": decision,
            "reason": route_reason,
            "severity": severity,
            "ts_ms": str(now_ms())
        }, maxlen=MAXLEN, approximate=True)

        await r.xadd(OUT_AUDIT, {
            "bundle_id": bundle_id,
            "decision": decision,
            "ts_ms": str(now_ms())
        }, maxlen=MAXLEN, approximate=True)

        await r.hset(LAST_METRIC, "bundle_id", bundle_id)
        await r.hset(LAST_METRIC, "decision", decision)
        await r.hset(LAST_METRIC, "reason", route_reason)
        await r.hset(LAST_METRIC, "ts_ms", str(now_ms()))

        await persist_decision(db_url, bundle_id, decision, route_reason, severity)

    except Exception:
        status = "error"
    finally:
        if RUNS:
            RUNS.labels(status=status, decision=decision).inc()
        if LAT:
            LAT.observe(max(time.perf_counter() - started, 0.0))

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)

    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    with contextlib.suppress(Exception):
        await r.xgroup_create(IN_BUNDLES_STREAM, CG_NAME, id="0", mkstream=True)

    while True:
        try:
            resp = await r.xreadgroup(CG_NAME, CONS_NAME, {IN_BUNDLES_STREAM: ">"}, count=10, block=2000)
            if LAST_RUN:
                LAST_RUN.set(time.time())

            if not resp:
                continue

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    mid = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    f = decode_dict(fields)
                    await process_msg(r, db_url, mid, f)
                    await r.xack(stream_name, CG_NAME, msg_id)
        except Exception:
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
