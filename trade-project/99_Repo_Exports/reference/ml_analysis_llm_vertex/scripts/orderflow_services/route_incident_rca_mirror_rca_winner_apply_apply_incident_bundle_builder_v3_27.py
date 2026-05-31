from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
import uuid
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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_incident_bundle_builder_v3_27"

STREAM_CTRL_JOURNAL = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_JOURNAL", "stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_journal")
STREAM_RB_JOURNAL = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_JOURNAL", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal")
STREAM_ESCALATIONS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ESCALATIONS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_escalations")
STREAM_VERIFY_RES = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_RESULTS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results")
STREAM_RETRY_RES = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_RESULTS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry_results")
STREAM_SLO = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_ROLLUPS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups")

OUT_BUNDLES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles")
OUT_AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES_AUDIT", "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles_audit")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES_PORT", "9951"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES_MAXLEN", "2000"))

LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES_LOOKBACK_COUNT", "50"))
TRIGGER_ON_APPLY_DECISIONS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES_TRIGGER_ON_APPLY_DECISIONS", "APPLY_PRIMARY_ARM_SHADOW,APPLY_SINGLE_ARM").split(",")
ONLY_SEVERITY = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_INCIDENT_BUNDLES_ONLY_SEVERITY", "warning,critical").split(",")

POLL_INTERVAL_SEC = 30.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles_runs_total", "Runs", ("status", "trigger_type"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles_last_run_ts_seconds", "Last run")

BUNDLES_TOTAL = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles_total", "Bundles", ("severity", "trigger_type"))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def fetch_recent(r: Any, stream: str, count: int) -> List[Dict[str, Any]]:
    try:
        hist = await r.xrevrange(stream, max="+", min="-", count=count)
        return [{"msg_id": msg_id.decode() if isinstance(msg_id, bytes) else msg_id, **decode_dict(fields)} for msg_id, fields in hist] if hist else []
    except Exception:
        return []

async def persist_bundle(db_url: str, bundle_id: str, trigger_type: str, severity: str, payload_json: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles (
                    bundle_id, trigger_type, severity, payload_json, ts_ms
                ) VALUES (
                    %(bundle_id)s, %(trigger_type)s, %(severity)s, %(payload_json)s, %(ts_ms)s
                )
                """,
                {
                    "bundle_id": bundle_id,
                    "trigger_type": trigger_type,
                    "severity": severity,
                    "payload_json": payload_json,
                    "ts_ms": now_ms(),
                },
            )
            conn.commit()

def determine_trigger(journal: List[Dict], rbs: List[Dict], esc: List[Dict], last_handled_ts: int) -> Tuple[bool, str, str, Dict]:
    triggers = []
    
    for j in journal:
        ts = int(j.get("ts_ms", 0))
        if ts > last_handled_ts and j.get("strategy") in TRIGGER_ON_APPLY_DECISIONS:
            triggers.append((ts, "apply", "warning", j))
            
    for rb in rbs:
        ts = int(rb.get("ts_ms", 0))
        if ts > last_handled_ts:
            triggers.append((ts, "rollback", "critical", rb))
            
    for e in esc:
        ts = int(e.get("ts_ms", 0))
        if ts > last_handled_ts and e.get("severity") in ONLY_SEVERITY:
            triggers.append((ts, "escalation", e.get("severity", "warning"), e))
            
    triggers.sort(key=lambda x: x[0])
    
    if not triggers:
        return False, "none", "info", {}
        
    ts, trig_type, sev, meta = triggers[-1]  # Take the most recent
    
    # We don't advance till successful build, but we will return the most recent active trigger
    # Wait, actually to avoid multiple bundles back to back for the same event storm we just process one per run.
    return True, trig_type, sev, meta

def trim_evidence(ev: List[Dict[str, Any]], count: int) -> List[Dict[str, Any]]:
    # Reverse cron -> older first -> last 'count' -> revert again. Just giving last N.
    return ev[:count]

async def build_and_emit_bundle(r: Any, db_url: str, trigger_type: str, severity: str, meta: Dict) -> None:
    bundle_id = str(uuid.uuid4())
    
    # GATHER ALL EVIDENCE
    ev_journal = await fetch_recent(r, STREAM_CTRL_JOURNAL, LOOKBACK_COUNT)
    ev_rbs = await fetch_recent(r, STREAM_RB_JOURNAL, LOOKBACK_COUNT)
    ev_verify = await fetch_recent(r, STREAM_VERIFY_RES, LOOKBACK_COUNT)
    ev_retry = await fetch_recent(r, STREAM_RETRY_RES, LOOKBACK_COUNT)
    ev_esc = await fetch_recent(r, STREAM_ESCALATIONS, LOOKBACK_COUNT)
    ev_slo = await fetch_recent(r, STREAM_SLO, LOOKBACK_COUNT)
    
    payload = {
        "bundle_id": bundle_id,
        "contour": "route_incident_rca_mirror_rca_winner_apply_apply_governance",
        "timestamp_ms": now_ms(),
        "trigger": {
            "type": trigger_type,
            "severity": severity,
            "metadata": meta
        },
        "evidence": {
            "apply_journal": trim_evidence(ev_journal, LOOKBACK_COUNT),
            "rollback_journal": trim_evidence(ev_rbs, LOOKBACK_COUNT),
            "verification_results": trim_evidence(ev_verify, LOOKBACK_COUNT),
            "retry_results": trim_evidence(ev_retry, LOOKBACK_COUNT),
            "escalations": trim_evidence(ev_esc, LOOKBACK_COUNT),
            "slo_rollups": trim_evidence(ev_slo, LOOKBACK_COUNT)
        },
        "summary": {
            "applies_in_window": len(ev_journal),
            "rollbacks_in_window": len(ev_rbs),
            "verifications_in_window": len(ev_verify),
            "retries_in_window": len(ev_retry),
            "escalations_in_window": len(ev_esc)
        }
    }
    
    pj = json.dumps(payload)
    
    await r.xadd(OUT_BUNDLES_STREAM, {
        "bundle_id": bundle_id,
        "trigger_type": trigger_type,
        "severity": severity,
        "payload_json": pj,
        "ts_ms": str(now_ms())
    }, maxlen=MAXLEN, approximate=True)
    
    await r.xadd(OUT_AUDIT_STREAM, {
        "bundle_id": bundle_id,
        "trigger_type": trigger_type,
        "severity": severity,
        "ts_ms": str(now_ms())
    }, maxlen=MAXLEN, approximate=True)
    
    await persist_bundle(db_url, bundle_id, trigger_type, severity, pj)
    
    await r.hset(LAST_METRIC, "bundle_id", bundle_id)
    await r.hset(LAST_METRIC, "trigger_type", trigger_type)
    await r.hset(LAST_METRIC, "severity", severity)
    await r.hset(LAST_METRIC, "ts_ms", str(now_ms()))
    
    if BUNDLES_TOTAL:
        BUNDLES_TOTAL.labels(severity=severity, trigger_type=trigger_type).inc()

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
        
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    # simple state memory
    last_handled_ts = now_ms() - (240 * 60 * 1000) # minus 4 hours default

    while True:
        started = time.perf_counter()
        status = "ok"
        trig_type_tag = "none"
        
        try:
            journal_entries = await fetch_recent(r, STREAM_CTRL_JOURNAL, 100)
            rb_entries = await fetch_recent(r, STREAM_RB_JOURNAL, 100)
            esc_entries = await fetch_recent(r, STREAM_ESCALATIONS, 100)
            
            should_build, trig_type, severity, meta = determine_trigger(journal_entries, rb_entries, esc_entries, last_handled_ts)
            trig_type_tag = trig_type
            
            if should_build:
                await build_and_emit_bundle(r, db_url, trig_type, severity, meta)
                last_handled_ts = int(meta.get("ts_ms", now_ms()))

            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, trigger_type=trig_type_tag).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
