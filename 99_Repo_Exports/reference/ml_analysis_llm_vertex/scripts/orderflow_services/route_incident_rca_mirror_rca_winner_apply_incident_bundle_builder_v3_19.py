from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
import uuid
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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_incident_bundle_builder_v3_19"

# Source Streams
JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_journal")
VERIFICATION_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_RESULTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results")
RETRY_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RETRY_RESULTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_retry_results")
ROLLBACK_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal")
ESCALATIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ESCALATIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_escalations")
SLO_ROLLUPS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_SLO_ROLLUPS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_slo_rollups")

# Output Streams
BUNDLES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_incident_bundles")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_incident_bundles_audit")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_PORT", "9940"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_MAXLEN", "1000"))

LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_LOOKBACK_COUNT", "50"))
TRIGGER_ON_APPLY_DECISIONS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_TRIGGER_ON_APPLY_DECISIONS", "APPLY_PRIMARY_ARM_SHADOW,APPLY_SINGLE_ARM").split(",")
ONLY_SEVERITY = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_ONLY_SEVERITY", "warning,critical").split(",")

POLL_INTERVAL_SEC = 5.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_incident_bundles_runs_total", "Runs", ("status", "trigger_type"))
TOTAL_BUNDLES = _counter("ml_route_incident_rca_mirror_rca_winner_apply_incident_bundles_total", "Total Bundles", ("severity", "trigger_type"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_incident_bundles_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_incident_bundles_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_incident_bundles_last_run_ts_seconds", "Last run")


def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def read_recent(r: Any, stream_name: str, count: int) -> List[Dict[str, Any]]:
    res = await r.xrevrange(stream_name, max="+", min="-", count=count)
    if not res:
        return []
    return [decode_dict(fields) for _, fields in res]


def find_unprocessed_triggers(last_times: Dict[str, int], current_data: Dict[str, List[Dict[str, Any]]]) -> List[Tuple[str, str, str, Dict[str, Any]]]:
    # Looking for applies, rollbacks, and escalations as triggers
    triggers = []
    
    # Check applies
    for a in current_data.get("applies", []):
        ts = int(a.get("ts_ms", "0"))
        if ts > last_times.get("applies", 0):
            action = a.get("action", "")
            if action in TRIGGER_ON_APPLY_DECISIONS:
                triggers.append(("apply", "warning", action, a))
                
    # Check rollbacks
    for rb in current_data.get("rollbacks", []):
        ts = int(rb.get("ts_ms", "0"))
        if ts > last_times.get("rollbacks", 0):
            triggers.append(("rollback", "critical", rb.get("reason", "unknown"), rb))
            
    # Check escalations
    for e in current_data.get("escalations", []):
        ts = int(e.get("ts_ms", "0"))
        if ts > last_times.get("escalations", 0):
            sev = e.get("severity", "info")
            if sev in ONLY_SEVERITY:
                triggers.append(("escalation", sev, f"escalation_{sev}", e))
                
    # Sort triggers by timestamp ascending
    triggers.sort(key=lambda x: int(x[3].get("ts_ms", "0")))
    return triggers


def build_bundle(trigger_type: str, severity: str, desc: str, trigger_msg: Dict[str, Any], context: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    bundle_id = f"bndl_wa_{uuid.uuid4().hex[:8]}"
    
    bundle = {
        "bundle_id": bundle_id,
        "contour": "route_incident_rca_mirror_rca_winner_apply",
        "timestamp_ms": now_ms(),
        "trigger": {
            "type": trigger_type,
            "severity": severity,
            "description": desc,
            "message": trigger_msg
        },
        "evidence_slices": {
            "applies": context.get("applies", []),
            "validations": context.get("validations", []),
            "retries": context.get("retries", []),
            "rollbacks": context.get("rollbacks", []),
            "escalations": context.get("escalations", []),
            "slo_rollups": context.get("slo_rollups", [])
        },
        "summary_counts": {
            "applies": len(context.get("applies", [])),
            "validations": len(context.get("validations", [])),
            "retries": len(context.get("retries", [])),
            "rollbacks": len(context.get("rollbacks", [])),
            "escalations": len(context.get("escalations", [])),
            "slo_rollups": len(context.get("slo_rollups", []))
        }
    }
    
    return bundle


async def persist_bundle(db_url: str, bundle: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_incident_bundles (
                    bundle_id, trigger_type, severity, bundle_json, ts_ms
                ) VALUES (
                    %(bundle_id)s, %(trigger_type)s, %(severity)s, %(bundle_json)s, %(ts_ms)s
                )
                """,
                {
                    "bundle_id": bundle["bundle_id"],
                    "trigger_type": bundle["trigger"]["type"],
                    "severity": bundle["trigger"]["severity"],
                    "bundle_json": json.dumps(bundle),
                    "ts_ms": bundle["timestamp_ms"],
                },
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

    # State tracking
    last_times = {
        "applies": now_ms(),
        "rollbacks": now_ms(),
        "escalations": now_ms()
    }
    
    # We prime the times to not emit old bundles on startup
    applies_tmp = await read_recent(r, JOURNAL_STREAM, 1)
    if applies_tmp: last_times["applies"] = int(applies_tmp[0].get("ts_ms", last_times["applies"]))
    
    rollbacks_tmp = await read_recent(r, ROLLBACK_STREAM, 1)
    if rollbacks_tmp: last_times["rollbacks"] = int(rollbacks_tmp[0].get("ts_ms", last_times["rollbacks"]))
    
    escalations_tmp = await read_recent(r, ESCALATIONS_STREAM, 1)
    if escalations_tmp: last_times["escalations"] = int(escalations_tmp[0].get("ts_ms", last_times["escalations"]))

    while True:
        started = time.perf_counter()
        status = "ok"
        trigger_type = "none"
        
        try:
            # Poll context streams
            current_data = {
                "applies": await read_recent(r, JOURNAL_STREAM, LOOKBACK_COUNT),
                "validations": await read_recent(r, VERIFICATION_STREAM, LOOKBACK_COUNT),
                "retries": await read_recent(r, RETRY_STREAM, LOOKBACK_COUNT),
                "rollbacks": await read_recent(r, ROLLBACK_STREAM, LOOKBACK_COUNT),
                "escalations": await read_recent(r, ESCALATIONS_STREAM, LOOKBACK_COUNT),
                "slo_rollups": await read_recent(r, SLO_ROLLUPS_STREAM, LOOKBACK_COUNT)
            }
            
            triggers = find_unprocessed_triggers(last_times, current_data)
            
            for t_type, severity, desc, t_msg in triggers:
                trigger_type = t_type
                ts = int(t_msg.get("ts_ms", "0"))
                last_times[t_type + "s"] = max(last_times[t_type + "s"], ts)
                
                bundle = build_bundle(t_type, severity, desc, t_msg, current_data)
                bundle_json = json.dumps(bundle)
                
                await persist_bundle(db_url, bundle)
                
                await r.xadd(BUNDLES_STREAM, {
                    "bundle_id": bundle["bundle_id"],
                    "bundle_json": bundle_json
                }, maxlen=MAXLEN, approximate=True)
                
                await r.xadd(AUDIT_STREAM, {
                    "bundle_id": bundle["bundle_id"],
                    "action": "BUNDLE_BUILT",
                    "trigger_type": t_type,
                    "severity": severity,
                    "ts_ms": str(now_ms())
                }, maxlen=MAXLEN, approximate=True)
                
                if TOTAL_BUNDLES:
                    TOTAL_BUNDLES.labels(severity=severity, trigger_type=t_type).inc()
            
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, trigger_type=trigger_type).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
