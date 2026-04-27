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

APP_NAME = "route_incident_rca_mirror_incident_bundle_builder_v3_11"
JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rollout_journal",
)
ESCALATIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ESCALATIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_escalations",
)
VERIFICATION_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_STREAM",
    "stream:ml:route_incident_rca_mirror_verification_results",
)
RETRY_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_STREAM",
    "stream:ml:route_incident_rca_mirror_retry_results",
)

OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_BUNDLES_STREAM",
    "stream:ml:route_incident_rca_mirror_incident_bundles",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_BUNDLES_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_incident_bundles_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_BUNDLES_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_incident_bundles:last",
)

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_BUNDLES_PORT", "9929"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_BUNDLES_MAXLEN", "1000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_BUNDLES_LOOKBACK_COUNT", "50"))
POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_incident_bundles_runs_total", "Bundle builder runs", ("status", "trigger_type"))
TOTAL = _counter("ml_route_incident_rca_mirror_incident_bundles_total", "Bundles generated", ("severity", "trigger_type"))
LAT = _hist("ml_route_incident_rca_mirror_incident_bundles_latency_seconds", "Bundle builder latency")
UP = _gauge("ml_route_incident_rca_mirror_incident_bundles_up", "Bundle builder up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_incident_bundles_last_run_ts_seconds", "Bundle builder last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def fetch_history(r: Any, stream: str, count: int) -> List[Dict[str, Any]]:
    # Extract last COUNT elements from stream
    entries = await r.xrevrange(stream, "+", "-", count=count)
    return [decode_dict(fields) for msg_id, fields in entries]

def normalize_trigger(event_dict: Dict[str, Any], event_id: str) -> Dict[str, Any]:
    # It can be a journal event (transition_type) or escalation event (severity)
    if "transition_type" in event_dict:
        severity = "info"
        if event_dict.get("transition_type") == "MIRROR_TO_AUDIT":
            severity = "warning"
        return {
            "trigger_source": "journal",
            "trigger_type": event_dict.get("transition_type"),
            "event_id": event_id,
            "severity": severity,
            "raw": event_dict,
        }
    elif "severity" in event_dict:
        severity = event_dict.get("severity", "info")
        return {
            "trigger_source": "escalation",
            "trigger_type": f"ESCALATION_{severity.upper()}",
            "event_id": event_id,
            "severity": severity,
            "raw": event_dict,
        }
    return {}

async def build_bundle(r: Any, trigger_ctx: Dict[str, Any]) -> Dict[str, Any]:
    verification = await fetch_history(r, VERIFICATION_STREAM, LOOKBACK_COUNT)
    retry = await fetch_history(r, RETRY_STREAM, LOOKBACK_COUNT)
    escalations = await fetch_history(r, ESCALATIONS_STREAM, LOOKBACK_COUNT)
    journal = await fetch_history(r, JOURNAL_STREAM, LOOKBACK_COUNT)
    
    return {
        "bundle_id": str(uuid.uuid4()),
        "contour": "route_incident_rca_mirror",
        "trigger": trigger_ctx,
        "summary_counts": {
            "verification": len(verification),
            "retry": len(retry),
            "escalations": len(escalations),
            "journal": len(journal),
        },
        "evidence_slices": {
            "verification": verification,
            "retry": retry,
            "escalations": escalations,
            "journal": journal,
        },
        "ts_ms": now_ms(),
    }

async def persist_bundle(db_url: str, bundle: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_incident_bundles (
                    bundle_id, ts_ms, trigger_source, trigger_type, severity, bundle_json
                ) VALUES (
                    %(bundle_id)s, %(ts_ms)s, %(trigger_source)s, %(trigger_type)s, %(severity)s, %(bundle_json)s
                )
                """,
                {
                    "bundle_id": bundle["bundle_id"],
                    "ts_ms": bundle["ts_ms"],
                    "trigger_source": bundle["trigger"]["trigger_source"],
                    "trigger_type": bundle["trigger"]["trigger_type"],
                    "severity": bundle["trigger"]["severity"],
                    "bundle_json": json.dumps(bundle, ensure_ascii=False),
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
    
    last_journal_id = "$"
    last_escalation_id = "$"
    
    while True:
        started = time.perf_counter()
        status = "ok"
        trigger_type = "none"
        
        try:
            streams = {
                JOURNAL_STREAM: last_journal_id,
                ESCALATIONS_STREAM: last_escalation_id,
            }
            results = await r.xread(streams, count=5, block=int(POLL_INTERVAL_SEC*1000))
            if results:
                for stream_name, events in results:
                    s_name = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                    for msg_id, fields in events:
                        m_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        decoded = decode_dict(fields)
                        
                        trigger = normalize_trigger(decoded, m_id)
                        if trigger:
                            trigger_type = trigger["trigger_type"]
                            if trigger_type in ("AUDIT_TO_MIRROR", "MIRROR_TO_AUDIT", "ESCALATION_WARNING", "ESCALATION_CRITICAL"):
                                bundle = await build_bundle(r, trigger)
                                await persist_bundle(db_url, bundle)
                                b_json = json.dumps(bundle, separators=(",",":"), ensure_ascii=False)
                                
                                await r.xadd(OUTPUT_STREAM, {"bundle_id": bundle["bundle_id"], "severity": trigger["severity"], "bundle_json": b_json}, maxlen=MAXLEN, approximate=True)
                                await r.xadd(AUDIT_STREAM, {"event_type": "BUNDLE_GENERATED", "bundle_id": bundle["bundle_id"], "trigger_type": trigger["trigger_type"]}, maxlen=MAXLEN, approximate=True)
                                await r.hset(LAST_HASH, mapping={"bundle_id": bundle["bundle_id"], "ts_ms": str(now_ms())})
                                
                                if TOTAL:
                                    TOTAL.labels(severity=trigger["severity"], trigger_type=trigger_type).inc()
                        
                        if s_name == JOURNAL_STREAM:
                            last_journal_id = m_id
                        elif s_name == ESCALATIONS_STREAM:
                            last_escalation_id = m_id
                            
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "BUNDLE_GENERATION_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
            await asyncio.sleep(2)
        finally:
            if RUNS:
                RUNS.labels(status=status, trigger_type=trigger_type).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
