import json
import logging
import time
import uuid
from typing import Any, Dict, Optional, List

from prometheus_client import Counter, Histogram
from services.analytics_db import get_conn
import os
import redis

logger = logging.getLogger("atr_incident_control")

def get_redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

try:
    atr_incidents_total = Counter("atr_incidents_total", "Total incidents", ["incident_class", "severity", "status"])
    atr_incident_actions_total = Counter("atr_incident_actions_total", "Total operator actions on incidents", ["action", "incident_class"])
    atr_incident_ack_latency_sec = Histogram("atr_incident_ack_latency_sec", "Latency of acknowledging incidents", ["severity"])
    atr_incident_mitigate_latency_sec = Histogram("atr_incident_mitigate_latency_sec", "Latency to mitigate incidents", ["severity"])
    atr_incident_without_evidence_total = Counter("atr_incident_without_evidence_total", "Incidents closed without evidence pack", [])
    atr_incident_runbook_apply_total = Counter("atr_incident_runbook_apply_total", "Runbooks applied", ["runbook_id"])
except Exception:
    atr_incidents_total = None
    atr_incident_actions_total = None
    atr_incident_ack_latency_sec = None
    atr_incident_mitigate_latency_sec = None
    atr_incident_without_evidence_total = None
    atr_incident_runbook_apply_total = None


# Detector -> incident mapping rule
DETECTOR_TO_INCIDENT_CLASS = {
    "book_stale": "DATA_QUALITY_FREEZE",
    "tick_gap_critical": "DATA_QUALITY_FREEZE",
    "atr_unavailable": "DATA_QUALITY_FREEZE",
    "drift:hard": "FEATURE_DRIFT_FREEZE",
    "negative_ev": "EDGE_COST_SHOCK",
    "spread_too_wide": "EDGE_COST_SHOCK",
    "TRADE_RETCODE_REQUOTE": "VENUE_MT5_DEGRADED",
    "TRADE_RETCODE_CONNECTION": "VENUE_MT5_DOWN",
    "redis_health_ping_failed": "REDIS_BLIND"
}

CLASS_TO_SEVERITY = {
    "REDIS_BLIND": "SEV-1",
    "VENUE_MT5_DOWN": "SEV-1",
    "DATA_QUALITY_FREEZE": "SEV-1",
    "FEATURE_DRIFT_FREEZE": "SEV-2",
    "VENUE_MT5_DEGRADED": "SEV-2",
    "EDGE_COST_SHOCK": "SEV-3"
}

def log_incident_action(cur, incident_id: str, action: str, actor: str, reason_code: str, action_json: dict):
    cur.execute("""
        INSERT INTO atr_incident_actions (incident_id, action, actor, reason_code, action_json)
        VALUES (%s, %s, %s, %s, %s)
    """, (incident_id, action, actor, reason_code, json.dumps(action_json)))
    if atr_incident_actions_total:
        cur.execute("SELECT incident_class FROM atr_incidents WHERE incident_id = %s", (incident_id,))
        cls_row = cur.fetchone()
        if cls_row:
            atr_incident_actions_total.labels(action=action, incident_class=cls_row[0]).inc()

def determine_incident_class_from_detector(reason_code: str) -> str:
    for prefix, cls in DETECTOR_TO_INCIDENT_CLASS.items():
        if reason_code.startswith(prefix):
            return cls
    return "UNKNOWN_INCIDENT"

def open_incident(
    scope_kind: str,
    detected_by: str,
    reason_code: str,
    incident_json: Dict[str, Any],
    source: str = "",
    venue: str = "",
    symbol="",
    scenario: str = "",
    regime: str = "",
    risk_horizon_bucket: str = "",
    layer: str = "",
    policy_ver: int = 0
) -> Optional[str]:
    """Detects and opens a new incident, recording it in the DB and triggering advisory alerts."""
    incident_class = determine_incident_class_from_detector(reason_code)
    severity = CLASS_TO_SEVERITY.get(incident_class, "SEV-4")
    
    incident_id = f"inc_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    now_ms = int(time.time() * 1000)
    status = "OPEN"
    
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Check if there is already an OPEN or MITIGATING incident for this class + scope
            cur.execute("""
                SELECT incident_id FROM atr_incidents 
                WHERE incident_class = %s AND scope_kind = %s AND symbol = %s AND status IN ('OPEN', 'ACKED', 'MITIGATING')
            """, (incident_class, scope_kind, symbol))
            existing = cur.fetchone()
            if existing:
                logger.info(f"Incident {incident_class} for {symbol} already tracked as {existing[0]}")
                return existing[0]

            cur.execute("""
                INSERT INTO atr_incidents (
                    incident_id, incident_class, severity, scope_kind, source, venue, symbol,
                    scenario, regime, risk_horizon_bucket, layer, policy_ver, status, owner, 
                    detected_by, reason_code, incident_json, opened_at_ms, updated_at_ms
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
            """, (
                incident_id, incident_class, severity, scope_kind, source, venue, symbol,
                scenario, regime, risk_horizon_bucket, layer, policy_ver, status, "unassigned",
                detected_by, reason_code, json.dumps(incident_json), now_ms, now_ms
            ))
            
            log_incident_action(cur, incident_id, "SYSTEM_OPEN", detected_by, reason_code, incident_json)
            
            if atr_incidents_total:
                atr_incidents_total.labels(incident_class=incident_class, severity=severity, status=status).inc()

            # Advisory Mode: Write to Redis for Telegram notification, without automated no_new_risk applies yet.
            r = get_redis()
            payload = {
                "incident_id": incident_id,
                "class": incident_class,
                "severity": severity,
                "scope": scope_kind,
                "symbol": symbol,
                "reason": reason_code,
                "ts": now_ms
            }
            r.xadd("notify:telegram", {"channel": "ops", "type": "incident_opened", "payload": json.dumps(payload)})
            
            # Auto-create postmortem for SEV-1 incidents
            if severity == "SEV-1":
                try:
                    from services.atr_postmortem_control_service import create_postmortem_for_incident
                    create_postmortem_for_incident(incident_id, severity, reason_code, f"PM for {incident_class} ({symbol})")
                except Exception as e:
                    logger.error(f"Failed to auto-create postmortem: {e}")

            conn.commit()
            return incident_id
    except Exception as e:
        logger.error(f"Failed to open incident {incident_id}: {e}")
        return None

def change_status(incident_id: str, new_status: str, actor: str, reason_code: str, action_json: dict) -> bool:
    now_ms = int(time.time() * 1000)
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT status, severity, opened_at_ms FROM atr_incidents WHERE incident_id = %s FOR UPDATE", (incident_id,))
            row = cur.fetchone()
            if not row:
                return False
            
            old_status = row["status"]
            # Enforce state machine simplified: OPEN -> ACKED -> MITIGATING -> STABILIZED -> RECOVERING -> RECOVERED -> CLOSED
            valid_forward = {
                "OPEN": ["ACKED"],
                "ACKED": ["MITIGATING", "STABILIZED"],
                "MITIGATING": ["STABILIZED"],
                "STABILIZED": ["RECOVERING"],
                "RECOVERING": ["RECOVERED"],
                "RECOVERED": ["CLOSED"]
            }
            
            if new_status not in valid_forward.get(old_status, []):
                logger.warning(f"Invalid transition from {old_status} to {new_status} for {incident_id}")
                return False

            if new_status == "ACKED" and atr_incident_ack_latency_sec:
                latency = (now_ms - row["opened_at_ms"]) / 1000.0
                atr_incident_ack_latency_sec.labels(severity=row["severity"]).observe(latency)
                
            if new_status == "MITIGATING" and atr_incident_mitigate_latency_sec:
                latency = (now_ms - row["opened_at_ms"]) / 1000.0
                atr_incident_mitigate_latency_sec.labels(severity=row["severity"]).observe(latency)

            fields_to_update = {"status": new_status, "updated_at_ms": now_ms}
            if new_status == "ACKED":
                fields_to_update["owner"] = actor
            if new_status == "CLOSED":
                fields_to_update["closed_at_ms"] = now_ms

            set_clause = ", ".join([f"{k} = %s" for k in fields_to_update.keys()])
            values = list(fields_to_update.values()) + [incident_id]
            
            cur.execute(f"UPDATE atr_incidents SET {set_clause} WHERE incident_id = %s", values)
            
            log_incident_action(cur, incident_id, new_status, actor, reason_code, action_json)
            
            # Send status update to Telegram
            r = get_redis()
            payload = {"incident_id": incident_id, "status": new_status, "actor": actor}
            r.xadd("notify:telegram", {"channel": "ops", "type": "incident_status_change", "payload": json.dumps(payload)})
            
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to change status for {incident_id}: {e}")
        return False

def ack_incident(incident_id: str, actor: str) -> bool:
    return change_status(incident_id, "ACKED", actor, "OPERATOR_ACK", {"actor": actor})

def apply_runbook_action(incident_id: str, actor: str) -> bool:
    """Read runbook config from DB and apply immediate mitigation."""
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT incident_class FROM atr_incidents WHERE incident_id = %s", (incident_id,))
            inc = cur.fetchone()
            if not inc: return False
            
            cls = inc["incident_class"]
            cur.execute("SELECT runbook_id, runbook_json FROM atr_incident_runbooks WHERE incident_class = %s AND is_current = true LIMIT 1", (cls,))
            rb = cur.fetchone()
            if not rb:
                logger.warning(f"No runbook found for {cls}")
                return False
                
            manifest = rb["runbook_json"]
            r = get_redis()
            
            # Mock executing actions
            actions = manifest.get("immediate_actions", [])
            for action in actions:
                logger.info(f"Executing: {action}")
                # Real logic would translate this to redis states 
                # (e.g. `cfg:kill_switch...` or `cfg:degrade_state...`)
                # Advisory mode active -> log only for now.
                logger.info(f"[ADVISORY MODE] Would apply runbook logic: {action}")
            
            if atr_incident_runbook_apply_total:
                atr_incident_runbook_apply_total.labels(runbook_id=rb["runbook_id"]).inc()

            # Record transition
            return change_status(incident_id, "MITIGATING", actor, "RUNBOOK_APPLIED", {"runbook_id": rb["runbook_id"]})
    except Exception as e:
        logger.error(f"Failed to apply runbook for {incident_id}: {e}")
        return False

def attach_evidence_pack(incident_id: str, actor: str, evidence: dict) -> bool:
    # Requires RECOVERED state to close.
    now_ms = int(time.time() * 1000)
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT status FROM atr_incidents WHERE incident_id = %s FOR UPDATE", (incident_id,))
            st = cur.fetchone()
            if not st or st["status"] != "RECOVERED":
                return False

            evidence_id = f"ev_{incident_id}_{now_ms}"
            cur.execute("""
                INSERT INTO atr_incident_evidence_packs (evidence_id, incident_id, evidence_json)
                VALUES (%s, %s, %s)
            """, (evidence_id, incident_id, json.dumps(evidence)))

            change_status(incident_id, "CLOSED", actor, "EVIDENCE_ATTACHED", {"evidence_id": evidence_id})
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to attach evidence {incident_id}: {e}")
        return False

def get_open_incidents() -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM atr_incidents WHERE status != 'CLOSED' ORDER BY updated_at_ms DESC")
        return cur.fetchall()
