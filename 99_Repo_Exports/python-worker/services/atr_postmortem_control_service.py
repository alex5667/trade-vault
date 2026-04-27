import json
import logging
import time
import uuid
import os
import redis
from typing import Dict, Any, Optional, List

from prometheus_client import Counter, Histogram
from services.analytics_db import get_conn
from services.atr_failure_signature_service import detect_and_record_signature

logger = logging.getLogger("atr_postmortem_control")

def get_redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

try:
    atr_postmortems_total = Counter("atr_postmortems_total", "Total postmortems", ["severity", "status", "root_cause_class"])
    atr_corrective_actions_total = Counter("atr_corrective_actions_total", "Total corrective actions", ["status", "priority", "action_type"])
    atr_corrective_actions_overdue_total = Counter("atr_corrective_actions_overdue_total", "Overdue corrective actions", ["priority"])
    atr_postmortem_verification_total = Counter("atr_postmortem_verification_total", "Verifications", ["status", "verification_kind"])
    atr_postmortem_reopened_total = Counter("atr_postmortem_reopened_total", "Postmortems reopened", [])
except Exception:
    atr_postmortems_total = None
    atr_corrective_actions_total = None
    atr_corrective_actions_overdue_total = None
    atr_postmortem_verification_total = None
    atr_postmortem_reopened_total = None


def create_postmortem_for_incident(incident_id: str, severity: str, reason_code: str, title: str) -> Optional[str]:
    """Creates a draft postmortem automatically for SEV-1 incidents or repeated SEV-2s."""
    pm_id = f"pm_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    now_ms = int(time.time() * 1000)
    
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Check if PM already exists for this incident
            cur.execute("SELECT postmortem_id FROM atr_postmortems WHERE incident_id = %s", (incident_id,))
            if cur.fetchone():
                return None
                
            cur.execute("""
                INSERT INTO atr_postmortems (
                    postmortem_id, incident_id, severity, status, title, owner, facilitator,
                    root_cause_class, reason_code, summary_json, timeline_json, impact_json, 
                    contributing_factors_json, created_at_ms, updated_at_ms
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                pm_id, incident_id, severity, "draft", title, "unassigned", "unassigned",
                "pending", reason_code, json.dumps({}), json.dumps({}), json.dumps({}),
                json.dumps({}), now_ms, now_ms
            ))
            
            if atr_postmortems_total:
                atr_postmortems_total.labels(severity=severity, status="draft", root_cause_class="pending").inc()
            
            conn.commit()
            
            # Notify Telegram
            r = get_redis()
            payload = {
                "postmortem_id": pm_id,
                "incident_id": incident_id,
                "severity": severity,
                "status": "draft",
                "reason_code": reason_code
            }
            r.xadd("notify:telegram", {"channel": "ops", "type": "postmortem_required", "payload": json.dumps(payload)})
            
            return pm_id
    except Exception as e:
        logger.error(f"Failed to create postmortem for incident {incident_id}: {e}")
        return None

def change_postmortem_status(pm_id: str, new_status: str, actor: str) -> bool:
    """Transitions postmortem through draft -> review -> approved -> corrective_open -> verified -> closed"""
    now_ms = int(time.time() * 1000)
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT status, severity, root_cause_class FROM atr_postmortems WHERE postmortem_id = %s FOR UPDATE", (pm_id,))
            row = cur.fetchone()
            if not row:
                return False
                
            old_status = row["status"]
            # Validate transition logic
            valid_transitions = {
                "draft": ["review"],
                "review": ["approved", "draft"],
                "approved": ["corrective_open"],
                "corrective_open": ["verified"],
                "verified": ["closed"],
                "closed": ["reopened"],
                "reopened": ["corrective_open", "review"]
            }
            
            if new_status not in valid_transitions.get(old_status, []):
                logger.warning(f"Invalid transition from {old_status} to {new_status} for {pm_id}")
                return False
                
            # Cannot close if there are pending verifications or open corrective actions
            if new_status == "closed" or new_status == "verified":
                cur.execute("SELECT COUNT(*) as open_actions FROM atr_corrective_actions WHERE postmortem_id = %s AND status NOT IN ('verified', 'dropped')", (pm_id,))
                open_acts = cur.fetchone()
                if open_acts and open_acts["open_actions"] > 0:
                    logger.warning(f"Cannot transition to {new_status} for {pm_id} with {open_acts['open_actions']} open actions")
                    return False
            
            fields_to_update = {"status": new_status, "updated_at_ms": now_ms}
            if new_status == "closed":
                fields_to_update["closed_at_ms"] = now_ms
                
            set_clause = ", ".join([f"{k} = %s" for k in fields_to_update.keys()])
            values = list(fields_to_update.values()) + [pm_id]
            cur.execute(f"UPDATE atr_postmortems SET {set_clause} WHERE postmortem_id = %s", values)
            
            if atr_postmortems_total:
                atr_postmortems_total.labels(severity=row["severity"], status=new_status, root_cause_class=row["root_cause_class"]).inc()
                
            conn.commit()
            
            # Notify Telegram
            r = get_redis()
            payload = {
                "postmortem_id": pm_id,
                "status": new_status,
                "actor": actor
            }
            r.xadd("notify:telegram", {"channel": "ops", "type": "postmortem_status_change", "payload": json.dumps(payload)})
            
            return True
            
    except Exception as e:
        logger.error(f"Failed to change status for postmortem {pm_id}: {e}")
        return False

def create_corrective_action(pm_id: str, action_type: str, priority: str, title: str, reason_code: str, due_at_ms: int, owner: str, payload_json: dict) -> Optional[str]:
    action_id = f"act_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    severity = "SEV-2" # Match PM severity typically
    
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT severity FROM atr_postmortems WHERE postmortem_id = %s", (pm_id,))
            pm_row = cur.fetchone()
            if pm_row:
                severity = pm_row[0]
                
            cur.execute("""
                INSERT INTO atr_corrective_actions (
                    action_id, postmortem_id, action_type, severity, owner, status, priority, title, reason_code, due_at_ms, verification_required, action_json
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (action_id, pm_id, action_type, severity, owner, "open", priority, title, reason_code, due_at_ms, True, json.dumps(payload_json)))
            
            # Change PM to corrective_open automatically if it's approved
            cur.execute("UPDATE atr_postmortems SET status = 'corrective_open' WHERE postmortem_id = %s AND status = 'approved'", (pm_id,))
            
            if atr_corrective_actions_total:
                atr_corrective_actions_total.labels(status="open", priority=priority, action_type=action_type).inc()
            conn.commit()
            
            return action_id
    except Exception as e:
        logger.error(f"Failed to create corrective action for {pm_id}: {e}")
        return None

def verify_action(action_id: str, actor: str, verification_kind: str, payload: dict) -> bool:
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT postmortem_id FROM atr_corrective_actions WHERE action_id = %s AND status != 'verified' FOR UPDATE", (action_id,))
            act = cur.fetchone()
            if not act:
                return False
                
            now_ms = int(time.time() * 1000)
            pm_id = act["postmortem_id"]
            vid = f"ver_{int(time.time())}_{uuid.uuid4().hex[:6]}"
            
            cur.execute("""
                INSERT INTO atr_postmortem_verifications (verification_id, postmortem_id, action_id, verification_kind, status, summary_json)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (vid, pm_id, action_id, verification_kind, "passed", json.dumps(payload)))
            
            cur.execute("""
                UPDATE atr_corrective_actions SET status = 'verified', completed_at_ms = %s, verification_json = %s WHERE action_id = %s
            """, (now_ms, json.dumps(payload), action_id))
            
            if atr_postmortem_verification_total:
                atr_postmortem_verification_total.labels(status="passed", verification_kind=verification_kind).inc()
            
            conn.commit()
            return True
            
    except Exception as e:
        logger.error(f"Failed to verify action {action_id}: {e}")
        return False

def reopen_postmortem(pm_id: str, actor: str, reason: str) -> bool:
    if atr_postmortem_reopened_total:
        atr_postmortem_reopened_total.inc()
    return change_postmortem_status(pm_id, "reopened", actor)

def get_postmortem(pm_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM atr_postmortems WHERE postmortem_id = %s", (pm_id,))
        return cur.fetchone()

def get_open_actions(pm_id: str) -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM atr_corrective_actions WHERE postmortem_id = %s AND status NOT IN ('verified', 'dropped')", (pm_id,))
        return cur.fetchall()

def get_active_postmortems() -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM atr_postmortems WHERE status NOT IN ('closed') ORDER BY updated_at_ms DESC")
        return cur.fetchall()
