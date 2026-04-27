import json
import logging
import os
import redis
try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None
from services.atr_incident_control_service import ack_incident, apply_runbook_action, change_status

logger = logging.getLogger("atr_incident_telegram")

def get_redis():
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def handle_incident_callback(callback_data: dict, actor: str) -> dict:
    """
    Called by atr_policy_telegram_callback_worker.py when an ink button is clicked
    payload expected: {"action": "ack_incident" | "apply_runbook" | "mark_recovered", "incident_id": "..."}
    """
    action = callback_data.get("action")
    incident_id = callback_data.get("incident_id")
    
    if not incident_id or not action:
        return {"ok": False, "message": "Missing incident_id or action"}
        
    logger.info(f"Operator {actor} clicked {action} on incident {incident_id}")
    
    try:
        if action == "ack_incident":
            success = ack_incident(incident_id, actor)
            if success:
                return {"ok": True, "message": f"Incident {incident_id} acknowledged by {actor}"}
            else:
                return {"ok": False, "message": f"Could not ACK incident {incident_id}. Wrong state?"}
                
        elif action == "apply_runbook":
            success = apply_runbook_action(incident_id, actor)
            if success:
                return {"ok": True, "message": f"Runbook applied for {incident_id} by {actor}"}
            else:
                return {"ok": False, "message": "Could not apply runbook. Please check logs."}
                
        elif action == "mark_stabilized":
            success = change_status(incident_id, "STABILIZED", actor, "OPERATOR_STABILIZED", {})
            return {"ok": success, "message": "Incident moved to STABILIZED" if success else "Failed to stabilize"}
            
        elif action == "mark_recovered":
            success = change_status(incident_id, "RECOVERED", actor, "OPERATOR_RECOVERED", {})
            return {"ok": success, "message": "Incident moved to RECOVERED" if success else "Failed to recover"}
            
        else:
            return {"ok": False, "message": f"Unknown action: {action}"}
            
    except Exception as e:
        logger.error(f"Error handling incident callback: {e}")
        return {"ok": False, "message": "Internal error processing incident callback"}

def register_telemetry() -> None:
    """No-op: handle_incident_callback is imported directly by atr_policy_telegram_callback_worker.
    Kept for interface compatibility."""
    pass

def format_incident_alert_html(payload: dict) -> str:
    incident_id = payload.get("incident_id", "???")
    cls = payload.get("class", "UNKNOWN")
    sev = payload.get("severity", "SEV-4")
    scope = payload.get("scope", "global")
    sym = payload.get("symbol", "")
    reason = payload.get("reason", "unknown")
    
    html = f"🚨 <b>{sev} INCIDENT: {cls}</b>\n\n"
    html += f"<b>ID:</b> <code>{incident_id}</code>\n"
    html += f"<b>Scope:</b> {scope} " + (f"({sym})" if sym else "") + "\n"
    html += f"<b>Detector:</b> <code>{reason}</code>\n\n"
    html += "<i>Advisory Mode: Automatic runbooks are DISABLED. Operator action required.</i>"
    
    return html

def create_incident_inline_keyboard(payload: dict) -> dict:
    inc_id = payload.get("incident_id")
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Acknowledge", "callback_data": f"incident|ack_incident|{inc_id}"},
                {"text": "🛠 Apply Runbook Override", "callback_data": f"incident|apply_runbook|{inc_id}"}
            ],
            [
                {"text": "⏸ Pause Rollout", "callback_data": f"rollout|pause"},
                {"text": "🛑 Immediate Freeze", "callback_data": f"incident|emergency_freeze|{inc_id}"}
            ]
        ]
    }
