import logging

from services.atr_postmortem_control_service import change_postmortem_status, get_open_actions, verify_action

logger = logging.getLogger("atr_postmortem_telegram_surface")

def handle_postmortem_callback(payload: dict, actor: str) -> dict:
    action = payload.get("action")
    pm_id = payload.get("postmortem_id")

    if not pm_id or not action:
        return {"ok": False, "message": "Missing arguments"}

    logger.info(f"Operator {actor} clicked {action} on postmortem {pm_id}")

    try:
        if action == "mark_in_progress":
            ok = change_postmortem_status(pm_id, "corrective_open", actor)
            return {"ok": ok, "message": "Postmortem is in progress" if ok else "Failed"}
        elif action == "review_pm":
            # Real logic would DM the operator with specifics or set to review
            ok = change_postmortem_status(pm_id, "review", actor)
            return {"ok": ok, "message": "Postmortem is under review" if ok else "Failed"}
        elif action == "attach_verification":
            # Simplification: in real life this would be a multi-step modal or drill response
            actions = get_open_actions(pm_id)
            if actions:
                act = actions[0]
                ok = verify_action(act["action_id"], actor, "manual_operator_review", {"note": "via telegram"})
                if ok:
                    return {"ok": True, "message": f"Action {act['action_id']} verified"}
            return {"ok": False, "message": "No action to verify or failed"}
        elif action == "reopen":
            from services.atr_postmortem_control_service import reopen_postmortem
            ok = reopen_postmortem(pm_id, actor, "Reopened by operator")
            return {"ok": ok, "message": "Postmortem reopened" if ok else "Failed"}
        elif action == "close":
            ok = change_postmortem_status(pm_id, "closed", actor)
            return {"ok": ok, "message": "Postmortem closed" if ok else "Validation failed. Ensure all actions verified."}
        else:
            return {"ok": False, "message": "Unknown action"}

    except Exception as e:
        logger.error(f"Error handling postmortem callback: {e}")
        return {"ok": False, "message": "Internal error"}

def register_telemetry() -> None:
    """No-op: handle_postmortem_callback is imported directly by atr_policy_telegram_callback_worker.
    Kept for interface compatibility."""
    pass

def format_postmortem_alert_html(payload: dict) -> str:
    pm_id = payload.get("postmortem_id", "???")
    status = payload.get("status", "draft")
    sev = payload.get("severity", "SEV-4")

    html = "📋 <b>ATR Postmortem</b>\n\n"
    html += f"<b>ID:</b> <code>{pm_id}</code>\n"
    html += f"<b>Status:</b> {status.upper().replace('_', ' ')}\n"
    html += f"<b>Severity:</b> {sev}\n\n"
    html += "<i>Ensure all corrective actions are assigned and verified.</i>"

    return html

def create_postmortem_inline_keyboard(payload: dict) -> dict:
    pm_id = payload.get("postmortem_id")
    return {
        "inline_keyboard": [
            [  # type: ignore
                {"text": "📖 Review PM", "callback_data": f"postmortem|review_pm|{pm_id}"},
                {"text": "⏳ Mark In Progress", "callback_data": f"postmortem|mark_in_progress|{pm_id}"}
            ]
            [
                {"text": "✅ Attach Verification", "callback_data": f"postmortem|attach_verification|{pm_id}"},
                {"text": "🔄 Reopen", "callback_data": f"postmortem|reopen|{pm_id}"}
            ]
            [
                {"text": "🔒 Close PM", "callback_data": f"postmortem|close|{pm_id}"}
            ]
        ]
    }
