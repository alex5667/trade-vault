import json
import logging
from typing import Any

import psycopg2.extras

from services.analytics_db import get_conn
from services.atr_go_live_readiness_service import REQUIRED_ROLES, ATRGoLiveReadinessService

logger = logging.getLogger("atr_go_live_telegram")

def render_golive_package_message(package_id: str) -> dict[str, Any]:
    """Build a detailed Telegram message for a Go-Live Readiness package."""
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. Fetch Package
            cur.execute("SELECT * FROM atr_go_live_readiness_packages WHERE package_id = %s", (package_id,))
            pkg = cur.fetchone()
            if not pkg:
                return {"text": f"❌ Package {package_id} not found."}

            # 2. Fetch Domain Checks
            cur.execute("SELECT domain, status, severity FROM atr_go_live_readiness_checks WHERE package_id = %s", (package_id,))
            checks = cur.fetchall()

            # 3. Fetch Signoffs
            cur.execute("SELECT signer_role, signer, status FROM atr_go_live_signoffs WHERE package_id = %s", (package_id,))
            signoffs = {s["signer_role"]: s for s in cur.fetchall()}

    except Exception as e:
        logger.error(f"Failed to fetch golive package {package_id}: {e}")
        return {"text": f"⚠️ Error fetching package {package_id}: {e}"}

    verdict = pkg["verdict"]
    status = pkg["package_status"]

    icon = "✅" if verdict == "GO_LIVE" else "🟡" if verdict == "GO_LIVE_WITH_CONSTRAINTS" else "🚫"
    if status == "draft": icon = "📝"

    msg_lines = [
        f"{icon} <b>ATR Go-Live Ceremony</b>",
        f"Package: <code>{package_id}</code>",
        f"Scope: <b>{pkg['target_scope']}</b>",
        f"Status: <b>{status.upper()}</b>",
        f"Current Verdict: <b>{verdict}</b>\n",
        "🛡️ <b>Domain Readiness:</b>"
    ]

    # Render Domains
    domain_icons = {"passed": "✅", "failed": "❌", "warning": "⚠️", "skipped": "⚪"}
    for d in ["signal_and_gates", "dispatch_and_runtime", "execution", "protective_lifecycle", "control_plane_governance", "dr_replay_archive"]:
        check = next((c for c in checks if c["domain"] == d), {"status": "skipped"})
        msg_lines.append(f" {domain_icons.get(check['status'], '⚪')} {d.replace('_', ' ').title()}")

    msg_lines.append("\n✍️ <b>Sign-off Matrix:</b>")
    for role in REQUIRED_ROLES:
        s = signoffs.get(role)
        if s:
            s_icon = "✅" if s["status"] == "approved" else "🚫"
            msg_lines.append(f" {s_icon} {role.replace('_', ' ').title()}: <i>@{s['signer']}</i>")
        else:
            msg_lines.append(f" ⏳ {role.replace('_', ' ').title()}: <b>PENDING</b>")

    if pkg.get("expires_at"):
        msg_lines.append(f"\n⏰ Expires: {pkg['expires_at'].strftime('%Y-%m-%d %H:%M')}")

    text = "\n".join(msg_lines)

    # Keyboard for signoffs
    inline_keyboard = []
    # Two signoff buttons per row
    for i in range(0, len(REQUIRED_ROLES), 2):
        row = []
        for role in REQUIRED_ROLES[i:i+2]:
            if role not in signoffs:
                row.append({"text": f"Sign as {role.split('_')[0]}", "callback_data": f"golive:sign:{package_id}:{role}"})
        if row:
            inline_keyboard.append(row)

    # Control buttons
    inline_keyboard.append([
        {"text": "🔄 Refresh", "callback_data": f"golive:refresh:{package_id}:none"},
        {"text": "🏁 Finalize", "callback_data": f"golive:finalize:{package_id}:none"}
    ])

    return {
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": json.dumps({"inline_keyboard": inline_keyboard})
    }

def handle_golive_callback(callback_query: dict[str, Any]) -> dict[str, Any]:
    """Handle /golive callback actions."""
    data = callback_query.get("data", "")
    from_user = callback_query.get("from", {}).get("username", "unknown_user")

    parts = data.split(" ")
    if len(parts) < 3:
        return {"text": "Invalid golive callback format"}

    action = parts[1]
    package_id = parts[2]

    svc = ATRGoLiveReadinessService()

    if action == "sign":
        if len(parts) < 4: return {"text": "Missing role"}
        role = parts[3]
        try:
            with get_conn() as conn:
                # 1. Record signoff
                signoffs = {role: {"status": "approved", "signer": from_user}}
                svc.request_signoffs(conn, package_id, signoffs)
                # 2. Return updated message
                return render_golive_package_message(package_id)
        except Exception as e:
            return {"text": f"❌ Sign-off failed: {e}"}

    elif action == "refresh":
        return render_golive_package_message(package_id)

    elif action == "finalize":
        try:
            with get_conn() as conn:
                verdict = svc.compute_final_go_live_verdict(conn, package_id)
                msg = render_golive_package_message(package_id)
                msg["text"] += f"\n\n🏁 <b>Finalized Verdict: {verdict}</b>"
                return msg
        except Exception as e:
            return {"text": f"❌ Finalization failed: {e}"}

    return {"text": f"Unknown action: {action}"}
