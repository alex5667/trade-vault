import json
import logging
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_control_plane_telegram")

def render_auditor_graph_dashboard() -> dict[str, Any]:
    """
    Renders the unified control-plane graph explorer dashboard.
    """
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            # Stats
            cur.execute("SELECT count(*) as c FROM atr_control_plane_nodes")
            total_nodes = cur.fetchone()["c"]

            cur.execute("SELECT count(*) as c FROM v_control_plane_blockers")
            active_blockers = cur.fetchone()["c"]

            # Illegal transitions in last 24h
            cur.execute("SELECT count(*) as c FROM atr_illegal_transition_attempts WHERE created_at > now() - interval '24 hours'")
            illegal_last_24h = cur.fetchone()["c"]

            # Projection drift
            # Example heuristic: Nodes updated in last 5m but projection lag > 1s (Simulated here)
            # Real drift involves reading from Redis, which is done by Prometheus,
            # so we'll just show active live nodes

            cur.execute("SELECT count(*) as c FROM v_control_plane_active_nodes WHERE node_type = 'RolloutState' AND scope_value LIKE '%%live%%'")
            live_nodes = cur.fetchone()["c"]

    except Exception as e:
        logger.error(f"Failed to fetch auditor graph dashboard stats: {e}")
        return {
            "text": f"⚠️ Error rendering control-plane graph stats: {e}",
            "parse_mode": "HTML",
        }

    msg_lines = [
        "🌐 <b>ATR Control-Plane Graph Dashboard</b>\n",
        f"Total Graph Nodes: <code>{total_nodes}</code>",
        f"Active Blockers/Overrides: <code>{active_blockers}</code>",
        f"Active Live Scopes: <code>{live_nodes}</code>",
        f"Illegal Transitions (24h): <code>{illegal_last_24h}</code>",
        "\n✅ Source: Event-Journal <code>atr_control_plane_events</code>"
    ]

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "Refresh", "callback_data": "/cpgraph refresh"},
                {"text": "List Blockers", "callback_data": "/cpgraph blockers"}
            ],
            [
                {"text": "Run Cert Check", "callback_data": "/cpgraph cert_check"}
            ]
        ]
    }

    return {
        "text": "\n".join(msg_lines),
        "parse_mode": "HTML",
        "reply_markup": json.dumps(keyboard)
    }

def handle_telegram_callback(callback_query: dict[str, Any]) -> dict[str, Any]:
    """
    Handle /cpgraph callback actions.
    """
    data = callback_query.get("data", "")
    actor = callback_query.get("from", {}).get("username", "unknown_user")

    parts = data.split(" ")
    if len(parts) < 2:
        return render_auditor_graph_dashboard()

    action_type = parts[1]

    if action_type == "refresh":
        return render_auditor_graph_dashboard()

    elif action_type == "blockers":
        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM v_control_plane_blockers LIMIT 10")
                rows = cur.fetchall()

            text = "🛑 <b>Active Graph Blockers (Top 10)</b>\n"
            for r in rows:
                text += f"- {r['edge_type']} from {r['from_node_id']} to {r['to_node_id']}\n"
            if not rows:
                text += "\nNo active blockers."

            return {"text": text, "parse_mode": "HTML"}
        except Exception as e:
            return {"text": f"Error fetching blockers: {e}"}

    elif action_type == "cert_check":
        from services.atr_control_plane_cert_service import ControlPlaneCertService
        res = ControlPlaneCertService.check_graph_consistency()
        if res.get("status") == "passed":
            return {"text": "✅ <b>Graph Consistency Check: PASSED</b>", "parse_mode": "HTML"}
        else:
            issues = res.get("issues", [])
            text = "⚠️ <b>Graph Consistency Check: FAILED</b>\n\nIssues:\n" + "\n".join(issues)
            return {"text": text, "parse_mode": "HTML"}

    return {"text": f"Unknown callback: {action_type}"}
