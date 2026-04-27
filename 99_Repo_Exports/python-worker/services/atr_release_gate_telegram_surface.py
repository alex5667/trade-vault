import json
import logging
import os
from typing import Any, Dict
import psycopg2.extras

from services.atr_release_gate_service import build_scorecard, record_release_decision

logger = logging.getLogger("atr_release_gate_telegram")

_GRAPH_GATE_ENABLE = os.getenv("ATR_GRAPH_RELEASE_GATE_ENABLE", "0") == "1"


def render_scorecard_message(change_id: str) -> Dict[str, Any]:
    """Build a standard Telegram message for a release scorecard."""
    try:
        scorecard = build_scorecard(change_id)
    except Exception as e:
        logger.error(f"Failed to build scorecard for rendering {change_id}: {e}")
        return {
            "text": f"⚠️ Error building scorecard for {change_id}: {e}",
            "parse_mode": "HTML"
        }

    decision = scorecard.get("decision", "unknown")
    score = scorecard.get("readiness_score", 0.0)

    icon = "✅" if decision == "allow" else "⚠️" if decision == "allow_with_override" else "🚫"

    msg_lines = [
        f"{icon} <b>ATR Release Readiness</b>\n",
        f"Change: <code>{change_id}</code>",
        f"Decision: <b>{decision.upper()}</b>",
        f"Score: <b>{score:.1f}</b>\n"
    ]

    summary = scorecard.get("summary", {})
    msg_lines.extend([
        f"Replay: {summary.get('replay_status')}",
        f"Rollout cert: {summary.get('rollout_cert_status')}",
        f"Open SEV-1 incidents: {summary.get('incidents_open')}",
        f"Overdue actions: {summary.get('overdue_actions')}\n"
    ])

    if scorecard.get("blockers"):
        msg_lines.append("🛑 <b>Blockers:</b>")
        for b in scorecard["blockers"]:
            msg_lines.append(f" - {b}")
        msg_lines.append("")

    if scorecard.get("warnings"):
        msg_lines.append("⚠️ <b>Warnings:</b>")
        for w in scorecard["warnings"]:
            msg_lines.append(f" - {w}")
        msg_lines.append("")

    # Phase 8.2: surface graph source when active
    if _GRAPH_GATE_ENABLE and scorecard.get("_graph_source"):
        src = scorecard["_graph_source"]
        crit = len(scorecard.get("_critical_drifts", []))
        msg_lines.append(f"🔬 <i>Graph source: {src} | critical drifts: {crit}</i>")

    text = "\n".join(msg_lines)

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "Approve release", "callback_data": f"/release approve {change_id}"},
                {"text": "Deny release",    "callback_data": f"/release deny {change_id}"}
            ],
            [
                {"text": "Override release", "callback_data": f"/release override {change_id}"}
            ],
            [
                {"text": "📊 Graph board",   "callback_data": "/release graph_board"},
                {"text": "🌊 Drift board",   "callback_data": "/release drift_board"},
            ] if _GRAPH_GATE_ENABLE else [],
        ]
    }
    # remove empty rows
    keyboard["inline_keyboard"] = [r for r in keyboard["inline_keyboard"] if r]

    return {
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": json.dumps(keyboard)
    }


def _render_graph_board() -> Dict[str, Any]:
    """Phase 8.2 — render equivalence check board (last 10)."""
    try:
        from services.analytics_db import get_conn
        with get_conn() as conn, conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(
                """
                SELECT change_id, scope_value, legacy_decision,
                       graph_decision, status, created_at
                FROM v_governance_release_graph_board
                LIMIT 10
                """
            )
            rows = cur.fetchall()
    except Exception as exc:
        return {"text": f"⚠️ Error fetching graph board: {exc}", "parse_mode": "HTML"}

    lines = ["📊 <b>ATR Release Graph Board</b> (last 10)\n"]
    for r in rows:
        icon = "✅" if r["status"] == "passed" else "❌"
        lines.append(
            f"{icon} <code>{r['scope_value']}</code> "
            f"legacy=<code>{r['legacy_decision']}</code> "
            f"graph=<code>{r['graph_decision']}</code> "
            f"({r['created_at'].strftime('%H:%M')})"
        )
    if not rows:
        lines.append("No equivalence checks yet. Enable ATR_GRAPH_RELEASE_GATE_ENABLE=1.")

    return {"text": "\n".join(lines), "parse_mode": "HTML"}


def _render_drift_board() -> Dict[str, Any]:
    """Phase 8.2 — render open release drift board."""
    try:
        from services.analytics_db import get_conn
        with get_conn() as conn, conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(
                """
                SELECT change_id, scope_value, drift_kind, severity, created_at
                FROM v_governance_release_drift_board
                LIMIT 15
                """
            )
            rows = cur.fetchall()
    except Exception as exc:
        return {"text": f"⚠️ Error fetching drift board: {exc}", "parse_mode": "HTML"}

    lines = ["🌊 <b>ATR Release Drift Board</b> (open)\n"]
    icons = {"critical": "🔴", "error": "🟠", "warn": "🟡"}
    for r in rows:
        ic = icons.get(r["severity"], "⚪")
        lines.append(
            f"{ic} <code>{r['drift_kind']}</code> | "
            f"scope=<code>{r['scope_value']}</code> | "
            f"{r['created_at'].strftime('%m-%d %H:%M')}"
        )
    if not rows:
        lines.append("✅ No open drifts.")

    return {"text": "\n".join(lines), "parse_mode": "HTML"}


def _render_cutover_status() -> Dict[str, Any]:
    """Phase 8.2 — render current cutover readiness status."""
    try:
        from services.analytics_db import get_conn
        with get_conn() as conn, conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(
                """
                SELECT status, summary_json, created_at
                FROM atr_release_cutover_readiness
                WHERE component = 'release_gate'
                ORDER BY created_at DESC LIMIT 1
                """
            )
            row = cur.fetchone()
    except Exception as exc:
        return {"text": f"⚠️ Error fetching cutover status: {exc}", "parse_mode": "HTML"}

    if not row:
        return {"text": "ℹ️ No cutover readiness record yet.", "parse_mode": "HTML"}

    from services.atr_graph_backed_release_gate import render_cutover_ready
    text = render_cutover_ready(row["status"], row["summary_json"] or {})
    return {"text": text, "parse_mode": "HTML"}


def handle_telegram_callback(callback_query: Dict[str, Any]) -> Dict[str, Any]:
    """Handle /release callback actions from the Operator."""
    data      = callback_query.get("data", "")
    from_user = callback_query.get("from", {}).get("username", "unknown_user")

    parts = data.split(" ")
    if len(parts) < 2:
        return {"text": "Invalid release callback format"}

    action_type = parts[1]

    # ── Phase 8.2 read-only board commands ────────────────────────────────────
    if action_type == "graph_board":
        return _render_graph_board()

    if action_type == "drift_board":
        return _render_drift_board()

    if action_type == "cutover_status":
        return _render_cutover_status()

    if action_type == "run_cert":
        if not _GRAPH_GATE_ENABLE:
            return {"text": "ℹ️ Graph gate disabled (ATR_GRAPH_RELEASE_GATE_ENABLE=0)."}
        from services.atr_release_equivalence_cert_service import ReleaseEquivalenceCertService
        cert = ReleaseEquivalenceCertService.run_cert(window_days=7)
        text = ReleaseEquivalenceCertService.render_telegram(cert)
        return {"text": text, "parse_mode": "HTML"}

    # ── Legacy decision commands ───────────────────────────────────────────────
    if len(parts) < 3:
        return {"text": "Invalid release callback format (missing change_id)"}

    change_id = parts[2]

    try:
        scorecard    = build_scorecard(change_id)
        scorecard_id = scorecard.get("scorecard_id")
    except Exception as e:
        return {"text": f"Error building scorecard for decision: {e}"}

    if action_type == "approve":
        if scorecard.get("decision") != "allow":
            return {
                "text": (
                    f"Cannot purely approve {change_id}. "
                    f"Current decision policy is {scorecard.get('decision')}. Use Override."
                )
            }
        record_release_decision(
            change_id, scorecard_id, from_user, "approve_release", "OPERATOR_APPROVED", scorecard
        )
        return {
            "text": f"✅ Change <code>{change_id}</code> approved by {from_user}",
            "parse_mode": "HTML"
        }

    elif action_type == "deny":
        record_release_decision(
            change_id, scorecard_id, from_user, "deny_release", "OPERATOR_DENIED", scorecard
        )
        return {
            "text": f"🚫 Change <code>{change_id}</code> denied by {from_user}",
            "parse_mode": "HTML"
        }

    elif action_type == "override":
        if scorecard.get("decision") == "deny":
            return {
                "text": (
                    f"Cannot override {change_id}. "
                    f"There are HARD blockers: {scorecard.get('blockers')}"
                )
            }
        record_release_decision(
            change_id, scorecard_id, from_user, "override_release", "OPERATOR_OVERRIDE", scorecard
        )
        return {
            "text": (
                f"⚠️ Change <code>{change_id}</code> "
                f"APPROVED WITH OVERRIDE by {from_user}"
            ),
            "parse_mode": "HTML"
        }

    return {"text": f"Unknown action: {action_type}"}

