from typing import Any


def render_window_open_message(window_kind: str, target_scope: str, checks: dict[str, Any]) -> dict[str, Any]:
    """Render Telegram message for an opened release window."""

    def check_status(check_group: dict[str, Any]) -> str:
        # A simple mock representation of 'green' status logic.
        return "green" if check_group else "unknown"

    control_plane_status = check_status(checks.get("control_plane", {}))
    signal_gates_status = check_status(checks.get("signal_gates", {}))
    execution_status = check_status(checks.get("execution", {}))
    protective_status = check_status(checks.get("protective", {}))

    rollback_ready = checks.get("rollback_ready", {}).get("rollback_bundle_prepared", False)
    rollback_status = "ready" if rollback_ready else "not ready"

    msg_lines = [
        "<b>ATR Release Window</b>",
        "",
        f"<b>Window:</b> {window_kind}",
        f"<b>Scope:</b> {target_scope}",
        "<b>Status:</b> OPEN",
        "",
        "<b>Checklist:</b>",
        f"- control-plane: {control_plane_status}",
        f"- signal/gates: {signal_gates_status}",
        f"- execution: {execution_status}",
        f"- protective: {protective_status}",
        f"- rollback bundle: {rollback_status}"
    ]

    return {
        "text": "\n".join(msg_lines),
        "parse_mode": "HTML"
    }


def render_window_blocked_message(change_id: str, change_class: str, blockers: list[str]) -> dict[str, Any]:
    """Render Telegram message for a blocked release window."""
    msg_lines = [
        "<b>ATR Release Window Blocked</b>",
        "",
        f"<b>Change:</b> {change_id}",
        f"<b>Class:</b> {change_class}",
        "<b>Status:</b> BLOCKED",
        "",
        "<b>Blockers:</b>"
    ]
    for blocker in blockers:
        msg_lines.append(f"- {blocker}")

    return {
        "text": "\n".join(msg_lines),
        "parse_mode": "HTML"
    }


def render_checklist_approved_message(change_id: str, change_class: str, signoffs: list[str]) -> dict[str, Any]:
    """Render Telegram message for an approved checklist."""
    msg_lines = [
        "<b>ATR Pre-Release Checklist Approved</b>",
        "",
        f"<b>Change:</b> {change_id}",
        f"<b>Class:</b> {change_class}",
        "<b>Signoffs:</b>"
    ]
    for signoff in signoffs:
        msg_lines.append(f"- {signoff}")

    return {
        "text": "\n".join(msg_lines),
        "parse_mode": "HTML"
    }
