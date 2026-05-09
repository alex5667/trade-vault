from typing import Any


class ATRFreezeTelegramSurface:
    """
    Format Freeze and Unfreeze events for the operator/auditor Telegram channel.
    Provides board formatting for 'v_governance_freeze_board'.
    """

    @staticmethod
    def format_freeze_event(event: dict[str, Any], advisory_only: bool) -> str:
        """
        Format a freeze escalation or creation event.
        Expected keys in event: scope_kind, scope_value, freeze_state, trigger_kind, source_reason_code, expires_at, recovery_not_before
        """
        mode_str = " [ADVISORY]" if advisory_only else " [ENFORCE]"
        state = event.get('freeze_state', 'unknown')
        status = event.get('status', 'created')

        header = f"❄️ ATR FREEZE {status.upper()}{mode_str} ❄️"

        allowed, blocked = ATRFreezeTelegramSurface._get_effect_matrix(state)

        lines = [
            header,
            f"Scope:  {event.get('scope_kind')} | {event.get('scope_value')}",
            f"State:  {state}",
            f"Trigger: {event.get('trigger_kind')}",
            f"Reason:  {event.get('source_reason_code')}",
            f"Expires at: {event.get('expires_at')}",
            f"Recovery not before: {event.get('recovery_not_before')}",
            "",
            "Allowed:",
        ]
        for a in allowed:
            lines.append(f"- {a}")

        lines.append("Blocked:")
        for b in blocked:
            lines.append(f"- {b}")

        return "\n".join(lines)

    @staticmethod
    def format_unfreeze_event(event: dict[str, Any]) -> str:
        """
        Format recovering / released events.
        """
        status = event.get('new_status', 'unknown')
        reason_code = event.get('reason_code', 'unknown')

        if status == "recovering":
            header = "🌱 ATR FREEZE RECOVERING 🌱"
            next_step = "Pending cert or staged clip"
        elif status == "released":
            header = "☀️ ATR SCOPE RELEASED ☀️"
            next_step = "Normal operations resumed"
        else:
            header = "⚠️ ATR FREEZE RE-ESCALATED ⚠️"
            next_step = "Health checks failed during recovery window."

        lines = [
            header,
            f"Freeze ID: {event.get('freeze_id')}",
            f"Reason:  {reason_code}",
            "",
            f"Next: {next_step}"
        ]
        return "\n".join(lines)

    @staticmethod
    def format_board(board_rows: list[dict[str, Any]]) -> str:
        """
        Format the v_governance_freeze_board outputs.
        """
        if not board_rows:
            return "✅ Governance Freeze Board: CLEAR (0 active freezes)"

        lines = [f"📊 Governance Freeze Board ({len(board_rows)} Active)"]

        for row in board_rows:
            lines.append(
                f"[{row.get('status', '').upper()}] {row.get('freeze_state')} "
                f"({row.get('scope_kind')}:{row.get('scope_value')}) "
                f"| {row.get('trigger_kind')} -> Dwell: {row.get('recovery_not_before', 'N/A')[:16]}"
            )

        return "\n".join(lines)

    @staticmethod
    def _get_effect_matrix(state: str):
        allowed = ["protective exits"]
        blocked = []

        if state == "normal":
            allowed.extend(["new entries", "rollout advancement", "allocator rebalance", "release approvals"])
        elif state == "clip":
            allowed.extend(["new entries (clipped risk)", "allocator rebalance (limited)"])
            blocked.extend(["rollout advancement", "release approvals for related scope"])
        elif state == "no_new_risk":
            blocked.extend(["new entries", "rollout advancement", "allocator rebalance on scope", "release approvals"])
        elif state == "scope_frozen":
            blocked.extend(["new entries on scope", "rollout advancement on scope", "release approvals on scope"])
        elif state == "venue_frozen":
            blocked.extend(["new entries on venue", "rollout advancement for venue", "release approvals for venue"])
        elif state == "promotions_frozen":
            allowed.extend(["new entries (unchanged)"])
            blocked.extend(["rollout advancement", "release approvals (denied)"])
        elif state == "release_frozen":
            allowed.extend(["new entries (unchanged)"])
            blocked.extend(["rollout advancement (to live)", "release approvals"])
        elif state == "hard_freeze":
            blocked.extend(["new entries", "rollout advancement", "allocator rebalance", "release approvals"])

        return allowed, blocked
