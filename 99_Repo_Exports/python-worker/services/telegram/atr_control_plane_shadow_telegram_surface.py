import json
from typing import Dict, Any, List

class ControlPlaneShadowTelegramSurface:
    """
    Formats messages for the Telegram auditor, informing operators of the 
    Phase 8.1 shadow graph status (drifts, consistency certs, cutover readiness).
    """

    @staticmethod
    def format_drift_alert(drifts: List[Dict[str, Any]], scope: str) -> str:
        """
        Formats an alert when the projection cert service detects state or projection drift.
        """
        lines = [
            "⚠️ <b>[SHADOW GRAPH DRIFT DETECTED]</b> ⚠️",
            f"<b>Scope:</b> <code>{scope}</code>",
            "",
            "The Control Plane Shadow Graph has drifted from the Legacy State.",
            "This suggests the legacy controllers mutating state are not perfectly bound to graph events.",
            ""
        ]
        
        for idx, d in enumerate(drifts):
            lines.append(f"<b>Drift {idx+1}: {d.get('drift_type')}</b>")
            lines.append(f"  Field: <code>{d.get('field')}</code>")
            lines.append(f"  Legacy: <code>{d.get('legacy_val')}</code>")
            lines.append(f"  Graph:  <code>{d.get('shadow_val')}</code>")
            lines.append("")
            
        lines.append("<i>Action required: review phase-service emission hooks.</i>")
        lines.append("#atr #shadow_graph_drift #phase8_1")
        return "\n".join(lines)

    @staticmethod
    def format_readiness_report(readiness: Dict[str, Any]) -> str:
        """
        Formats a report indicating cutover readiness for Phase 8.2.
        """
        score = readiness.get("readiness_score", 0)
        is_ready = readiness.get("is_ready", False)
        
        header = "✅ <b>[CUTOVER READY]</b>" if is_ready else "❌ <b>[CUTOVER BLOCKED]</b>"
        
        lines = [
            f"{header} - Shadow Graph Phase 8.1",
            f"<b>Score:</b> {score}/100",
            f"<b>Nodes Tracked:</b> {readiness.get('total_nodes_tracked')}",
            f"<b>Active Drifts:</b> {readiness.get('total_drifts_active')}",
            ""
        ]
        
        blockers = readiness.get("blockers", [])
        if blockers:
            lines.append("<b>🚫 Blockers:</b>")
            for b in blockers:
                lines.append(f" - {b}")
            lines.append("")
            
        warnings = readiness.get("warnings", [])
        if warnings:
            lines.append("<b>⚠️ Warnings:</b>")
            for w in warnings:
                lines.append(f" - {w}")
            lines.append("")
            
        if is_ready:
            lines.append("The Shadow Graph mirrors reality. Phase 8.2 cutover may proceed.")
            
        lines.append("#atr #shadow_graph_readiness #phase8_2")
        return "\n".join(lines)
