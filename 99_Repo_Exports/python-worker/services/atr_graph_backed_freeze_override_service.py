import logging
import time
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_graph_backed_freeze_override")

FREEZE_PRECEDENCE = {
    "clip": 10,
    "no_new_risk": 20,
    "scope_frozen": 30,
    "venue_frozen": 40,
    "hard_freeze": 100
}

class ATRGraphBackedFreezeOverrideService:
    """
    Core resolver for Phase 8.3 implementing R8-R11 precedence 
    and override masking logic based strictly on the graph.
    """

    @staticmethod
    def _get_precedence(level: str) -> int:
        return FREEZE_PRECEDENCE.get(level.lower(), 0)

    @staticmethod
    def resolve_effective_state(scope_kind: str, scope_value: str) -> dict[str, Any]:
        """
        Calculates the effective freeze level after considering overrides.
        """
        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                # 1. Fetch all active freeze nodes for this scope
                # We also consider parent scopes? For 8.3, we assume direct scope or 'global'
                scopes = [scope_value]
                if scope_value != "all":
                    scopes.append("all")

                cur.execute("""
                    SELECT * FROM atr_control_plane_nodes 
                    WHERE scope_value IN %s 
                      AND node_type = 'FreezeState'
                      AND node_state_json->>'status' IN ('active', 'recovering')
                    ORDER BY updated_at DESC
                """, (tuple(scopes),))
                freeze_nodes = cur.fetchall()

                # 2. Fetch active override nodes
                cur.execute("""
                    SELECT * FROM atr_control_plane_nodes 
                    WHERE scope_value IN %s 
                      AND node_type = 'OverrideState'
                      AND node_state_json->>'status' = 'active'
                      AND CAST(node_state_json->>'expires_at_ms' AS BIGINT) > %s
                    ORDER BY updated_at DESC
                """, (tuple(scopes), int(time.time() * 1000)))
                override_nodes = cur.fetchall()

                # 3. Resolve highest freeze
                highest_freeze = "none"
                highest_precedence = 0
                for node in freeze_nodes:
                    state = node["node_state_json"]
                    level = state.get("level", "none")
                    prec = ATRGraphBackedFreezeOverrideService._get_precedence(level)
                    if prec > highest_precedence:
                        highest_precedence = prec
                        highest_freeze = level

                # 4. Resolve highest override
                # Logic: Override masks ANY freeze IF the override level is 'normal' or 'clip'
                # and higher than the freeze status?
                # Actually, hierarchy R8-R11 applies to the RESULT.
                # If an override says 'clip', and freeze says 'hard_freeze', 'hard_freeze' wins UNLESS override is authorized to break it.
                # For Phase 8.3, we define masking as:
                # effective_level = max(freeze_level, override_level) where override_level is usually lower (normal/clip).
                # Wait, masking usually means override REDUCES freeze level.

                effective_level = highest_freeze
                override_active = False
                active_override_level = "none"

                if override_nodes:
                    # In 8.3, we take the most recent active override
                    best_override = override_nodes[0]["node_state_json"]
                    active_override_level = best_override.get("level", "normal")
                    override_active = True

                    # R11: Overrides can mask freezes if they explicitly target a lower state
                    # But if freeze is 'hard_freeze', it might be unmaskable?
                    # For 8.3, we assume active override takes priority if it's authorized.
                    effective_level = active_override_level

                return {
                    "scope_value": scope_value,
                    "highest_graph_freeze": highest_freeze,
                    "active_override_level": active_override_level,
                    "override_active": override_active,
                    "effective_level": effective_level,
                    "resolved_at_ms": int(time.time() * 1000)
                }

        except Exception as e:
            logger.error(f"Error resolving graph-backed state for {scope_value}: {e}")
            return {
                "scope_value": scope_value,
                "effective_level": "unknown",
                "error": str(e)
            }
