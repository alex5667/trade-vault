import logging
import os
import time
from datetime import datetime
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_effective_state_resolver")

from services.atr_constants import PRECEDENCE_MAP


class EffectiveStateResolver:
    """
    Computes the effective runtime state of a scope by resolving all relevant Control-Plane nodes
    and their active edges (freezes, overrides, blockers). 
    Supports 3 modes: legacy_only, shadow_compare, graph_read_primary.
    """

    @staticmethod
    def _parse_scope(scope_value: str) -> dict[str, str]:
        parts = scope_value.split("|")
        if len(parts) >= 8:
            return {
                "source": parts[0],
                "venue": parts[1],
                "symbol": parts[2],
                "scenario": parts[3],
                "regime": parts[4],
                "risk_horizon_bucket": parts[5],
                "layer": parts[6],
                "policy_ver": parts[7]
            }
        return {"raw": scope_value}

    @staticmethod
    def _get_precedence(state: str) -> int:
        return PRECEDENCE_MAP.get(state.lower(), 0)

    @staticmethod
    def resolve_legacy(scope_kind: str, scope_value: str) -> dict[str, Any]:
        rollout_stage = "none"
        freeze_state = "none"
        override_state = "none"
        release_blocked = False

        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                # 1. Rollout
                cur.execute("""
                    SELECT rollout_stage FROM atr_policy_rollouts
                    WHERE symbol = %s AND is_current = true
                    ORDER BY updated_at_ms DESC LIMIT 1
                """, (scope_value,))
                rollout_row = cur.fetchone()
                if rollout_row:
                    rollout_stage = rollout_row["rollout_stage"]  # type: ignore

                # 2. Freeze
                cur.execute("""
                    SELECT freeze_state FROM atr_active_freezes
                    WHERE scope_value = %s AND status != 'released'
                    ORDER BY started_at DESC LIMIT 1
                """, (scope_value,))
                freeze_row = cur.fetchone()
                if freeze_row:
                    freeze_state = freeze_row["freeze_state"]  # type: ignore

                # 3. Override
                symbol_filter = scope_value if scope_value != "all" else None
                cur.execute("""
                    SELECT status FROM atr_override_requests 
                    WHERE (symbol = %s OR symbol IS NULL) AND status = 'active' 
                      AND not_after > NOW()
                    ORDER BY created_at DESC LIMIT 1
                """, (symbol_filter,))
                override_row = cur.fetchone()
                if override_row:
                    override_state = "active"

                # 4. Release Blocked
                cur.execute("""
                    SELECT action FROM atr_release_decisions
                    WHERE (decision_json->>'symbol' = %s OR decision_json->>'scope' = %s)
                    ORDER BY decision_id DESC LIMIT 1
                """, (scope_value, scope_value))
                rel_row = cur.fetchone()
                if rel_row:
                    release_blocked = (rel_row["action"] == "deny_release")  # type: ignore

        except Exception as e:
            logger.error(f"Legacy resolve error for {scope_value}: {e}")

        # Combine
        eff_state = "normal"
        if rollout_stage in ("none", "stopped"):
            eff_state = "no_new_risk"

        if freeze_state != "none" and override_state == "none":
            if freeze_state in PRECEDENCE_MAP:
                eff_state = freeze_state
            else:
                eff_state = "frozen"
        elif override_state != "none" and freeze_state != "none":
             eff_state = "clip"  # naive legacy assumption

        return EffectiveStateResolver._build_output(
            scope_value, rollout_stage, eff_state,
            "blocked" if release_blocked else "allowed",
            freeze_state, override_state
        )

    @staticmethod
    def resolve_from_graph(scope_kind: str, scope_value: str) -> dict[str, Any]:
        rollout_stage = "none"
        freeze_state = "none"
        override_state = "none"
        effective_level = "normal"
        release_blocked = False

        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                # Rollout
                cur.execute("""
                    SELECT node_state_json FROM atr_control_plane_nodes
                    WHERE scope_value = %s AND node_type = 'RolloutState'
                    ORDER BY updated_at DESC LIMIT 1
                """, (scope_value,))
                rollout_node = cur.fetchone()
                if rollout_node:
                    rollout_stage = rollout_node["node_state_json"].get("rollout_stage", "none")  # type: ignore

                # Freeze and override via Graph Freeze Override logic
                from services.atr_graph_backed_freeze_override_service import ATRGraphBackedFreezeOverrideService
                graph_state = ATRGraphBackedFreezeOverrideService.resolve_effective_state(scope_kind, scope_value)
                freeze_state = graph_state.get("highest_graph_freeze", "none")

                # Override logic: Cannot weaken forbidden paths
                # But can override to clip for allowed temporary safe state
                if graph_state.get("override_active"):
                    override_state = "active"
                    override_level = graph_state.get("active_override_level", "clip")

                    # Merge precedence
                    freeze_prec = EffectiveStateResolver._get_precedence(freeze_state)
                    override_prec = EffectiveStateResolver._get_precedence(override_level)
                    if override_prec <= freeze_prec and freeze_prec < 50:
                        # Allow narrow to clip/normal if freeze is not hard_freeze
                        effective_level = override_level
                    else:
                        effective_level = freeze_state # Override ignored or it was higher
                else:
                    effective_level = freeze_state

                # Check release block
                cur.execute("""
                    SELECT count(*) as c FROM atr_control_plane_edges e
                    JOIN atr_control_plane_nodes n ON e.from_node_id = n.node_id
                    WHERE n.scope_value = %s AND e.edge_type = 'blocks'
                """, (scope_value,))
                release_blocked = cur.fetchone()["c"] > 0  # type: ignore

                # if release_blocked is true we can also deduce release_frozen.
                # If rollout is stopped/none it implies no_new_risk
                if effective_level == "none" or EffectiveStateResolver._get_precedence(effective_level) < 20:
                     if rollout_stage in ("none", "stopped"):
                         effective_level = "no_new_risk"

        except Exception as e:
            logger.error(f"Graph resolve error for {scope_value}: {e}")

        # Ensure we map none to normal
        if effective_level == "none":
            effective_level = "normal"

        return EffectiveStateResolver._build_output(  # type: ignore
            scope_value, rollout_stage, effective_level,
            "blocked" if release_blocked else "allowed",
            freeze_state, override_state,
        ),

    @staticmethod
    def _build_output(scope_val, rollout, eff_state, release, freeze, override) -> dict[str, Any]:

        # Constraints logic
        new_entries_allowed = True,
        if EffectiveStateResolver._get_precedence(eff_state) >= 20: # no_new_risk or higher,
            new_entries_allowed = False,

        release_allowed = (release != "blocked"),

        return {
            "scope": EffectiveStateResolver._parse_scope(scope_val),
            "states": {
                "rollout_stage": rollout,
                "effective_runtime_state": eff_state,
                "release_state": release,
                "freeze_state": freeze,
                "override_state": override,
                "allocator_state": "fresh",
                "budget_state": "healthy",
                "portfolio_state": "within_cap"
            },
            "constraints": {
                "risk_mult_cap": 0.25 if eff_state == "clip" else 1.0,
                "new_entries_allowed": new_entries_allowed,
                "protective_exits_allowed": True, # Always true under F4 requirements
                "promotion_allowed": release_allowed,
                "release_allowed": release_allowed
            },
            "projection_ver": int(datetime.utcnow().timestamp()),
            "updated_at_ms": int(time.time() * 1000)
        }

    @staticmethod
    def resolve_scope(scope_kind: str, scope_value: str, is_shadow_graph_mode: bool = False) -> dict[str, Any]:
        """ Backward compatibility for 8.1 / existing code expecting this """
        if is_shadow_graph_mode:
            return EffectiveStateResolver.resolve_from_graph(scope_kind, scope_value)
        return EffectiveStateResolver.resolve_legacy(scope_kind, scope_value)

    @staticmethod
    def resolve(scope_kind: str, scope_value: str, mode: str = None) -> dict[str, Any]:  # type: ignore
        """
        Mode can be: legacy_only, shadow_compare, graph_primary
        """
        if not mode:
            mode = os.getenv("ATR_GRAPH_EFFECTIVE_STATE_MODE", "legacy_only")

        if mode == "graph_primary" or mode == "graph_read_primary":
            return EffectiveStateResolver.resolve_from_graph(scope_kind, scope_value)

        # Phase 8.8: Dynamic component cutover check
        try:
            from services.atr_graph_reconciliation_service import ATRGraphReconciliationService
            if ATRGraphReconciliationService.is_component_graph_primary("effective_state", scope_value):
                return EffectiveStateResolver.resolve_from_graph(scope_kind, scope_value)
        except Exception as e:
            logger.error(f"Error checking graph primary component for effective_state: {e}")

        if mode == "shadow_compare":
            # Just resolving sequentially and throwing away the shadow result.
            # To actually compare and log drift, caller or EquivalenceCertService must handle it.
            # Here we just execute both so they're exercised in the resolver itself, returning legacy.
            graph_res = EffectiveStateResolver.resolve_from_graph(scope_kind, scope_value)
            leg_res = EffectiveStateResolver.resolve_legacy(scope_kind, scope_value)
            return leg_res
        else:
             # legacy_only
             return EffectiveStateResolver.resolve_legacy(scope_kind, scope_value)
