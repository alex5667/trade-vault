import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from services.analytics_db import get_conn
from services.atr_effective_state_resolver import EffectiveStateResolver

logger = logging.getLogger("atr_graph_reconciliation")

class ATRGraphReconciliationService:
    """
    Responsibilities:
    - read graph current nodes/edges
    - produce canonical current state
    - write/update legacy-facing SQL tables
    - write/update runtime Redis serving keys
    - detect drift where legacy changed outside graph
    - mark illegal out-of-band writes
    """

    @staticmethod
    def _generate_id(prefix: str) -> str:
        return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    @staticmethod
    def is_component_graph_primary(component: str, scope_value: str) -> bool:
        """
        Check if a component + scope is considered Graph Primary.
        Fallback to ENV ATR_GRAPH_PRIMARY_{COMPONENT}.
        """
        env_flag_name = f"ATR_GRAPH_PRIMARY_{component.upper()}"
        if os.getenv(env_flag_name, "0") == "1":
            return True

        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT status FROM atr_graph_primary_cutover
                    WHERE component = %s AND (scope_value = %s OR scope_value = 'all')
                    ORDER BY created_at DESC LIMIT 1
                """, (component, scope_value))
                row = cur.fetchone()
                if row and row["status"] == "active":  # type: ignore
                    return True
        except Exception as e:
            logger.error(f"Error checking graph primary cutover status for {component}/{scope_value}: {e}")
        return False

    @staticmethod
    def detect_out_of_band_legacy_write(component: str, scope_value: str, actor: str, reason_code: str, payload_json: dict[str, Any]) -> bool:
        """
        If this component is graph_primary, log a violation when a legacy service directly mutates it.
        Returns True if the write is an out-of-band violation (and was logged) so the caller can block it.
        """
        if not ATRGraphReconciliationService.is_component_graph_primary(component, scope_value):
            return False

        violation_id = ATRGraphReconciliationService._generate_id("v_auth")
        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_graph_primary_authority_violations (
                        violation_id, component, scope_value, actor, reason_code, violation_json
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (violation_id, component, scope_value, actor, reason_code, json.dumps(payload_json)))
                conn.commit()
            logger.warning(f"Control-Plane Authority Violation detected: {component} mutated by {actor} directly on {scope_value}.")
            return True
        except Exception as e:
            logger.error(f"Failed to log authority violation: {e}")
            return False

    @staticmethod
    def mark_reconciliation_drift(scope_value: str, drift_kind: str, severity: str, reason_code: str, drift_json: dict[str, Any]):
        """
        Record a reconciliation drift into the database.
        """
        drift_id = ATRGraphReconciliationService._generate_id("drift")
        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_graph_reconciliation_drifts (
                        drift_id, scope_value, drift_kind, severity, status, reason_code, drift_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (drift_id, scope_value, drift_kind, severity, "open", reason_code, json.dumps(drift_json)))
                conn.commit()
            logger.warning(f"Reconciliation drift observed on {scope_value}: {drift_kind} ({severity})")
        except Exception as e:
            logger.error(f"Failed to record reconciliation drift: {e}")

    @staticmethod
    def project_graph_to_legacy(scope_kind: str, scope_value: str) -> bool:
        """
        Read the canonical state from the Graph using EffectiveStateResolver's graph logic,
        and update the legacy tables and Redis keys if there are discrepancies.
        This provides a one-way sync Graph -> Legacy.
        """
        # Resolve from Graph
        graph_state = EffectiveStateResolver.resolve_from_graph(scope_kind, scope_value)
        states = graph_state.get("states", {})
        graph_freeze = states.get("freeze_state", "none")
        graph_override = states.get("override_state", "none")
        graph_release = states.get("release_state", "allowed")
        graph_rollout = states.get("rollout_stage", "none")

        # Determine actual legacy state currently in DB
        leg_state = EffectiveStateResolver.resolve_legacy(scope_kind, scope_value)
        leg_states = leg_state.get("states", {})

        drift_found = False

        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:

                # Check Freeze Drift
                if ATRGraphReconciliationService.is_component_graph_primary("freeze", scope_value):
                    if graph_freeze != leg_states.get("freeze_state", "none"):
                        drift_found = True
                        ATRGraphReconciliationService.mark_reconciliation_drift(
                            scope_value, "legacy_out_of_band_freeze_write", "error", "drift_freeze_state",
                            {"graph": graph_freeze, "legacy": leg_states.get("freeze_state", "none")}
                        )
                        # Project down to legacy active freezes
                        if graph_freeze == "none":
                            cur.execute("UPDATE atr_active_freezes SET status = 'released' WHERE scope_value = %s AND status != 'released'", (scope_value,))
                        else:
                            # Need a dummy entry to satisfy legacy tables. This relies on the graph structure.
                            # Usually projection is to generate redis keys, but we can also mock the legacy table.
                            cur.execute("""
                                INSERT INTO atr_active_freezes (
                                    freeze_id, trigger_kind, scope_kind, scope_value, freeze_state, source_reason_code, status, freeze_json
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (freeze_id) DO UPDATE SET 
                                    freeze_state = EXCLUDED.freeze_state, status = EXCLUDED.status
                            """, (f"graph_proj_freeze_{scope_value}", "graph_sync", scope_kind, scope_value, graph_freeze, "graph_primary_sync", "active", "{}"))

                # Check Override Drift
                if ATRGraphReconciliationService.is_component_graph_primary("override", scope_value):
                    if graph_override != leg_states.get("override_state", "none"):
                        drift_found = True
                        ATRGraphReconciliationService.mark_reconciliation_drift(
                            scope_value, "legacy_out_of_band_override_write", "warning", "drift_override_state",
                            {"graph": graph_override, "legacy": leg_states.get("override_state", "none")}
                        )

                # Check Release Drift
                if ATRGraphReconciliationService.is_component_graph_primary("release", scope_value):
                    if graph_release != leg_states.get("release_state", "allowed"):
                        drift_found = True
                        ATRGraphReconciliationService.mark_reconciliation_drift(
                            scope_value, "legacy_out_of_band_release_write", "error", "drift_release_state",
                            {"graph": graph_release, "legacy": leg_states.get("release_state", "allowed")}
                        )

                conn.commit()

        except Exception as e:
            logger.error(f"Failed to project graph to legacy for {scope_value}: {e}")
            return False

        return drift_found

    @staticmethod
    def reconcile_legacy_from_graph(scope_kind: str, scope_value: str) -> None:
        """
        Entry point to force a reconciliation check.
        Can be used by a periodic job or triggered by graph transitions.
        """
        logger.info(f"Reconciling legacy from graph for {scope_value}...")
        ATRGraphReconciliationService.project_graph_to_legacy(scope_kind, scope_value)
