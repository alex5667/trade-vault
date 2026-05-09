import json
import logging
import uuid
from datetime import datetime
from typing import Any

from services.analytics_db import get_conn
from services.atr_effective_state_resolver import EffectiveStateResolver

logger = logging.getLogger("atr_graph_backed_runtime_gate")

class ATRGraphBackedRuntimeGateService:
    """
    Provides the runtime routing decision (allow/clip/deny) based on the Graph Control-Plane.
    Also handles equivalence checking against the legacy routing decisions.
    """

    _graph_state_cache: dict[str, Any] = {}
    # Increased from 5s → 30s: with 5+ symbols a 5s TTL causes frequent DB stampedes
    # (5 symbols × 12 calls/min = 60 DB queries/min vs 10 at 30s TTL).
    _graph_state_cache_ttl: float = 30.0
    _MAX_CACHE_SIZE: int = 5000

    @staticmethod
    def build_graph_runtime_state(scope_value: str) -> dict[str, Any]:
        """Reads graph-backed effective state (with TTL cache to protect ThreadPool/DB)"""
        import time
        now = time.time()

        cached = ATRGraphBackedRuntimeGateService._graph_state_cache.get(scope_value)
        if cached and (now - cached['ts'] < ATRGraphBackedRuntimeGateService._graph_state_cache_ttl):
            return cached['data']

        eff_state = EffectiveStateResolver.resolve_from_graph("symbol", scope_value)

        # Keep cache size bounded
        if len(ATRGraphBackedRuntimeGateService._graph_state_cache) > ATRGraphBackedRuntimeGateService._MAX_CACHE_SIZE:
            ATRGraphBackedRuntimeGateService._graph_state_cache.clear()

        ATRGraphBackedRuntimeGateService._graph_state_cache[scope_value] = {'ts': now, 'data': eff_state}
        return eff_state

    @staticmethod
    def decide_runtime_from_graph(signal: dict[str, Any], scope_value: str) -> str:
        """
        Returns 'allow', 'clip', or 'deny' based on graph state.
        Mapping:
        - "hard_freeze", "venue_frozen", "scope_frozen", "no_new_risk": deny
        - "clip": clip
        - "normal" and others: allow
        """
        try:
            eff_state = ATRGraphBackedRuntimeGateService.build_graph_runtime_state(scope_value)
            state_str = eff_state.get("states", {}).get("effective_runtime_state", "normal")

            precedence = EffectiveStateResolver._get_precedence(state_str)
            if precedence >= 20: # no_new_risk, scope_frozen, venue_frozen, hard_freeze
                return "deny"
            elif precedence == 10: # clip
                return "clip"
            else:
                return "allow"
        except Exception as e:
            logger.error(f"Error deciding runtime from graph for {scope_value}: {e}")
            # Fail-open / fallback to allow.
            # In publisher, if this fails, we will usually fall back to legacy if legacy is active.
            return "allow"

    @staticmethod
    def decide_legacy_runtime(legacy_highest_precedence: int) -> str:
        """
        Takes the highest precedence found by the legacy Freeze Projection overlay in async publisher.
        """
        if legacy_highest_precedence >= 20:
            return "deny"
        elif legacy_highest_precedence == 10:
            return "clip"
        else:
            return "allow"

    @staticmethod
    def compare_with_legacy_runtime(legacy_decision: str, graph_decision: str, scope_value: str) -> None:
        """
        Compares the decisions and records the result in atr_runtime_gate_equivalence_checks.
        Emits drifts if they mismatch.
        """
        status = "passed"
        if legacy_decision != graph_decision:
            status = "failed"

        # Optimization: Only record 1% of successful equivalence checks to save DB I/O on hot path
        if status == "passed":
            import random
            if random.random() > 0.01:
                return

        check_id = f"rt_chk_{uuid.uuid4().hex[:12]}"
        created_at = datetime.utcnow()
        summary = {
            "legacy_decision": legacy_decision,
            "graph_decision": graph_decision,
            "timestamp": created_at.isoformat()
        }

        # 1) Save Equivalent check
        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_runtime_gate_equivalence_checks
                    (check_id, scope_value, legacy_decision, graph_decision, status, summary_json, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    check_id, scope_value, legacy_decision, graph_decision, status,
                    json.dumps(summary), created_at
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to record runtime gate equivalence check: {e}")

        # 2) Emit drift if failed
        if status == "failed":
            ATRGraphBackedRuntimeGateService.emit_runtime_drift(legacy_decision, graph_decision, scope_value)

    @staticmethod
    def emit_runtime_drift(legacy_decision: str, graph_decision: str, scope_value: str) -> None:
        drift_kind = "decision_mismatch"
        reason_code = f"{legacy_decision}_vs_{graph_decision}_mismatch"

        # Severity evaluation
        # legacy=deny, graph=allow -> critical
        # legacy=clip, graph=allow -> critical
        # legacy=allow, graph=deny -> error
        severity = "warn"
        if legacy_decision == "deny" and graph_decision == "allow":
            severity = "critical"
            drift_kind = "allow_vs_deny_mismatch"
        elif legacy_decision == "clip" and graph_decision == "allow":
            severity = "critical"
            drift_kind = "clip_vs_allow_mismatch"
        elif legacy_decision == "allow" and graph_decision == "deny":
            severity = "error"
            drift_kind = "allow_vs_deny_mismatch"
        elif legacy_decision == "clip" and graph_decision == "deny":
            severity = "error"
            drift_kind = "clip_vs_deny_mismatch"

        drift_id = f"rt_drift_{uuid.uuid4().hex[:12]}"
        drift_json = {
            "legacy": legacy_decision,
            "graph": graph_decision,
            "reason_code": reason_code
        }

        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_runtime_gate_drifts
                    (drift_id, scope_value, drift_kind, severity, status, reason_code, drift_json, created_at)
                    VALUES (%s, %s, %s, %s, 'open', %s, %s, %s)
                """, (
                    drift_id, scope_value, drift_kind, severity, reason_code,
                    json.dumps(drift_json), datetime.utcnow()
                ))
                conn.commit()
            logger.info(f"Recorded runtime gate drift for {scope_value}: {drift_kind} ({severity})")
        except Exception as e:
            logger.error(f"Failed to emit runtime gate drift: {e}")

    @staticmethod
    def mark_runtime_cutover_readiness(status: str, summary: dict[str, Any]) -> None:
        """
        not_ready | shadow_healthy | ready_for_canary | ready_for_live
        """
        readiness_id = f"readiness_rt_{uuid.uuid4().hex[:12]}"
        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_runtime_gate_cutover_readiness
                    (readiness_id, component, status, summary_json, created_at)
                    VALUES (%s, 'runtime_gate', %s, %s, %s)
                """, (
                    readiness_id, status, json.dumps(summary), datetime.utcnow()
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to mark runtime cutover readiness: {e}")
