import json
import logging
import os
import time
import uuid
import psycopg2.extras
from typing import Any, Dict, List, Optional
from datetime import datetime

from services.analytics_db import get_conn

logger = logging.getLogger("atr_release_gate")

# Phase 8.2 — graph-backed dual-read (lazy import to avoid circular deps)
_GRAPH_GATE_ENABLE = os.getenv("ATR_GRAPH_RELEASE_GATE_ENABLE", "0") == "1"

def _generate_id(prefix: str) -> str:
    return f"{prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

def _check_inv_no_live_without_replay(change: Dict[str, Any], replay_status: str) -> Optional[str]:
    """
    Enforces INV_NO_LIVE_STAGE_WITHOUT_REPLAY_PASS and INV_SIGNAL_ID_STABLE_IN_REPLAY.
    No release touching live order flow can bypass a passed replay certification.
    """
    is_live = False
    
    # Check if the change targets the live stage based on scenario or layer
    if str(change.get("scenario", "")).startswith("live") or str(change.get("layer", "")).startswith("live"):
        is_live = True
        
    if is_live and replay_status != "passed":
        return "unpassed_replay_for_live_release"
    return None


def build_scorecard(change_id: str) -> Dict[str, Any]:
    """
    Build a release readiness scorecard for a given change_id.
    Aggregates evidence across replay, rollout, incidents, and actions.
    """
    now_ms = int(time.time() * 1000)
    
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 1. Fetch Change Request and its Risk Level
        cur.execute("SELECT * FROM atr_change_requests WHERE change_id = %s", (change_id,))
        change = cur.fetchone()
        if not change:
            raise ValueError(f"Change ID {change_id} not found")
            
        scope_kind = change.get("scope_kind", "global")
        risk_level = change.get("risk_level", "medium")
        
        # 2. Fetch Replay Status
        cur.execute("SELECT status FROM atr_replay_manifests WHERE change_id = %s ORDER BY created_at DESC LIMIT 1", (change_id,))
        replay = cur.fetchone()
        replay_status = replay["status"] if replay else "missing"
        
        # 3. Fetch Rollout Cert Status
        cur.execute("SELECT status, rollout_stage FROM atr_rollout_certifications WHERE change_id = %s ORDER BY created_at DESC LIMIT 1", (change_id,))
        rollout_cert = cur.fetchone()
        rollout_cert_status = f"{rollout_cert['rollout_stage']}_{rollout_cert['status']}" if rollout_cert else "not_applicable"
        
        # 4. Open Incidents (Global or related scope)
        cur.execute("SELECT count(*) as c FROM atr_incidents WHERE status != 'closed' AND severity = 'SEV-1'")
        sev1_open = cur.fetchone()["c"]
        
        # 5. Overdue Corrective Actions
        cur.execute("SELECT count(*) as c FROM atr_corrective_actions WHERE status NOT IN ('done', 'verified', 'dropped') AND due_at_ms < %s", (now_ms,))
        overdue_actions = cur.fetchone()["c"]
        
        # 5.2 Fetch related Error Budget Statuses
        cur.execute("""
            SELECT budget_status 
            FROM atr_invariant_budget_states 
            WHERE scope_value IN (%s, %s, %s) AND budget_status IN ('exhausted', 'warning')
        """, (change.get("symbol"), change.get("layer"), change.get("policy_ver")))
        budget_states = cur.fetchall()
        budget_is_exhausted = any(b["budget_status"] == "exhausted" for b in budget_states)
        budget_is_warning = any(b["budget_status"] == "warning" for b in budget_states)
        
        # 5.5. Unresolved Critical Invariant Violations
        cur.execute("""
            SELECT count(*) as c 
            FROM atr_invariant_violations v 
            JOIN atr_invariants i ON v.invariant_id = i.invariant_id 
            WHERE v.status != 'resolved' AND i.enforcement_mode = 'release_block'
        """)
        unresolved_invariants = cur.fetchone()["c"]
        
        # 6. Build Blockers and Warnings
        blockers = []
        warnings = []
        infos = []
        
        if unresolved_invariants > 0:
            blockers.append("INV_UNRESOLVED_CRITICAL_INVARIANTS_ON_SCOPE")
        
        if budget_is_exhausted:
            blockers.append("INVARIANT_ERROR_BUDGET_EXHAUSTED")
        elif budget_is_warning:
            warnings.append("INVARIANT_ERROR_BUDGET_WARNING")
        
        # Cross-referential consistency checkpoint
        inv_live_blocker = _check_inv_no_live_without_replay(change, replay_status)
        if inv_live_blocker:
            blockers.append(inv_live_blocker)

        if replay_status == "missing" and risk_level in ["medium", "high", "critical"]:
            blockers.append("replay_missing")
        elif replay_status == "failed":
            blockers.append("replay_failed")
            
        if sev1_open > 0:
            blockers.append("INV_NO_LIVE_SCOPE_WITH_OPEN_CRITICAL_INCIDENT")
            
        if overdue_actions > 0 and risk_level in ["high", "critical"]:
            blockers.append("INV_NO_OVERRIDE_RELEASE_WITH_UNRESOLVED_CRITICAL_POSTMORTEM_ACTION")
        elif overdue_actions > 0:
            warnings.append("medium_overdue_action")
            
        # Target stage evaluation
        scenario = str(change.get("scenario") or "")
        target_stage_live_or_canary = scenario.startswith("live") or scenario.startswith("canary")
        
        # Formal Phase 7.2 check
        if target_stage_live_or_canary and (not rollout_cert or rollout_cert["status"] != "passed"):
            blockers.append("INV_NO_STAGE_ADVANCE_WITHOUT_ROLLOUT_CERT")
        elif not target_stage_live_or_canary and (not rollout_cert or rollout_cert["status"] != "passed"):
            if risk_level in ["high", "critical"]:
                blockers.append("required_rollout_cert_failed")
            else:
                warnings.append("low_sample_rollout_stage")
                
        # --- [Phase 10.2] Charter Enforcement Map (L3/L4) ---
        try:
            from services.atr_charter_compliance_engine import ATRCharterComplianceEngine
            engine = ATRCharterComplianceEngine()
            # Evaluate compliance for this release
            bundle = engine.evaluate_context(context_kind="release_context", context_ref=change_id)
            
            enforcement = bundle.get("enforcement", {})
            overall_action = enforcement.get("overall_action", "allow")
            
            if overall_action == "BLOCK_RELEASE":
                blockers.append("BLOCK_RELEASE_BY_CHARTER_ENFORCEMENT")
            elif overall_action == "BLOCK_PROMOTION":
                if target_stage_live_or_canary: # Promotion to higher stages
                    blockers.append("BLOCK_PROMOTION_BY_CHARTER_ENFORCEMENT")
            elif overall_action == "WARN":
                warnings.append("CHARTER_ENFORCEMENT_WARNING")
                
            scorecard_summary = bundle.get("summary_json", {})
            if scorecard_summary.get("failed", 0) > 0:
                infos.append(f"charter_failures: {scorecard_summary.get('failed_ids')}")
        except Exception as ce:
            logger.error("Phase 10.2 charter enforcement check failed for %s: %s", change_id, ce)
            # Fail-open for enforcement engine crashes, rely on other blockers
                
        # Calculate Readiness Score
        score = 35.0
        if "replay_passed" in replay_status: score += 20.0
        if "passed" in rollout_cert_status: score += 20.0
        if sev1_open == 0: score += 15.0
        if overdue_actions == 0: score += 10.0
        
        if "low_sample_rollout_stage" in warnings: score -= 6.0
        if "medium_overdue_action" in warnings: score -= 5.0
        
        # Bound score between 0 and 100
        score = max(0.0, min(100.0, score))
        
        # Check for active TEMP_RELEASE_OVERRIDE
        cur.execute("""
            SELECT override_id FROM atr_override_requests
            WHERE status = 'active' AND override_class = 'TEMP_RELEASE_OVERRIDE'
              AND scope_kind IN ('global', %s)
            ORDER BY created_at DESC LIMIT 1
        """, (scope_kind,)),
        active_rel_override = cur.fetchone(),
        
        # Determine Decision
        if len(blockers) > 0:
            if active_rel_override and "INV_UNRESOLVED_CRITICAL_INVARIANTS_ON_SCOPE" not in blockers and "INV_NO_LIVE_SCOPE_WITH_OPEN_CRITICAL_INCIDENT" not in blockers:
                decision = "allow_with_override",
                infos.append(f"blockers_bypassed_via_override_{active_rel_override['override_id']}"),
            else:
                decision = "deny",
        elif len(warnings) > 0:
            decision = "allow_with_override",
        else:
            decision = "allow",
            
        scorecard = {
            "scorecard_id": _generate_id("sc"),
            "change_id": change_id,
            "scope": {
                "source": change.get("source"),
                "symbol": change.get("symbol"),
                "scenario": change.get("scenario"),
                "regime": change.get("regime"),
                "risk_horizon_bucket": change.get("risk_horizon_bucket"),
                "layer": change.get("layer"),
                "policy_ver": change.get("policy_ver")
            },
            "decision": decision,
            "readiness_score": score,
            "blockers": blockers,
            "warnings": warnings,
            "infos": infos,
            "summary": {
                "replay_status": replay_status,
                "rollout_cert_status": rollout_cert_status,
                "rollback_cert_status": "not_applicable",
                "incidents_open": sev1_open,
                "overdue_actions": overdue_actions
            }
        }
        
        # Persist Scorecard
        cur.execute("""
            INSERT INTO atr_release_scorecards (
                scorecard_id, change_id, scope_kind, source, venue, symbol,
                scenario, regime, risk_horizon_bucket, layer, policy_ver,
                readiness_score, decision, blockers_json, warnings_json, infos_json, summary_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
        """, (
            scorecard["scorecard_id"], change_id, scope_kind, scorecard["scope"]["source"],
            change.get("venue"), scorecard["scope"]["symbol"], scorecard["scope"]["scenario"],
            scorecard["scope"]["regime"], scorecard["scope"]["risk_horizon_bucket"],
            scorecard["scope"]["layer"], scorecard["scope"]["policy_ver"],
            scorecard["readiness_score"], scorecard["decision"],
            json.dumps(scorecard["blockers"]), json.dumps(scorecard["warnings"]),
            json.dumps(scorecard["infos"]), json.dumps(scorecard["summary"])
        ))
        conn.commit()
        return scorecard

def decide_release(change_id: str) -> Dict[str, Any]:
    """Generates a scorecard and returns the release decision.

    Phase 8.2: when ATR_GRAPH_RELEASE_GATE_ENABLE=1, wraps the legacy
    scorecard with the graph-backed dual-read evaluator.
    
    Phase 8.7: Global Graph Consistency Proxy.
    Checks consistency before returning.
    
    The returned dict always contains 'decision', 'scorecard', and
    optionally 'graph_state', 'compare', 'check_id'.
    """
    legacy_scorecard = build_scorecard(change_id)

    # 1) Phase 8.2 graph dual-read (if enabled)
    if _GRAPH_GATE_ENABLE:
        try:
            from services.atr_graph_backed_release_gate import evaluate_release as _eval
            result = _eval(change_id, legacy_scorecard)
            legacy_scorecard["decision"] = result["decision"]
            legacy_scorecard["_graph_source"] = result.get("source")
            legacy_scorecard["_graph_state"] = result.get("graph_state")
            legacy_scorecard["_compare"] = result.get("compare")
            legacy_scorecard["_check_id"] = result.get("check_id")
            legacy_scorecard["_critical_drifts"] = result.get("critical_drifts", [])
        except Exception as exc:
            logger.error("Phase 8.2 graph gate evaluate_release failed for %s: %s", change_id, exc)
            
    # 2) Phase 8.7 Graph Consistency Gate
    try:
        from services.atr_graph_consistency_gate import decide_graph_consistency, _ENFORCE
        # Fetch risk_level
        with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT risk_level FROM atr_change_requests WHERE change_id = %s", (change_id,))
            ch_row = cur.fetchone()
            risk_level = ch_row["risk_level"] if ch_row else "medium"
            
        scope_value = legacy_scorecard.get("scope", {}).get("symbol", "global")
        
        gc_result = decide_graph_consistency(change_id, scope_value, risk_level)
        legacy_scorecard["_graph_consistency"] = gc_result
        
        if _ENFORCE:
            if gc_result["decision"] == "deny":
                legacy_scorecard["decision"] = "deny"
                legacy_scorecard["blockers"].extend(gc_result.get("blockers", []))
            elif gc_result["decision"] == "allow_with_override" and legacy_scorecard["decision"] == "allow":
                if risk_level in ("medium", "high") and risk_level != "critical":
                    legacy_scorecard["decision"] = "allow_with_override"
                else:
                    legacy_scorecard["decision"] = "deny"
                legacy_scorecard["warnings"].extend(gc_result.get("warnings", []))
    except Exception as exc:
        logger.error("Phase 8.7 graph consistency gate failed for %s: %s", change_id, exc)

    return legacy_scorecard

def require_override(change_id: str) -> bool:
    """Check if the latest scorecard requires an override."""
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT decision FROM atr_release_scorecards WHERE change_id = %s ORDER BY created_at DESC LIMIT 1", (change_id,))
        row = cur.fetchone()
        if not row:
            return True
        return row["decision"] == "allow_with_override"

def record_release_decision(change_id: str, scorecard_id: str, actor: str, action: str, reason_code: str, decision_json: Dict[str, Any]) -> bool:
    """Record an explicit release decision (e.g. from an operator via Telegram)."""
    decision_id = _generate_id("rel")
    
    from services.atr_control_plane_graph_service import ControlPlaneGraphService
    from services.atr_graph_reconciliation_service import ATRGraphReconciliationService
    
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT status, scope_kind, symbol FROM atr_change_requests WHERE change_id = %s", (change_id,))
            row = cur.fetchone()
            old_status = row[0] if row else "NONE"
            scope_kind = row[1] if row and len(row) > 1 else "global"
            symbol = row[2] if row and len(row) > 1 else "all"

            cur.execute("""
                INSERT INTO atr_release_decisions (
                    decision_id, change_id, scorecard_id, actor, action, reason_code, decision_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (decision_id, change_id, scorecard_id, actor, action, reason_code, json.dumps(decision_json)))
            
            # If action is 'approve_release' or 'override_release', update the change status to APPROVED.
            if action in ("approve_release", "override_release"):
                now_ms = int(time.time() * 1000)
                
                # Phase 8.8: Graph Authority Check
                if ATRGraphReconciliationService.detect_out_of_band_legacy_write(
                    component="release",
                    scope_value=symbol,
                    actor=actor,
                    reason_code=reason_code,
                    payload_json={"action": action, "change_id": change_id, "scorecard_id": scorecard_id}
                ):
                    logger.warning(f"Blocked legacy release write for {symbol} due to Graph Primary Authority.")
                    return False

                cur.execute("UPDATE atr_change_requests SET status = 'APPROVED', updated_at_ms = %s WHERE change_id = %s", (now_ms, change_id))
                
                # record transition
                cur.execute("""
                    INSERT INTO atr_change_transitions (change_id, old_status, new_status, reason_code, transition_json)
                    VALUES (%s, %s, %s, %s, %s)
                """, (change_id, old_status, "APPROVED", "RELEASE_DECISION_"+action.upper(), json.dumps({"decision_id": decision_id, "scorecard_id": scorecard_id})))
                
            ControlPlaneGraphService.emit_graph_event(
                scope_kind=scope_kind,
                scope_value=symbol,
                event_type="release_decided",
                payload={"action": action}
            )

            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to record release decision for {change_id}: {e}")
        return False
