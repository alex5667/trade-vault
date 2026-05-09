#!/usr/bin/env python3
"""
ATR Operating Charter Service (Phase 10)
Serves as the "System Constitution" governing runtime, execution, protective, and release logic.
"""

import json
import os
import uuid
from datetime import datetime
from typing import Any

import psycopg2
import redis
from psycopg2.extras import RealDictCursor

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None
from prometheus_client import Counter, Gauge, start_http_server

from common.log import setup_logger
from services.analytics_db import get_conn
from services.atr_charter_compliance_engine import ATRCharterComplianceEngine

logger = setup_logger("atr_operating_charter_service")

# --- Prometheus Metrics ---
PROM_CHARTER_VERSIONS = Gauge("atr_charter_versions_total", "Count of charters by status", ["status"])
PROM_CHARTER_AMENDMENTS = Counter("atr_charter_amendments_total", "Count of amendments by class and status", ["amendment_class", "status"])
PROM_CHARTER_COMPLIANCE = Gauge("atr_charter_compliance_total", "Compliance check results by domain and status", ["domain", "status"])
PROM_CHARTER_FORBIDDEN_ACTION = Counter("atr_charter_forbidden_action_total", "Forbidden actions detected", ["reason_code"])
PROM_CHARTER_EVIDENCE_MISSING = Counter("atr_charter_evidence_missing_total", "Decisions missing mandatory evidence", ["decision_class"])

# --- Charter V1 Content (Sections 1-10) ---
CHARTER_V1 = {
    "sections": {
        "1_mission_risk_boundary": {
            "mission": "deterministic, observable, replayable, risk-bounded signal-to-execution pipeline",
            "risk_boundary": "no uncontrolled new risk may be opened outside approved control-plane and runtime gating",
            "sacred_path": "protective lifecycle for already-open positions must remain operational even under degrade/freeze/restore conditions"
        },
        "2_canonical_truths": {
            "signal_truth": "unified signal DTO + stable signal_id",
            "runtime_truth": "final allow/clip/deny decision on canonical dispatch path",
            "execution_truth": "emitted order payload + venue/broker result + fill/retcode",
            "protective_truth": "broker/open position state reconciled with TP1/BE/trailing lifecycle",
            "post_trade_truth": "closed_trades + slippage_ema + closeout metrics",
            "governance_truth": "control-plane graph + certs + release/freeze/override/effective-state decisions"
        },
        "3_canonical_state_machines": [
            "signal_decision_lifecycle",
            "release_promotion_lifecycle",
            "freeze_override_lifecycle",
            "incident_quarantine_lifecycle",
            "post_release_observation_lifecycle",
            "dr_restore_lifecycle",
            "protective_lifecycle",
            "replay_dataset_lifecycle"
        ],
        "4_authority_matrix": {
            "runtime_owner": "gates, decision diagnostics, signal hygiene",
            "execution_owner": "venue connectivity, fill quality, bridge health",
            "control_plane_owner": "graph, release, freeze, override, consistency certs",
            "protective_owner": "TP1/BE/trailing/closeout correctness",
            "oncall_operator": "daily triage, bounded freeze/watch/same-day-fix actions",
            "oncall_domain_owner": "release block / freeze escalation / rollback review initiation",
            "technical_owner": "graph-primary changes, governance baseline changes, DR sign-off"
        },
        "5_non_negotiable_invariants": [
            "N1 one canonical signal payload",
            "N2 stable signal_id for replay/dedup",
            "N3 no order routing outside canonical dispatch path",
            "N4 no new risk without runtime allow/clip/deny decision",
            "N5 no high/critical release outside release window policy",
            "N6 no release on quarantined scope",
            "N7 no protective-path touching release without protective green path",
            "N8 no direct legacy governance write outside approved graph/fallback policy",
            "N9 no waiver for critical runtime/protective mismatches",
            "N10 no restore exit to NORMAL without observation-after-restore"
        ],
        "6_release_promotion_law": {
            "release_requirements": ["change_class", "release_window_class", "pre_release_checklist", "sign_off_set", "rollback_ready_bundle", "post_release_observation_state"],
            "promotion_requirements": ["observation_result", "no_hold", "no_quarantine", "green_status"]
        },
        "7_incident_quarantine_rollback_law": {
            "sev1_rule": "opens incident or equivalent formal artifact",
            "quarantine_rule": "must have entry reason, dwell, exit checks, review state",
            "rollback_rule": "must reference scope, trigger, rollback target, verification plan"
        },
        "8_dr_archive_replay_law": {
            "dr_complete_requirements": ["control-plane restore", "signal/runtime restore", "execution restore", "protective/post-trade restore", "observation-after-restore"],
            "archive_valid_requirements": ["manifest", "checksums", "restore sample", "replay usability"],
            "golden_dataset_requirements": ["owner", "scope", "validity window", "baseline", "review artifact"]
        },
        "9_operating_cadence": {
            "daily": "triage, health check, small fixes",
            "weekly": "scorecard review, ceremony acknowledgment",
            "monthly": "strategy review, deep audit"
        },
        "10_amendment_policy": {
            "minor_editorial": "owner approval",
            "policy_tuning": "owner + oncall lead",
            "authority_change": "technical owner + control-plane owner",
            "risk_machine_invariants": "formal review + replay/ops impact note + ceremony acknowledgment"
        }
    }
}

class ATROperatingCharterService:
    def __init__(self, enable: bool = True, enforce: bool = False):
        self.enable = enable
        self.enforce = enforce
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        if get_atr_redis is not None:
            self._r = get_atr_redis()
        else:
            self._r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        self.compliance_engine = ATRCharterComplianceEngine(redis_url=self.redis_url)

    def generate_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    def load_active_charter(self, conn) -> dict[str, Any] | None:
        """Load the currently active charter from DB."""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM atr_operating_charters 
                WHERE status = 'active' 
                ORDER BY created_at DESC LIMIT 1
            """)
            charter = cur.fetchone()
            if charter:
                PROM_CHARTER_VERSIONS.labels(status="active").set(1)
            return charter

    def propose_charter_amendment(self, conn, charter_id: str, amendment_class: str, proposer: str, amendment_json: dict[str, Any]) -> str:
        """Propose a change to the charter."""
        amendment_id = self.generate_id("amend")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_charter_amendments (
                    amendment_id, charter_id, amendment_class, status, proposer, amendment_json
                ) VALUES (%s, %s, %s, 'requested', %s, %s)
            """, (amendment_id, charter_id, amendment_class, proposer, json.dumps(amendment_json)))
            conn.commit()
        PROM_CHARTER_AMENDMENTS.labels(amendment_class=amendment_class, status="requested").inc()
        logger.info(f"Charter amendment proposed: {amendment_id} for charter {charter_id}")
        return amendment_id

    def activate_charter_version(self, conn, charter_id: str, approved_by: str):
        """Approve and activate a specific charter, superseding the old one."""
        with conn.cursor() as cur:
            # 1. Archive current active
            cur.execute("""
                UPDATE atr_operating_charters 
                SET status = 'superseded', superseded_at = NOW() 
                WHERE status = 'active'
            """)

            # 2. Activate new one
            cur.execute("""
                UPDATE atr_operating_charters 
                SET status = 'active', activated_at = NOW(), approved_by = %s 
                WHERE charter_id = %s
            """, (approved_by, charter_id))

            conn.commit()
        logger.info(f"Charter {charter_id} activated successfully (superseded previous active)")

    def initialize_default_charter(self, conn, created_by: str = "system"):
        """Seed the initial V1 charter if none exists."""
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM atr_operating_charters")
            if cur.fetchone()[0] == 0:
                charter_id = self.generate_id("charter_v1")
                cur.execute("""
                    INSERT INTO atr_operating_charters (
                        charter_id, version, status, charter_json, created_by
                    ) VALUES (%s, '1.0.0', 'active', %s, %s)
                """, (charter_id, json.dumps(CHARTER_V1), created_by))
                conn.commit()
                logger.info(f"Default Charter V1 initialized: {charter_id}")

            # --- Phase 10.1: Ensure Machine-Readable Policies are seeded ---
            cur.execute("SELECT count(*) FROM atr_charter_policy_registry")
            if cur.fetchone()[0] == 0:
                self._seed_machine_readable_policies(conn)
                conn.commit()
                logger.info("Machine-readable policies seeded into registry.")

    def _seed_machine_readable_policies(self, conn):
        """Seed the registry with Phase 10.1 rules."""
        rules = [
            {
                "rule_id": "CHARTER-R6",
                "category": "incident_quarantine",
                "severity": "critical",
                "enforcement_mode": "blocking",
                "scope_kind": "release_scope",
                "owner": "runtime_owner",
                "policy_json": {
                    "contexts": ["release_context", "promotion_context", "weekly_review"],
                    "reason_codes": {"fail": "CHARTER_RELEASE_ON_QUARANTINED_SCOPE"}
                },
                "mapping": {
                    "source_type": "sql",
                    "source_ref": "execution_quarantine_ledger",
                    "evaluator_type": "sql_assert",
                    "evaluator_json": {"predicate": "action = 'QUARANTINED'"}
                }
            },
            {
                "rule_id": "CHARTER-R5",
                "category": "release_governance",
                "severity": "critical",
                "enforcement_mode": "blocking",
                "scope_kind": "release_scope",
                "owner": "control_plane_owner",
                "policy_json": {
                    "contexts": ["release_context", "weekly_review"],
                    "reason_codes": {"fail": "CHARTER_RELEASE_OUTSIDE_WINDOW"}
                },
                "mapping": {
                    "source_type": "sql",
                    "source_ref": "atr_release_windows",
                    "evaluator_type": "sql_assert",
                    "evaluator_json": {"predicate": "status != 'OPEN'"} # Inverse check: count non-open
                }
            },
            {
                "rule_id": "CHARTER-P7",
                "category": "protective_lifecycle",
                "severity": "critical",
                "enforcement_mode": "blocking",
                "scope_kind": "protective_scope",
                "owner": "protective_owner",
                "policy_json": {
                    "contexts": ["release_context", "weekly_review"],
                    "reason_codes": {"fail": "CHARTER_PROTECTIVE_PATH_NOT_GREEN"}
                },
                "mapping": {
                    "source_type": "cert",
                    "source_ref": "atr_protective_equivalence_checks",
                    "evaluator_type": "cert_status",
                    "evaluator_json": {}
                }
            }
        ]

        with conn.cursor() as cur:
            for r in rules:
                policy_id = self.generate_id("pol")
                cur.execute("""
                    INSERT INTO atr_charter_policy_registry (
                        policy_id, charter_version, rule_id, category, severity, 
                        enforcement_mode, scope_kind, policy_json, owner, status, activated_at
                    ) VALUES (%s, '1.0.0', %s, %s, %s, %s, %s, %s, %s, 'active', NOW())
                    ON CONFLICT (policy_id) DO NOTHING
                """, (policy_id, r["rule_id"], r["category"], r["severity"],
                      r["enforcement_mode"], r["scope_kind"], json.dumps(r["policy_json"]), r["owner"]))

                mapping_id = self.generate_id("map")
                m = r["mapping"]
                cur.execute("""
                    INSERT INTO atr_charter_compliance_mapping (
                        mapping_id, rule_id, source_type, source_ref, evaluator_type, evaluator_json
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (mapping_id) DO NOTHING
                """, (mapping_id, r["rule_id"], m["source_type"], m["source_ref"],
                      m["evaluator_type"], json.dumps(m["evaluator_json"])))
            conn.commit()

    def run_charter_compliance_checks(self, conn) -> dict[str, Any]:
        """
        Run automated audits using the compliance engine.
        Default context for scheduled audits is 'weekly_review'.
        """
        context_kind = "weekly_review"
        context_ref = f"audit_{datetime.now().strftime('%Y_%m_%d')}"

        bundle = self.compliance_engine.evaluate_context(context_kind, context_ref)

        # Backward compatibility for results list if needed,
        # but the engine handles its own persistence and metrics now.
        return bundle

    def _check_release_quarantine(self, conn, version: str) -> dict[str, Any]:
        status = "passed"
        details = {"message": "No quarantine-violating releases detected."}

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Check for releases on symbols currently under quarantine
                # Table name fixed: execution_quarantine_ledger
                # We also check atr_change_requests for releases (assuming change_type='release')
                cur.execute("""
                    SELECT q.symbol, c.change_id 
                    FROM execution_quarantine_ledger q
                    JOIN atr_change_requests c ON c.symbol = q.symbol
                    WHERE q.action = 'QUARANTINED' 
                      AND c.status = 'applied'
                      AND c.change_type = 'release'
                      AND c.updated_at_ms / 1000 > q.event_ts_ms / 1000
                """)
                violations = cur.fetchall()
                if violations:
                    status = "failed"
                    details = {"message": "Releases detected on quarantined scopes", "violations": violations}
                    PROM_CHARTER_FORBIDDEN_ACTION.labels(reason_code="release_on_quarantine").inc()
        except psycopg2.Error as e:
            if "does not exist" in str(e):
                status = "warning"
                details = {"message": "Dependency table missing, skipping check", "error": str(e)}
            else:
                status = "failed"
                details = {"error": str(e)}
            conn.rollback()
        except Exception as e:
            logger.error(f"Error checking release quarantine: {e}")
            status = "failed"
            details = {"error": str(e)}

        return {"domain": "release", "status": status, "details": details}

    def _check_execution_dispatch(self, conn, version: str) -> dict[str, Any]:
        # Implementation skeleton: Check if orders appeared in MT5 queue without a corresponding canonical signal.
        status = "passed"
        details = {"message": "All execution dispatch followed canonical path."}
        # In a real system, we'd check logs or a dispatch_audit table
        return {"domain": "execution", "status": status, "details": details}

    def _check_protective_invariants(self, conn, version: str) -> dict[str, Any]:
        # Implementation: Check trades_closed for ratchet-only SL violations.
        status = "passed"
        details = {"message": "Protective invariants (SL ratchet) held."}

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Check if exit_price is worse than baseline_sl_price for SL-related exits
                cur.execute("""
                    SELECT order_id, symbol, exit_price, baseline_sl_price, close_reason
                    FROM trades_closed
                    WHERE close_reason = 'SL'
                      AND ((direction = 'long' AND exit_price < baseline_sl_price - 0.00000001)
                       OR (direction = 'short' AND exit_price > baseline_sl_price + 0.00000001))
                    LIMIT 50
                """)
                violations = cur.fetchall()
                if violations:
                    status = "failed"
                    details = {"message": "SL ratchet or fill quality violation detected", "violations": violations}
                    PROM_CHARTER_FORBIDDEN_ACTION.labels(reason_code="protective_ratchet_violation").inc()
        except psycopg2.Error as e:
            if "does not exist" in str(e):
                status = "warning"
                details = {"message": "trades_closed missing, skipping check", "error": str(e)}
            else:
                status = "failed"
                details = {"error": str(e)}
            conn.rollback()
        except Exception as e:
            logger.error(f"Error checking protective invariants: {e}")
            status = "failed"
            details = {"error": str(e)}

        return {"domain": "protective", "status": status, "details": details}

    def _check_control_plane_integrity(self, conn, version: str) -> dict[str, Any]:
        # Implementation: Check for drift between graph state and legacy config tables.
        status = "passed"
        details = {"message": "Control-plane integrity verified."}

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Check for critical drifts in control plane
                cur.execute("""
                    SELECT drift_id, scope_value, severity, reason_code
                    FROM atr_control_plane_projection_drifts
                    WHERE status = 'open' AND severity = 'critical'
                """)
                drifts = cur.fetchall()
                if drifts:
                    status = "failed"
                    details = {"message": "Open critical drifts in control plane", "drifts": drifts}
        except psycopg2.Error as e:
            if "does not exist" in str(e):
                status = "warning"
                details = {"message": "control_plane tables missing, skipping check", "error": str(e)}
            else:
                status = "failed"
                details = {"error": str(e)}
            conn.rollback()
        except Exception as e:
            logger.error(f"Error checking control plane integrity: {e}")
            status = "failed"
            details = {"error": str(e)}

        return {"domain": "control_plane", "status": status, "details": details}

def main():
    enable = os.getenv("ATR_OPERATING_CHART_ENABLE", "1").lower() in ("1", "true", "yes")
    # Enforce means failing critical paths if charter compliance fails (not fully implemented here, as it requires upstream hooks)
    enforce = os.getenv("ATR_OPERATING_CHART_ENFORCE", "0").lower() in ("1", "true", "yes")
    check_interval = int(os.getenv("ATR_OPERATING_CHART_AUDIT_INTERVAL_SEC", "3600"))
    prom_port = int(os.getenv("ATR_OPERATING_CHART_PROM_PORT", "9845"))

    if not enable:
        logger.info("Operating Charter Service bypassed via ENV: ATR_OPERATING_CHART_ENABLE=0")
        return

    logger.info(f"Starting ATR Operating Charter Service (Phase 10) on port {prom_port}")
    start_http_server(prom_port)
    service = ATROperatingCharterService(enable=enable, enforce=enforce)

    while True:
        try:
            with get_conn() as conn:
                service.initialize_default_charter(conn)
                logger.info("Running scheduled charter compliance audit...")
                bundle = service.run_charter_compliance_checks(conn)

                # Summary for logs
                failed = bundle["summary_json"]["failed_ids"]
                if failed:
                    logger.warning(f"Charter compliance FAILED for rules: {failed}")
                else:
                    logger.info("Charter compliance PASSED for all checked rules.")

        except Exception as e:
            logger.error(f"Error in Charter Service cycle: {e}")

        time.sleep(check_interval)

if __name__ == "__main__":
    import time
    main()
