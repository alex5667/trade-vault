#!/usr/bin/env python3
"""
ATR Go-Live Readiness Service (Phase 10.5)
Final package computation for steady-state go-live approval.
"""

import json
import os
import uuid
from datetime import UTC, datetime, timedelta

from prometheus_client import Counter, Gauge, start_http_server
from psycopg2.extras import RealDictCursor

from common.log import setup_logger
from services.analytics_db import get_conn

logger = setup_logger("atr_go_live_readiness")

# --- Prometheus Metrics ---
PROM_GO_LIVE_PACKAGES = Gauge("atr_go_live_packages_total", "Count of packages by status and verdict", ["package_status", "verdict"])
PROM_GO_LIVE_DOMAIN_CHECKS = Counter("atr_go_live_domain_checks_total", "Domain checks by status", ["domain", "status", "severity"])
PROM_GO_LIVE_SIGNOFFS = Counter("atr_go_live_signoffs_total", "Signoffs by role and status", ["signer_role", "status"])
PROM_GO_LIVE_CONSTRAINTS = Gauge("atr_go_live_constraints_total", "Active constrained packages")
PROM_GO_LIVE_REJECTIONS = Counter("atr_go_live_rejections_total", "Rejections by reason", ["reason_code"])

REQUIRED_DOMAINS = [
    "signal_and_gates",
    "dispatch_and_runtime",
    "execution",
    "protective_lifecycle",
    "control_plane_governance",
    "dr_replay_archive"
]

REQUIRED_ROLES = [
    "runtime_owner",
    "execution_owner",
    "protective_owner",
    "control_plane_owner",
    "oncall",
    "technical_owner"
]

class ATRGoLiveReadinessService:
    def __init__(self, enable: bool = True, enforce: bool = False):
        self.enable = enable
        self.enforce = enforce

    def generate_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    def build_go_live_package(self, conn, target_scope: str, charter_version: str) -> str:
        package_id = self.generate_id("golive")
        logger.info(f"Building initial Go-Live Package: {package_id} for scope: {target_scope}")

        # Insert draft
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_go_live_readiness_packages (
                    package_id, target_scope, charter_version, package_status, verdict, summary_json, signed_at, expires_at
                ) VALUES (%s, %s, %s, 'draft', 'PENDING', %s, NULL, NULL)
            """, (package_id, target_scope, charter_version, json.dumps({})))
            conn.commit()

        PROM_GO_LIVE_PACKAGES.labels(package_status="draft", verdict="PENDING").inc()
        return package_id

    def _assess_domain(self, domain: str, severity: str, status: str, details: dict, conn, package_id: str):
        check_id = self.generate_id("chk")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_go_live_readiness_checks (
                    check_id, package_id, domain, check_name, status, severity, details_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (check_id, package_id, domain, f"{domain}_eval", status, severity, json.dumps(details)))
        PROM_GO_LIVE_DOMAIN_CHECKS.labels(domain=domain, status=status, severity=severity).inc()

    def evaluate_readiness_domains(self, conn, package_id: str, evidence: dict):
        """Evaluate domains based on provided evidence."""
        # This is a reference implementation evaluating explicit rules

        with conn.cursor() as cur:
            # 1. signal_and_gates
            sig_status = "passed"
            sig_sev = "info"
            if evidence.get("veto_mix_error") or evidence.get("active_critical_gate_drift"):
                sig_status = "failed"
                sig_sev = "critical"

            self._assess_domain("signal_and_gates", sig_sev, sig_status, evidence.get("signal_and_gates", {}), conn, package_id)

            # 2. dispatch_and_runtime
            disp_status = "passed"
            disp_sev = "info"
            if evidence.get("unknown_order_bypass") or evidence.get("critical_allow_deny_drift"):
                disp_status = "failed"
                disp_sev = "critical"

            self._assess_domain("dispatch_and_runtime", disp_sev, disp_status, evidence.get("dispatch_and_runtime", {}), conn, package_id)

            # 3. execution
            exec_status = "passed"
            exec_sev = "info"
            if evidence.get("venue_instability") or evidence.get("slippage_regime_violation"):
                exec_status = "failed"
                exec_sev = "critical"
            elif evidence.get("execution_yellow"):
                exec_status = "warning"
                exec_sev = "warn"

            self._assess_domain("execution", exec_sev, exec_status, evidence.get("execution", {}), conn, package_id)

            # 4. protective_lifecycle
            prot_status = "passed"
            prot_sev = "info"
            if evidence.get("protective_drift") or evidence.get("be_before_tp1_violation"):
                prot_status = "failed"
                prot_sev = "critical"

            self._assess_domain("protective_lifecycle", prot_sev, prot_status, evidence.get("protective", {}), conn, package_id)

            # 5. control_plane_governance
            cp_status = "passed"
            cp_sev = "info"
            if evidence.get("charter_compliance_fail") or evidence.get("coverage_audit_fail_critical"):
                cp_status = "failed"
                cp_sev = "critical"

            self._assess_domain("control_plane_governance", cp_sev, cp_status, evidence.get("control_plane", {}), conn, package_id)

            # 6. dr_replay_archive
            dr_status = "passed"
            dr_sev = "info"
            if evidence.get("dr_restore_fail") or evidence.get("invalid_golden_datasets"):
                dr_status = "failed"
                dr_sev = "critical"

            self._assess_domain("dr_replay_archive", dr_sev, dr_status, evidence.get("dr_replay", {}), conn, package_id)

            conn.commit()

    def collect_required_evidence(self, conn) -> dict:
        """Simulate or query real systems for evidence collection."""
        # For a full live integration, we query atr_operating_charters, atr_charter_compliance_checks, etc.
        # This acts as a stub returning perfectly healthy defaults
        return {
            "execution_yellow": False,
            "dr_restore_fail": False,
            "charter_compliance_fail": False,
            "protective_drift": False
        }

    def request_signoffs(self, conn, package_id: str, signoffs_data: dict):
        """Submit signoffs."""
        with conn.cursor() as cur:
            for role, data in signoffs_data.items():
                status = data.get("status", "rejected")
                signer = data.get("signer", "system")
                signoff_id = self.generate_id("sign")
                cur.execute("""
                    INSERT INTO atr_go_live_signoffs (
                        signoff_id, package_id, signer_role, signer, status, signoff_json
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (signoff_id, package_id, role, signer, status, json.dumps(data)))
                PROM_GO_LIVE_SIGNOFFS.labels(signer_role=role, status=status).inc()
            conn.commit()

    def compute_final_go_live_verdict(self, conn, package_id: str, constraints_block: dict | None = None) -> str:
        """Calculate and finalize the decision."""

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Check domains
            cur.execute("""
                SELECT domain, status, severity FROM atr_go_live_readiness_checks
                WHERE package_id = %s
            """, (package_id,))
            checks = cur.fetchall()

            critical_fails = sum(1 for c in checks if c["status"] == "failed" and c["severity"] == "critical")
            warnings = sum(1 for c in checks if c["status"] == "warning")
            incomplete_domains = len(REQUIRED_DOMAINS) - len(set(c["domain"] for c in checks))

            # 2. Check signoffs
            cur.execute("""
                SELECT signer_role, status FROM atr_go_live_signoffs
                WHERE package_id = %s
            """, (package_id,))
            signoffs = cur.fetchall()

            approved_roles = set(s["signer_role"] for s in signoffs if s["status"] == "approved")
            rejected_roles = set(s["signer_role"] for s in signoffs if s["status"] == "rejected")

            # Determine verdict
            verdict = "GO_LIVE"
            package_status = "signed"

            if "protective_owner" in rejected_roles or "technical_owner" in rejected_roles or "execution_owner" in rejected_roles or "control_plane_owner" in rejected_roles:
                verdict = "NO_GO"

            if "protective_owner" in rejected_roles and critical_fails > 0:
                # Based on requirement: protective_owner reject + critical drift -> NO_GO or ROLLBACK_ONLY
                verdict = "ROLLBACK_ONLY"

            if verdict not in ["NO_GO", "ROLLBACK_ONLY"]:
                if critical_fails > 0:
                    verdict = "NO_GO"
                elif incomplete_domains > 0 or len(approved_roles) < len(REQUIRED_ROLES):
                    verdict = "HOLD"
                elif warnings > 0:
                    if constraints_block:
                        verdict = "GO_LIVE_WITH_CONSTRAINTS"
                    else:
                        verdict = "HOLD" # Missing constrained definitions but warnings exist

            if verdict == "GO_LIVE" and constraints_block:
                # Might have constraints anyway
                verdict = "GO_LIVE_WITH_CONSTRAINTS"

            if verdict == "GO_LIVE_WITH_CONSTRAINTS" and not constraints_block:
                 verdict = "HOLD" # Invalid: needs constraints

            if verdict in ("NO_GO", "ROLLBACK_ONLY"):
                package_status = "rejected"
                PROM_GO_LIVE_REJECTIONS.labels(reason_code=verdict).inc()

            expires_at = datetime.now(UTC)
            if verdict == "GO_LIVE":
                expires_at += timedelta(days=30)
            elif verdict == "GO_LIVE_WITH_CONSTRAINTS":
                expires_at += timedelta(days=7)
                PROM_GO_LIVE_CONSTRAINTS.inc()
            else:
                expires_at += timedelta(days=1)

            summary = {
                "critical_fails": critical_fails,
                "warnings": warnings,
                "missing_domains": incomplete_domains,
                "approved_roles": list(approved_roles),
                "constraints": constraints_block
            }

            cur.execute("""
                UPDATE atr_go_live_readiness_packages
                SET package_status = %s, verdict = %s, summary_json = %s,
                    signed_at = NOW(), expires_at = %s
                WHERE package_id = %s
            """, (package_status, verdict, json.dumps(summary), expires_at, package_id))

            conn.commit()

            PROM_GO_LIVE_PACKAGES.labels(package_status=package_status, verdict=verdict).inc()
            logger.info(f"Package {package_id} finalized with verdict: {verdict}")

            return verdict

    def check_active_package(self, conn, target_scope: str = "global") -> bool:
        """Helper for Compliance Engine to verify go-live status."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*) FROM atr_go_live_readiness_packages
                WHERE target_scope = %s 
                  AND verdict IN ('GO_LIVE', 'GO_LIVE_WITH_CONSTRAINTS')
                  AND package_status = 'signed'
                  AND (expires_at IS NULL OR expires_at > NOW())
            """, (target_scope,))
            count = cur.fetchone()[0]
            return count > 0

    def ensure_weekly_draft(self, conn, target_scope: str = "global"):
        """
        Weekly automation: ensures a DRAFT package exists for the current ISO week.
        Creates one if missing.
        """
        # ISO week identifier (e.g., 2026-W16)
        week_id = datetime.now().strftime("%G-W%V")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check if any package (draft or signed) exists for this week
            cur.execute("""
                SELECT package_id FROM atr_go_live_readiness_packages
                WHERE target_scope = %s AND (summary_json->>'week_id') = %s
                LIMIT 1
            """, (target_scope, week_id))

            if not cur.fetchone():
                logger.info(f"Mandatory Weekly Check: No package for {week_id}. Creating DRAFT.")
                pkg_id = self.build_go_live_package(conn, target_scope, "1.0.0")

                # Tag it with week_id and run initial evidence collection
                with conn.cursor() as cur2:
                    cur2.execute("""
                        UPDATE atr_go_live_readiness_packages
                        SET summary_json = summary_json || jsonb_build_object('week_id', %s)
                        WHERE package_id = %s
                    """, (week_id, pkg_id))
                    conn.commit()

                # Run initial evaluation to highlight gaps early
                evidence = self.collect_required_evidence(conn)
                self.evaluate_readiness_domains(conn, pkg_id, evidence)
                logger.info(f"Weekly DRAFT {pkg_id} for {week_id} initialized with baseline evidence.")
            else:
                logger.debug(f"Weekly Go-Live package for {week_id} already exists.")


def run_mock_ceremony(conn, service: ATRGoLiveReadinessService):
    """Run an advisory-mode mock ceremony for go-live package generation."""
    try:
        pkg_id = service.build_go_live_package(conn, target_scope="global", charter_version="1.0.0")
        ev = service.collect_required_evidence(conn)
        service.evaluate_readiness_domains(conn, pkg_id, ev)

        signoffs = {role: {"status": "approved", "signer": f"{role}_mock"} for role in REQUIRED_ROLES}
        service.request_signoffs(conn, pkg_id, signoffs)

        v = service.compute_final_go_live_verdict(conn, pkg_id)
        logger.info(f"Mock Ceremony Complete. Final verdict: {v}")
    except Exception as e:
        logger.error(f"Error during mock ceremony: {e}")
        conn.rollback()

if __name__ == "__main__":
    import time
    enable = os.getenv("ATR_GO_LIVE_READINESS_ENABLE", "1").lower() in ("1", "true", "yes")
    enforce = os.getenv("ATR_GO_LIVE_READINESS_ENFORCE", "0").lower() in ("1", "true", "yes")
    prom_port = int(os.getenv("ATR_GO_LIVE_READINESS_PROM_PORT", "9883"))

    if not enable:
        logger.info("ATR Go-Live Readiness Service bypassed via ENV: ATR_GO_LIVE_READINESS_ENABLE=0")
        exit(0)

    start_http_server(prom_port)
    logger.info(f"Starting ATR Go-Live Readiness Service on port {prom_port} (enforce={enforce})")

    svc = ATRGoLiveReadinessService(enable=enable, enforce=enforce)

    # Run once at startup then periodically
    last_weekly_check = 0.0

    while True:
        try:
            with get_conn() as conn:
                # 1. Weekly Draft Automation
                if time.monotonic() - last_weekly_check > 3600: # Check once per hour
                    svc.ensure_weekly_draft(conn)
                    last_weekly_check = time.monotonic()

                # 2. Cleanup or other maintenance could go here
                pass
        except Exception as e:
            logger.error(f"Go-Live Loop Error: {e}")
        time.sleep(60)
