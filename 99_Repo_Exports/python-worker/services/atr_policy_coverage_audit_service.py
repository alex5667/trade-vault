"""
ATR Policy Coverage Audit Service (Phase 10.3)

Evaluates policy coverage across all critical surfaces of the trade scanner infrastructure.
Detects gaps in enforcement, evidence, certs, alerts, and protective rollback logic.
Automatically spawns gap closure tasks for critical shortcomings.
"""

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from prometheus_client import Counter, Gauge
from psycopg2.extras import RealDictCursor

from services.analytics_db import get_conn as get_analytics_conn

logger = logging.getLogger(__name__)

# Dimensions of coverage
DIMENSIONS = [
    "RULE_COVERAGE",
    "ENFORCEMENT_COVERAGE",
    "EVIDENCE_COVERAGE",
    "CERT_COVERAGE",
    "ALERT_COVERAGE",
    "ROLLBACK_OR_FREEZE_COVERAGE",
    "REPLAY_COVERAGE",
    "DR_COVERAGE",
    "OWNER_COVERAGE",
    "OBSERVABILITY_COVERAGE"
]

# Gap types
GAP_TYPES = [
    "NO_RULE",
    "NO_ENFORCEMENT",
    "NO_EVIDENCE",
    "NO_CERT",
    "NO_ALERT",
    "NO_ACTION_PATH",
    "NO_REPLAY",
    "NO_DR",
    "NO_OWNER",
    "PARTIAL_COVERAGE",
    "STALE_COVERAGE"
]

# Prometheus Metrics
atr_policy_coverage_results_total = Counter(
    "atr_policy_coverage_results_total",
    "Policy coverage results",
    ["dimension", "status", "severity"]
)
atr_policy_gap_closure_total = Counter(
    "atr_policy_gap_closure_total",
    "Policy gap closures",
    ["gap_type", "severity", "remediation_status"]
)
atr_policy_coverage_audit_total = Counter(
    "atr_policy_coverage_audit_total",
    "Overall coverage audits",
    ["overall_status", "scope_kind"]
)
atr_policy_coverage_critical_open_total = Gauge(
    "atr_policy_coverage_critical_open_total",
    "Open critical gaps per domain",
    ["domain"]
)
atr_policy_coverage_waiver_total = Counter(
    "atr_policy_coverage_waiver_total",
    "Waivers processed for coverage gaps",
    ["severity", "status"]
)


class ATRPolicyCoverageAuditService:
    def __init__(self, db_conn=None):
        """Initialize the coverage audit service."""
        # Use provided connection for testing, otherwise fallback to standard analytics get_conn()
        self._conn_factory = lambda: db_conn if db_conn else get_analytics_conn()

        # We optionally respect enforcement flags
        self.audit_enable = os.getenv("ATR_POLICY_COVERAGE_AUDIT_ENABLE", "1") == "1"
        self.audit_enforce = os.getenv("ATR_POLICY_COVERAGE_AUDIT_ENFORCE", "0") == "1"

    def _execute_write(self, query: str, params: tuple):
        """Execute a write query."""
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB Write Error: {e} - Query: {query}")
            raise
        finally:
            if not getattr(conn, '_is_test_mock', False) and db_conn is None:
                conn.close()

    def _execute_read(self, query: str, params: tuple = None) -> list[dict[str, Any]]:
        """Execute a read query and return dicts."""
        conn = self._conn_factory()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"DB Read Error: {e} - Query: {query}")
            raise
        finally:
            if not getattr(conn, '_is_test_mock', False) and db_conn is None:
                conn.close()

    def load_surface_inventory(self, domain: str | None = None) -> list[dict[str, Any]]:
        """Load surfaces from the inventory table."""
        if domain:
            query = "SELECT * FROM atr_policy_coverage_inventory WHERE domain = %s"
            return self._execute_read(query, (domain,))
        return self._execute_read("SELECT * FROM atr_policy_coverage_inventory")

    def _determine_severity(self, surface_id: str, dimension: str, status: str, gap_type: str) -> str:
        """Apply strict critical gap mapping based on surface vs dimension."""
        if status == "covered":
            return "info"

        # Specific overrides for critical mappings
        critical_mappings = {
            "runtime_allow_clip_deny": ["NO_ENFORCEMENT"],
            "order_queue_dispatch": ["NO_RULE", "NO_ACTION_PATH"],
            "risk_sizing_contract": ["NO_EVIDENCE", "NO_REPLAY"],
            "sl_ratchet_invariant": ["NO_CERT", "NO_ACTION_PATH"],
            "quarantine_gate": ["NO_RULE", "NO_ENFORCEMENT"],
            "dr_restore": ["NO_DR", "NO_OWNER"],
            "graph_consistency": ["NO_ALERT", "NO_ACTION_PATH"]  # NO_BLOCKING_ACTION roughly aligns to NO_ACTION_PATH
        }

        if surface_id in critical_mappings:
            if gap_type in critical_mappings[surface_id]:
                return "critical"

        # General taxonomy severity mappings
        if dimension in ["ROLLBACK_OR_FREEZE_COVERAGE", "ENFORCEMENT_COVERAGE", "RULE_COVERAGE"]:
            if gap_type in ["NO_ACTION_PATH", "NO_ENFORCEMENT", "NO_RULE"]:
                return "critical"

        if dimension in ["CERT_COVERAGE", "EVIDENCE_COVERAGE", "REPLAY_COVERAGE"]:
            if status == "missing" or status == "partial":
                return "error"

        if dimension in ["OBSERVABILITY_COVERAGE", "ALERT_COVERAGE"]:
            return "warn"

        return "error"

    def evaluate_surface_coverage(self, surface: dict[str, Any], evaluation_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Evaluate a surface against all dimensions and return results."""
        results = []
        surface_id = surface["surface_id"]

        for dim in DIMENSIONS:
            # Look up evaluation data for this dimension
            eval_val = evaluation_data.get(dim, {})
            status = eval_val.get("status", "missing")
            gap_type = eval_val.get("gap_type", f"NO_{dim.replace('_COVERAGE', '')}")

            # If covered, then gap type is null equivalent
            if status == "covered":
                gap_type = "NONE"

            severity = self._determine_severity(surface_id, dim, status, gap_type)

            result_id = str(uuid.uuid4())
            result_dict = {
                "result_id": result_id,
                "surface_id": surface_id,
                "dimension": dim,
                "status": status,
                "severity": severity,
                "reason_code": eval_val.get("reason_code", f"AUTO_{gap_type}"),
                "details_json": {"measured_at": datetime.now(UTC).isoformat(), "eval_data": eval_val}
            }
            results.append(result_dict)

            # Store in DB
            q = """
            INSERT INTO atr_policy_coverage_results
            (result_id, surface_id, dimension, status, severity, reason_code, details_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (result_id) DO NOTHING
            """
            self._execute_write(q, (
                result_id, surface_id, dim, status, severity,
                result_dict["reason_code"], json.dumps(result_dict["details_json"])
            ))

            # Metrics
            atr_policy_coverage_results_total.labels(dimension=dim, status=status, severity=severity).inc()

            # Automatically spawn gap closure items for missing/partial entries
            if status != "covered":
                self.open_gap_closure_item(surface_id, gap_type, severity, surface.get("owner", "unknown"))

        return results

    def open_gap_closure_item(self, surface_id: str, gap_type: str, severity: str, owner: str) -> str:
        """Create a gap remediation item."""
        # Check if one already exists that isn't verified or waived
        q_check = """
        SELECT row_id, remediation_status FROM atr_policy_gap_closure_matrix 
        WHERE surface_id = %s AND gap_type = %s 
        AND remediation_status NOT IN ('verified', 'waived')
        LIMIT 1
        """
        existing = self._execute_read(q_check, (surface_id, gap_type))
        if existing:
            return existing[0]["row_id"]

        row_id = str(uuid.uuid4())
        remediation_status = "open"
        q = """
        INSERT INTO atr_policy_gap_closure_matrix
        (row_id, surface_id, gap_type, severity, owner, remediation_status, remediation_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self._execute_write(q, (
            row_id, surface_id, gap_type, severity, owner, remediation_status,
            json.dumps({"source": "automatic_coverage_audit"})
        ))

        atr_policy_gap_closure_total.labels(
            gap_type=gap_type, severity=severity, remediation_status=remediation_status
        ).inc()

        logger.warning(f"ATR Gap Closure Item Opened - Surface: {surface_id}, Gap: {gap_type}, Severity: {severity}, Owner: {owner}")

        return row_id

    def waive_gap_closure_item(self, row_id: str, reason: str) -> bool:
        """Waive a gap closure item, subject to strict critical gap rules."""
        q_get = "SELECT surface_id, gap_type, severity, remediation_status FROM atr_policy_gap_closure_matrix WHERE row_id = %s"
        existing = self._execute_read(q_get, (row_id,))
        if not existing:
            return False

        gap = existing[0]
        severity = gap["severity"]
        surface_id = gap["surface_id"]

        # Forbidden waivers logic
        forbidden_surfaces = ["runtime_allow_clip_deny", "mt5_execution", "binance_execution", "sl_ratchet_invariant", "break_even_transition", "quarantine_gate", "dr_restore"]

        if severity == "critical" and surface_id in forbidden_surfaces:
            logger.error(f"Waiver REJECTED for {row_id}: Critical gaps on {surface_id} cannot be waived.")
            atr_policy_coverage_waiver_total.labels(severity=severity, status="rejected").inc()
            return False

        q_update = """
        UPDATE atr_policy_gap_closure_matrix 
        SET remediation_status = 'waived', remediation_json = remediation_json || %s, closed_at = now()
        WHERE row_id = %s
        """
        waive_data = json.dumps({"waived_reason": reason, "waived_at": datetime.now(UTC).isoformat()})
        self._execute_write(q_update, (waive_data, row_id))

        atr_policy_coverage_waiver_total.labels(severity=severity, status="approved").inc()
        return True

    def build_gap_matrix(self) -> list[dict[str, Any]]:
        """Return the current open gap matrix."""
        q = """
        SELECT * FROM atr_policy_gap_closure_matrix 
        WHERE remediation_status NOT IN ('verified', 'waived')
        ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'error' THEN 2 ELSE 3 END
        """
        return self._execute_read(q)

    def compute_coverage_audit(self, scope_kind: str, scope_value: str, surface_eval_data: dict[str, dict[str, Any]]) -> dict[str, Any]:
        """
        Conduct a periodic/release audit.
        surface_eval_data: dict of surface_id -> dict of dimension -> evaluation data
        """
        if not self.audit_enable:
            logger.info("Policy Coverage Audit is disabled.")
            return {"overall_status": "passed", "message": "disabled"}

        # 1. Update inventories and calculate gaps
        inventory = self.load_surface_inventory(domain=scope_value if scope_kind == "domain" else None)

        critical_count = 0
        error_count = 0
        warn_count = 0

        for surface in inventory:
            s_id = surface["surface_id"]
            eval_data = surface_eval_data.get(s_id, {})
            results = self.evaluate_surface_coverage(surface, eval_data)

            for res in results:
                if res["severity"] == "critical": critical_count += 1
                elif res["severity"] == "error": error_count += 1
                elif res["severity"] == "warn": warn_count += 1

        # 2. Re-read total open gaps for the scope to be sure
        gaps = self.build_gap_matrix()

        # Determine overall status
        if critical_count > 0:
            overall_status = "failed"
        elif error_count > 0 or warn_count > 0:
            overall_status = "warning"
        else:
            overall_status = "passed"

        audit_id = str(uuid.uuid4())
        summary = {
            "critical_gaps_found": critical_count,
            "error_gaps_found": error_count,
            "warning_gaps_found": warn_count,
            "total_open_gaps": len(gaps),
            "enforced": self.audit_enforce
        }

        q = """
        INSERT INTO atr_policy_coverage_audits
        (audit_id, scope_kind, scope_value, overall_status, summary_json)
        VALUES (%s, %s, %s, %s, %s)
        """
        self._execute_write(q, (audit_id, scope_kind, scope_value, overall_status, json.dumps(summary)))

        atr_policy_coverage_audit_total.labels(overall_status=overall_status, scope_kind=scope_kind).inc()

        # Log outcome for Telegram UX compatibility
        if overall_status == "failed":
            logger.error(f"ATR Policy Coverage Audit FAILED | Scope: {scope_kind}={scope_value} | Critical gaps: {critical_count}")
        elif overall_status == "warning":
            logger.warning(f"ATR Policy Coverage Audit PASSED with WARNINGS | Scope: {scope_kind}={scope_value} | Errors: {error_count}, Warns: {warn_count}")
        else:
            logger.info(f"ATR Policy Coverage Audit PASSED | Scope: {scope_kind}={scope_value}")

        return {
            "audit_id": audit_id,
            "overall_status": overall_status,
            "summary": summary
        }
