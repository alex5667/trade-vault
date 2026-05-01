#!/usr/bin/env python3
"""
Phase 8.7 — Bootstrap Graph Consistency Gate
Creates tables and views for the global release blocker proxy.
"""
import logging
import sys
import os

# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from services.analytics_db import get_conn

logger = logging.getLogger("bootstrap_phase87")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def bootstrap():
    queries = [
        """
        CREATE TABLE IF NOT EXISTS atr_graph_consistency_gate_checks (
            check_id text PRIMARY KEY,
            change_id text NOT NULL,
            scope_value text NOT NULL,
            risk_level text NOT NULL,
            graph_score double precision NOT NULL,
            decision text NOT NULL,
            blockers_json jsonb NOT NULL,
            warnings_json jsonb NOT NULL,
            summary_json jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
        );
        """
        """
        CREATE TABLE IF NOT EXISTS atr_graph_consistency_drifts (
            drift_id text PRIMARY KEY,
            change_id text,
            scope_value text NOT NULL,
            drift_family text NOT NULL,
            drift_kind text NOT NULL,
            severity text NOT NULL,
            status text NOT NULL,
            reason_code text NOT NULL,
            drift_json jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            resolved_at timestamptz
        );
        """
        """
        CREATE TABLE IF NOT EXISTS atr_graph_consistency_waivers (
            waiver_id text PRIMARY KEY,
            drift_id text NOT NULL,
            approver text NOT NULL,
            reason_code text NOT NULL,
            ttl_sec integer NOT NULL,
            not_after timestamptz NOT NULL,
            waiver_json jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            expired_at timestamptz
        );
        """
        """
        CREATE OR REPLACE VIEW v_governance_graph_consistency_gate_board AS
        SELECT
            change_id,
            scope_value,
            risk_level,
            graph_score,
            decision,
            created_at
        FROM atr_graph_consistency_gate_checks
        ORDER BY created_at DESC;
        """
        """
        CREATE OR REPLACE VIEW v_governance_graph_consistency_drift_board AS
        SELECT
            change_id,
            scope_value,
            drift_family,
            drift_kind,
            severity,
            status,
            created_at
        FROM atr_graph_consistency_drifts
        WHERE status = 'open'
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 1
                WHEN 'error' THEN 2
                ELSE 3
            END,
            created_at DESC;
        """
    ]

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for q in queries:
                    cur.execute(q)
            conn.commit()
            logger.info("Successfully bootstrapped Phase 8.7 tables and views.")
            
            # Additional check: Attempt to grant trading user privs
            try:
                with conn.cursor() as cur:
                    cur.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading;")
                    cur.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading;")
                conn.commit()
                logger.info("Granted privileges to 'trading' role if it exists.")
            except Exception as w:
                logger.warning(f"Could not grant explicit privileges, safe to ignore if running as superuser/owner: {w}")

    except Exception as e:
        logger.error(f"Error bootstrapping graph consistency schema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    bootstrap()
