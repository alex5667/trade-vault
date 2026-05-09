import json
import logging
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_invariant_remediation_registry")

def get_active_remediation_policies() -> dict[str, dict[str, Any]]:
    """
    Fetches active remediation policies from postgres.
    Returns dict: invariant_id -> policy dict
    """
    policies = {}
    conn = get_conn()
    if not conn:
        logger.warning("Could not connect to DB for remediation policies.")
        return policies
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT invariant_id, remediation_kind, policy_json
                FROM atr_invariant_remediation_policies
                WHERE is_auto_enabled = true
            """)
            rows = cur.fetchall()
            for row in rows:
                inv_id, kind, pol_json = row
                if isinstance(pol_json, str):
                    try:
                        pol_json = json.loads(pol_json)
                    except Exception:
                        pol_json = {}
                policies[inv_id] = {
                    "remediation_kind": kind,
                    "policy_json": pol_json
                }
    except Exception as e:
        logger.error(f"Error fetching remediation policies: {e}")
    finally:
        conn.close()
    return policies
