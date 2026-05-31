import json
import logging
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_invariants_registry")

# Initial hardcoded invariants (Source of Truth)
# These follow the taxonomy and rules requested.
INITIAL_INVARIANTS = [
    {
        "invariant_id": "INV_PAYLOAD_BUY_ORDERING",
        "invariant_class": "payload",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "runtime_deny",
        "title": "BUY Ordering",
        "reason_code": "INV_PAYLOAD_BUY_ORDERING",
        "invariant_json": {
            "when": "side == BUY",
            "must_hold": "sl_price < entry_price && entry_price < tp1_price"
        }
    },
    {
        "invariant_id": "INV_PAYLOAD_SELL_ORDERING",
        "invariant_class": "payload",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "runtime_deny",
        "title": "SELL Ordering",
        "reason_code": "INV_PAYLOAD_SELL_ORDERING",
        "invariant_json": {
            "when": "side == SELL",
            "must_hold": "sl_price > entry_price && entry_price > tp1_price"
        }
    },
    {
        "invariant_id": "INV_SIGNAL_ID_REQUIRED",
        "invariant_class": "payload",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "runtime_deny",
        "title": "Signal ID Required",
        "reason_code": "INV_SIGNAL_ID_REQUIRED",
        "invariant_json": {
            "when": "always",
            "must_hold": "signal_id != null && signal_id != ''"
        }
    },
    {
        "invariant_id": "INV_SIGNAL_ID_STABLE_IN_REPLAY",
        "invariant_class": "replay",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "replay_fail",
        "title": "Signal ID Stable In Replay",
        "reason_code": "INV_SIGNAL_ID_STABLE_IN_REPLAY",
        "invariant_json": {
            "when": "replay_check",
            "must_hold": "candidate.signal_id == baseline.signal_id"
        }
    },
    {
        "invariant_id": "INV_TRADEABLE_REQUIRES_NO_HARD_VETO",
        "invariant_class": "gate",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "runtime_deny",
        "title": "Tradeable Requires No Hard Veto",
        "reason_code": "INV_TRADEABLE_REQUIRES_NO_HARD_VETO",
        "invariant_json": {
            "when": "tradeable == true",
            "must_hold": "veto_reason == null"
        }
    },
    {
        "invariant_id": "INV_NO_ORDER_WITHOUT_RISK_PCT",
        "invariant_class": "execution",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "runtime_deny",
        "title": "No Order Without Risk Pct",
        "reason_code": "INV_NO_ORDER_WITHOUT_RISK_PCT",
        "invariant_json": {
            "when": "always",
            "must_hold": "risk_pct > 0 || effective_risk_pct > 0"
        }
    },
    {
        "invariant_id": "INV_NO_ORDER_WITHOUT_SL",
        "invariant_class": "execution",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "runtime_deny",
        "title": "No Order Without SL",
        "reason_code": "INV_NO_ORDER_WITHOUT_SL",
        "invariant_json": {
            "when": "always",
            "must_hold": "sl_price > 0"
        }
    },
    {
        "invariant_id": "INV_NO_LIVE_STAGE_WITHOUT_REPLAY_PASS",
        "invariant_class": "governance",
        "scope_kind": "policy_ver",
        "severity": "critical",
        "enforcement_mode": "release_block",
        "title": "No Live Stage Without Replay Pass",
        "reason_code": "INV_NO_LIVE_STAGE_WITHOUT_REPLAY_PASS",
        "invariant_json": {
            "when": "target_stage == live_*",
            "must_hold": "replay_status == passed"
        }
    },
    {
        "invariant_id": "INV_NO_STAGE_ADVANCE_WITHOUT_ROLLOUT_CERT",
        "invariant_class": "governance",
        "scope_kind": "policy_ver",
        "severity": "critical",
        "enforcement_mode": "release_block",
        "title": "No Stage Advance Without Rollout Cert",
        "reason_code": "INV_NO_STAGE_ADVANCE_WITHOUT_ROLLOUT_CERT",
        "invariant_json": {
            "when": "next_stage_requested",
            "must_hold": "rollout_cert_status == passed"
        }
    },
    {
        "invariant_id": "INV_RELEASE_DENY_ON_CRITICAL_BLOCKER",
        "invariant_class": "governance",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "release_block",
        "title": "Release Deny On Critical Blocker",
        "reason_code": "INV_RELEASE_DENY_ON_CRITICAL_BLOCKER",
        "invariant_json": {
            "when": "release_check",
            "must_hold": "unresolved_critical_invariants == 0"
        }
    },
    {
        "invariant_id": "INV_TRAILING_AFTER_TP1_ONLY",
        "invariant_class": "position",
        "scope_kind": "global",
        "severity": "error",
        "enforcement_mode": "advisory", # Hard to deny pre-dispatch, mostly an audit invariant
        "title": "Trailing After TP1 Only",
        "reason_code": "INV_TRAILING_AFTER_TP1_ONLY",
        "invariant_json": {
            "when": "trailing_active == true",
            "must_hold": "tp1_hit == true"
        }
    },
    {
        "invariant_id": "INV_SL_RATCHET_ONLY",
        "invariant_class": "position",
        "scope_kind": "global",
        "severity": "critical",
        "enforcement_mode": "advisory",
        "title": "SL Ratchet Only (Never Widen)",
        "reason_code": "INV_SL_RATCHET_ONLY",
        "invariant_json": {
            "when": "sl_update_event",
            "must_hold": "new_sl better_or_equal old_sl"
        }
    }
]

def initialize_registry_in_db() -> None:
    """Updates the PostgreSQL tables to ensure the baseline invariants exist."""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            for inv in INITIAL_INVARIANTS:
                cur.execute("""
                    INSERT INTO atr_invariants (
                        invariant_id, invariant_class, scope_kind, severity, 
                        enforcement_mode, title, reason_code, invariant_json, is_enabled
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true)
                    ON CONFLICT (invariant_id) DO UPDATE SET
                        invariant_class = EXCLUDED.invariant_class,
                        severity = EXCLUDED.severity,
                        enforcement_mode = EXCLUDED.enforcement_mode,
                        title = EXCLUDED.title,
                        reason_code = EXCLUDED.reason_code,
                        invariant_json = EXCLUDED.invariant_json
                """, (
                    inv["invariant_id"], inv["invariant_class"], inv["scope_kind"],
                    inv["severity"], inv["enforcement_mode"], inv["title"],
                    inv["reason_code"], json.dumps(inv["invariant_json"])
                ))
            conn.commit()
            logger.info("✅ atr_invariants baseline registry initialized/updated.")
    except Exception as e:
        logger.error(f"Failed to initialize Invariant Registry in DB: {e}")

def get_active_invariants() -> list[dict[str, Any]]:
    """Fetch active invariants from DB. Falls back to static list if DB fails."""
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM atr_invariants WHERE is_enabled = true")
            return cur.fetchall()  # type: ignore
    except Exception as e:
        logger.error(f"Failed to fetch active invariants from DB: {e}")
        return [inv for inv in INITIAL_INVARIANTS]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    initialize_registry_in_db()
