import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_invariant_budget")

def _generate_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

def evaluate_budgets(time_window_ms: int = None) -> list[dict[str, Any]]:  # type: ignore
    """
    Evaluates budget consumption for all enabled SLO policies.
    """
    now_ms = int(time.time() * 1000)
    actions_triggered = []

    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            # 1. Fetch enabled policies
            cur.execute("SELECT * FROM atr_invariant_slo_policies WHERE is_enabled = true")
            policies = cur.fetchall()

            for policy in policies:
                window_sec = policy["window_sec"]  # type: ignore
                window_start_ms = now_ms - (window_sec * 1000)

                # Fetch violations grouped by scope matching this policy
                class_filter_sql = ""
                class_filter_args = []
                if policy["invariant_class"] != "*":  # type: ignore
                    class_filter_sql = "AND i.invariant_class = %s"
                    class_filter_args.append(policy["invariant_class"])  # type: ignore

                query = f"""
                    SELECT v.scope_kind, v.scope_value, count(*) as count
                    FROM atr_invariant_violations v
                    JOIN atr_invariants i ON v.invariant_id = i.invariant_id
                    WHERE v.surface = %s 
                      AND i.severity = %s
                      {class_filter_sql}
                      AND v.created_at_ms >= %s
                    GROUP BY v.scope_kind, v.scope_value
                """
                args = [policy["surface"], policy["severity"]] + class_filter_args + [window_start_ms]  # type: ignore
                cur.execute(query, tuple(args))
                grouped_violations = cur.fetchall()

                for group in grouped_violations:
                    count = group["count"]  # type: ignore
                    max_violations = policy["max_violations"]  # type: ignore
                    burn_rate = count / max_violations if max_violations > 0 else 0.0

                    status = "healthy"
                    if burn_rate >= policy["burn_rate_critical"]:  # type: ignore
                        status = "exhausted"
                    elif burn_rate >= policy["burn_rate_warn"]:  # type: ignore
                        status = "warning"

                    # Generate a deterministic state ID based on window to throttle actions
                    # Not strictly unique over time, but good enough to track current state
                    state_id = f"s_{policy['policy_id']}_{group['scope_kind']}_{group['scope_value']}"  # type: ignore

                    summary = {
                        "policy_id": policy["policy_id"],  # type: ignore
                        "window_sec": window_sec
                    }

                    # Upsert state
                    cur.execute("""
                        INSERT INTO atr_invariant_budget_states (
                            state_id, invariant_class, surface, severity, scope_kind, scope_value,
                            window_sec, violations_count, max_violations, burn_rate, budget_status, summary_json, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (state_id) DO UPDATE SET
                            violations_count = EXCLUDED.violations_count,
                            burn_rate = EXCLUDED.burn_rate,
                            budget_status = EXCLUDED.budget_status,
                            summary_json = EXCLUDED.summary_json,
                            updated_at = now()
                    """, (
                        state_id, policy["invariant_class"], policy["surface"], policy["severity"],  # type: ignore
                        group["scope_kind"], group["scope_value"], window_sec, count, max_violations,  # type: ignore
                        burn_rate, status, json.dumps(summary)
                    ))

                    # Trigger auto-action if exhausted
                    if status == "exhausted" and policy["auto_action"] != "none":  # type: ignore
                        # Ensure we haven't triggered this action recently (e.g. within this window)
                        action_cutoff = now_ms - (window_sec * 1000)
                        cur.execute("""
                            SELECT count(*) as c FROM atr_invariant_budget_actions 
                            WHERE state_id = %s 
                              AND auto_action = %s
                              AND created_at >= to_timestamp(%s)
                        """, (state_id, policy["auto_action"], action_cutoff / 1000.0))  # type: ignore

                        action_exists = cur.fetchone()["c"] > 0  # type: ignore
                        if not action_exists:
                            action_id = _generate_id("act")
                            cur.execute("""
                                INSERT INTO atr_invariant_budget_actions (
                                    action_id, state_id, auto_action, status, reason_code, action_json
                                ) VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                action_id, state_id, policy["auto_action"], "requested",  # type: ignore
                                "EXHAUSTED_SLO_BUDGET", json.dumps({"burn_rate": burn_rate})
                            ))
                            actions_triggered.append({
                                "action_id": action_id,
                                "auto_action": policy["auto_action"],  # type: ignore
                                "scope_kind": group["scope_kind"],  # type: ignore
                                "scope_value": group["scope_value"]  # type: ignore
                            })

            conn.commit()
    except Exception as e:
        logger.error(f"Error evaluating budgets: {e}")

    return actions_triggered

def record_synthetic_burn(surface: str, severity: str, scope_kind: str, scope_value: str, reason_code: str):
    """
    Inserts a synthetic violation into atr_invariant_violations to artificially increment the burn rate.
    Uses 'governance' as the default class if an explicit class isn't provided.
    """
    try:
        violation_id = _generate_id("syn_viol")
        now_ms = int(time.time() * 1000)

        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            # First, try to find an invariant ID for the synthetic violation.
            # We'll grab a dummy or generic governance one, or insert a synthetic invariant.
            synthetic_inv_id = "INV_SYNTHETIC_BUDGET_BURN"
            cur.execute("""
                INSERT INTO atr_invariants (invariant_id, invariant_class, scope_kind, severity, enforcement_mode, title, reason_code, invariant_json)
                VALUES (%s, 'governance', %s, %s, 'observe', 'Synthetic Budget Burn', %s, '{}')
                ON CONFLICT (invariant_id) DO NOTHING
            """, (synthetic_inv_id, scope_kind, severity, reason_code))

            cur.execute("""
                INSERT INTO atr_invariant_violations (
                    violation_id, invariant_id, scope_kind, scope_value, surface, 
                    severity, status, reason_code, violation_json, created_at_ms, check_mode
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                violation_id, synthetic_inv_id, scope_kind, scope_value, surface,
                severity, "open", reason_code, json.dumps({"source": "synthetic_burn"}), now_ms, "enforce"
            ))
            conn.commit()

    except Exception as e:
        logger.error(f"Failed to record synthetic burn: {e}")
