import json
import logging
import time
from datetime import datetime
from typing import Any

from services.analytics_db import get_conn
from services.atr_rollout_cert_telegram import (
    send_cert_outcome_message,
    send_cert_start_message,
    send_closeout_pack_ready,
    send_stop_condition_message,
)

logger = logging.getLogger("atr_rollout_cert_service")

# --- Thresholds defaults ---
DEFAULT_THRESHOLDS = {
    "canary_5": {
        "min_n_trades": 20,
        "min_avg_pnl_bps": -2.0,
        "max_avg_slippage_bps": 6.0,
        "max_stop_rate": 0.60,
        "min_tp1_rate": 0.25,
        "max_avg_mae_pct": 0.02,
        "min_hours": 12,
        "max_hours": 24
    },
    "canary_25": {
        "min_n_trades": 50,
        "min_avg_pnl_bps": 0.0,
        "max_avg_slippage_bps": 5.0,
        "max_stop_rate": 0.55,
        "min_tp1_rate": 0.35,
        "max_avg_mae_pct": 0.015,
        "min_hours": 24,
        "max_hours": 48
    },
    "live_100": {
        "min_n_trades": 100,
        "min_avg_pnl_bps": 0.5,
        "max_avg_slippage_bps": 4.5,
        "max_stop_rate": 0.50,
        "min_tp1_rate": 0.40,
        "max_avg_mae_pct": 0.01,
        "min_hours": 48,
        "max_hours": 72
    }
}

def create_certification(
    cert_id: str,
    change_id: str,
    rollout_stage: str,
    policy_ver: int,
    monitoring_window_from: datetime,
    monitoring_window_to: datetime,
    thresholds: dict[str, Any] = None
) -> bool:
    """Initialize a new certification tracker."""
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS.get(rollout_stage, DEFAULT_THRESHOLDS["canary_5"])

    sql = """
        INSERT INTO atr_rollout_certifications (
            cert_id, change_id, rollout_stage, scope_kind, status, 
            policy_ver,
            monitoring_window_from, monitoring_window_to,
            thresholds_json, checks_json, summary_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cert_id) DO NOTHING
    """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (
                cert_id, change_id, rollout_stage, "policy", "pending", policy_ver,
                monitoring_window_from, monitoring_window_to,
                json.dumps(thresholds), "{}", "{}"
            ))

            cur.execute("""
                INSERT INTO atr_rollout_cert_events (cert_id, change_id, rollout_stage, action, reason_code, event_json)
                VALUES (%s, %s, %s, 'start', 'CERT_INIT', %s)
            """, (cert_id, change_id, rollout_stage, json.dumps({"thresholds": thresholds})))
            conn.commit()

            send_cert_start_message(change_id, rollout_stage, thresholds)
            return True
    except Exception as e:
        logger.error(f"Failed to create certification for {change_id}: {e}")
        return False

def get_post_trade_truth(policy_ver: int, t_from: datetime, t_to: datetime) -> dict[str, float]:
    """Query closed_trades for the real outcomes."""
    sql = """
        SELECT 
            COUNT(*) as n_trades,
            AVG(pnl_net_bps) as avg_pnl_bps,
            AVG(entry_slippage_bps + exit_slippage_bps) as avg_slippage_bps,
            AVG(CASE WHEN tp1_reached THEN 1.0 ELSE 0.0 END) as tp1_rate,
            AVG(CASE WHEN close_reason IN ('stop_loss', 'stop_ttl') THEN 1.0 ELSE 0.0 END) as stop_rate,
            MAX(max_mae_pct) as max_mae_pct
        FROM trades_closed
        WHERE atr_policy_ver = %s
          AND exit_ts_ms >= (EXTRACT(EPOCH FROM %s) * 1000)
          AND exit_ts_ms <= (EXTRACT(EPOCH FROM %s) * 1000)
    """
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute(sql, (policy_ver, t_from, t_to))
            row = cur.fetchone()
            if not row or row['n_trades'] == 0:
                row = {'n_trades': 0, 'avg_pnl_bps': 0.0, 'avg_slippage_bps': 0.0, 'tp1_rate': 0.0, 'stop_rate': 0.0, 'max_mae_pct': 0.0}
            else:
                row = {k: float(v) if v is not None else 0.0 for k,v in row.items()}
                row['n_trades'] = int(row['n_trades'])
            return row
    except Exception as e:
        logger.error(f"Failed fetching stats for policy_ver {policy_ver}: {e}")
        return {'n_trades': 0, 'avg_pnl_bps': 0.0, 'avg_slippage_bps': 0.0, 'tp1_rate': 0.0, 'stop_rate': 0.0, 'max_mae_pct': 0.0}

def evaluate_metrics(stats: dict[str, float], thresholds: dict[str, Any]) -> tuple[str, str, dict[str, bool]]:
    """Determine checks and state."""
    checks = {}
    n_t = stats.get('n_trades', 0)

    checks["min_n_trades"] = n_t >= thresholds.get("min_n_trades", 10)
    checks["avg_pnl_bps"] = stats.get("avg_pnl_bps", -99) >= thresholds.get("min_avg_pnl_bps", -99)
    checks["avg_slippage_bps"] = stats.get("avg_slippage_bps", 99) <= thresholds.get("max_avg_slippage_bps", 99)
    checks["stop_rate"] = stats.get("stop_rate", 1.0) <= thresholds.get("max_stop_rate", 1.0)
    checks["tp1_rate"] = stats.get("tp1_rate", 0.0) >= thresholds.get("min_tp1_rate", 0.0)
    checks["max_mae_pct"] = stats.get("max_mae_pct", 1.0) <= thresholds.get("max_avg_mae_pct", 1.0)

    # HARD STOP CONDITIONS
    if not checks["avg_pnl_bps"] and n_t > (thresholds.get("min_n_trades", 10)/2):
        if stats.get("avg_pnl_bps", 0) < thresholds.get("min_avg_pnl_bps", 0) - 5.0: # e.g. severe drop
            return "failed", "ROLL_CERT_NEGATIVE_PNL", checks
    if not checks["avg_slippage_bps"] and stats.get("avg_slippage_bps", 0) > thresholds.get("max_avg_slippage_bps", 0) + 3.0:
        return "failed", "ROLL_CERT_SLIPPAGE_SPIKE", checks
    if not checks["stop_rate"] and stats.get("stop_rate", 0) > thresholds.get("max_stop_rate", 0) + 0.15:
        return "failed", "ROLL_CERT_STOP_RATE_BREACH", checks

    if not checks["min_n_trades"]:
        return "pending", "WAIT_TRADES", checks

    all_pass = all(checks.values())

    if all_pass:
        return "passed", "ROLL_CERT_PASS", checks
    return "pending", "WAIT_METRICS_IMPROVE", checks

def certify_stage(cert_id: str, change_id: str, rollout_stage: str, advisory_only: bool = True) -> dict:
    """Evaluate current cert and move if applicable."""
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM atr_rollout_certifications WHERE cert_id = %s FOR UPDATE", (cert_id,))
            cert = cur.fetchone()
            if not cert:
                return {"error": "not found"}

            if cert['status'] in ('passed', 'failed', 'rolled_back'):
                return {"status": cert['status']}

            stats = get_post_trade_truth(cert['policy_ver'], cert['monitoring_window_from'], cert['monitoring_window_to'])

            thresholds = cert['thresholds_json']
            new_status, reason, checks = evaluate_metrics(stats, thresholds)

            if new_status != cert['status']:
                cur.execute("""
                    UPDATE atr_rollout_certifications 
                    SET status = %s, checks_json = %s, summary_json = %s, finished_at = %s
                    WHERE cert_id = %s
                """, (new_status, json.dumps(checks), json.dumps(stats), datetime.now(), cert_id))

                action = 'pass' if new_status == 'passed' else 'fail' if new_status == 'failed' else 'hold'

                cur.execute("""
                    INSERT INTO atr_rollout_cert_events (cert_id, change_id, rollout_stage, action, reason_code, event_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (cert_id, change_id, rollout_stage, action, reason, json.dumps(stats)))

                conn.commit()

                payload = {
                    "cert_id": cert_id,
                    "change_id": change_id,
                    "rollout_stage": rollout_stage,
                    "status": new_status,
                    "reason_code": reason,
                    "checks": checks,
                    "summary": stats,
                    "next_action": "PROMOTE" if new_status == 'passed' else "ROLLBACK_PENDING" if new_status == 'failed' else "WAIT"
                }

                if new_status == 'failed':
                    send_stop_condition_message(change_id, rollout_stage, reason, stats)

                send_cert_outcome_message(payload)

                # IN ADVISORY MODE, do NOT actually trigger change control auto-move
                if not advisory_only and new_status == 'failed':
                    # Need to integrate with atr_change_control to actually halt!
                    pass

                return payload

            # just update summary internally
            cur.execute("""
                UPDATE atr_rollout_certifications 
                SET checks_json = %s, summary_json = %s
                WHERE cert_id = %s
            """, (json.dumps(checks), json.dumps(stats), cert_id))
            conn.commit()

            return {
                "cert_id": cert_id,
                "status": cert['status'],
                "summary": stats
            }

    except Exception as e:
        logger.error(f"Failed to certify step {cert_id}: {e}")
        return {"error": str(e)}

def closeout_change(change_id: str, final_status: str) -> bool:
    """Build the final closeout evidence pack."""
    sql_certs = "SELECT cert_id FROM atr_rollout_certifications WHERE change_id = %s"
    sql_trades = "SELECT COUNT(*) as c FROM trades_closed WHERE atr_policy_ver = (SELECT policy_ver FROM atr_change_requests WHERE change_id = %s)"

    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute(sql_certs, (change_id,))
            certs = [r['cert_id'] for r in cur.fetchall()]

            cur.execute(sql_trades, (change_id,))
            row = cur.fetchone()
            trades_c = row.get('c', 0) if row else 0

            evidence = {
                "change_id": change_id,
                "rollout_certifications": certs,
                "total_trades": trades_c,
                "final_status": final_status
            }

            cur.execute("""
                INSERT INTO atr_rollout_closeout_packs (closeout_id, change_id, final_status, evidence_json)
                VALUES (%s, %s, %s, %s)
            """, (f"closeout_{change_id}_{int(time.time())}", change_id, final_status, json.dumps(evidence)))

            conn.commit()

            send_closeout_pack_ready(change_id, final_status, trades_c)
            return True
    except Exception as e:
        logger.error(f"Failed to build closeout pack {change_id}: {e}")
        return False
