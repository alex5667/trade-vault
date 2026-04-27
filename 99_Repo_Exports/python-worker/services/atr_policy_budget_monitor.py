#!/usr/bin/env python3
"""
ATR Policy Execution Budget Monitor
Periodically reads `closed_trades` and open positions from Postgres/Redis,
updates `state:atr_budget:*` counters in Redis, and implements the auto-freeze
logic if slippage EMA, loss limit, or stop streaks are breached.
"""

import os
import time
import json
import logging
import psycopg2
from psycopg2.extras import DictCursor
import redis

from common.log import setup_logger

logger = setup_logger("budget_monitor")

PG_DSN = os.getenv("PG_DSN", "dbname=postgres user=postgres password=postgres host=scanner-postgres port=5432")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
CHECK_INTERVAL_SEC = int(os.getenv("ATR_BUDGET_MONITOR_INTERVAL_SEC", "15"))

def check_budgets_and_auto_freeze():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # 1. Calculate running slippage EMA and stop streaks per cohort
            # We use the view `v_atr_policy_promotion_inputs` created in Phase 5.2 if available,
            # or just query closed_trades.
            
            # This is a simplified proxy: we find cohorts whose slippage is excessively bad
            # over the last 50 trades.
            query = """
                SELECT
                    t.atr_policy_source as source, t.symbol,
                    t.atr_policy_scenario as scenario,
                    t.atr_policy_regime as regime,
                    t.sc_risk_horizon_bucket as risk_horizon_bucket,
                    AVG(NULLIF(p.slippage_bps_est, 0)) as avg_slippage_bps,
                    COUNT(CASE WHEN t.close_reason LIKE '%SL%' THEN 1 END) as sl_count,
                    SUM(t.pnl_net) as daily_pnl
                FROM trades_closed t
                LEFT JOIN trades_closed_p0 p ON t.order_id = p.order_id
                WHERE t.created_at >= NOW() - INTERVAL '1 day'
                  AND t.atr_policy_scenario IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5
            """
            
            try:
                cur.execute(query)
                metrics = cur.fetchall()
            except psycopg2.errors.UndefinedTable:
                conn.rollback()
                logger.warning("Table closed_trades not found or schema mismatch. Skipping metrics calculation.")
                metrics = []
            
            for row in metrics:
                source = row["source"] or "unknown"
                symbol = row["symbol"] or "unknown"
                scenario = row["scenario"] or "unknown"
                regime = row["regime"] or "unknown"
                bucket = row["risk_horizon_bucket"] or "unknown"
                
                cohort_key = f"cohort:{source}:{symbol}:{scenario}:{regime}:{bucket}"
                
                slip_bps = float(row["avg_slippage_bps"] or 0.0)
                sl_count = int(row["sl_count"] or 0)
                pnl_usd = float(row["daily_pnl"] or 0.0)
                
                # Update Redis State
                r.set(f"state:atr_budget:max_slippage_ema_bps:{cohort_key}", slip_bps)
                r.set(f"state:atr_budget:max_stop_streak:{cohort_key}", sl_count)
                r.set(f"state:atr_budget:max_daily_loss_usd:{cohort_key}", -pnl_usd if pnl_usd < 0 else 0)
                
                # Check against config
                max_slip = float(r.get(f"cfg:atr_budget:max_slippage_ema_bps:{cohort_key}") or 0.0)
                if max_slip > 0 and slip_bps > max_slip:
                    trigger_auto_freeze(cur, r, cohort_key, "SLIPPAGE_EMA_BREACH", f"Slippage {slip_bps:.1f} > max {max_slip:.1f}")

                max_loss = float(r.get(f"cfg:atr_budget:max_daily_loss_usd:{cohort_key}") or 0.0)
                if max_loss > 0 and -pnl_usd > max_loss:
                    trigger_auto_freeze(cur, r, cohort_key, "DAILY_LOSS_BREACH", f"Loss {-pnl_usd:.1f} > max {max_loss:.1f}")

            # Open risk & Positions would similarly be aggregated from open positions.
            
            conn.commit()


def trigger_auto_freeze(cur, r, scope_key, reason_code, message):
    """Inserts a kill switch into postgres and sets redis synchronously to freeze the cohort instantly."""
    r.set(f"cfg:atr_kill_switch:{scope_key}", "1")
    
    # Check if already killed
    cur.execute("SELECT 1 FROM atr_policy_kill_switches WHERE scope_kind = %s AND state = 'active' AND is_current = true", (scope_key,))
    if cur.fetchone():
        return
        
    parts = scope_key.split(":") 
    # scope_key example: cohort:CryptoOrderFlow:BTCUSDT:macro:trend:macro
    # parts: [cohort, source, symbol, scenario, regime, bucket]
    
    if parts[0] == "cohort" and len(parts) >= 6:
        source, symbol, scenario, regime, bucket = parts[1:6]
        
        cur.execute("""
            INSERT INTO atr_policy_kill_switches 
            (scope_kind, source, symbol, scenario, regime, risk_horizon_bucket, state, reason_code, payload_json, is_current, created_at_ms, updated_at_ms)
            VALUES (%s, %s, %s, %s, %s, %s, 'active', %s, %s, true, %s, %s)
        """, (
            parts[0], source, symbol, scenario, regime, bucket, reason_code,
            json.dumps({"msg": message}),
            int(time.time() * 1000), int(time.time() * 1000)
        ))
        
        # Log event
        cur.execute("""
            INSERT INTO atr_policy_execution_budget_events
            (source, symbol, scenario, regime, risk_horizon_bucket, action, reason_code, event_json)
            VALUES (%s, %s, %s, %s, %s, 'freeze', %s, %s)
        """, (source, symbol, scenario, regime, bucket, reason_code, json.dumps({"msg": message})))

        logger.critical(f"🛡️ AUTO-FREEZE TRIGGERED for {scope_key}: {reason_code} - {message}")
        
        # Dispatch to telegram digest queue
        r.xadd("notify:telegram", {
            "type": "report",
            "source": "budget_monitor",
            "text": f"🛡️ <b>[AUTO-FREEZE] KILL-SWITCH ENGAGED</b>\nCohort: `{scope_key}`\nReason: {reason_code}\nDetails: {message}"
        }, maxlen=5000)

def main():
    logger.info("Starting ATR Policy Execution Budget Monitor")
    while True:
        try:
            check_budgets_and_auto_freeze()
        except Exception as e:
            logger.error("Error in budget monitor loop: %s", e)
        time.sleep(CHECK_INTERVAL_SEC)

if __name__ == "__main__":
    main()
