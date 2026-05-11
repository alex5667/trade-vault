#!/usr/bin/env python3
"""
ATR Policy Execution Budget Controller
Syncs SQL `atr_policy_execution_budgets` to fast Redis `cfg:` keys for the Gate.
Runs periodically.
"""

import os
import time
import traceback

import psycopg2
import redis
from prometheus_client import Counter, Gauge, start_http_server
from psycopg2.extras import DictCursor

from common.log import setup_logger

logger = setup_logger("budget_controller")

PG_DSN = os.getenv("PG_DSN", "dbname=postgres user=postgres password=postgres host=scanner-postgres port=5432")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
SYNC_INTERVAL = int(os.getenv("ATR_BUDGET_SYNC_INTERVAL", "60"))
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9845"))

SYNC_COUNT = Counter("atr_budget_sync_total", "Number of budget syncs from DB to Redis")
SYNC_ERRORS = Counter("atr_budget_sync_errors_total", "Number of budget sync errors")
ACTIVE_BUDGETS = Gauge("atr_budget_active_configs", "Number of active budget configs loaded")

def build_redis_key(prefix: str, row: dict) -> str:
    """
    Constructs the target scope key string.
    Example: cfg:atr_budget:max_open_risk_pct:cohort:CryptoOrderFlow:BTCUSDT:macro:trend:macro
    """
    scope_kind = row["scope_kind"]
    if scope_kind == "global":
        return f"{prefix}:global"
    elif scope_kind == "venue":
        return f"{prefix}:venue:{row['venue']}"
    elif scope_kind == "cohort":
        return f"{prefix}:cohort:{row['source']}:{row['symbol']}:{row['scenario']}:{row['regime']}:{row['risk_horizon_bucket']}"
    elif scope_kind == "layer":
        return f"{prefix}:layer:{row['source']}:{row['symbol']}:{row['scenario']}:{row['regime']}:{row['risk_horizon_bucket']}:{row['layer']}"
    elif scope_kind == "policy_ver":
        return f"{prefix}:policy:{row['source']}:{row['symbol']}:{row['scenario']}:{row['regime']}:{row['risk_horizon_bucket']}:{row['layer']}:{row.get('policy_ver', 0)}"
    return ""


def sync_budgets_and_kill_switches():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # 1) Kill-switches
            cur.execute("SELECT * FROM atr_policy_kill_switches WHERE is_current = true")
            ks_rows = cur.fetchall()

            pipeline = r.pipeline()
            # We don't wipe all kill switches, just update the valid ones to avoid resetting manually set ones.
            # However ideally we prefix wipe. We'll just set them.
            for row in ks_rows:
                key = build_redis_key("cfg:atr_kill_switch", row)  # type: ignore
                if key:
                    val = "1" if row["state"] == "active" else "0"
                    pipeline.set(key, val)

            # 2) Budgets
            cur.execute("SELECT * FROM atr_policy_execution_budgets WHERE is_enabled = true")
            bg_rows = cur.fetchall()

            for row in bg_rows:
                base_key = build_redis_key("cfg:atr_budget", row)  # type: ignore
                if base_key:
                    pipeline.set(base_key.replace("cfg:atr_budget:", "cfg:atr_budget:max_open_risk_pct:"), str(row["max_open_risk_pct"]))
                    pipeline.set(base_key.replace("cfg:atr_budget:", "cfg:atr_budget:max_open_positions:"), str(row["max_open_positions"]))
                    pipeline.set(base_key.replace("cfg:atr_budget:", "cfg:atr_budget:max_daily_trades:"), str(row["max_daily_trades"]))
                    pipeline.set(base_key.replace("cfg:atr_budget:", "cfg:atr_budget:max_daily_loss_usd:"), str(row["max_daily_loss_usd"]))
                    pipeline.set(base_key.replace("cfg:atr_budget:", "cfg:atr_budget:max_daily_loss_bps:"), str(row["max_daily_loss_bps"]))
                    pipeline.set(base_key.replace("cfg:atr_budget:", "cfg:atr_budget:max_slippage_ema_bps:"), str(row["max_slippage_ema_bps"]))
                    pipeline.set(base_key.replace("cfg:atr_budget:", "cfg:atr_budget:max_stop_streak:"), str(row["max_stop_streak"]))

            pipeline.execute()

            ACTIVE_BUDGETS.set(len(bg_rows))
            logger.info("Synced %d kill switches and %d budgets to Redis", len(ks_rows), len(bg_rows))
            SYNC_COUNT.inc()

def run_loop():
    logger.info("Starting ATR Policy Execution Budget Controller on port %d...", PROMETHEUS_PORT)
    try:
        start_http_server(PROMETHEUS_PORT)
    except Exception as e:
        logger.warning(f"Prometheus exporter failed to start on port {PROMETHEUS_PORT}: {e}")

    while True:
        try:
            sync_budgets_and_kill_switches()
        except Exception as e:
            logger.error("Failed to sync budgets: %s\n%s", e, traceback.format_exc())
            SYNC_ERRORS.inc()
        time.sleep(SYNC_INTERVAL)

if __name__ == "__main__":
    run_loop()
