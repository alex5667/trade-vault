#!/usr/bin/env python3
"""
ATR Policy Execution Budget Telegram Digest
Provides periodic digest of active freeze gates and denylist status to operators.
"""

import os
import time

import psycopg2
import redis
from psycopg2.extras import DictCursor

from common.log import setup_logger
from core.redis_keys import RedisStreams as RS

logger = setup_logger("budget_telegram_digest")

PG_DSN = os.getenv("PG_DSN", "dbname=postgres user=postgres password=postgres host=scanner-postgres port=5432")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
DIGEST_INTERVAL_SEC = int(os.getenv("ATR_BUDGET_DIGEST_INTERVAL_SEC", "3600"))

def generate_digest():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # 1. Fetch active kill-switches (Auto-Frozen)
            cur.execute("SELECT scope_kind, symbol, reason_code FROM atr_policy_kill_switches WHERE state = 'active' AND is_current = true")
            ks_rows = cur.fetchall()

            # 2. Fetch top recent denials (last hour)
            cur.execute("""
                SELECT symbol, reason_code, COUNT(*) as cnt 
                FROM atr_policy_execution_budget_events 
                WHERE action = 'deny' AND created_at >= NOW() - INTERVAL '1 hour'
                GROUP BY 1, 2
                ORDER BY cnt DESC
                LIMIT 5
            """)
            deny_rows = cur.fetchall()

    header = "📊 <b>ATR Budget Governance Digest</b>\n\n"

    body = "<b>Active Kill-Switches (Frozen):</b>\n"
    if ks_rows:
        for row in ks_rows:
            body += f"• `{row['scope_kind']}` | <b>{row['symbol']}</b>: <i>{row['reason_code']}</i>\n"
    else:
        body += "• None\n"

    body += "\n<b>Recent Denials (Last 1H):</b>\n"
    if deny_rows:
        for row in deny_rows:
            body += f"• <b>{row['symbol']}</b> ({row['cnt']}x): <i>{row['reason_code']}</i>\n"
    else:
        body += "• None\n"

    fail_policy = os.getenv("ATR_POLICY_EXEC_BUDGET_FAIL_POLICY", "CLOSED").upper()
    body += f"\n<i>Fail-Closed Mode:</i> <b>{fail_policy}</b>"

    try:
        r.xadd(RS.NOTIFY_TELEGRAM, {
            "type": "report",
            "source": "budget_digest",
            "text": header + body
        }, maxlen=5000)
        logger.info("Digest sent successfully")
    except Exception as e:
        logger.error("Failed to send telegram digest: %s", e)


def main():
    logger.info("Starting ATR Policy Budget Telegram Digest | Interval: %ds", DIGEST_INTERVAL_SEC)
    while True:
        try:
            generate_digest()
        except Exception as e:
            logger.error("Error generating digest: %s", e)

        time.sleep(DIGEST_INTERVAL_SEC)

if __name__ == "__main__":
    main()
