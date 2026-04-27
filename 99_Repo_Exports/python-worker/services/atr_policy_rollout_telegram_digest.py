from __future__ import annotations

import os
import psycopg2
import psycopg2.extras
import redis

def _dsn():
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def run_once() -> bool:
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_rollout_tg_digest")
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  symbol, scenario, regime, risk_horizon_bucket,
                  layer, policy_ver, rollout_stage, rollout_share
                FROM atr_policy_rollouts
                WHERE is_current = true
                ORDER BY symbol, scenario, layer
                LIMIT 20
            """)
            rows = cur.fetchall()

        lines = ["ATR Policy Rollout", ""]
        for r in rows:
            lines.append(
                f"- {r['symbol']} | {r['scenario']} | {r['layer']} | "
                f"v{r['policy_ver']} | {r['rollout_stage']} ({float(r['rollout_share']):.2f})"
            )

        payload = {"text": "\n".join(lines)}
        chat_id = os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "")
        if chat_id:
            payload["chat_id"] = chat_id
        _redis().xadd("notify:telegram", payload, maxlen=5000, approximate=True)
        return True
    finally:
        conn.close()

if __name__ == "__main__":
    run_once()
