from __future__ import annotations

import os

import psycopg2
import psycopg2.extras
import redis

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None
from common.log import setup_logger
from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS

logger = setup_logger("portfolio_tg_digest")

def _dsn():
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _redis():
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def run_once() -> bool:
    logger.info("Running Portfolio Telegram Digest...")
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_portfolio_tg_digest")
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  factor_cluster,
                  SUM((event_json->>'incoming_risk_pct')::double precision) AS denied_risk_pct,
                  COUNT(*) AS n_denials
                FROM atr_policy_portfolio_events
                WHERE created_at > now() - interval '24 hours'
                  AND action = 'deny'
                GROUP BY factor_cluster
                ORDER BY denied_risk_pct DESC NULLS LAST
                LIMIT 10
            """)
            crowded = cur.fetchall()

        lines = ["🛡️ <b>ATR Portfolio Correlation Control</b>", "", "<b>Crowded clusters (last 24h):</b>"]
        for r in crowded:
            lines.append(
                f"- {r['factor_cluster']} | denied_risk={float(r['denied_risk_pct'] or 0):.2f}% | n={r['n_denials']}"
            )

        if not crowded:
            lines.append("- No denied risks in the last 24h")

        payload = {"text": "\n".join(lines)}
        chat_id = os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "")
        if chat_id:
            payload["chat_id"] = chat_id

        _redis().xadd(os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM), payload, maxlen=STREAM_RETENTION[RS.NOTIFY_TELEGRAM], approximate=True)  # type: ignore
        logger.info("Sent Portfolio Telegram Digest")
        return True
    except Exception as e:
        logger.error(f"Error sending Portfolio Telegram Digest: {e}", exc_info=True)
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    run_once()
