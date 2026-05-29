import os
import time

import psycopg2
import psycopg2.extras
import redis

from common.log import setup_logger

logger = setup_logger("factor_cluster_service")

MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}
MEMES = {"DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "FLOKIUSDT", "BONKUSDT", "WIFUSDT"}
AI = {"AGIXUSDT", "RNDRUSDT", "OCEANUSDT"}

def _dsn():
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def rule_based_cluster(symbol: str) -> str:
    s = symbol.upper()
    if s in MAJORS:
        return "majors_L1"
    if s in MEMES:
        return "meme_high_beta"
    if s in AI:
        return "ai_beta"
    return "unclassified"

def run_once() -> bool:
    logger.info("Starting factor cluster update...")
    try:
        r = _redis()
        # Fetch practically all traded symbols from our context/redis or DB, or just scan what's available.
        # But for rule-based, we can just insert the explicit rules + any symbol currently having an atr_rollout_stage or open positions.
        # An easy way is to scan DB for distinct symbols in v_atr_policy_allocator_inputs or atr_policy_execution_budgets
        conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_factor_cluster_service")

        with conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Let's collect symbols from closed trades and current allocator inputs to capture everything active
            cur.execute("""
                SELECT DISTINCT symbol FROM (
                    SELECT symbol FROM v_atr_policy_allocator_inputs
                    UNION ALL 
                    SELECT symbol FROM atr_policy_execution_budgets
                ) sub WHERE symbol IS NOT NULL AND symbol != ''
            """)
            symbols = [row['symbol'] for row in cur.fetchall()]

        with conn, conn.cursor() as cur:
            for s in symbols:
                cluster = rule_based_cluster(s)
                # Write to redis
                r.set(f"cfg:atr_symbol_cluster:{s}", cluster)
                # Upsert to DB
                cur.execute("""
                    INSERT INTO atr_policy_factor_clusters (
                        symbol, factor_cluster, updated_at_ms, beta_leader
                    ) VALUES (%s, %s, %s, False)
                    ON CONFLICT (symbol) DO UPDATE SET 
                        factor_cluster = EXCLUDED.factor_cluster,
                        updated_at_ms = EXCLUDED.updated_at_ms
                """, (s, cluster, int(time.time() * 1000)))

            conn.commit()

        # Optional: Set defaults if requested in env
        cluster_cap = float(os.getenv("ATR_PORTFOLIO_CLUSTER_DEFAULT_CAP_PCT", "1.50"))
        venue_cap = float(os.getenv("ATR_PORTFOLIO_VENUE_DEFAULT_CAP_PCT", "2.00"))
        policy_cap = float(os.getenv("ATR_PORTFOLIO_POLICY_DEFAULT_CAP_PCT", "0.75"))

        # Apply defaults to redis only if not set, or maybe just hard set it
        # Actually, configuration of caps should be done via tooling. We'll set default just to help bootstrap.
        for cluster in ["majors_L1", "meme_high_beta", "ai_beta", "unclassified"]:
            if not r.exists(f"cfg:atr_portfolio:max_factor_cluster_risk_pct:factor:{cluster}"):
                r.set(f"cfg:atr_portfolio:max_factor_cluster_risk_pct:factor:{cluster}", cluster_cap)

        if not r.exists("cfg:atr_portfolio:max_venue_risk_pct:venue:binance_futures"):
            r.set("cfg:atr_portfolio:max_venue_risk_pct:venue:binance_futures", venue_cap)

        logger.info(f"Updated factor clusters for {len(symbols)} symbols.")
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Error in factor cluster service: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    run_once()
