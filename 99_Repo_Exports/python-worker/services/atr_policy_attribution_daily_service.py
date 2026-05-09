import logging
import time

from services.analytics_db import get_conn

logger = logging.getLogger("atr_attribution_daily")

def setup_table() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS atr_policy_attribution_daily (
        date DATE NOT NULL,
        atr_policy_ver INT NOT NULL,
        atr_policy_tag VARCHAR(16) NOT NULL,
        atr_policy_source VARCHAR(32) NOT NULL,
        symbol VARCHAR(32) NOT NULL,
        total_trades INT DEFAULT 0,
        win_trades INT DEFAULT 0,
        total_pnl_net DOUBLE PRECISION DEFAULT 0.0,
        max_drawdown_net DOUBLE PRECISION DEFAULT 0.0,
        PRIMARY KEY (date, atr_policy_tag, symbol)
    );
    """
    with get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.warning(f"Failed to create daily attribution table: {e}")

def run_once() -> None:
    """Агрегирует данные по provenance за последние 14 дней в дневную таблицу."""
    logger.info("Running ATR Policy Attribution Daily Aggregator...")
    setup_table()

    sql = """
        INSERT INTO atr_policy_attribution_daily (
            date, atr_policy_ver, atr_policy_tag, atr_policy_source, symbol,
            total_trades, win_trades, total_pnl_net, max_drawdown_net
        )
        SELECT 
            TO_TIMESTAMP(exit_ts_ms / 1000)::DATE AS date,
            atr_policy_ver,
            atr_policy_tag,
            atr_policy_source,
            symbol,
            COUNT(*) as total_trades,
            SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END) as win_trades,
            SUM(pnl_net) as total_pnl_net,
            MIN(pnl_net) as max_drawdown_net
        FROM trades_closed
        WHERE exit_ts_ms >= (EXTRACT(EPOCH FROM NOW() - INTERVAL '14 days') * 1000)
          AND atr_policy_ver > 0
        GROUP BY 
            TO_TIMESTAMP(exit_ts_ms / 1000)::DATE,
            atr_policy_ver,
            atr_policy_tag,
            atr_policy_source,
            symbol
        ON CONFLICT (date, atr_policy_tag, symbol) DO UPDATE SET
            total_trades = EXCLUDED.total_trades,
            win_trades = EXCLUDED.win_trades,
            total_pnl_net = EXCLUDED.total_pnl_net,
            max_drawdown_net = EXCLUDED.max_drawdown_net,
            atr_policy_source = EXCLUDED.atr_policy_source,
            atr_policy_ver = EXCLUDED.atr_policy_ver;
    """

    start = time.time()
    with get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(sql)
            conn.commit()
            logger.info("Successfully aggregated ATR policy attributions (took %.2fs).", time.time() - start)
        except Exception as e:
            logger.error("Error executing attribution aggregation: %s", e)

if __name__ == "__main__":
    import logging.config

    import yaml
    try:
        with open("logging.yaml") as f:
            logging.config.dictConfig(yaml.safe_load(f))
    except Exception:
        logging.basicConfig(level=logging.INFO)

    run_once()
