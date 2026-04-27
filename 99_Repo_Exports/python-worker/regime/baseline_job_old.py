#!/usr/bin/env python3
"""
Оффлайн-джоб для расчета baseline-порогов по signal family.

Запуск: python -m regime.baseline_job

Читает историю сигналов из TimescaleDB (signal_executions),
считает winrate, expectancy_R, max_dd_R по каждому (family, venue, symbol, timeframe),
записывает p10/лимиты в signal_family_baseline.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import psycopg2
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.log import setup_logger

# Config
WINDOW_DAYS = int(os.getenv("BASELINE_WINDOW_DAYS", "30"))  # последние N дней
MIN_TRADES_DEFAULT = int(os.getenv("BASELINE_MIN_TRADES", "50"))  # минимум сделок для статистики

# Percentiles
WR_P10 = 10  # 10-й перцентиль winrate
EXP_R_P10 = 10  # 10-й перцентиль expectancy_R
DD_R_LIMIT_MULT = float(os.getenv("BASELINE_DD_LIMIT_MULT", "1.5"))  # лимит dd = p50_dd * mult


def calculate_baseline_for_family(
    pg_conn,
    family: str,
    venue: str,
    symbol: str,
    timeframe: str,
    window_days: int = WINDOW_DAYS,
) -> Optional[Dict]:
    """
    Считает baseline для конкретной комбинации family/venue/symbol/timeframe.

    Возвращает dict с p10_wr, p10_exp_r, limit_dd_r, min_trades или None если недостаточно данных.
    """
    cutoff_date = datetime.now() - timedelta(days=window_days)

    query = """
    SELECT
        pnl_net,
        risk_amount,
        CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END as win
    FROM signal_executions
    WHERE family = %s
      AND venue = %s
      AND symbol = %s
      AND timeframe = %s
      AND closed_at >= %s
      AND risk_amount > 0
    ORDER BY closed_at
    """

    with pg_conn.cursor() as cur:
        cur.execute(query, (family, venue, symbol, timeframe, cutoff_date))
        rows = cur.fetchall()

    if len(rows) < MIN_TRADES_DEFAULT:
        return None

    pnls = []
    risks = []
    wins = []
    r_values = []  # pnl / risk

    for pnl_net, risk_amount, win in rows:
        pnls.append(float(pnl_net))
        risks.append(float(risk_amount))
        wins.append(int(win))
        r_values.append(float(pnl_net) / float(risk_amount))

    r_values = np.array(r_values)

    # Winrate percentiles
    wr_values = [1.0 if r > 0 else 0.0 for r in r_values]
    wr_p10 = np.percentile(wr_values, WR_P10) if len(wr_values) > 0 else 0.0

    # Expectancy_R percentiles
    exp_r_values = r_values
    exp_r_p10 = np.percentile(exp_r_values, EXP_R_P10) if len(exp_r_values) > 0 else 0.0

    # Max drawdown по R
    equity = 0.0
    max_equity = 0.0
    max_dd = 0.0

    for r in r_values:
        equity += r
        if equity > max_equity:
            max_equity = equity
        dd = equity - max_equity
        if dd < max_dd:
            max_dd = dd

    # Лимит dd = |max_dd| * mult (делаем положительным для хранения)
    dd_r_limit = abs(max_dd) * DD_R_LIMIT_MULT if max_dd < 0 else 0.0

    return {
        "wr_p10": float(wr_p10),
        "wr_p50": float(np.percentile(wr_values, 50)),
        "exp_r_p10": float(exp_r_p10),
        "exp_r_p50": float(np.percentile(exp_r_values, 50)),
        "dd_r_limit": float(dd_r_limit),
        "trades_count": len(r_values),
        "min_trades": MIN_TRADES_DEFAULT,
    }


def update_baseline_table(pg_conn, baselines: Dict[Tuple[str, str, str, str], Dict]):
    """Записывает рассчитанные baseline в таблицу signal_family_baseline."""

    with pg_conn.cursor() as cur:
        for (family, venue, symbol, timeframe), data in baselines.items():
            cur.execute(
                """
                INSERT INTO signal_family_baseline (
                    family, venue, symbol, timeframe,
                    wr_p10, wr_p50, exp_r_p10, exp_r_p50, dd_r_limit, min_trades, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (family, venue, symbol, timeframe)
                DO UPDATE SET
                    wr_p10 = EXCLUDED.wr_p10,
                    wr_p50 = EXCLUDED.wr_p50,
                    exp_r_p10 = EXCLUDED.exp_r_p10,
                    exp_r_p50 = EXCLUDED.exp_r_p50,
                    dd_r_limit = EXCLUDED.dd_r_limit,
                    min_trades = EXCLUDED.min_trades,
                    updated_at = now()
                """,
                (
                    family, venue, symbol, timeframe,
                    data["wr_p10"], data["wr_p50"], data["exp_r_p10"], data["exp_r_p50"],
                    data["dd_r_limit"], data["min_trades"]
                )
            )

    pg_conn.commit()


def get_all_family_combinations(pg_conn) -> List[Tuple[str, str, str, str]]:
    """Получает все уникальные комбинации (family, venue, symbol, timeframe) из истории."""

    query = """
    SELECT DISTINCT family, venue, symbol, timeframe
    FROM signal_executions
    WHERE family IS NOT NULL AND venue IS NOT NULL AND symbol IS NOT NULL AND timeframe IS NOT NULL
    """

    with pg_conn.cursor() as cur:
        cur.execute(query)
        return [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]


def main():
    logger = setup_logger("baseline_job")

    # Database connection
    pg_dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
    if not pg_dsn:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    try:
        pg_conn = psycopg2.connect(pg_dsn)
        logger.info("Connected to TimescaleDB")

        # Получаем все комбинации
        combinations = get_all_family_combinations(pg_conn)
        logger.info(f"Found {len(combinations)} family combinations to process")

        # Считаем baseline для каждой
        baselines = {}
        for family, venue, symbol, timeframe in combinations:
            logger.info(f"Processing {family}/{venue}/{symbol}/{timeframe}")

            baseline = calculate_baseline_for_family(
                pg_conn, family, venue, symbol, timeframe
            )

            if baseline:
                baselines[(family, venue, symbol, timeframe)] = baseline
                logger.info(
                    f"  ✓ Calculated baseline: wr_p10={baseline['wr_p10']:.3f}, "
                    f"exp_r_p10={baseline['exp_r_p10']:.3f}, dd_limit={baseline['dd_r_limit']:.3f}, "
                    f"trades={baseline['trades_count']}"
                )
            else:
                logger.warning(f"  ✗ Insufficient data for {family}/{venue}/{symbol}/{timeframe}")

        # Записываем в базу
        if baselines:
            update_baseline_table(pg_conn, baselines)
            logger.info(f"Updated {len(baselines)} baseline records")
        else:
            logger.warning("No baselines calculated")

    except Exception as e:
        logger.exception(f"Baseline job failed: {e}")
        sys.exit(1)
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()


if __name__ == "__main__":
    main()
