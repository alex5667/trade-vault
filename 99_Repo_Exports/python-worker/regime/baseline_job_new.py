#!/usr/bin/env python3
"""
Оффлайн-джоб для расчета baseline-квантилей по истории сигналов.

Читает signal_exec_summary, считает скользящие окна и квантили
для hit_rate и expectancy_R по каждому семейству сигналов.
"""

import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.log import setup_logger

from .baseline_calc import BaselineQuantiles, SignalExecRow, compute_family_baseline

# Config
DEFAULT_WINDOW_SIZE = int(os.getenv("BASELINE_WINDOW_SIZE", "50"))
DEFAULT_HORIZON_DAYS = int(os.getenv("BASELINE_HORIZON_DAYS", "180"))


class SignalFamilyBaselineJob:
    """
    Джоб для расчета baseline-квантилей по истории сигналов.
    """

    def __init__(
        self,
        dsn: str,
        window_size: int = DEFAULT_WINDOW_SIZE,
        horizon_days: int = DEFAULT_HORIZON_DAYS,
    ) -> None:
        self.dsn = dsn
        self.window_size = window_size
        self.horizon_days = horizon_days
        self.logger = setup_logger("baseline_job")

    def _fetch_signals(self, conn) -> dict[tuple[str, str], list[SignalExecRow]]:
        """
        Читает сигналы за horizon_days и группирует по (symbol, family).
        """
        cutoff = datetime.now() - timedelta(days=self.horizon_days)

        self.logger.info(f"Fetching signals since {cutoff} (horizon: {self.horizon_days} days)")

        by_key: dict[tuple[str, str], list[SignalExecRow]] = defaultdict(list)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT signal_id, symbol, family, opened_at, result_r
                FROM signal_exec_summary
                WHERE opened_at >= %s
                ORDER BY symbol, family, opened_at
                """
                (cutoff,),
            )

            rows_count = 0
            for row in cur:
                signal_id, symbol, family, opened_at, result_r = row
                by_key[(symbol, family)].append(
                    SignalExecRow(
                        signal_id=signal_id,
                        symbol=symbol,
                        family=family,
                        opened_at=opened_at,
                        result_r=result_r,
                    )
                )
                rows_count += 1

            self.logger.info(f"Fetched {rows_count} signals for {len(by_key)} symbol-family combinations")

        return by_key

    def _upsert_baseline_row(
        self,
        conn,
        symbol: str,
        family: str,
        metric: str,
        q: BaselineQuantiles,
    ) -> None:
        """
        Записывает/обновляет baseline для одной метрики.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO signal_family_baseline (
                    symbol, family, metric,
                    window_size, horizon_days,
                    p05, p10, p25, p50, p75, p90, p95,
                    sample_size, computed_at
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, NOW()
                )
                ON CONFLICT (symbol, family, metric, window_size, horizon_days)
                DO UPDATE SET
                    p05 = EXCLUDED.p05,
                    p10 = EXCLUDED.p10,
                    p25 = EXCLUDED.p25,
                    p50 = EXCLUDED.p50,
                    p75 = EXCLUDED.p75,
                    p90 = EXCLUDED.p90,
                    p95 = EXCLUDED.p95,
                    sample_size = EXCLUDED.sample_size,
                    computed_at = EXCLUDED.computed_at
                """
                (
                    symbol,
                    family,
                    metric,
                    self.window_size,
                    self.horizon_days,
                    q.p05,
                    q.p10,
                    q.p25,
                    q.p50,
                    q.p75,
                    q.p90,
                    q.p95,
                    q.sample_size,
                )
            )

    def run(self) -> None:
        """
        Основной entrypoint: читаем историю, считаем квантили, пишем baseline.
        """
        self.logger.info("Starting baseline calculation job")
        self.logger.info(f"Parameters: window_size={self.window_size}, horizon_days={self.horizon_days}")

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            # Читаем сигналы
            by_key = self._fetch_signals(conn)

            processed_families = 0
            total_baselines = 0

            # Обрабатываем каждое семейство
            for (symbol, family), rows in by_key.items():
                if len(rows) < self.window_size:
                    self.logger.debug(
                        f"Skipping {symbol}:{family} - insufficient signals: {len(rows)} < {self.window_size}"
                    )
                    continue

                self.logger.debug(f"Processing {symbol}:{family} with {len(rows)} signals")

                # Считаем baseline
                baselines = compute_family_baseline(rows, self.window_size)

                # Записываем результаты
                for metric_name, q in baselines.items():
                    if q.sample_size == 0:
                        self.logger.debug(f"No windows for {symbol}:{family}:{metric_name}")
                        continue

                    self._upsert_baseline_row(
                        conn,
                        symbol=symbol,
                        family=family,
                        metric=metric_name,
                        q=q,
                    )

                    self.logger.debug(
                        f"Updated baseline for {symbol}:{family}:{metric_name}: "
                        f"p10={q.p10:.3f}, p50={q.p50:.3f}, p90={q.p90:.3f}, "
                        f"samples={q.sample_size}"
                    )
                    total_baselines += 1

                processed_families += 1

        self.logger.info("Baseline calculation completed:")
        self.logger.info(f"  Processed families: {processed_families}")
        self.logger.info(f"  Total baselines: {total_baselines}")


def main():
    """Главная функция для запуска джоба"""
    logger = setup_logger("baseline_job_main")

    # Database connection
    pg_dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
    if not pg_dsn:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    # Параметры джоба
    window_size = int(os.getenv("BASELINE_WINDOW_SIZE", str(DEFAULT_WINDOW_SIZE)))
    horizon_days = int(os.getenv("BASELINE_HORIZON_DAYS", str(DEFAULT_HORIZON_DAYS)))

    try:
        job = SignalFamilyBaselineJob(
            dsn=pg_dsn,
            window_size=window_size,
            horizon_days=horizon_days,
        )
        job.run()

    except Exception as e:
        logger.exception(f"Baseline job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
