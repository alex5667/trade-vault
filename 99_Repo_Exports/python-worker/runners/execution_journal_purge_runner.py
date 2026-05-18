"""Nightly purge runner for execution journal hot tables.

Точка запуска: python -m runners.execution_journal_purge_runner

Вызывает purge_execution_hot_tables(cutoff_ts_ms) в PostgreSQL:
  - архивирует execution_order_events → execution_order_events_archive
  - архивирует execution_quarantine_ledger → execution_quarantine_ledger_archive
  - удаляет archived строки из hot-таблиц
  - создаёт monthly partitions для archive-таблиц (текущий + следующий месяц)

ENV:
  DATABASE_URL          — psycopg2 DSN (обязателен)
  PURGE_RETENTION_DAYS  — глубина хранения в hot-таблицах (default: 30)
  PURGE_DRY_RUN         — 1 = не коммитить, только логировать (default: 0)
"""

from __future__ import annotations

import logging
import os
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("execution-journal-purge")


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes")


def _ensure_archive_partitions(cur, cutoff_ts_ms: int, now_ms: int) -> None:  # noqa: ANN001
    """Pre-create monthly partitions for both archive tables covering cutoff and now."""
    for ts_ms in (cutoff_ts_ms, now_ms):
        cur.execute(
            "SELECT ensure_monthly_range_partition('execution_order_events_archive', 'eoe_archive_', %s)",
            (ts_ms,),
        )
        cur.execute(
            "SELECT ensure_monthly_range_partition('execution_quarantine_ledger_archive', 'eql_archive_', %s)",
            (ts_ms,),
        )


def run_purge(dsn: str, retention_days: int, dry_run: bool) -> None:
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not available — cannot connect to Postgres")
        sys.exit(1)

    now_ms = int(time.time() * 1000)
    cutoff_ts_ms = now_ms - retention_days * 86_400_000

    logger.info(
        "purge: retention_days=%d cutoff=%d dry_run=%s",
        retention_days, cutoff_ts_ms, dry_run,
    )

    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_archive_partitions(cur, cutoff_ts_ms, now_ms)
            cur.execute(
                "SELECT purged_events, purged_quarantine FROM purge_execution_hot_tables(%s)",
                (cutoff_ts_ms,),
            )
            row = cur.fetchone() or (0, 0)
            purged_events, purged_quarantine = int(row[0] or 0), int(row[1] or 0)

        if dry_run:
            conn.rollback()
            logger.info("DRY-RUN: would purge events=%d quarantine=%d", purged_events, purged_quarantine)
        else:
            conn.commit()
            logger.info("purge OK: events=%d quarantine=%d", purged_events, purged_quarantine)
    except Exception:
        conn.rollback()
        logger.exception("purge FAILED — rolled back")
        raise
    finally:
        conn.close()


def main() -> None:
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        logger.error("DATABASE_URL not set — abort")
        sys.exit(1)

    retention_days = _env_int("PURGE_RETENTION_DAYS", 30)
    dry_run = _env_bool("PURGE_DRY_RUN", False)

    run_purge(dsn, retention_days, dry_run)


if __name__ == "__main__":
    main()
