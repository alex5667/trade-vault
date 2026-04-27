"""
Online signal quality computation job.

This module maintains rolling quality assessment based on recent signal performance.
"""

from __future__ import annotations

import logging

import psycopg2
from psycopg2.extras import DictCursor

from .offline_job import ALPHA, compute_quality_score, fmean, _var_cvar

logger = logging.getLogger(__name__)

# Rolling window configuration
ROLL_N: int = 200  # Rolling window size — how many recent signals to consider

# Minimum number of recent signals required for online assessment
_ONLINE_MIN_N: int = 10


def run_online_quality_job(
    pg_dsn: str,
    horizon: str = "R_main",
    roll_n: int = ROLL_N,
    alpha: float = ALPHA,
) -> None:
    """
    Run online rolling quality computation job.

    This function maintains recent quality metrics for each signal type
    using a rolling window of the most recent signals.

    Args:
        pg_dsn: PostgreSQL connection string
        horizon: R horizon ('R_main', 'R_30m', etc.)
        roll_n: Rolling window size
        alpha: VaR/CVaR quantile
    """
    # roll_n is an internal int — safe to interpolate (not user-supplied data)
    assert isinstance(roll_n, int) and roll_n > 0, f"roll_n must be a positive int, got {roll_n!r}"

    logger.info(
        "Starting online quality computation (horizon=%s, roll_n=%d, alpha=%.2f)",
        horizon,
        roll_n,
        alpha,
    )

    conn = psycopg2.connect(pg_dsn)
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Get rolling window of recent signals for each (symbol, signal_type, side).
            # roll_n is validated as a positive int above — safe f-string interpolation.
            cur.execute(
                f"""
                WITH ranked AS (
                    SELECT
                        symbol,
                        signal_type,
                        side,
                        pnl_r,
                        ts,
                        ROW_NUMBER() OVER (
                          PARTITION BY symbol, signal_type, side
                          ORDER BY ts DESC
                        ) AS rn
                    FROM signals
                    WHERE pnl_r IS NOT NULL
                      AND symbol IS NOT NULL
                      AND signal_type IS NOT NULL
                      AND side IS NOT NULL
                )
                SELECT symbol, signal_type, side, pnl_r
                FROM ranked
                WHERE rn <= {roll_n}
                """
            )
            rows = cur.fetchall()

        logger.info("Loaded %d recent signals", len(rows))

        # Group by (symbol, signal_type, side)
        groups: dict[tuple[str, str, str], list[float]] = {}
        for r in rows:
            key = (r["symbol"], r["signal_type"], r["side"])
            groups.setdefault(key, []).append(r["pnl_r"])

        logger.info("Grouped into %d signal type combinations", len(groups))

        # Compute and store rolling quality metrics
        processed_groups = 0
        with conn.cursor() as cur:
            for (symbol, stype, side), rs in groups.items():
                n = len(rs)
                if n < _ONLINE_MIN_N:
                    continue

                wr = sum(1 for x in rs if x > 0) / n
                exp_r = fmean(rs)
                var_r, cvar_r = _var_cvar(rs, alpha)
                q_online = compute_quality_score(exp_r, wr, var_r, cvar_r, n)

                # Determine status based on quality and sample size
                status = "ok"
                if n >= 50 and exp_r < 0.0:
                    status = "degraded"
                elif n >= 100 and exp_r < -0.3:
                    status = "disabled"

                cur.execute(
                    """
                    INSERT INTO signal_quality_online
                        (symbol, signal_type, side, horizon,
                         n_recent, win_rate_recent, expectancy_r_recent,
                         var_r_recent, cvar_r_recent,
                         quality_score_online, status, updated_at)
                    VALUES (%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,
                            %s,%s,now())
                    ON CONFLICT (symbol, signal_type, side, horizon)
                    DO UPDATE SET
                        n_recent           = EXCLUDED.n_recent,
                        win_rate_recent    = EXCLUDED.win_rate_recent,
                        expectancy_r_recent = EXCLUDED.expectancy_r_recent,
                        var_r_recent       = EXCLUDED.var_r_recent,
                        cvar_r_recent      = EXCLUDED.cvar_r_recent,
                        quality_score_online = EXCLUDED.quality_score_online,
                        status             = EXCLUDED.status,
                        updated_at         = now()
                    """,
                    (
                        symbol, stype, side, horizon,
                        n, wr, exp_r, var_r, cvar_r,
                        q_online, status,
                    ),
                )

                processed_groups += 1
                if processed_groups % 50 == 0:
                    logger.debug("Processed %d signal types...", processed_groups)

        conn.commit()
        logger.info("Online quality computation completed: %d signal types processed", processed_groups)

    except Exception:
        logger.exception("Error during online quality computation")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python online_job.py <pg_dsn> [horizon] [roll_n]")
        sys.exit(1)

    pg_dsn = sys.argv[1]
    horizon = sys.argv[2] if len(sys.argv) > 2 else "R_main"
    roll_n = int(sys.argv[3]) if len(sys.argv) > 3 else ROLL_N

    run_online_quality_job(pg_dsn, horizon, roll_n)
