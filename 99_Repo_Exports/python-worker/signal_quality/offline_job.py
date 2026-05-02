from __future__ import annotations
"""
Offline signal quality computation job.

This module processes historical signal data to compute quality metrics
by feature clusters and stores results in the database.
"""


import logging
from statistics import fmean
from typing import Iterator

import psycopg2
from psycopg2.extras import DictCursor, execute_values

from .bucketing import make_feature_bucket

logger = logging.getLogger(__name__)

# Configuration constants (single source of truth — imported by online_job.py)
ALPHA: float = 0.05    # Tail VaR/CVaR quantile (5% tail)
MIN_N: int = 30        # Minimum observations in bucket
LOOKBACK_DAYS: int = 180  # Historical lookback period


def _var_cvar(xs: list[float], alpha: float) -> tuple[float, float]:
    """
    Calculate Value at Risk (VaR) and Conditional VaR (CVaR) at given quantile.

    Args:
        xs: List of returns (R values)
        alpha: Tail quantile (e.g., 0.05 for 5% tail)

    Returns:
        Tuple of (VaR, CVaR) at the alpha quantile
    """
    if not xs:
        return (0.0, 0.0)

    xs_sorted = sorted(xs)
    n = len(xs_sorted)

    # VaR: value at alpha quantile (worst alpha% of returns)
    idx = max(0, min(n - 1, int(alpha * n) - 1))
    var_val = xs_sorted[idx]

    # CVaR: mean of the tail (values worse than VaR)
    tail = xs_sorted[: idx + 1]
    cvar_val = fmean(tail) if tail else var_val

    return (var_val, cvar_val)


def compute_quality_score(
    expectancy_r: float,
    win_rate: float,
    var_r: float,
    cvar_r: float,
    n: int,
) -> float:
    """
    Compute quality score (0-100) from statistical metrics.

    Args:
        expectancy_r: Expected return (R)
        win_rate: Win rate (0-1)
        var_r: Value at Risk
        cvar_r: Conditional VaR
        n: Number of observations

    Returns:
        Quality score (0-100)
    """
    if n < MIN_N:
        return 0.0  # Insufficient data for reliable assessment

    # Normalize expectancy: expect 0..2R to be relevant range
    exp_norm = max(0.0, min(2.0, expectancy_r)) / 2.0  # Scale to 0..1

    # Win rate is already 0..1
    wr_norm = max(0.0, min(1.0, win_rate))

    # Tail risk penalty: penalise severely negative CVaR
    tail_penalty = 1.0
    if cvar_r < -1.0:
        # Linear penalty: -1R → 1.0, -5R → 0.0
        tail_penalty = max(0.1, 1.0 + cvar_r / 5.0)

    # Combine metrics with weights
    base = 0.6 * exp_norm + 0.4 * wr_norm
    base *= tail_penalty
    base = max(0.0, min(1.0, base))

    return base * 100.0


def load_signals(conn: psycopg2.extensions.connection, lookback_days: int = LOOKBACK_DAYS) -> Iterator:
    """
    Load historical signals for quality computation via a server-side cursor.

    Using a server-side (named) cursor ensures memory-safe streaming of large
    result sets — the driver fetches rows in chunks rather than materialising
    the full table in Python memory.

    Args:
        conn: Database connection
        lookback_days: Days to look back

    Yields:
        Signal records with required fields
    """
    # lookback_days is an int controlled by internal callers only — safe to
    # interpolate into the SQL literal (not user-supplied data).
    sql = f"""
        SELECT
            symbol,
            signal_type,
            side,
            COALESCE(session, 'mixed') AS session,
            COALESCE(regime, 'mixed') AS regime,
            pnl_r,
            delta_spike_z,
            obi,
            weak_progress,
            atr_quantile
        FROM signals
        WHERE ts >= NOW() - INTERVAL '{lookback_days} days'
          AND pnl_r IS NOT NULL
          AND symbol IS NOT NULL
          AND signal_type IS NOT NULL
          AND side IS NOT NULL
    """
    # Name the cursor so psycopg2 uses a server-side cursor (streaming).
    with conn.cursor(name="sq_offline_load", cursor_factory=DictCursor) as cur:
        cur.itersize = 2000  # Fetch 2 000 rows at a time from the server
        cur.execute(sql)
        yield from cur


def run_offline_quality_job(
    pg_dsn: str,
    horizon: str = "R_main",
    lookback_days: int = LOOKBACK_DAYS,
    min_n: int = MIN_N,
    alpha: float = ALPHA,
) -> None:
    """
    Run offline quality computation job.

    This function processes historical signal data, groups signals by
    feature buckets, computes quality metrics, and stores results.

    Args:
        pg_dsn: PostgreSQL connection string
        horizon: R horizon ('R_main', 'R_30m', etc.)
        lookback_days: Historical lookback period
        min_n: Minimum observations per bucket
        alpha: VaR/CVaR quantile
    """
    logger.info(
        "Starting offline quality computation (horizon=%s, lookback=%dd, min_n=%d, alpha=%.2f)",
        horizon,
        lookback_days,
        min_n,
        alpha,
    )

    conn = psycopg2.connect(pg_dsn)
    try:
        # Group signals by feature buckets
        # key: (symbol, signal_type, side, session, regime, feature_bucket)
        buckets: dict[tuple[str, ...], list[float]] = {}
        total_signals = 0

        for row in load_signals(conn, lookback_days):
            total_signals += 1

            fb = make_feature_bucket(
                delta_spike_z=row["delta_spike_z"],
                obi=row["obi"],
                weak_progress=row["weak_progress"],
                atr_quantile=row["atr_quantile"],
            )

            key = (
                row["symbol"],
                row["signal_type"],
                row["side"],
                row["session"],
                row["regime"],
                fb,
            )
            buckets.setdefault(key, []).append(row["pnl_r"])

        logger.info("Processed %d signals into %d buckets", total_signals, len(buckets))

        # Compute metrics and collect all rows for a single batch upsert
        insert_rows: list[tuple] = []
        skipped = 0

        for (symbol, stype, side, session, regime, fb), rs in buckets.items():
            n = len(rs)
            if n < min_n:
                skipped += 1
                continue

            wr = sum(1 for r in rs if r > 0) / n
            exp_r = fmean(rs)
            var_r, cvar_r = _var_cvar(rs, alpha)
            q = compute_quality_score(exp_r, wr, var_r, cvar_r, n)

            insert_rows.append(
                (symbol, stype, side, session, regime, fb, horizon, n, wr, exp_r, var_r, cvar_r, q)
            )

        logger.info(
            "Computed metrics: %d buckets to insert, %d skipped (n < %d)",
            len(insert_rows),
            skipped,
            min_n,
        )

        # Batch upsert — far fewer round-trips than per-row execute()
        if insert_rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO signal_quality_offline
                        (symbol, signal_type, side, session, regime,
                         feature_bucket, horizon,
                         n_signals, win_rate, expectancy_r, var_r, cvar_r,
                         quality_score, updated_at)
                    VALUES %s
                    ON CONFLICT (symbol, signal_type, side, session, regime, feature_bucket, horizon)
                    DO UPDATE SET
                        n_signals      = EXCLUDED.n_signals,
                        win_rate       = EXCLUDED.win_rate,
                        expectancy_r   = EXCLUDED.expectancy_r,
                        var_r          = EXCLUDED.var_r,
                        cvar_r         = EXCLUDED.cvar_r,
                        quality_score  = EXCLUDED.quality_score,
                        updated_at     = now()
                    """,
                    # Append now() for updated_at via template
                    [(r + (psycopg2.extensions.AsIs("now()"),)) for r in insert_rows],
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                )
            conn.commit()

        logger.info("Offline quality computation completed: %d buckets processed", len(insert_rows))

    except Exception:
        logger.exception("Error during offline quality computation")
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
        print("Usage: python offline_job.py <pg_dsn> [horizon] [lookback_days]")
        sys.exit(1)

    pg_dsn = sys.argv[1]
    horizon = sys.argv[2] if len(sys.argv) > 2 else "R_main"
    lookback_days = int(sys.argv[3]) if len(sys.argv) > 3 else LOOKBACK_DAYS

    run_offline_quality_job(pg_dsn, horizon, lookback_days)
