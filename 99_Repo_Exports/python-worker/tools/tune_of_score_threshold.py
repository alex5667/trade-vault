"""
tune_of_score_threshold.py — utility-based OF score threshold optimizer.

Reads closed trades from the analytics DB and finds the OF score / confidence
threshold that maximises sum utility (r_multiple) using the existing
threshold_opt_v1.best_threshold_by_utility().

Usage:
    cd python-worker
    python -m tools.tune_of_score_threshold [--hours 168] [--min-trades 50] [--symbol BTCUSDT]

Output:
    Optimal threshold, coverage, edge rate, mean/sum utility.
    Also prints a per-decile breakdown so you can see the distribution.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import numpy as np

# Allow running from python-worker root or as module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.threshold_opt_v1 import ThrResult, best_threshold_by_utility

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None  # type: ignore[assignment]

DEFAULT_DSN = os.getenv("TRADES_DB_DSN") or os.getenv("ANALYTICS_DB_DSN") or \
    "postgresql://trading:{}@localhost:5434/scanner_analytics".format(
        os.getenv("TRADING_PASSWORD", "")
    )


_QUERY = """
SELECT
    tc.order_id,
    tc.symbol,
    tc.direction,
    tc.pnl_net,
    tc.r_multiple,
    tc.close_reason,
    tc.tp1_hit,
    tc.entry_tag,
    -- OF score / confidence from features_json (trades_closed_p0)
    CAST(p0.features_json->>'confidence' AS FLOAT)      AS confidence,
    CAST(p0.features_json->>'of_confirm_score' AS FLOAT) AS of_confirm_score,
    CAST(p0.features_json->>'delta_z' AS FLOAT)         AS delta_z
FROM trades_closed tc
LEFT JOIN trades_closed_p0 p0 USING (order_id)
WHERE tc.exit_ts_ms > EXTRACT(EPOCH FROM NOW() - INTERVAL '{hours} hours') * 1000
  AND tc.is_virtual = true
  AND tc.pnl_net IS NOT NULL
  AND tc.r_multiple IS NOT NULL
  {sym_filter}
ORDER BY tc.exit_ts_ms DESC
LIMIT {limit}
"""


def _fetch(dsn: str, hours: int, symbol: str | None, limit: int) -> list[dict]:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed")
    sym_filter = f"AND tc.symbol = '{symbol.upper()}'" if symbol else ""
    sql = _QUERY.format(hours=hours, sym_filter=sym_filter, limit=limit)
    conn = psycopg2.connect(dsn, connect_timeout=5)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _pick_score_col(rows: list[dict]) -> tuple[np.ndarray, str]:
    """Return (score_array, col_name) — prefer of_confirm_score, fallback confidence/100."""
    of_scores = [r.get("of_confirm_score") for r in rows]
    if sum(v is not None for v in of_scores) > len(rows) * 0.5:
        arr = np.array([v if v is not None else 0.0 for v in of_scores], dtype=float)
        return arr, "of_confirm_score (0..1)"

    conf_scores = [r.get("confidence") for r in rows]
    if sum(v is not None for v in conf_scores) > len(rows) * 0.5:
        arr = np.array([v / 100.0 if v is not None else 0.0 for v in conf_scores], dtype=float)
        return arr, "confidence/100 (proxy)"

    raise RuntimeError("Neither of_confirm_score nor confidence is available in trades_closed_p0")


def _decile_breakdown(p: np.ndarray, y_edge: np.ndarray, util_r: np.ndarray) -> None:
    print("\nScore decile breakdown:")
    print(f"  {'Decile':>8} {'N':>6} {'Score range':>18} {'WinRate':>9} {'Mean R':>8}")
    print("  " + "-" * 55)
    quantiles = np.percentile(p, np.arange(0, 110, 10))
    for i in range(len(quantiles) - 1):
        lo, hi = quantiles[i], quantiles[i + 1]
        mask = (p >= lo) & (p < hi if i < 9 else p <= hi)
        n = int(mask.sum())
        if n == 0:
            continue
        wr = float(y_edge[mask].mean()) if n else 0.0
        mr = float(util_r[mask].mean()) if n else 0.0
        print(f"  {i*10:>5}-{(i+1)*10:<3}% {n:>6} [{lo:.3f} – {hi:.3f}] {wr:>8.1%} {mr:>8.3f}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=int, default=168, help="Look-back window in hours (default 168 = 7 days)")
    ap.add_argument("--min-trades", type=int, default=50, help="Min trades per threshold step (default 50)")
    ap.add_argument("--symbol", default=None, help="Filter to one symbol (e.g. BTCUSDT)")
    ap.add_argument("--limit", type=int, default=10_000, help="Max rows to fetch (default 10000)")
    ap.add_argument("--thr-min", type=float, default=0.45, help="Min threshold grid (default 0.45)")
    ap.add_argument("--thr-max", type=float, default=0.90, help="Max threshold grid (default 0.90)")
    ap.add_argument("--thr-step", type=float, default=0.01, help="Grid step (default 0.01)")
    ap.add_argument("--dsn", default=DEFAULT_DSN, help="Postgres DSN")
    args = ap.parse_args()

    print(f"Fetching trades: last {args.hours}h, symbol={args.symbol or 'all'}, limit={args.limit}")
    t0 = time.monotonic()
    try:
        rows = _fetch(args.dsn, args.hours, args.symbol, args.limit)
    except Exception as e:
        print(f"DB error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Fetched {len(rows)} rows in {time.monotonic()-t0:.1f}s")
    if not rows:
        print("No data. Check DSN and time range.")
        sys.exit(0)

    try:
        p, score_col = _pick_score_col(rows)
    except RuntimeError as e:
        print(f"Score column unavailable: {e}")
        print("Tip: run with --hours to extend window, or check trades_closed_p0 table.")
        sys.exit(1)

    # Win = pnl_net > 0 (or tp1_hit for edge label)
    y_edge = np.array([1.0 if (r.get("pnl_net") or 0) > 0 else 0.0 for r in rows])
    # Utility = r_multiple (positive = win, negative = loss)
    util_r = np.array([float(r.get("r_multiple") or 0.0) for r in rows])

    total = len(rows)
    wins = int(y_edge.sum())
    print(f"\nDataset: {total} trades, {wins} wins ({wins/total:.1%} winrate)")
    print(f"Score column: {score_col}")
    print(f"Score range: [{p.min():.3f}, {p.max():.3f}]")
    print(f"R-multiple: mean={util_r.mean():.3f}, median={np.median(util_r):.3f}")

    _decile_breakdown(p, y_edge, util_r)

    print(f"\nOptimizing threshold [{args.thr_min:.2f} – {args.thr_max:.2f}] step={args.thr_step:.3f}")
    result: ThrResult = best_threshold_by_utility(
        p=p,
        y_edge=y_edge,
        util_r=util_r,
        thr_min=args.thr_min,
        thr_max=args.thr_max,
        thr_step=args.thr_step,
        min_trades=args.min_trades,
    )

    print("\n" + "=" * 55)
    print(f"OPTIMAL THRESHOLD : {result.thr:.3f}")
    print(f"  Coverage (take rate): {result.take_rate:.1%}  ({result.n_take} trades)")
    print(f"  Edge rate (winrate) : {result.edge_rate:.1%}")
    print(f"  Mean R-multiple     : {result.mean_util:.3f}")
    print(f"  Sum R-multiple      : {result.sum_util:.2f}")
    print("=" * 55)

    # Recommendation
    current = float(os.getenv("OF_SCORE_MIN", "0.60"))
    if score_col.startswith("confidence"):
        suggested = result.thr * 100  # convert back to 0-100
        print(f"\nCurrent CRYPTO_SIGNAL_MIN_CONF = {current*100:.0f}")
        print(f"Suggested CRYPTO_SIGNAL_MIN_CONF = {suggested:.0f}")
    else:
        print(f"\nCurrent OF_SCORE_MIN = {current:.2f}")
        print(f"Suggested OF_SCORE_MIN = {result.thr:.2f}")

    if result.n_take == 0:
        print("\nWARNING: No trades passed min_trades threshold. Lower --min-trades or extend --hours.")


if __name__ == "__main__":
    main()
