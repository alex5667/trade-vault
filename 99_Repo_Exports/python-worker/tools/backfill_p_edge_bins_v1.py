"""P_Edge calibrator bin backfill from `trades_closed` (P1.9, 2026-05-26).

Replays N hours of closed trades through `p_edge_threshold_calibrator.observe()`
to populate empty / low-sample bins. Publishes the resulting snapshot to
`autocal:p_edge:state` so the live `p_edge_threshold_reader` picks it up.

This is an idempotent operation — re-runs simply re-build the in-memory
buffers; existing apply_ms throttles still apply on the live writer.

Usage:
    python -m tools.backfill_p_edge_bins_v1 --hours=168 --dry-run
    python -m tools.backfill_p_edge_bins_v1 --hours=168 --min-rows=50

Env:
    ANALYTICS_DB_DSN   — Postgres DSN (default: same as p_edge_threshold_calibrator_v1)
    REDIS_URL          — redis-worker-1 (default redis://redis-worker-1:6379/0)

Exit codes:
    0  — success (snapshot written, or dry-run completed)
    1  — DB connection / fatal error
    2  — fewer rows than --min-rows; snapshot NOT written
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any

from core.p_edge_threshold_calibrator import (
    DEFAULT_P_MIN,
    PEdgeThresholdCalibrator,
)
from core.redis_keys import RK
from core.reject_reason_weights import weight_for_reason

logger = logging.getLogger("backfill_p_edge_bins_v1")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


SQL = """
SELECT
    symbol,
    COALESCE(NULLIF(entry_regime, ''), regime, '*')    AS regime,
    COALESCE(NULLIF(entry_tag, ''), kind, '*')          AS kind,
    COALESCE(NULLIF(side, ''), direction, '*')          AS direction,
    p_edge,
    r_multiple,
    CASE
        WHEN result IS NOT NULL AND result <> '' THEN UPPER(result)
        WHEN r_multiple IS NULL THEN 'BE'
        WHEN r_multiple > 0 THEN 'WIN'
        WHEN r_multiple < 0 THEN 'LOSS'
        ELSE 'BE'
    END AS result,
    EXTRACT(EPOCH FROM close_ts)::BIGINT * 1000 AS ts_ms,
    COALESCE(NULLIF(v_gate_reason, ''), 'OK')           AS reject_reason
FROM trades_closed
WHERE close_ts > NOW() - INTERVAL '%(hours)s hours'
  AND p_edge IS NOT NULL
  AND r_multiple IS NOT NULL
ORDER BY close_ts ASC
"""


def _get_dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://trading:postgres@scanner-pgbouncer:5432/scanner_analytics"
    )


def _get_redis():
    import redis  # type: ignore
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    return redis.from_url(url, decode_responses=True)


def _fetch_rows(hours: int) -> list[dict[str, Any]]:
    import psycopg2
    import psycopg2.extras
    dsn = _get_dsn()
    rows: list[dict[str, Any]] = []
    conn = psycopg2.connect(dsn, connect_timeout=10)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SQL % {"hours": int(hours)})
            for r in cur:
                rows.append(dict(r))
    finally:
        conn.close()
    return rows


def _observe_all(
    cal: PEdgeThresholdCalibrator,
    rows: list[dict[str, Any]],
    *,
    apply_weights: bool,
) -> tuple[int, int]:
    accepted = 0
    skipped = 0
    for r in rows:
        try:
            sym = str(r.get("symbol") or "*").upper()
            reg = str(r.get("regime") or "*").lower()
            knd = str(r.get("kind") or "*").lower()
            dr = str(r.get("direction") or "*").lower()
            p = float(r.get("p_edge") or 0.0)
            rm = float(r.get("r_multiple") or 0.0)
            result = str(r.get("result") or "").upper()
            ts_ms = int(r.get("ts_ms") or 0)
            reason = str(r.get("reject_reason") or "OK")
            w = weight_for_reason(reason) if apply_weights else 1.0
            cal.observe(
                symbol=sym,
                regime=reg,
                kind=knd,
                p_edge=p,
                r_multiple=rm,
                result=result,
                ts_ms=ts_ms,
                direction=dr,
                weight=w,
            )
            accepted += 1
        except Exception as e:  # noqa: BLE001
            skipped += 1
            logger.debug("observe failed: %s row=%r", e, r)
    return accepted, skipped


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill p_edge calibrator bins from trades_closed")
    ap.add_argument("--hours", type=int, default=168, help="Backfill window in hours (default 168 = 7d)")
    ap.add_argument("--min-rows", type=int, default=50, help="Refuse to publish if rows < min-rows")
    ap.add_argument("--dry-run", action="store_true", help="Read + observe, but don't publish snapshot")
    ap.add_argument("--no-weights", action="store_true", help="Force weight=1.0 (ignore reject_reason_weights)")
    args = ap.parse_args()

    logger.info("Fetching trades_closed last %dh ...", args.hours)
    try:
        rows = _fetch_rows(args.hours)
    except Exception as e:  # noqa: BLE001
        logger.error("Postgres fetch failed: %s", e)
        return 1
    logger.info("Fetched %d rows", len(rows))

    if len(rows) < args.min_rows:
        logger.warning("Only %d rows < min-rows=%d — skipping snapshot write", len(rows), args.min_rows)
        return 2

    cal = PEdgeThresholdCalibrator(default_p_min=DEFAULT_P_MIN, enforce=False)
    accepted, skipped = _observe_all(cal, rows, apply_weights=not args.no_weights)
    logger.info("Observed: accepted=%d skipped=%d bins=%d", accepted, skipped, len(cal.bins))

    if args.dry_run:
        logger.info("DRY-RUN — snapshot NOT written. Top bins:")
        snap = cal.snapshot()
        for row in snap.get("bins", [])[:10]:
            logger.info("  %s", row)
        return 0

    snap = cal.snapshot()
    snap["_backfill"] = {
        "ts_ms": int(time.time() * 1000),
        "hours": args.hours,
        "rows_accepted": accepted,
        "rows_skipped": skipped,
        "tool_version": "v1",
    }

    try:
        rc = _get_redis()
        rc.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(snap))
        logger.info("Snapshot published to %s (%d bins)", RK.AUTOCAL_P_EDGE_STATE, len(snap.get("bins", [])))
    except Exception as e:  # noqa: BLE001
        logger.error("Redis publish failed: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
