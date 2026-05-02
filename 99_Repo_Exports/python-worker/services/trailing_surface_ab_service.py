from __future__ import annotations

import json
import os
import time
from typing import Any

import psycopg2
import psycopg2.extras
import redis
try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    ),


def _redis():
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _min_group_n() -> int:
    try:
        return int(os.getenv("TRAILING_SURFACE_AB_MIN_GROUP_N", "50") or 50)
    except Exception:
        return 50


def _window_days() -> int:
    try:
        return int(os.getenv("TRAILING_SURFACE_AB_WINDOW_DAYS", "21") or 21)
    except Exception:
        return 21


def run_once() -> int:
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="trailing_surface_ab_service")
    r = _redis()
    written = 0
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                WITH base AS (
                  SELECT
                    date_trunc('day', to_timestamp(t.exit_ts_ms / 1000.0))::date AS day,
                    t.source,
                    t.symbol,
                    lower(coalesce(nullif(p0.scenario, ''), 'unknown')) AS scenario,
                    lower(coalesce(nullif(p0.regime, ''), 'na')) AS regime,
                    coalesce(t.sc_risk_horizon_bucket, t.risk_horizon_bucket, 'unknown') AS risk_horizon_bucket,
                    coalesce(t.trailing_surface_applied, false) AS trailing_surface_applied,
                    count(*) AS n,
                    avg(t.pnl_bps) AS avg_pnl_bps,
                    percentile_disc(0.5) WITHIN GROUP (ORDER BY t.pnl_bps) AS median_pnl_bps,
                    avg(CASE WHEN t.pnl_net > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
                    avg(CASE WHEN coalesce(t.tp1_hit, false) THEN 1.0 ELSE 0.0 END) AS tp1_rate,
                    avg(CASE WHEN lower(coalesce(t.close_reason, '')) IN ('stop_loss','sl','stop') THEN 1.0 ELSE 0.0 END) AS stop_rate,
                    avg(coalesce(t.slippage_bps_est, 0.0)) AS avg_slippage_bps,
                    avg(coalesce(t.duration_ms, t.hold_ms, 0.0)) AS avg_hold_ms,
                    avg(coalesce(t.mae_bps, 0.0)) AS avg_mae_pct,
                    avg(coalesce(t.mfe_pnl, 0.0)) AS avg_mfe_pnl
                  FROM trades_closed t
                  LEFT JOIN trades_closed_p0 p0
                    ON p0.order_id = t.order_id
                  WHERE t.exit_ts_ms >= (extract(epoch from now()) * 1000)::bigint - (%s * 86400 * 1000)
                  GROUP BY 1,2,3,4,5,6,7
                ),
                SELECT * FROM base
                WHERE n >= %s
                ORDER BY day DESC, source, symbol, scenario, regime, risk_horizon_bucket, trailing_surface_applied
                """
                (_window_days(), _min_group_n()),
            ),
            rows = cur.fetchall()

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS horizon_trailing_surface_ab_daily (
                  day date NOT NULL,
                  source text NOT NULL,
                  symbol text NOT NULL,
                  scenario text NOT NULL,
                  regime text NOT NULL,
                  risk_horizon_bucket text NOT NULL,
                  trailing_surface_applied boolean NOT NULL,
                  n integer NOT NULL,
                  avg_pnl_bps double precision,
                  median_pnl_bps double precision,
                  win_rate double precision,
                  tp1_rate double precision,
                  stop_rate double precision,
                  avg_slippage_bps double precision,
                  avg_hold_ms double precision,
                  avg_mae_pct double precision,
                  avg_mfe_pnl double precision,
                  updated_at_ms bigint NOT NULL,
                  PRIMARY KEY (day, source, symbol, scenario, regime, risk_horizon_bucket, trailing_surface_applied)
                ),
                """
            ),

            upsert_sql = """
            INSERT INTO horizon_trailing_surface_ab_daily (
              day, source, symbol, scenario, regime, risk_horizon_bucket, trailing_surface_applied,
              n, avg_pnl_bps, median_pnl_bps, win_rate, tp1_rate, stop_rate,
              avg_slippage_bps, avg_hold_ms, avg_mae_pct, avg_mfe_pnl, updated_at_ms
            ) VALUES (
              %(day)s, %(source)s, %(symbol)s, %(scenario)s, %(regime)s, %(risk_horizon_bucket)s, %(trailing_surface_applied)s,
              %(n)s, %(avg_pnl_bps)s, %(median_pnl_bps)s, %(win_rate)s, %(tp1_rate)s, %(stop_rate)s,
              %(avg_slippage_bps)s, %(avg_hold_ms)s, %(avg_mae_pct)s, %(avg_mfe_pnl)s, %(updated_at_ms)s
            ),
            ON CONFLICT (day, source, symbol, scenario, regime, risk_horizon_bucket, trailing_surface_applied)
            DO UPDATE SET
              n = EXCLUDED.n,
              avg_pnl_bps = EXCLUDED.avg_pnl_bps,
              median_pnl_bps = EXCLUDED.median_pnl_bps,
              win_rate = EXCLUDED.win_rate,
              tp1_rate = EXCLUDED.tp1_rate,
              stop_rate = EXCLUDED.stop_rate,
              avg_slippage_bps = EXCLUDED.avg_slippage_bps,
              avg_hold_ms = EXCLUDED.avg_hold_ms,
              avg_mae_pct = EXCLUDED.avg_mae_pct,
              avg_mfe_pnl = EXCLUDED.avg_mfe_pnl,
              updated_at_ms = EXCLUDED.updated_at_ms
            """

            now_ms = int(time.time() * 1000)
            for row in rows:
                row["updated_at_ms"] = now_ms
                cur.execute(upsert_sql, row)
                written += 1

        # lightweight suggestion layer
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                WITH pair AS (
                  SELECT
                    source, symbol, scenario, regime, risk_horizon_bucket,
                    max(CASE WHEN trailing_surface_applied THEN n END) AS n_canary,
                    max(CASE WHEN NOT trailing_surface_applied THEN n END) AS n_control,
                    max(CASE WHEN trailing_surface_applied THEN avg_pnl_bps END) AS pnl_canary,
                    max(CASE WHEN NOT trailing_surface_applied THEN avg_pnl_bps END) AS pnl_control,
                    max(CASE WHEN trailing_surface_applied THEN tp1_rate END) AS tp1_canary,
                    max(CASE WHEN NOT trailing_surface_applied THEN tp1_rate END) AS tp1_control,
                    max(CASE WHEN trailing_surface_applied THEN avg_slippage_bps END) AS slip_canary,
                    max(CASE WHEN NOT trailing_surface_applied THEN avg_slippage_bps END) AS slip_control
                  FROM horizon_trailing_surface_ab_daily
                  WHERE day >= current_date - %s::int
                  GROUP BY 1,2,3,4,5
                ),
                SELECT * FROM pair
                WHERE coalesce(n_canary,0) >= %s
                  AND coalesce(n_control,0) >= %s
                """
                (_window_days(), _min_group_n(), _min_group_n()),
            ),
            for row in cur.fetchall():
                key = f"cfg:suggestions:atr_trailing_surface:{row['source']}:{row['symbol']}:{row['scenario']}:{row['regime']}:{row['risk_horizon_bucket']}"
                suggest = {
                    "source": row["source"],
                    "symbol": row["symbol"],
                    "scenario": row["scenario"],
                    "regime": row["regime"],
                    "risk_horizon_bucket": row["risk_horizon_bucket"],
                    "n_canary": row["n_canary"],
                    "n_control": row["n_control"],
                    "pnl_canary": row["pnl_canary"],
                    "pnl_control": row["pnl_control"],
                    "tp1_canary": row["tp1_canary"],
                    "tp1_control": row["tp1_control"],
                    "slip_canary": row["slip_canary"],
                    "slip_control": row["slip_control"],
                    "promote": bool(
                        (row["pnl_canary"] or 0.0) >= (row["pnl_control"] or 0.0)
                        and (row["tp1_canary"] or 0.0) >= (row["tp1_control"] or 0.0) - 0.01
                        and (row["slip_canary"] or 0.0) <= (row["slip_control"] or 0.0) + 0.5
                    ),
                    "updated_at_ms": int(time.time() * 1000),
                }
                r.set(key, json.dumps(suggest, ensure_ascii=False, sort_keys=True))

    finally:
        conn.close()
    return written


if __name__ == "__main__":
    print(run_once())
