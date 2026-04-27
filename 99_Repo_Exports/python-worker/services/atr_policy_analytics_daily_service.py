from __future__ import annotations

import os
import psycopg2
import logging

logger = logging.getLogger("atr_policy_analytics_daily")

def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def run_once() -> int:
    try:
        conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_analytics_daily_service")
    except Exception as e:
        logger.error(f"Failed to connect to analytics DB: {e}")
        return 0

    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS atr_policy_analytics_daily (
                  day date NOT NULL,
                  symbol text NOT NULL,
                  kind text NOT NULL,
                  atr_policy_ver integer NOT NULL,
                  atr_policy_tag text NOT NULL,
                  atr_policy_scenario text NOT NULL,
                  atr_policy_regime text NOT NULL,
                  atr_policy_bucket text NOT NULL,
                  atr_stop_ttl_mode text NOT NULL,
                  atr_trailing_mode text NOT NULL,
                  atr_recovery_run_id text NOT NULL,
                  atr_restore_cert_status text NOT NULL,
                  n_trades integer NOT NULL,
                  avg_pnl_bps double precision,
                  avg_slippage_bps double precision,
                  win_rate double precision,
                  stop_rate double precision,
                  tp1_rate double precision,
                  PRIMARY KEY (
                    day, symbol, kind, atr_policy_ver, atr_policy_tag,
                    atr_policy_scenario, atr_policy_regime, atr_policy_bucket,
                    atr_stop_ttl_mode, atr_trailing_mode,
                    atr_recovery_run_id, atr_restore_cert_status
                  )
                );
                GRANT ALL PRIVILEGES ON TABLE atr_policy_analytics_daily TO trading;
            """)

            cur.execute("""
                INSERT INTO atr_policy_analytics_daily (
                  day, symbol, kind,
                  atr_policy_ver, atr_policy_tag,
                  atr_policy_scenario, atr_policy_regime, atr_policy_bucket,
                  atr_stop_ttl_mode, atr_trailing_mode,
                  atr_recovery_run_id, atr_restore_cert_status,
                  n_trades, avg_pnl_bps, avg_slippage_bps, win_rate, stop_rate, tp1_rate
                )
                SELECT
                  date(to_timestamp(t.exit_ts_ms / 1000.0)) AS day,
                  t.symbol,
                  CASE WHEN t.is_virtual THEN 'virtual' ELSE 'live' END AS kind,
                  coalesce(t.atr_policy_ver, 0),
                  coalesce(t.atr_policy_tag, ''),
                  coalesce(t.atr_policy_scenario, ''),
                  coalesce(t.atr_policy_regime, ''),
                  coalesce(t.atr_policy_bucket, ''),
                  coalesce(t.atr_stop_ttl_mode, ''),
                  coalesce(t.atr_trailing_mode, ''),
                  coalesce(t.atr_recovery_run_id, ''),
                  coalesce(t.atr_restore_cert_status, ''),
                  count(*)::int,
                  avg(t.pnl_pct * 10000),
                  avg(p0.slippage_bps_est),
                  avg(CASE WHEN t.pnl_net > 0 THEN 1.0 ELSE 0.0 END),
                  avg(CASE WHEN t.close_reason = 'stop_loss' THEN 1.0 ELSE 0.0 END),
                  avg(CASE WHEN t.close_reason = 'tp1_hit' THEN 1.0 ELSE 0.0 END)
                FROM trades_closed t
                LEFT JOIN trades_closed_p0 p0 ON t.order_id = p0.order_id
                WHERE t.exit_ts_ms >= (extract(epoch from now() - interval '30 days') * 1000)::bigint
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
                ON CONFLICT (
                    day, symbol, kind, atr_policy_ver, atr_policy_tag,
                    atr_policy_scenario, atr_policy_regime, atr_policy_bucket,
                    atr_stop_ttl_mode, atr_trailing_mode,
                    atr_recovery_run_id, atr_restore_cert_status
                ) DO UPDATE SET
                  n_trades = EXCLUDED.n_trades,
                  avg_pnl_bps = EXCLUDED.avg_pnl_bps,
                  avg_slippage_bps = EXCLUDED.avg_slippage_bps,
                  win_rate = EXCLUDED.win_rate,
                  stop_rate = EXCLUDED.stop_rate,
                  tp1_rate = EXCLUDED.tp1_rate
            """)
        return 1
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    sys.exit(0 if run_once() else 1)
