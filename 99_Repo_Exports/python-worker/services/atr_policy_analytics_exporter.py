from __future__ import annotations

import os
import time
import psycopg2
import psycopg2.extras
from prometheus_client import Gauge, start_http_server

def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

g_policy_avg_pnl_bps = Gauge(
    "atr_policy_avg_pnl_bps",
    "Average pnl_bps by policy cohort",
    ["policy_ver", "symbol", "scenario", "regime", "bucket", "stop_mode", "trail_mode", "cert_status"]
)

g_policy_win_rate = Gauge(
    "atr_policy_win_rate",
    "Win rate by policy cohort",
    ["policy_ver", "symbol", "scenario", "regime", "bucket", "stop_mode", "trail_mode", "cert_status"]
)

g_policy_slippage_bps = Gauge(
    "atr_policy_slippage_bps",
    "Average slippage_bps by policy cohort",
    ["policy_ver", "symbol", "scenario", "regime", "bucket", "stop_mode", "trail_mode", "cert_status"]
)

g_policy_n_trades = Gauge(
    "atr_policy_n_trades",
    "Trade count by policy cohort",
    ["policy_ver", "symbol", "scenario", "regime", "bucket", "stop_mode", "trail_mode", "cert_status"]
)

def export_once():
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_analytics_exporter")
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  atr_policy_ver,
                  symbol,
                  atr_policy_scenario,
                  atr_policy_regime,
                  atr_policy_bucket,
                  atr_stop_ttl_mode,
                  atr_trailing_mode,
                  atr_restore_cert_status,
                  sum(n_trades) AS n_trades,
                  sum(avg_pnl_bps * n_trades) / nullif(sum(n_trades), 0) AS avg_pnl_bps,
                  sum(avg_slippage_bps * n_trades) / nullif(sum(n_trades), 0) AS avg_slippage_bps,
                  sum(win_rate * n_trades) / nullif(sum(n_trades), 0) AS win_rate
                FROM atr_policy_analytics_daily
                WHERE day >= CURRENT_DATE - 7
                GROUP BY 1,2,3,4,5,6,7,8
            """)
            for row in cur.fetchall():
                labels = [
                    str(row["atr_policy_ver"] or "0"),
                    row["symbol"] or "",
                    row["atr_policy_scenario"] or "",
                    row["atr_policy_regime"] or "",
                    row["atr_policy_bucket"] or "",
                    row["atr_stop_ttl_mode"] or "",
                    row["atr_trailing_mode"] or "",
                    row["atr_restore_cert_status"] or "",
                ]
                g_policy_n_trades.labels(*labels).set(float(row["n_trades"] or 0))
                g_policy_avg_pnl_bps.labels(*labels).set(float(row["avg_pnl_bps"] or 0))
                g_policy_slippage_bps.labels(*labels).set(float(row["avg_slippage_bps"] or 0))
                g_policy_win_rate.labels(*labels).set(float(row["win_rate"] or 0))
    finally:
        conn.close()

def run_forever():
    start_http_server(int(os.getenv("ATR_POLICY_ANALYTICS_METRICS_PORT", "9138")))
    interval = int(os.getenv("ATR_POLICY_ANALYTICS_EXPORT_SEC", "60"))
    while True:
        try:
            export_once()
        except Exception as e:
            print(f"Exporter error: {e}")
        time.sleep(interval)

if __name__ == "__main__":
    run_forever()
