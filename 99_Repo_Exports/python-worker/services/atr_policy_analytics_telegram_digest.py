from __future__ import annotations

import os
import psycopg2
import psycopg2.extras

try:
    import redis
except ImportError:
    redis = None

def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _redis():
    if not redis:
        raise RuntimeError("Redis library not available")
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def publish_digest() -> bool:
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_analytics_tg_digest")
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  symbol,
                  atr_policy_ver,
                  atr_restore_cert_status,
                  sum(n_trades) AS n_trades,
                  sum(avg_pnl_bps * n_trades) / nullif(sum(n_trades), 0) AS avg_pnl_bps,
                  sum(avg_slippage_bps * n_trades) / nullif(sum(n_trades), 0) AS avg_slippage_bps
                FROM atr_policy_analytics_daily
                WHERE day >= CURRENT_DATE - 3
                GROUP BY 1,2,3
                ORDER BY avg_pnl_bps DESC NULLS LAST
                LIMIT 5
            """)
            best = cur.fetchall()

            cur.execute("""
                SELECT
                  symbol,
                  atr_policy_ver,
                  atr_restore_cert_status,
                  sum(n_trades) AS n_trades,
                  sum(avg_pnl_bps * n_trades) / nullif(sum(n_trades), 0) AS avg_pnl_bps,
                  sum(avg_slippage_bps * n_trades) / nullif(sum(n_trades), 0) AS avg_slippage_bps
                FROM atr_policy_analytics_daily
                WHERE day >= CURRENT_DATE - 3
                GROUP BY 1,2,3
                ORDER BY avg_pnl_bps ASC NULLS LAST
                LIMIT 5
            """)
            worst = cur.fetchall()

        if not best and not worst:
            return True

        lines = ["ATR Policy Analytics Digest", "", "Best cohorts:"]
        for r in best:
            lines.append(
                f"- {r['symbol']} | v{r['atr_policy_ver']} | cert={r['atr_restore_cert_status'] or '-'} | "
                f"n={r['n_trades']} | pnl_bps={float(r['avg_pnl_bps'] or 0):.2f} | slip={float(r['avg_slippage_bps'] or 0):.2f}"
            )
        lines += ["", "Worst cohorts:"]
        for r in worst:
            lines.append(
                f"- {r['symbol']} | v{r['atr_policy_ver']} | cert={r['atr_restore_cert_status'] or '-'} | "
                f"n={r['n_trades']} | pnl_bps={float(r['avg_pnl_bps'] or 0):.2f} | slip={float(r['avg_slippage_bps'] or 0):.2f}"
            )

        payload = {"text": "\\n".join(lines)}
        chat_id = os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "")
        if chat_id:
            payload["chat_id"] = chat_id
        try:
            _redis().xadd("notify:telegram", payload, maxlen=5000, approximate=True)
        except Exception as e:
            print(f"Failed to publish digest: {e}")
            return False
            
        return True
    finally:
        conn.close()

def run_once() -> int:
    return 1 if publish_digest() else 0

if __name__ == "__main__":
    run_once()
