from __future__ import annotations

import os
import psycopg2
import psycopg2.extras
import redis

def _dsn():
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def run_once() -> bool:
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_allocator_tg_digest")
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  symbol, scenario, regime, risk_horizon_bucket, layer, policy_ver,
                  rollout_stage, restore_cert_status,
                  alloc_weight, risk_pct_mult, target_max_open_risk_pct
                FROM atr_policy_allocator_states
                WHERE is_current = true
                ORDER BY alloc_weight DESC
                LIMIT 10
            """)
            top = cur.fetchall()

            cur.execute("""
                SELECT
                  symbol, scenario, regime, risk_horizon_bucket, layer, policy_ver,
                  rollout_stage, restore_cert_status,
                  alloc_weight, risk_pct_mult
                FROM atr_policy_allocator_states
                WHERE is_current = true
                  AND alloc_weight = 0
                ORDER BY updated_at_ms DESC
                LIMIT 10
            """)
            zeroed = cur.fetchall()

        if not top and not zeroed:
            return False

        lines = ["💡 *ATR Policy Capital Allocator*", "", "📈 *Top allocations:*"]
        for r in top:
            lines.append(
                f"• {r['symbol']} | {r['scenario']} | {r['layer']} | v{r['policy_ver']} | "
                f"{r['rollout_stage']} | w={float(r['alloc_weight']):.3f} | "
                f"mult={float(r['risk_pct_mult']):.2f} | cap={float(r['target_max_open_risk_pct']):.2f}%"
            )
        lines += ["", "🛑 *Zeroed cohorts:*"]
        for r in zeroed:
            lines.append(
                f"• {r['symbol']} | {r['scenario']} | {r['layer']} | v{r['policy_ver']} | "
                f"{r['rollout_stage']} | cert={r.get('restore_cert_status') or '-'}"
            )

        payload = {
            "text": "\n".join(lines),
            "parse_mode": "Markdown"
        }
        chat_id = os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "")
        if chat_id:
            payload["chat_id"] = chat_id
            
        r = _redis()
        r.xadd("notify:telegram", payload, maxlen=5000, approximate=True)
        return True
    finally:
        conn.close()

if __name__ == "__main__":
    if os.getenv("ATR_POLICY_ALLOCATOR_ENABLE", "1") == "1":
        run_once()
        print("Telegram digest sent.")
    else:
        print("Allocator disabled via ATR_POLICY_ALLOCATOR_ENABLE.")
