import psycopg2
import os

DSN = os.getenv("ANALYTICS_DB_DSN") or "postgresql://postgres:12345@postgres:5432/scanner_analytics"

def run_migration():
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_atr_policy_audit_created_at
              ON atr_promotion_policy_audit (created_at DESC);
            """)
            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_atr_policy_audit_symbol_reason
              ON atr_promotion_policy_audit (symbol, reason_code, created_at DESC);
            """)
        print("Indexes created successfully.")
    except Exception as e:
        print(f"Failed to create indexes: {e}")

run_migration()
