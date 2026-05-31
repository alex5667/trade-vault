import psycopg2
import os

dsn = os.getenv("PG_DSN", "postgresql://trading:trading_password@127.0.0.1:5434/scanner_analytics")
try:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT raw_ctx->'evidence'->>'taker_flow_gate_shadow_veto' 
                FROM signals 
                WHERE raw_ctx->'evidence'->>'taker_flow_gate_shadow_veto' IS NOT NULL 
                LIMIT 1
            """)
            print(cur.fetchall())
except Exception as e:
    print(e)
