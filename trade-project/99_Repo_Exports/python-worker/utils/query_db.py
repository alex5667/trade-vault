import os

import psycopg2
import psycopg2.extras

pg_dsn = os.getenv("ANALYTICS_DB_DSN", f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}@scanner-postgres:5432/scanner_analytics")

def main():
    try:
        conn = psycopg2.connect(pg_dsn)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT payload_jsonb
                FROM execution_order_events
                WHERE symbol = 'SOLUSDT' AND payload_jsonb->>'action' = 'open'
                ORDER BY id DESC
                LIMIT 50;
            """)
            rows = cur.fetchall()
            for row in rows:
                p = row['payload_jsonb']
                if p.get('event_type') == 'state_transition' and p.get('details', {}).get('next_state') == 'PROTECTED':
                    d = p.get('details', {})
                    print("PROTECTED transition:", d.get('sl_algo_id'), d.get('tp_algo_ids'))
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
