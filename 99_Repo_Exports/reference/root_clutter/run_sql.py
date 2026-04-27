import psycopg2
import os

dsn = "postgresql://trading:trading_password@127.0.0.1:5434/scanner_analytics"

with open('python-worker/services/archivers/sql/20260224_of_gate_metrics_rollups_p78.sql', 'r') as f:
    sql = f.read()

try:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            print("SQL executed successfully")
except Exception as e:
    print(f"Error executing SQL: {e}")
