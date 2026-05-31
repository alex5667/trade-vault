
import os
import psycopg2

dsn = os.getenv("TRADES_DB_DSN", "postgresql://trading:trading_password@postgres:5432/scanner_analytics")
conn = psycopg2.connect(dsn)
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'trades_closed';")
rows = cur.fetchall()
print("Columns in trades_closed:")
for r in rows:
    print(r[0])
conn.close()
