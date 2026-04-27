import psycopg2

try:
    conn = psycopg2.connect("postgresql://postgres:12345@localhost:5434/scanner_analytics")
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = [r[0] for r in cur.fetchall()]
    print("Tables in scanner_analytics:")
    for t in tables:
        cur.execute(f"SELECT count(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f"{t}: {cnt}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
