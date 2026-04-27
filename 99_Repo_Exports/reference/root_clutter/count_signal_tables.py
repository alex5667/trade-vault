import psycopg2

try:
    conn = psycopg2.connect("postgresql://postgres:12345@localhost:5434/trade")
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'signal%'")
    tables = [r[0] for r in cur.fetchall()]
    print("Row counts for signal* tables:")
    for t in tables:
        cur.execute(f"SELECT count(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f"{t}: {cnt}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
