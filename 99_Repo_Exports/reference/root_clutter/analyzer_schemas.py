import psycopg2

def check_db(db_name):
    try:
        conn = psycopg2.connect(f"postgresql://postgres:12345@localhost:5434/{db_name}")
        cur = conn.cursor()
        cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = 'signals'")
        for schema, table in cur.fetchall():
            try:
                cur.execute(f"SELECT count(*) FROM {schema}.{table}")
                print(f"{db_name}.{schema}.{table}: {cur.fetchone()[0]} rows")
            except Exception as e:
                pass
        conn.close()
    except Exception: pass

check_db("trade")
check_db("scanner_analytics")
