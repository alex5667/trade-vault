import psycopg2
conn = psycopg2.connect("postgresql://postgres:12345@localhost:5434/postgres")
cur = conn.cursor()
cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
for d in cur.fetchall(): print(d[0])
conn.close()
