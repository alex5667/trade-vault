import psycopg2
import psycopg2.extras
import os
import json

DB_DSN = os.getenv("DATABASE_URL", "postgresql://trade:trade@localhost:5432/trade")

def run():
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cur.execute("""
        SELECT symbol, direction, entry_price, sl_price, tp_price, is_virtual, entry_ts 
        FROM trades 
        WHERE is_virtual = true 
        ORDER BY entry_ts DESC 
        LIMIT 5;
    """)
    rows = cur.fetchall()
    
    if not rows:
        print("No virtual trades in DB.")
        return
        
    for r in rows:
        print(dict(r))

run()
