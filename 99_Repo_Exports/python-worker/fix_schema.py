import os
import psycopg2
from services.analytics_db import get_conn
import logging

logging.basicConfig(level=logging.INFO)

try:
    with get_conn() as conn, conn.cursor() as cur:
        print("Connected to DB, running ALTER TABLE...")
        cur.execute("ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS trace_id TEXT DEFAULT '';")
        cur.execute("ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS event_id TEXT DEFAULT '';")
        conn.commit()
        print("Success! Added trace_id and event_id columns.")
except Exception as e:
    print(f"Failed: {e}")

