import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'python-worker'))
from services.analytics_db import get_conn
with open('python-worker/db/migrations/20260416_32_atr_release_gate_framework.sql', 'r') as f:
    sql = f.read()
with get_conn() as conn, conn.cursor() as cur:
    cur.execute(sql)
    conn.commit()
print("Migration applied successfully.")
