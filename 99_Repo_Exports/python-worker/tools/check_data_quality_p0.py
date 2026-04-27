#!/usr/bin/env python3
"""
Data quality check for P0 layer.
Checks for missing 'trades_closed_p0' entries for trades closed in the last 24h.
"""
import os
import sys

# Add python-worker to path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.analytics_db import get_conn

def check_missing_p0():
    sql = """
        SELECT count(*) AS missing_p0
        FROM trades_closed tc
        LEFT JOIN trades_closed_p0 p0
          ON p0.order_id = tc.order_id AND p0.exit_ts = tc.exit_ts
        WHERE tc.exit_ts >= now() - interval '24 hours'
          AND p0.order_id IS NULL;
    """
    
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        missing_count = row[0]
        
    print(f"Missing P0 entries (last 24h): {missing_count}")
    
    if missing_count > 0:
        print("ALERT: Integrity check failed. P0 coverage is incomplete.")
        sys.exit(1)
    else:
        print("OK: P0 integrity check passed.")

if __name__ == "__main__":
    check_missing_p0()
