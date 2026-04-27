from __future__ import annotations

import os
import time
import logging
from prometheus_client import Gauge, Counter, start_http_server

from services.analytics_db import get_conn

logger = logging.getLogger("atr_change_control_exporter")

# Metrics definitions
g_change_requests_total = Gauge(
    "atr_change_requests_total",
    "Current count of active change requests",
    ["status", "change_type"]
)

g_change_cycle_time_sec = Gauge(
    "atr_change_cycle_time_sec",
    "Average cycle time in seconds for completed changes",
    ["change_type"]
)

g_change_approval_latency_sec = Gauge(
    "atr_change_approval_latency_sec",
    "Average time waiting for approval in seconds",
    ["change_type"]
)

c_change_incident_without_record_total = Counter(
    "atr_change_incident_without_record_total",
    "Count of incident overrides running longer than permitted without formal record"
)

def export_once():
    """Queries DB and updates prometheus gauges."""
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT status, change_type, count(*) as cnt 
                FROM atr_change_requests 
                GROUP BY status, change_type
            """)
            # Reset gauges first for active reporting
            g_change_requests_total.clear()
            
            for row in cur.fetchall():
                g_change_requests_total.labels(
                    status=row["status"],
                    change_type=row["change_type"]
                ).set(float(row["cnt"]))
                
            cur.execute("""
                SELECT change_type, avg(updated_at_ms - created_at_ms) / 1000.0 as cycle_sec
                FROM atr_change_requests
                WHERE status = 'COMPLETED'
                GROUP BY change_type
            """)
            g_change_cycle_time_sec.clear()
            for row in cur.fetchall():
                g_change_cycle_time_sec.labels(change_type=row["change_type"]).set(float(row["cycle_sec"] or 0))
                
            # A rudimentary check for "incident overrides without formal change",
            # for example looking at some overrides DB or status table (assuming it's checked here 
            # or in a combined watchdog). We'll increment the counter if we find long-running overrides.
            # E.g.
            # cur.execute("SELECT count(*) FROM some_incident_overrides_table WHERE duration > X AND change_id IS NULL")
            # If so, c_change_incident_without_record_total.inc(val)
            
    except Exception as e:
        logger.error(f"Exporter error: {e}")

def run_forever():
    port = int(os.getenv("ATR_CHANGE_METRICS_PORT", "9139"))
    interval = int(os.getenv("ATR_CHANGE_EXPORT_SEC", "30"))
    start_http_server(port)
    logger.info(f"ATR Change Control Exporter started on port {port}")
    
    while True:
        try:
            export_once()
        except Exception as e:
            logger.error(f"Exporter error: {e}")
        time.sleep(interval)

if __name__ == "__main__":
    run_forever()
