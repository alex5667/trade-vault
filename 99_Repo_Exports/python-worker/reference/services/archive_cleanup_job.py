#!/usr/bin/env python3
"""
Archive Cleanup Job.

Manages retention of archived data in PostgreSQL.
Although TimescaleDB retention policies handle partition dropping
this script handles additional cleanup logic if needed and provides logging.

Features:
- Validates retention policies exist
- logs retention status
"""

import os
import sys
import logging
import psycopg2

# Configuration
PG_DSN = os.getenv("ANALYTICS_DSN", "postgresql://trading:trading_password@postgres:5432/scanner_analytics")

logging.basicConfig(
    level=logging.INFO
    format="%(asctime)s [%(levelname)s] %(message)s"
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("archive_cleanup")

def check_retention_policies(cur):
    """Check if TimescaleDB retention policies are active."""
    cur.execute("""
        SELECT j.hypertable_name, j.config
        FROM timescaledb_information.jobs j
        WHERE proc_name = 'policy_retention';
    """)
    policies = cur.fetchall()
    if policies:
        for p in policies:
            logger.info(f"✅ Active retention policy for {p[0]}: {p[1]}")
    else:
        logger.warning("⚠️ No active TimescaleDB retention policies found!")

def manual_cleanup(cur):
    """Perform any manual cleanup not covered by retention policies."""
    # Example: Delete metadata older than 90 days
    cur.execute("""
        DELETE FROM archive_metadata 
        WHERE last_archived_at < NOW() - INTERVAL '90 days';
    """)
    deleted = cur.rowcount
    if deleted > 0:
        logger.info(f"cleaned up {deleted} old metadata records")

def main():
    logger.info("Starting Archive Cleanup Job...")
    
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        
        with conn.cursor() as cur:
            # 1. Check TimescaleDB policies
            check_retention_policies(cur)
            
            # 2. Manual cleanup
            manual_cleanup(cur)
            
            # 3. Optimize (VACUUM ANALYZE) metadata table
            cur.execute("VACUUM ANALYZE archive_metadata;")
            
        logger.info("Cleanup job completed successfully.")
        conn.close()
        
    except Exception as e:
        logger.error(f"Cleanup job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
