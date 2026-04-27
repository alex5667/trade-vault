#!/usr/bin/env python3
import os
import sys
import argparse
import psycopg2

def run():
    print("🚀 Bootstrapping Phase 8.6: Protective Lifecycle Shadowing...")
    
    db_dsn = os.getenv("TRADES_DB_DSN", "postgresql://trading:SecureTrade99!@localhost:5432/trade")
    try:
        conn = psycopg2.connect(db_dsn)
        conn.autocommit = True
    except Exception as e:
        print(f"❌ Failed to connect to trade database: {e}")
        sys.exit(1)
        
    sql_path = os.path.join(os.path.dirname(__file__), "../sql/010_phase86_protective_lifecycle_graph.sql")
    if not os.path.exists(sql_path):
        print(f"❌ SQL file not found: {sql_path}")
        sys.exit(1)
        
    print(f"📂 Loading SQL from {sql_path}...")
    with open(sql_path, "r") as f:
        sql = f.read()
        
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            print("✅ Successfully applied Phase 8.6 SQL migrations")
    except Exception as e:
        print(f"❌ Error applying SQL: {e}")
        sys.exit(1)
        
    print("\n✅ Phase 8.6 Infrastructure initialized.")
    print("To enable the shadowing in production, add the following to your .env or docker-compose:")
    print("  ATR_GRAPH_PROTECTIVE_ENABLE=1")
    print("  ATR_GRAPH_PROTECTIVE_SYMBOLS=BCHUSDT,SOLUSDT,BTCUSDT,ETHUSDT  # Example")
    print("\nRemember: The system will operate in 'shadow_compare' mode and merely observe existing logic.")

if __name__ == "__main__":
    run()
