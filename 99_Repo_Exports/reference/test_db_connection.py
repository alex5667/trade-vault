#!/usr/bin/env python3
"""
Simple test script to verify database connection and table existence.
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

def test_connection():
    # Test DSN for scanner_analytics
    dsn = "postgresql://postgres:12345@localhost:5434/scanner_analytics"

    try:
        print("Testing connection to scanner_analytics...")
        conn = psycopg2.connect(dsn)
        print("✅ Connection successful")

        # Test table existence
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cur.fetchall()

        print(f"📋 Found {len(tables)} tables:")
        for (table_name,) in tables:
            print(f"  - {table_name}")

        # Test trades_closed structure
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'trades_closed'
            ORDER BY ordinal_position
            LIMIT 10
        """)
        columns = cur.fetchall()

        print("📊 trades_closed columns (first 10):")
        for col_name, data_type, is_nullable in columns:
            print(f"  - {col_name}: {data_type} {'NULL' if is_nullable == 'YES' else 'NOT NULL'}")

        cur.close()
        conn.close()

        print("✅ All tests passed!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
