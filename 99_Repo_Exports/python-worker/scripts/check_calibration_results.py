#!/usr/bin/env python3
"""
Script to check local calibration results in the database.

Usage:
    python scripts/check_calibration_results.py

Environment variables:
    PG_DSN - PostgreSQL connection string
"""

import os
import sys
from collections import defaultdict

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("❌ psycopg2 not installed. Install with: pip install psycopg2-binary")
    sys.exit(1)

def check_calibration_results():
    """Check calibration results in the database."""
    pg_dsn = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/trade")

    print(f"🔍 Checking calibration results in database...")
    print(f"📊 PG_DSN: {pg_dsn}")
    print()

    try:
        conn = psycopg2.connect(pg_dsn)
        with conn.cursor(cursor_factory=DictCursor) as cur:

            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'signal_local_calibration'
                )
            """)
            table_exists = cur.fetchone()[0]

            if not table_exists:
                print("❌ Table 'signal_local_calibration' does not exist!")
                print("   Run the SQL migration first:")
                print("   psql -d <database> -f python-worker/migrations/001_add_local_calibration.sql")
                return

            # Get total count
            cur.execute("SELECT COUNT(*) FROM signal_local_calibration")
            total_count = cur.fetchone()[0]
            print(f"📈 Total calibration entries: {total_count}")

            if total_count == 0:
                print("⚠️  No calibration data found!")
                print("   Run calibration first:")
                print("   python scripts/run_local_calibration.py")
                return

            # Get statistics by symbol
            cur.execute("""
                SELECT
                    symbol,
                    COUNT(*) as entries,
                    AVG(count_samples) as avg_samples,
                    MIN(updated_at) as oldest_update,
                    MAX(updated_at) as newest_update
                FROM signal_local_calibration
                GROUP BY symbol
                ORDER BY symbol
            """)

            print("\n📊 Statistics by symbol:")
            print("-" * 80)
            print("<10")
            for row in cur:
                symbol = row['symbol']
                entries = row['entries']
                avg_samples = row['avg_samples']
                oldest = row['oldest_update']
                newest = row['newest_update']
                print("<10")

            # Get statistics by session
            cur.execute("""
                SELECT session, COUNT(*) as count
                FROM signal_local_calibration
                GROUP BY session
                ORDER BY session
            """)

            print("\n🕐 Statistics by session:")
            print("-" * 30)
            for row in cur:
                print(f"  {row['session']:<8}: {row['count']}")

            # Get statistics by regime
            cur.execute("""
                SELECT regime, COUNT(*) as count
                FROM signal_local_calibration
                GROUP BY regime
                ORDER BY regime
            """)

            print("\n🎯 Statistics by regime:")
            print("-" * 30)
            for row in cur:
                print(f"  {row['regime']:<8}: {row['count']}")

            # Get statistics by metric
            cur.execute("""
                SELECT metric, COUNT(*) as count, AVG(chosen_threshold) as avg_threshold
                FROM signal_local_calibration
                GROUP BY metric
                ORDER BY metric
            """)

            print("\n📏 Statistics by metric:")
            print("-" * 50)
            for row in cur:
                metric = row['metric']
                count = row['count']
                avg_threshold = row['avg_threshold']
                print(f"  {metric:<30}: {count:>5} (Avg: {avg_threshold:>8.4f})")

            # Show sample entries
            print("\n🔍 Sample calibration entries:")
            print("-" * 100)
            cur.execute("""
                SELECT symbol, session, regime, metric, q90, q95, q98, chosen_threshold, count_samples
                FROM signal_local_calibration
                ORDER BY symbol, session, regime, metric
                LIMIT 10
            """)

            print(f"{'Symbol':<10} {'Session':<10} {'Regime':<10} {'Metric':<20} {'Q90':>8} {'Q95':>8} {'Q98':>8} {'Thresh':>8} {'Samples':>8}")
            for row in cur:
                print(f"{row['symbol']:<10} {row['session']:<10} {row['regime']:<10} {row['metric']:<20} "
                      f"{row['q90']:>8.2f} {row['q95']:>8.2f} {row['q98']:>8.2f} {row['chosen_threshold']:>8.2f} {row['count_samples']:>8}")

            # Check data quality
            print("\n✅ Data quality checks:")
            # Check for NULL values
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN q90 IS NULL THEN 1 END) as null_q90,
                    COUNT(CASE WHEN chosen_threshold IS NULL THEN 1 END) as null_threshold,
                    COUNT(CASE WHEN count_samples < 100 THEN 1 END) as low_samples
                FROM signal_local_calibration
            """)

            quality = cur.fetchone()
            total = quality['total']
            null_q90 = quality['null_q90']
            null_threshold = quality['null_threshold']
            low_samples = quality['low_samples']

            print(f"  Total entries: {total}")
            print(f"  NULL q90 values: {null_q90} ({null_q90/total*100:.1f}%)" if total > 0 else "  NULL q90 values: 0")
            print(f"  NULL thresholds: {null_threshold} ({null_threshold/total*100:.1f}%)" if total > 0 else "  NULL thresholds: 0")
            print(f"  Low sample count (<100): {low_samples} ({low_samples/total*100:.1f}%)" if total > 0 else "  Low sample count (<100): 0")

            # Check for recent updates
            cur.execute("""
                SELECT MAX(updated_at) as last_update,
                       EXTRACT(EPOCH FROM (NOW() - MAX(updated_at)))/3600 as hours_ago
                FROM signal_local_calibration
            """)

            update_info = cur.fetchone()
            if update_info['last_update']:
                hours_ago = update_info['hours_ago']
                print(f"  Last update: {update_info['last_update']} ({hours_ago:.1f} hours ago)")
            else:
                print("  Last update: Never")

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        print("💡 Make sure PG_DSN is correctly configured:")
        print(f"   Current: {pg_dsn}")
        print("   Example: postgresql://username:password@localhost:5432/database_name")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_calibration_results()
