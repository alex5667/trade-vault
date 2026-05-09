#!/usr/bin/env python3
"""
Update TTD Configuration Script

Calculates TTD quantiles from historical signal performance data
and updates the signal_ttd_config table.

Usage:
    python scripts/update_ttd_config.py

Environment Variables:
    PG_DSN: PostgreSQL connection string (default: from config)
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import psycopg2
    from psycopg2.extras import DictCursor

    from core.config import PG_DSN
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from python-worker directory")
    sys.exit(1)


def update_ttd_config(pg_dsn: str) -> None:
    """
    Update TTD configuration from historical performance data.

    Args:
        pg_dsn: PostgreSQL connection string
    """
    print("🔄 Updating TTD configuration...")

    # Read the SQL script
    sql_file = Path(__file__).parent / "update_ttd_config.sql"
    if not sql_file.exists():
        print(f"❌ SQL file not found: {sql_file}")
        return

    with open(sql_file) as f:
        sql_script = f.read()

    # Execute the update
    try:
        with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(sql_script)
            conn.commit()

        print("✅ TTD configuration updated successfully")

    except Exception as e:
        print(f"❌ Error updating TTD config: {e}")
        raise


def show_current_config(pg_dsn: str) -> None:
    """
    Display current TTD configuration.

    Args:
        pg_dsn: PostgreSQL connection string
    """
    print("\n📊 Current TTD Configuration:")
    print("-" * 80)

    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute("""
                    SELECT symbol, setup_type, ttd_q50_bars, ttd_q75_bars, ttd_q90_bars, expiry_bars, updated_at
                    FROM signal_ttd_config
                    ORDER BY symbol, setup_type
                """)

                rows = cur.fetchall()
                if not rows:
                    print("No TTD configuration found. Run update first.")
                    return

                print(f"{'Symbol':<10} {'Setup':<15} {'Q50':<5} {'Q75':<5} {'Q90':<5} {'Expiry':<7} {'Updated'}")
                print("-" * 80)

                for row in rows:
                    updated = row['updated_at'].strftime('%Y-%m-%d %H:%M') if row['updated_at'] else 'N/A'
                    print(f"{row['symbol']:<10} {row['setup_type']:<15} {row['ttd_q50_bars']:<5} {row['ttd_q75_bars']:<5} {row['ttd_q90_bars']:<5} {row['expiry_bars']:<7} {updated}")

    except Exception as e:
        print(f"❌ Error reading TTD config: {e}")


def main():
    """Main function."""
    # Get PG_DSN from environment or config
    pg_dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN")) or PG_DSN
    if not pg_dsn:
        print("❌ PG_DSN not found. Set PG_DSN environment variable or check config.py")
        sys.exit(1)

    print(f"🔌 Using database: {pg_dsn.replace(pg_dsn.split('@')[0], '***:***')}")

    # Update configuration
    update_ttd_config(pg_dsn)

    # Show results
    show_current_config(pg_dsn)

    print("\n✅ TTD configuration update complete!")


if __name__ == "__main__":
    main()
