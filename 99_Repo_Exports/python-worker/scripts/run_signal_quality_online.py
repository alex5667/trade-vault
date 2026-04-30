#!/usr/bin/env python3
"""
Run online signal quality computation job.

This script maintains rolling quality assessment based on recent signal performance.
"""

import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signal_quality import run_online_quality_job

def main():
    """Main function to run online quality job."""
    pg_dsn = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/trade")
    horizon = os.getenv("QUALITY_HORIZON", "R_main")
    roll_n = int(os.getenv("QUALITY_ROLLING_WINDOW", "200"))

    print("🚀 Running online signal quality computation")
    print(f"  PG_DSN: {pg_dsn}")
    print(f"  Horizon: {horizon}")
    print(f"  Rolling window: {roll_n} signals")
    print()

    try:
        run_online_quality_job(
            pg_dsn=pg_dsn
            horizon=horizon
            roll_n=roll_n
        )
        print("\n✅ Online quality computation completed successfully!")
        return 0
    except Exception as e:
        print(f"\n❌ Error during online quality computation: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
