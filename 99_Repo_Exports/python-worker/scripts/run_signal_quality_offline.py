#!/usr/bin/env python3
"""
Run offline signal quality computation job.

This script processes historical signal data and computes quality metrics
by feature clusters for use in real-time signal filtering.
"""

import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signal_quality import run_offline_quality_job

def main():
    """Main function to run offline quality job."""
    pg_dsn = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/trade")
    horizon = os.getenv("QUALITY_HORIZON", "R_main")
    lookback_days = int(os.getenv("QUALITY_LOOKBACK_DAYS", "180"))

    print("🚀 Running offline signal quality computation")
    print(f"  PG_DSN: {pg_dsn}")
    print(f"  Horizon: {horizon}")
    print(f"  Lookback: {lookback_days} days")
    print()

    try:
        run_offline_quality_job(
            pg_dsn=pg_dsn
            horizon=horizon
            lookback_days=lookback_days
        )
        print("\n✅ Offline quality computation completed successfully!")
        return 0
    except Exception as e:
        print(f"\n❌ Error during offline quality computation: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
