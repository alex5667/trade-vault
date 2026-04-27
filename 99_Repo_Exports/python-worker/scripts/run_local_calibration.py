#!/usr/bin/env python3
"""
Script to run local calibration offline.

This script loads historical data and calculates local calibration parameters.
Run this periodically (e.g., daily) to update calibration parameters.

Usage:
    python scripts/run_local_calibration.py

Environment variables:
    PG_DSN - PostgreSQL connection string
    CALIB_LOOKBACK_DAYS - Days to look back (default: 365)
    CALIB_MIN_TRADES_CLUSTER - Min trades per cluster (default: 300)
    CALIB_MIN_TRADES_BUCKET - Min trades per bucket (default: 30)
    CALIB_MIN_MEAN_PNL_R - Min mean PnL per bucket (default: 0.0)
"""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_calibration.calibrate_local_thresholds import main

if __name__ == "__main__":
    main()
