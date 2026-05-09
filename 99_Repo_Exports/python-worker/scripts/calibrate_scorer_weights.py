#!/usr/bin/env python3
"""
Data-driven Scorer Weight Calibration (Phase 2)
Expert Team: 20+ years in Quantitative Trading & Data Science.

Goal: Optimize bonus weights using historical signal outcomes from Postgres.
Inputs: signal_facts, trade_performance
Outputs: suggested_weights.json
"""

import argparse
import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from sklearn.linear_model import LogisticRegression

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("WeightCalibrator")

def get_dsn():
    return os.getenv("PG_DSN", f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}@localhost:5432/scanner_analytics")

def fetch_calibration_data(lookback_days: int):
    dsn = get_dsn()
    logger.info(f"Connecting to {dsn.split('@')[-1]} ...")

    query = f"""
    SELECT 
        s.signal_id,
        s.symbol,
        s.signal_family,
        s.delta_spike_z,
        s.obi_avg_20,
        s.weak_progress_ratio,
        s.conf_score as old_score,
        t.r as pnl_r,
        t.hit as is_win
    FROM signal_facts s
    JOIN trade_performance t ON s.signal_id = t.signal_id
    WHERE s.ts > NOW() - INTERVAL '{lookback_days} days'
    AND t.r IS NOT NULL
    """

    conn = psycopg2.connect(dsn)
    df = pd.read_sql(query, conn)
    conn.close()

    logger.info(f"Fetched {len(df)} signal-outcome pairs.")
    return df

def optimize_weights(df: pd.DataFrame):
    if df.empty:
        logger.warning("Empty dataset for optimization.")
        return {}

    # Define features to calibrate
    # Note: In real setup, we would extract 'confirmations' from extra_json if available.
    # For now, we calibrate base feature importance.
    features = ['delta_spike_z', 'obi_avg_20', 'weak_progress_ratio']
    X = df[features].fillna(0).values
    y = df['is_win'].astype(int).values

    if len(np.unique(y)) < 2:
        logger.warning("Single class in outcome. Cannot optimize.")
        return {f"w_{feat}": 1.0 for feat in features}

    model = LogisticRegression()
    model.fit(X, y)

    importances = model.coef_[0]
    # Normalize importances to suggest new weights
    norm_importances = importances / np.sum(np.abs(importances))

    suggested = {}
    for feat, imp in zip(features, norm_importances):
        suggested[f"w_{feat}"] = round(float(imp), 4)

    return suggested

def main():
    parser = argparse.ArgumentParser(description="Calibrate Scorer Weights")
    parser.add_argument("--lookback", type=int, default=30, help="Days to look back")
    parser.add_argument("--output", type=str, default="python-worker/config/suggested_weights.json")
    args = parser.parse_args()

    try:
        df = fetch_calibration_data(args.lookback)
        weights = optimize_weights(df)

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump({
                "phase": 2,
                "version": "1.0.0",
                "suggested_weights": weights,
                "sample_size": len(df)
            }, f, indent=4)

        logger.info(f"Successfully saved calibrated weights to {args.output}")
        print(json.dumps(weights, indent=2))

    except Exception as e:
        logger.error(f"Calibration failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
