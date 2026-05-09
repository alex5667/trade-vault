#!/usr/bin/env python3
"""
PnL-Driven Threshold Calibrator - Optimize thresholds based on actual profits.

Grid search over DELTA_Z_THRESHOLD and OBI_THRESHOLD to maximize
profitability (mean profit + Sharpe ratio).

Usage:
    python3 calibrate_thresholds_pnl.py \
        --data joined_features_exec.parquet \
        --out-env config/calibrated_gold.env
"""

import argparse
from itertools import product

import numpy as np
import pandas as pd


def objective(
    df: pd.DataFrame,
    dz: float,
    obi: float,
    require_weak: bool = False
) -> tuple[float, float, int]:
    """
    Calculate objective function for given thresholds.
    
    Args:
        df: DataFrame with delta_z, obi, profit columns
        dz: Delta Z threshold
        obi: OBI threshold
        require_weak: If True, require weak progress flag
        
    Returns:
        (mean_profit, sharpe, num_samples)
    """
    g = df.copy()

    # Build mask
    mask = (np.abs(g["delta_z"]) >= dz)

    if "obi" in g.columns:
        mask &= (np.abs(g["obi"]) >= obi)

    if require_weak and "weak" in g.columns:
        mask &= (g["weak"] > 0.5)

    # Filter
    sel = g[mask]

    if sel.empty or "profit" not in sel.columns:
        return -1e9, 0, 0

    # Calculate metrics
    mu = sel["profit"].mean()
    sd = sel["profit"].std(ddof=0)

    if pd.isna(sd) or sd <= 0:
        sd = 1e-9

    sharpe = mu / sd

    return mu, sharpe, len(sel)


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="PnL-driven threshold calibration"
    )
    ap.add_argument("--data", required=True, help="Input data (parquet/csv)")
    ap.add_argument("--out-env", required=True, help="Output .env file")
    ap.add_argument("--dz-grid", default="1.5,2,2.5,3,3.5,4", help="Delta Z grid")
    ap.add_argument("--obi-grid", default="0,0.2,0.3,0.4,0.5,0.6,0.7", help="OBI grid")
    ap.add_argument("--require-weak", action="store_true", help="Require weak progress")
    ap.add_argument("--weight-profit", type=float, default=0.7, help="Profit weight (0-1)")
    args = ap.parse_args()

    print("=" * 80)
    print("🎯 PnL-Driven Threshold Calibrator v7")
    print("=" * 80)
    print()

    # Load data
    print(f"📊 Loading data from {args.data}...")
    if args.data.endswith(".parquet"):
        df = pd.read_parquet(args.data)
    else:
        df = pd.read_csv(args.data)

    print(f"✅ Loaded {len(df)} records")

    # Check required columns
    required = ["delta_z", "profit"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"❌ Missing required columns: {missing}")
        print(f"   Available: {list(df.columns)}")
        return

    print(f"   Columns: {list(df.columns)}")
    print()

    # Parse grids
    dzs = [float(x) for x in args.dz_grid.split(",")]
    obis = [float(x) for x in args.obi_grid.split(",")]

    print("🔍 Grid search:")
    print(f"   Delta Z:  {dzs}")
    print(f"   OBI:      {obis}")
    print(f"   Total combinations: {len(dzs) * len(obis)}")
    print()

    # Grid search
    best = None
    results = []

    weight_profit = args.weight_profit
    weight_sharpe = 1 - weight_profit

    for dz, ob in product(dzs, obis):
        mu, sh, n = objective(df, dz, ob, args.require_weak)
        score = weight_profit * mu + weight_sharpe * sh

        results.append({
            "dz": dz,
            "obi": ob,
            "mean_profit": mu,
            "sharpe": sh,
            "samples": n,
            "score": score
        })

        if best is None or score > best[0]:
            best = (score, dz, ob, mu, sh, n)

    # Display results
    results_df = pd.DataFrame(results).sort_values("score", ascending=False)

    print("📈 Top 10 configurations:")
    print(results_df.head(10).to_string(index=False))
    print()

    if best is None or best[0] == -1e9:
        print("⚠️  No valid configuration found, using defaults")
        with open(args.out_env, "w") as f:
            f.write("DELTA_Z_THRESHOLD=3.0\n")
            f.write("OBI_THRESHOLD=0.5\n")
        return

    # Best configuration
    score, dz, ob, mu, sh, n = best

    print("=" * 80)
    print("🏆 BEST CONFIGURATION")
    print("=" * 80)
    print(f"DELTA_Z_THRESHOLD:  {dz}")
    print(f"OBI_THRESHOLD:      {ob}")
    print(f"Mean profit:        ${mu:.2f}")
    print(f"Sharpe ratio:       {sh:.3f}")
    print(f"Samples:            {n}")
    print(f"Score:              {score:.3f}")
    print()

    # Write to env file
    with open(args.out_env, "w") as f:
        f.write("# PnL-calibrated thresholds\n")
        f.write(f"# Generated from {len(df)} records\n")
        f.write(f"# Best config: mean_profit=${mu:.2f}, sharpe={sh:.3f}, samples={n}\n")
        f.write("\n")
        f.write(f"DELTA_Z_THRESHOLD={dz}\n")
        f.write(f"OBI_THRESHOLD={ob}\n")

    print(f"✅ Wrote configuration to {args.out_env}")
    print()


if __name__ == "__main__":
    main()

