#!/usr/bin/env python3
"""
Export Labels with PnL Calculation.

Reads labels/trades data, joins with features, calculates PnL from execution data.

Usage:
    python3 export_labels_pnl.py \
        --labels data/labels/trades \
        --features data/features/xauusd_m1.parquet \
        --exec reports/exec_today.parquet \
        --out data/labels/joined_pnl.parquet
"""

import argparse
import json
import pathlib
import pandas as pd
import numpy as np


def load_trades(src: str) -> pd.DataFrame:
    """
    Load trades from directory with JSONL files or HTTP endpoint.
    
    Args:
        src: Directory path or HTTP URL
        
    Returns:
        DataFrame with trades
    """
    p = pathlib.Path(src)
    rows = []
    
    if p.is_dir():
        print(f"📁 Loading from directory: {src}")
        for f in sorted(p.glob("*.jsonl")):
            for line in f.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    else:
        # HTTP endpoint returning NDJSON
        print(f"🌐 Loading from HTTP: {src}")
        import urllib.request
        with urllib.request.urlopen(src) as r:
            for line in r.read().decode("utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    
    df = pd.DataFrame(rows)
    
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    
    print(f"✅ Loaded {len(df)} trade records")
    return df


def join_features(df: pd.DataFrame, feats_path: str) -> pd.DataFrame:
    """
    Join with features using nearest timestamp.
    
    Args:
        df: Labels DataFrame
        feats_path: Features file path
        
    Returns:
        Joined DataFrame
    """
    print(f"📊 Loading features from: {feats_path}")
    
    if feats_path.endswith(".parquet"):
        feats = pd.read_parquet(feats_path)
    else:
        feats = pd.read_csv(feats_path)
    
    if "ts" in feats.columns:
        feats["ts"] = pd.to_datetime(feats["ts"], unit="ms", utc=True)
    
    print(f"✅ Loaded {len(feats)} feature records")
    
    # Nearest join within 1 minute tolerance
    df_sorted = df.sort_values("ts")
    feats_sorted = feats.sort_values("ts")
    
    merged = pd.merge_asof(
        df_sorted,
        feats_sorted,
        on="ts",
        direction="nearest",
        tolerance=pd.Timedelta("1min"),
        suffixes=("", "_feat")
    )
    
    print(f"✅ Joined {len(merged)} records")
    return merged


def join_exec(df: pd.DataFrame, exec_path: str) -> pd.DataFrame:
    """
    Join with execution data by sid.
    
    Args:
        df: Labels DataFrame
        exec_path: Execution file path
        
    Returns:
        Joined DataFrame
    """
    print(f"💰 Loading execution data from: {exec_path}")
    
    if exec_path.endswith(".parquet"):
        exec_df = pd.read_parquet(exec_path)
    else:
        exec_df = pd.read_csv(exec_path)
    
    print(f"✅ Loaded {len(exec_df)} execution records")
    
    # Join by sid
    if "sid" in df.columns and "sid" in exec_df.columns:
        merged = df.merge(
            exec_df[["sid", "profit", "price", "volume"]],
            on="sid",
            how="left",
            suffixes=("", "_exec")
        )
        print(f"✅ Joined {len(merged)} records with exec data")
        return merged
    
    return df


def compute_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate PnL from execution data with commission/swap.
    
    Expects columns:
        - exec_price (entry), exit_price, side, volume (lots)
        - Optional: tick_value, tick_size, point, contract_size
        - Optional: commission, swap (in account currency)
    
    Args:
        df: DataFrame with execution data
        
    Returns:
        DataFrame with pnl and pnl_net columns
    """
    if "exec_price" not in df.columns or "side" not in df.columns:
        print("⚠️  Missing exec_price or side columns, skipping PnL calculation")
        return df
    
    def money_per_point_per_lot(row):
        """
        Calculate money value per 1 point movement on 1 lot.
        
        Universal MT5 formula:
            money_per_point = (tick_value / tick_size) × point
        """
        tv = row.get("tick_value", np.nan)
        ts = row.get("tick_size", np.nan)
        pt = row.get("point", np.nan)
        
        # Use tick specs if available
        if not np.isnan(tv) and not np.isnan(ts) and not np.isnan(pt) and ts > 0:
            return (tv / ts) * pt
        
        # Fallback: contract_size × point
        cs = row.get("contract_size", 100.0)  # XAUUSD default
        point_val = row.get("point", 0.01)
        return cs * point_val
    
    def calc_pnl(row):
        """Calculate PnL for a single trade."""
        if pd.isna(row.get("exit_price")):
            return np.nan, np.nan
        
        # Direction multiplier
        mul = 1.0 if row["side"] == "LONG" else -1.0
        
        # Points difference
        pts = (row["exit_price"] - row["exec_price"]) * mul
        
        # Money per point per lot
        mpp = money_per_point_per_lot(row)
        
        # Volume
        volume = row.get("volume", 0.01)
        
        # Gross PnL
        gross_pnl = pts * mpp * volume
        
        # Fees: commission + swap
        commission = float(row.get("commission", 0.0))
        swap = float(row.get("swap", 0.0))
        fees = commission + swap
        
        # Net PnL
        net_pnl = gross_pnl - fees
        
        return gross_pnl, net_pnl
    
    if "exec_price" in df.columns and "volume" in df.columns:
        df[["pnl_gross", "pnl_net"]] = df.apply(
            calc_pnl,
            axis=1,
            result_type='expand'
        )
        
        # Use pnl_net as main pnl
        df["pnl"] = df["pnl_net"]
        
        valid_pnl = df["pnl"].notna().sum()
        print(f"✅ Calculated PnL for {valid_pnl} records")
        
        if "commission" in df.columns or "swap" in df.columns:
            total_fees = df["commission"].fillna(0).sum() + df["swap"].fillna(0).sum()
            print(f"   Total fees (commission + swap): ${total_fees:.2f}")
    
    return df


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Export labels with features and PnL"
    )
    ap.add_argument("--labels", required=True, help="Dir with *.jsonl or HTTP NDJSON")
    ap.add_argument("--features", required=True, help="CSV/Parquet with features")
    ap.add_argument("--exec", help="CSV/Parquet with execution reports (optional)")
    ap.add_argument("--out", required=True, help="Output path (.parquet or .csv)")
    args = ap.parse_args()
    
    print("=" * 80)
    print("📊 Labels Export with PnL v7.1")
    print("=" * 80)
    print()
    
    # Load trades/labels
    df = load_trades(args.labels)
    
    if df.empty:
        print("⚠️  No trade records found")
        return
    
    # Join with features
    df = join_features(df, args.features)
    
    # Join with execution reports (optional)
    if args.exec:
        df = join_exec(df, args.exec)
        df = compute_pnl(df)
    
    # Export
    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    
    if out.suffix.lower() == ".parquet":
        df.to_parquet(out, index=False)
    else:
        df.to_csv(out, index=False)
    
    print()
    print(f"✅ Wrote {out} with {len(df)} rows")
    
    # Summary
    if "pnl" in df.columns and df["pnl"].notna().any():
        total_pnl = df["pnl"].sum()
        avg_pnl = df["pnl"].mean()
        win_rate = (df["pnl"] > 0).mean()
        
        print()
        print("=" * 80)
        print("📈 PnL Summary")
        print("=" * 80)
        print(f"Total PnL:    ${total_pnl:.2f}")
        print(f"Average PnL:  ${avg_pnl:.2f}")
        print(f"Win rate:     {win_rate:.1%}")
        print()


if __name__ == "__main__":
    main()

