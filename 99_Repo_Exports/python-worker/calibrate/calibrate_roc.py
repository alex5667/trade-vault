#!/usr/bin/env python3
"""
ROC-based Threshold Calibrator - Find optimal thresholds using Youden's J.

Uses ROC curve analysis to find optimal decision thresholds for each feature.

Usage:
    python3 calibrate_roc.py \
        --joined data/labels/joined_pnl.parquet \
        --out-yaml config/defaults/xauusd.yaml \
        --symbol XAUUSD
"""

import argparse
import pathlib
from datetime import datetime

import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score, roc_curve
import yaml


# Features to analyze
FEATURES = ["delta_z", "obi_signed", "weakProgress_inv"]


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="ROC-based threshold calibration"
    )
    ap.add_argument("--joined", required=True, help="Joined parquet/csv")
    ap.add_argument("--out-yaml", required=True, help="Output YAML config")
    ap.add_argument("--symbol", default="XAUUSD", help="Symbol name")
    args = ap.parse_args()
    
    print("=" * 80)
    print("🎯 ROC-Based Threshold Calibrator v7.1")
    print("=" * 80)
    print()
    
    # Load data
    print(f"📊 Loading data from: {args.joined}")
    if args.joined.endswith(".parquet"):
        df = pd.read_parquet(args.joined)
    else:
        df = pd.read_csv(args.joined)
    
    print(f"✅ Loaded {len(df)} records")
    print(f"   Columns: {list(df.columns)}")
    print()
    
    # Prepare features
    if "weakProgress" in df.columns:
        df["weakProgress_inv"] = df["weakProgress"].astype(float)
    
    # Create label
    if "pnl" in df.columns:
        df["label"] = (df["pnl"] > 0).astype(int)
    elif "profit" in df.columns:
        df["label"] = (df["profit"] > 0).astype(int)
    else:
        print("❌ No pnl or profit column found")
        return
    
    print(f"📊 Label distribution:")
    print(f"   Wins: {df['label'].sum()} ({df['label'].mean():.1%})")
    print(f"   Losses: {(~df['label'].astype(bool)).sum()}")
    print()
    
    # Analyze each feature
    results = {}
    
    for feat in FEATURES:
        if feat not in df.columns:
            print(f"⚠️  Feature '{feat}' not found, skipping")
            continue
        
        s = df[feat].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        
        try:
            # Calculate ROC
            fpr, tpr, thresholds = roc_curve(df["label"], s)
            auc = roc_auc_score(df["label"], s)
            
            # Youden's J statistic: max(TPR - FPR)
            j = tpr - fpr
            best_idx = j.argmax()
            best_threshold = thresholds[best_idx]
            
            results[feat] = {
                "auc": float(auc),
                "threshold": float(best_threshold),
                "tpr": float(tpr[best_idx]),
                "fpr": float(fpr[best_idx]),
                "youden_j": float(j[best_idx])
            }
            
            print(f"✅ {feat}:")
            print(f"   AUC: {auc:.3f}")
            print(f"   Optimal threshold: {best_threshold:.3f}")
            print(f"   TPR: {tpr[best_idx]:.3f}, FPR: {fpr[best_idx]:.3f}")
            print(f"   Youden's J: {j[best_idx]:.3f}")
            print()
            
        except Exception as e:
            print(f"❌ Failed to analyze {feat}: {e}")
            print()
    
    # Build config
    cfg = {
        "symbol": args.symbol,
        "thresholds": {
            "deltaSpikeZ": float(results.get("delta_z", {}).get("threshold", 2.0)),
            "obi_signed": float(results.get("obi_signed", {}).get("threshold", 0.25)),
            "weakProgress": 0.3  # Fixed methodologically
        },
        "aucs": {
            k: v["auc"] for k, v in results.items()
        },
        "calibration": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "samples": len(df),
            "win_rate": float(df["label"].mean())
        }
    }
    
    # Save
    pathlib.Path(args.out_yaml).parent.mkdir(parents=True, exist_ok=True)
    
    with open(args.out_yaml, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)
    
    print("=" * 80)
    print(f"✅ Saved configuration to: {args.out_yaml}")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()

