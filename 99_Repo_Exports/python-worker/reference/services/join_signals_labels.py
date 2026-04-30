#!/usr/bin/env python3
"""
Join Signals with Labels.

Merges exported features (from export_features.py) with labels (from labels:trades)
based on signal ID (sid). This enables supervised learning and calibration based
on actual trader decisions.

Usage:
    python3 join_signals_labels.py \\
        --features features.parquet \\
        --labels labels.parquet \\
        --out joined.parquet
"""

import argparse

try:
    import pandas as pd
except ImportError:
    print("Error: pandas not installed. Run: pip install pandas pyarrow")
    exit(1)

# Опциональный GPU стек
_GPU_AVAILABLE = False
try:
    import cudf  # type: ignore
    _GPU_AVAILABLE = True
except Exception:
    cudf = None  # type: ignore
    _GPU_AVAILABLE = False


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Join exported features with trade labels"
        formatter_class=argparse.RawDescriptionHelpFormatter
        epilog="""
Examples:
  # Join Parquet files
  python3 join_signals_labels.py \\
      --features features.parquet \\
      --labels labels.parquet \\
      --out joined.parquet
  
  # Join CSV files
  python3 join_signals_labels.py \\
      --features features.csv \\
      --labels labels.csv \\
      --out joined.csv
        """
    )
    ap.add_argument("--features", required=True, help="Features file (parquet/csv)")
    ap.add_argument("--labels", required=True, help="Labels file (parquet/csv)")
    ap.add_argument("--out", required=True, help="Output file (parquet/csv)")
    ap.add_argument(
        "--use-gpu"
        action="store_true"
        default=False
        help="Enable GPU acceleration with cuDF (fallback to pandas)."
    )
    args = ap.parse_args()
    
    # Load features
    use_gpu = bool(args.use_gpu and _GPU_AVAILABLE)
    if args.use_gpu and not _GPU_AVAILABLE:
        print("⚠️ GPU requested but cuDF not available, using CPU\n")
    elif use_gpu:
        print("🚀 GPU mode enabled (cuDF)\n")

    print(f"📂 Loading features from {args.features}...")
    if use_gpu:
        df = cudf.read_parquet(args.features) if args.features.endswith(".parquet") else cudf.read_csv(args.features)
    else:
        df = pd.read_parquet(args.features) if args.features.endswith(".parquet") else pd.read_csv(args.features)
    print(f"✅ Loaded {len(df)} feature rows")
    
    # Load labels
    print(f"📂 Loading labels from {args.labels}...")
    if use_gpu:
        lb = cudf.read_parquet(args.labels) if args.labels.endswith(".parquet") else cudf.read_csv(args.labels)
    else:
        lb = pd.read_parquet(args.labels) if args.labels.endswith(".parquet") else pd.read_csv(args.labels)
    print(f"✅ Loaded {len(lb)} label rows")
    
    # Join
    if "sid" in df.columns and "sid" in lb.columns:
        print(f"🔗 Joining on 'sid' column...")
        out = df.merge(lb, on="sid", how="left", suffixes=("", "_label"))
        matched = int(out["action"].notna().sum())
        print(f"✅ Matched {matched}/{len(df)} signals with labels ({matched/len(df)*100:.1f}%)")
    else:
        print(f"⚠️  No 'sid' column found, keeping features as-is")
        out = df
    
    # Save
    print(f"💾 Saving to {args.out}...")
    if args.out.endswith(".parquet"):
        out.to_parquet(args.out, index=False)
    else:
        out.to_csv(args.out, index=False)
    
    print(f"✅ Wrote {len(out)} rows to {args.out}")
    
    # Summary
    if "action" in out.columns:
        print("\n📊 Label Summary:")
        print(out["action"].value_counts())


if __name__ == "__main__":
    main()

