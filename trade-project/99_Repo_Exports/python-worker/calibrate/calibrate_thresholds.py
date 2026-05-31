#!/usr/bin/env python3
"""
Auto-calibration of  signal thresholds.

Analyzes historical feature data and suggests optimal threshold values
based on statistical properties and quantiles.

Usage:
    python3 calibrate_thresholds.py --data features.parquet \\
                                     --out-env config/calibrated_gold.env
"""

import argparse
import os

try:
    import numpy as np
    import pandas as pd
except ImportError:
    print("Error: pandas and numpy required. Run: pip install pandas numpy pyarrow")
    exit(1)

# Опциональный GPU сервис
_GPU_AVAILABLE = False
try:
    from services.gpu_compute_service import get_gpu_service

    _gpu_service = get_gpu_service()
    _GPU_AVAILABLE = bool(_gpu_service and _gpu_service.is_gpu_available())
except Exception:
    _gpu_service = None
    _GPU_AVAILABLE = False


def _quantile(values: pd.Series, q: float, use_gpu: bool) -> float:
    """Безопасное вычисление квантиля с GPU fallback."""
    arr = values.to_numpy(dtype=np.float32)
    if arr.size == 0:
        return 0.0
    q = max(0.0, min(1.0, float(q)))
    if use_gpu and _GPU_AVAILABLE and _gpu_service:
        try:
            res = _gpu_service.compute_quantiles(arr, [q])
            return float(res[0])
        except Exception:
            pass
    return float(np.quantile(arr, q))

def suggest_thresholds(df: pd.DataFrame, use_gpu: bool) -> dict[str, float]:
    """
    Suggest optimal thresholds based on data analysis.
    
    Strategy:
    - DELTA_Z_THRESHOLD: High quantile (98th) to keep signals rare (~1-2%)
    - WEAK_PROGRESS_ATR: Low quantile (30th) for "weak" market moves
    - OBI_THRESHOLD: High quantile (80th) for strong imbalance
    - ICEBERG_*: Conservative defaults based on typical market behavior
    - DIST_ATR_THRESHOLD: Moderate (50th percentile)
    
    Args:
        df: DataFrame with features (delta_z, mid, obi, etc.)
        
    Returns:
        Dictionary of threshold values
    """
    out = {}

    print("📊 Analyzing data...")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {', '.join(df.columns)}")
    print()

    # ═══════════════════════════════════════════════════════════════
    # 1. Delta Z-score threshold
    # ═══════════════════════════════════════════════════════════════
    if 'delta_z' in df.columns:
        abs_z = np.abs(df['delta_z'].dropna())
        if len(abs_z) > 100:
            # Use 98th percentile to keep signals rare
            q = _quantile(abs_z, 0.98, use_gpu=use_gpu)
            out['DELTA_Z_THRESHOLD'] = round(max(1.5, float(q)), 2)
            print(f"✅ DELTA_Z_THRESHOLD: {out['DELTA_Z_THRESHOLD']} (98th percentile)")
        else:
            out['DELTA_Z_THRESHOLD'] = 3.0
            print(f"⚠️  DELTA_Z_THRESHOLD: {out['DELTA_Z_THRESHOLD']} (default, insufficient data)")
    else:
        out['DELTA_Z_THRESHOLD'] = 3.0
        print(f"⚠️  DELTA_Z_THRESHOLD: {out['DELTA_Z_THRESHOLD']} (default, no delta_z column)")

    # ═══════════════════════════════════════════════════════════════
    # 2. Weak progress threshold (range/ATR ratio)
    # ═══════════════════════════════════════════════════════════════
    if 'mid' in df.columns:
        mid = df['mid'].values
        if len(mid) > 120:
            # Compute rolling range over 60-second windows (assume ~1s sampling)
            N = 60
            ranges = pd.Series(mid).rolling(N).apply(lambda x: x.max() - x.min(), raw=True)

            # ATR proxy: 14-period moving average of range
            atr_proxy = ranges.rolling(14).mean()

            # Range/ATR ratio
            ratio = (ranges / atr_proxy).replace([np.inf, -np.inf], np.nan)

            # Weak progress = lower 30th percentile
            valid_ratio = ratio.dropna()
            if len(valid_ratio) > 100:
                thr = _quantile(valid_ratio, 0.30, use_gpu=use_gpu)
                out['WEAK_PROGRESS_ATR'] = round(max(0.05, float(thr)), 2)
                print(f"✅ WEAK_PROGRESS_ATR: {out['WEAK_PROGRESS_ATR']} (30th percentile)")
            else:
                out['WEAK_PROGRESS_ATR'] = 0.10
                print(f"⚠️  WEAK_PROGRESS_ATR: {out['WEAK_PROGRESS_ATR']} (default, insufficient data)")
        else:
            out['WEAK_PROGRESS_ATR'] = 0.10
            print(f"⚠️  WEAK_PROGRESS_ATR: {out['WEAK_PROGRESS_ATR']} (default, insufficient samples)")
    else:
        out['WEAK_PROGRESS_ATR'] = 0.10
        print(f"⚠️  WEAK_PROGRESS_ATR: {out['WEAK_PROGRESS_ATR']} (default, no mid column)")

    # ═══════════════════════════════════════════════════════════════
    # 3. OBI threshold
    # ═══════════════════════════════════════════════════════════════
    if 'obi' in df.columns:
        obi = df['obi'].dropna()
        if len(obi) > 100:
            # Use 80th percentile of absolute OBI
            abs_obi = np.abs(obi)
            thr = _quantile(abs_obi, 0.80, use_gpu=use_gpu)
            out['OBI_THRESHOLD'] = round(max(0.3, float(thr)), 2)
            print(f"✅ OBI_THRESHOLD: {out['OBI_THRESHOLD']} (80th percentile)")
        else:
            out['OBI_THRESHOLD'] = 0.5
            print(f"⚠️  OBI_THRESHOLD: {out['OBI_THRESHOLD']} (default, insufficient data)")
    else:
        out['OBI_THRESHOLD'] = 0.5
        print(f"⚠️  OBI_THRESHOLD: {out['OBI_THRESHOLD']} (default, no obi column)")

    # ═══════════════════════════════════════════════════════════════
    # 4. Iceberg parameters (conservative defaults)
    # ═══════════════════════════════════════════════════════════════
    out['ICEBERG_MIN_DURATION'] = 1.5  # seconds
    out['ICEBERG_REFRESH_COUNT'] = 2   # minimum refreshes
    out['ICEBERG_REFRESH_MIN_ABS'] = 1.0  # minimum volume change

    print(f"✅ ICEBERG_MIN_DURATION: {out['ICEBERG_MIN_DURATION']}s")
    print(f"✅ ICEBERG_REFRESH_COUNT: {out['ICEBERG_REFRESH_COUNT']}")
    print(f"✅ ICEBERG_REFRESH_MIN_ABS: {out['ICEBERG_REFRESH_MIN_ABS']}")

    # ═══════════════════════════════════════════════════════════════
    # 5. Distance to pivot (ATR share)
    # ═══════════════════════════════════════════════════════════════
    out['DIST_ATR_THRESHOLD'] = 0.5  # within 0.5 ATR of pivot
    print(f"✅ DIST_ATR_THRESHOLD: {out['DIST_ATR_THRESHOLD']} ATR")

    # ═══════════════════════════════════════════════════════════════
    # 6. OBI sustained duration
    # ═══════════════════════════════════════════════════════════════
    out['OBI_MIN_DURATION'] = 2.0  # seconds
    print(f"✅ OBI_MIN_DURATION: {out['OBI_MIN_DURATION']}s")

    print()
    return out


def write_env_file(thresholds: dict[str, float], path: str) -> None:
    """
    Write thresholds to .env file.
    
    Args:
        thresholds: Dictionary of threshold values
        path: Output file path
    """
    lines = [
        "# Auto-calibrated thresholds for "
        f"# Generated: {pd.Timestamp.now().isoformat()}",
        "",
    ]

    for key in sorted(thresholds.keys()):
        value = thresholds[key]
        lines.append(f"{key}={value}")

    # Create directory if needed
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"✅ Wrote thresholds to {path}")


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Auto-calibrate  signal thresholds from historical data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calibrate from Parquet
  python3 calibrate_thresholds.py --data features.parquet \\
                                   --out-env config/calibrated_gold.env
  
  # Calibrate from CSV
  python3 calibrate_thresholds.py --data features.csv \\
                                   --out-env calibrated.env
        """
    )
    ap.add_argument("--data", required=True, help="Features file (.parquet or .csv)")
    ap.add_argument("--out-env", default="config/calibrated_gold.env",
                    help="Output .env file (default: config/calibrated_gold.env)")
    ap.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enable GPU quantiles if available (fallback to CPU).",
    )
    args = ap.parse_args()

    # Load data
    print(f"📂 Loading data from {args.data}...")
    if args.data.endswith(".parquet"):
        df = pd.read_parquet(args.data)
    elif args.data.endswith(".csv"):
        df = pd.read_csv(args.data)
    else:
        raise SystemExit("Error: Data file must be .parquet or .csv")

    print(f"✅ Loaded {len(df)} rows\n")

    use_gpu = bool(args.use_gpu and _GPU_AVAILABLE)
    if args.use_gpu and not _GPU_AVAILABLE:
        print("⚠️ GPU requested but not available, using CPU quantiles")
    elif use_gpu:
        print("🚀 GPU quantiles enabled")

    # Suggest thresholds
    thresholds = suggest_thresholds(df, use_gpu=use_gpu)

    # Write to file
    write_env_file(thresholds, args.out_env)

    print("\n🎯 Summary:")
    print("=" * 60)
    for key, value in sorted(thresholds.items()):
        print(f"  {key:30s} = {value}")
    print("=" * 60)
    print("\n✅ Calibration complete!")
    print(f"\n💡 To use: source {args.out_env}")


if __name__ == "__main__":
    main()

