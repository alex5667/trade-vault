#!/usr/bin/env python3
"""
Distribution and ROC Plotting for  Features.

Generates visualization reports:
- Histograms of delta_z and OBI distributions
- ROC curves for signal quality assessment

Usage:
    python3 plot_distributions.py \\
        --data features.parquet \\
        --outdir ./reports_out \\
        --horizon 60
"""

import argparse
import os

try:
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    from sklearn.metrics import auc, roc_curve
except ImportError:
    print("Error: Required packages not installed")
    print("Run: pip install pandas numpy matplotlib scikit-learn")
    exit(1)


def plot_hist(series: pd.Series, title: str, out_png: str):
    """
    Plot histogram of feature distribution.
    
    Args:
        series: Data series
        title: Plot title
        out_png: Output PNG file path
    """
    plt.figure(figsize=(10, 6))

    # Clean data
    series = series.replace([np.inf, -np.inf], np.nan).dropna()

    # Plot
    plt.hist(series, bins=100, edgecolor='black', alpha=0.7)
    plt.title(title, fontsize=14, fontweight='bold')
    plt.xlabel('Value', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.grid(True, alpha=0.3)

    # Stats
    mean_val = series.mean()
    std_val = series.std()
    plt.axvline(mean_val, color='r', linestyle='--', label=f'Mean: {mean_val:.3f}')
    plt.axvline(mean_val + std_val, color='g', linestyle=':', label='±1 STD')
    plt.axvline(mean_val - std_val, color='g', linestyle=':')
    plt.legend()

    plt.savefig(out_png, bbox_inches='tight', dpi=150)
    plt.close()
    print(f"✅ Saved {out_png}")


def make_labels_from_forward(
    mid: pd.Series,
    horizon: int = 60,
    side_series: pd.Series | None = None
) -> np.ndarray:
    """
    Create binary labels from forward returns.
    
    Args:
        mid: Mid price series
        horizon: Forward window (samples)
        side_series: Trade side (+1/-1), optional
        
    Returns:
        Binary labels (1 = success, 0 = failure)
    """
    # Forward return
    fwd = (mid.shift(-horizon) - mid) / mid

    if side_series is None:
        # Generic: up = 1, down = 0
        y = (fwd > 0).astype(int)
    else:
        # Directional: success if side * fwd > 0
        y = ((side_series * fwd) > 0).astype(int)

    return y.fillna(0).values


def plot_roc(
    feature: np.ndarray,
    labels: np.ndarray,
    title: str,
    out_png: str
):
    """
    Plot ROC curve.
    
    Args:
        feature: Feature values (predictor)
        labels: Binary labels
        title: Plot title
        out_png: Output PNG file path
    """
    # Compute ROC
    fpr, tpr, _ = roc_curve(labels, feature)
    roc_auc = auc(fpr, tpr)

    # Plot
    plt.figure(figsize=(10, 8))
    plt.plot(fpr, tpr, color='b', lw=2, label=f'ROC curve (AUC = {roc_auc:.3f})')
    plt.plot([0, 1], [0, 1], color='gray', lw=1, linestyle='--', label='Random')

    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate', fontsize=12)
    plt.ylabel('True Positive Rate', fontsize=12)
    plt.title(title, fontsize=14, fontweight='bold')
    plt.legend(loc="lower right", fontsize=11)
    plt.grid(True, alpha=0.3)

    plt.savefig(out_png, bbox_inches='tight', dpi=150)
    plt.close()
    print(f"✅ Saved {out_png} (AUC = {roc_auc:.3f})")


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Generate distribution and ROC plots from features",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python3 plot_distributions.py \\
      --data features.parquet \\
      --outdir ./reports_out
  
  # Custom horizon
  python3 plot_distributions.py \\
      --data features.csv \\
      --outdir ./reports \\
      --horizon 120
        """
    )
    ap.add_argument("--data", required=True, help="Features file (parquet/csv)")
    ap.add_argument("--outdir", required=True, help="Output directory for plots")
    ap.add_argument("--horizon", type=int, default=60,
                    help="Forward horizon in samples (default: 60)")
    args = ap.parse_args()

    # Load data
    print(f"📂 Loading data from {args.data}...")
    if args.data.endswith(".parquet"):
        df = pd.read_parquet(args.data)
    else:
        df = pd.read_csv(args.data)
    print(f"✅ Loaded {len(df)} rows")
    print(f"   Columns: {', '.join(df.columns)}")
    print()

    # Create output directory
    os.makedirs(args.outdir, exist_ok=True)
    print(f"📁 Output directory: {args.outdir}")
    print()

    # ═══════════════════════════════════════════════════════════════
    # 1. Histograms
    # ═══════════════════════════════════════════════════════════════
    print("📊 Generating histograms...")

    if 'delta_z' in df.columns:
        plot_hist(
            df['delta_z'],
            "Delta Z-Score Distribution",
            os.path.join(args.outdir, "hist_delta_z.png")
        )

    if 'obi' in df.columns:
        plot_hist(
            df['obi'].fillna(0.0),
            "Order Book Imbalance (OBI) Distribution",
            os.path.join(args.outdir, "hist_obi.png")
        )

    print()

    # ═══════════════════════════════════════════════════════════════
    # 2. ROC Curves
    # ═══════════════════════════════════════════════════════════════
    print(f"📈 Generating ROC curves (horizon={args.horizon} samples)...")

    # ROC for |delta_z| predicting forward direction
    if 'mid' in df.columns and 'delta_z' in df.columns:
        side = np.sign(df['delta_z']).replace(0, np.nan).fillna(0.0)
        labels = make_labels_from_forward(df['mid'], args.horizon, side)
        feature = np.abs(df['delta_z']).values

        plot_roc(
            feature,
            labels,
            f"ROC: |ΔZ| Predicting Success @ {args.horizon}s Horizon",
            os.path.join(args.outdir, "roc_abs_dz.png")
        )

    # ROC for sign(delta_z) * OBI
    if 'mid' in df.columns and 'obi' in df.columns and 'delta_z' in df.columns:
        side = np.sign(df['delta_z']).replace(0, np.nan).fillna(0.0)
        labels = make_labels_from_forward(df['mid'], args.horizon, side)
        feature = (np.sign(df['delta_z']) * df['obi'].fillna(0.0)).values

        plot_roc(
            feature,
            labels,
            f"ROC: sign(ΔZ) × OBI @ {args.horizon}s Horizon",
            os.path.join(args.outdir, "roc_sign_dz_obi.png")
        )

    print()
    print(f"✅ All plots saved to {args.outdir}")
    print("\n📊 Report Summary:")
    print(f"   Delta Z range: [{df['delta_z'].min():.2f}, {df['delta_z'].max():.2f}]")
    if 'obi' in df.columns:
        print(f"   OBI range: [{df['obi'].min():.2f}, {df['obi'].max():.2f}]")
    print(f"   Data points: {len(df)}")


if __name__ == "__main__":
    main()

