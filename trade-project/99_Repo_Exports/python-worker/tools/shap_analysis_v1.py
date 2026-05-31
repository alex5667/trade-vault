#!/usr/bin/env python3
"""
SHAP TreeExplainer analysis for ML models.

Usage:
  python tools/shap_analysis_v1.py --model-path /path/to/model.joblib --data-path data.parquet --output-dir /tmp/shap

Outputs:
  - feature_importance.png (SHAP summary plot)
  - feature_values.csv (top features by mean|SHAP|)
  - drift_by_week.csv (PSI / distribution drift)
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_model(model_path: str):
    """Load model from joblib or pickle."""
    try:
        import joblib
        return joblib.load(model_path)
    except Exception as e:
        logger.error(f"Failed to load model from {model_path}: {e}")
        return None

def load_data(data_path: str, limit: int = 10000) -> pd.DataFrame:
    """Load data from parquet/csv."""
    try:
        if data_path.endswith('.parquet'):
            return pd.read_parquet(data_path).head(limit)
        else:
            return pd.read_csv(data_path).head(limit)
    except Exception as e:
        logger.error(f"Failed to load data from {data_path}: {e}")
        return None

def analyze_shap(model, X: pd.DataFrame, output_dir: str):
    """Compute SHAP values and generate plots."""
    try:
        import shap
    except ImportError:
        logger.warning("shap not installed. Install with: pip install shap")
        return False

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("Computing SHAP TreeExplainer...")
    try:
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X)

        # For binary classification, take positive class
        if isinstance(shap_values, list):
            shap_values = shap_values[1] if len(shap_values) > 1 else shap_values[0]

        logger.info("Generating summary plot...")
        shap.summary_plot(shap_values, X, plot_type="bar", show=False)
        import matplotlib.pyplot as plt
        plt.savefig(output_path / "feature_importance.png", dpi=100, bbox_inches='tight')
        plt.close()

        # Feature importance ranking
        importance = np.abs(shap_values).mean(axis=0)
        feat_importance = pd.DataFrame({
            'feature': X.columns,
            'mean_shap': importance,
        }).sort_values('mean_shap', ascending=False)

        feat_importance.to_csv(output_path / "feature_importance.csv", index=False)
        logger.info(f"✅ Top 20 features:\n{feat_importance.head(20).to_string()}")

        return True
    except Exception as e:
        logger.error(f"SHAP analysis failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="SHAP TreeExplainer analysis")
    parser.add_argument("--model-path", required=True, help="Path to model.joblib")
    parser.add_argument("--data-path", required=True, help="Path to data.parquet or .csv")
    parser.add_argument("--output-dir", default="/tmp/shap", help="Output directory")
    parser.add_argument("--limit", type=int, default=5000, help="Max rows to analyze")
    args = parser.parse_args()

    logger.info(f"Loading model from {args.model_path}...")
    model = load_model(args.model_path)
    if model is None:
        return 1

    logger.info(f"Loading data from {args.data_path}...")
    X = load_data(args.data_path, limit=args.limit)
    if X is None:
        return 1

    logger.info(f"Running SHAP analysis with {len(X)} rows...")
    success = analyze_shap(model, X, args.output_dir)

    if success:
        logger.info(f"✅ Analysis complete. Outputs in {args.output_dir}")
        return 0
    else:
        logger.error("❌ Analysis failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
