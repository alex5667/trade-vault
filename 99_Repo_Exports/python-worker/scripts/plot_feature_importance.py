import argparse
import os
import joblib
import matplotlib.pyplot as plt
import lightgbm as lgb
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("feature_importance")

def main():
    parser = argparse.ArgumentParser("Plot Feature Importance for ML Scorer")
    parser.add_argument("--model", type=str, required=True, help="Path to joblib model file")
    parser.add_argument("--output", type=str, default="importance.png", help="Path to save PNG")
    args = parser.parse_args()

    if not os.path.exists(args.model):
        logger.error(f"Model file not found: {args.model}")
        return

    try:
        pack = joblib.load(args.model)
        model = pack.get("model")
        feature_names = pack.get("feature_names", [])
        
        if not model or not feature_names:
            logger.error("Invalid model pack: missing 'model' or 'feature_names'")
            return
            
        logger.info(f"Loaded model from {args.model} with {len(feature_names)} features")
        
        # Plot LightGBM feature importance (Split count)
        fig, ax = plt.subplots(figsize=(10, 8))
        lgb.plot_importance(model, max_num_features=30, ax=ax, title="Feature Importance (Split)", importance_type="split")
        
        # Replace default feature names (Column_X) with actual feature names if possible
        if hasattr(model, "feature_name"):
            model.feature_name = feature_names
        
        plt.tight_layout()
        plt.savefig(args.output)
        logger.info(f"Saved feature importance plot to {args.output}")
        
    except Exception as e:
        logger.error(f"Failed to plot: {e}")

if __name__ == "__main__":
    main()
