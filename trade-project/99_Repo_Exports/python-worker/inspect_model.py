import joblib
import os

path = "/var/lib/trade/ml_models/scorer_v3/scorer_v3.joblib"
if not os.path.exists(path):
    print(f"File not found: {path}")
    exit(1)

pack = joblib.load(path)
print(f"Pack keys: {pack.keys()}")
print(f"Kind: {pack.get('kind')}")
print(f"Trained at: {pack.get('trained_at')}")
print(f"Num samples: {pack.get('n_samples')}")

model = pack.get("model")
if model:
    try:
        # LightGBM booster or sklearn wrapper
        if hasattr(model, "n_features_"):
            print(f"Model n_features_: {model.n_features_}")
        elif hasattr(model, "booster_"):
            print(f"Booster num_feature: {model.booster_.num_feature()}")
        else:
            print("Could not determine num features from model object")
    except Exception as e:
        print(f"Error getting num features: {e}")

features = pack.get("feature_names", [])
print(f"Num feature names in pack: {len(features)}")
print(f"Feature names list: {features}")
