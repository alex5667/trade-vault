import joblib
import os

model_path = "/var/lib/trade/ml_models/edge_stack_v13_of/champions/edge_stack_v1_candidate.joblib"
if os.path.exists(model_path):
    model = joblib.load(model_path)
    if "models" in model and "ALL" in model["models"]:
        m = model["models"]["ALL"]
        print("Model ALL features:")
        if hasattr(m, "feature_names_in_"):
            print(list(m.feature_names_in_))
        else:
            print("No feature_names_in_ attribute")
else:
    print("Model file not found")
