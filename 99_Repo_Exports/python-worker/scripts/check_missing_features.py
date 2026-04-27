import pandas as pd
import json
import sys

try:
    df = pd.read_parquet("/var/lib/trade/of_reports/datasets/nightly_meta_v4.parquet")
    features = json.load(open("/var/lib/trade/of_reports/models/meta_lr_v4_nightly.json")).get("features", [])

    missing = []
    valid = []
    
    if "indicators" in df.columns and not df.empty and df["indicators"].notna().any():
        sample = df["indicators"].dropna().iloc[0]
        
        for f in features:
            if f in df.columns or (isinstance(sample, dict) and f in sample.keys()):
                valid.append(f)
            else:
                missing.append(f)
        
        print(f"Missing count: {len(missing)} / {len(features)}\nMissing features: {missing}")
    else:
        print("Missing indicators or dataframe is empty")
except Exception as e:
    print(f"Error: {e}")
