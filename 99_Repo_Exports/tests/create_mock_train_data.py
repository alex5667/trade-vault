import pandas as pd
import numpy as np
import os
from core.meta_features_v1 import META_FEAT_V1_COLS

def create_mock_train_data(path):
    n = 100
    data = {}
    for col in META_FEAT_V1_COLS:
        data[col] = np.random.randn(n)
    
    # Extra fields required by builder
    data['have'] = np.random.randint(0, 10, n)
    data['need'] = np.random.randint(10, 20, n)
    data['ok_soft'] = np.zeros(n)
    data['rule_score'] = np.random.rand(n)
    data['exec_risk_norm'] = np.random.rand(n)
    data['exec_risk_bps'] = np.random.rand(n) * 10
    data['scenario_v4'] = np.random.choice(['trend', 'range'], n)
    data['age_ms'] = np.random.rand(n) * 1000
    
    # Label
    data['is_profitable'] = (np.random.rand(n) > 0.5).astype(int)
    
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    # Using CSV because parquet engine might be missing in test env
    df.to_csv(path, index=False)
    print(f"Mock data created at {path}")

if __name__ == "__main__":
    create_mock_train_data("/home/alex/front/trade/scanner_infra/tmp/mock_train.csv")
