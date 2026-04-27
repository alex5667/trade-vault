import pandas as pd
from ml_analysis.tools.tune_conf_score_weights_v1 import tune, TuneCfg

def test_tune_conf_score_weights():
    # Simple test for tuning logic
    data = {"regime": ["trend", "trend", "range"], 
            "conf_0": [1, 0, 1], 
            "is_win": [1, 0, 1], 
            "return_pct": [0.05, -0.01, 0.02]}
    df = pd.DataFrame(data)
    cfg = TuneCfg(min_n_key=1, min_n_regime=1, uplift_min=0.0)
    res = tune(df, cfg)
    
    # We just expect the function to run and return a dict
    assert isinstance(res, dict)
    assert "by_regime" in res
