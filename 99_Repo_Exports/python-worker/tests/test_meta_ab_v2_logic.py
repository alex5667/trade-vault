import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock
from tools.meta_ab_winner_evaluator_v2 import evaluate_v2, recommend_next_share, V2Config

def test_recommend_next_share_edge_cases():
    cfg = V2Config(ramp_step=0.05, max_share=0.50)
    
    # 1. С учетом freeze_max_share (более жесткий лимит)
    next_s, act = recommend_next_share("challenger", 0.20, cfg, 0.22)
    assert next_s == pytest.approx(0.22)
    assert act == "increase_share"
    
    # 2. Не растет выше max_share
    next_s, act = recommend_next_share("challenger", 0.50, cfg, None)
    assert next_s == pytest.approx(0.50)
    assert act == "increase_share"
    
    # 3. Резкое падение при проигрыше champion
    next_s, act = recommend_next_share("champion", 0.02, cfg, None)
    assert next_s == pytest.approx(0.0)
    assert act == "decrease_share"

def test_evaluate_v2_ci_gate():
    # Mock models
    champ = MagicMock()
    chal = MagicMock()
    
    # Mock data: 1000 rows
    df = pd.DataFrame({
        "y": [1]*1000,
        "r_mult": [0.1]*1000,
        "ok": [1]*1000,
        "symbol": ["BTCUSDT"]*1000
    })
    
    cfg = V2Config(
        min_n=500,
        bootstrap=1,
        require_ci_positive=1,
        min_delta_exp_r=0.01
    )
    
    # Patch score_model_proba to return fixed values
    import tools.meta_ab_winner_evaluator_v2 as tools
    orig_score = tools.score_model_proba
    tools.score_model_proba = MagicMock(side_effect=[
        np.array([0.5]*1000), # Case A: champ
        np.array([1.0]*1000), # Case A: chal
        np.array([0.5]*1000), # Case B: champ
        np.array([1.0]*1000)  # Case B: chal
    ])
    
    # Case A: CI lo is negative -> tie despite exp_r improvement
    # We need to mock boostrap_mean_diff in tools
    with MagicMock() as mock_ci:
        from core.bootstrap_ci import BootstrapCI
        # Simulating CI that crosses zero: mean=0.05, lo=-0.01, hi=0.11
        tools.bootstrap_mean_diff = MagicMock(return_value=BootstrapCI(mean=0.05, lo=-0.01, hi=0.11, n=2000, n_boot=400, seed=7, alpha=0.05))
        tools.bootstrap_rate_diff = MagicMock(return_value=BootstrapCI(mean=0.0, lo=-0.01, hi=0.01, n=2000, n_boot=400, seed=7, alpha=0.05))
        
        rep = evaluate_v2(df, champ, chal, cfg)
        assert rep["winner"] == "tie"
        assert rep["reason"] == "ci_not_positive"

    # Case B: CI lo is positive -> challenger wins
    with MagicMock() as mock_ci:
        from core.bootstrap_ci import BootstrapCI
        tools.bootstrap_mean_diff = MagicMock(return_value=BootstrapCI(mean=0.05, lo=0.01, hi=0.09, n=2000, n_boot=400, seed=7, alpha=0.05))
        tools.bootstrap_rate_diff = MagicMock(return_value=BootstrapCI(mean=0.0, lo=-0.01, hi=0.01, n=2000, n_boot=400, seed=7, alpha=0.05))
        
        rep = evaluate_v2(df, champ, chal, cfg)
        assert rep["winner"] == "challenger"
        
    # Restore
    tools.score_model_proba = orig_score

def test_evaluate_v2_strata_guard():
    # Mock data with one bad symbol
    # BTC: 1000 rows, all gain 0.1
    # ETH: 100 rows, all loss -2.0
    df = pd.DataFrame({
        "y": [1]*1000 + [0]*100,
        "r_mult": [0.1]*1000 + [-2.0]*100,
        "ok": [1]*1100,
        "symbol": ["BTCUSDT"]*1000 + ["ETHUSDT"]*100
    })
    
    cfg = V2Config(
        min_n=500,
        bootstrap=0, 
        strata_cols=("symbol",),
        min_delta_exp_r=0.001,
        tail_slack=0.01,
        tail_r=-1.0
    )
    
    import tools.meta_ab_winner_evaluator_v2 as tools
    orig_score = tools.score_model_proba
    
    # Champ is better globaly and locally on ETH
    def mock_score(model, df_el):
        if model == "champ":
            return np.array([0.6]*1000 + [0.0]*100) # Fires on BTC, avoids ETH
        else:
            return np.array([0.6]*1000 + [0.6]*100) # Fires on both
            
    tools.score_model_proba = MagicMock(side_effect=mock_score)
    
    rep = evaluate_v2(df, "champ", "chal", cfg)
    
    # Chal is worse on ETH, so Champ wins globally or tie
    assert rep["winner"] in ["champion", "tie"]
    
    # Restore
    tools.score_model_proba = orig_score
