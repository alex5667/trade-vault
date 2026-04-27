import pytest
from handlers.crypto_orderflow.scoring.confidence_scorer import ConfidenceScorer

class MockContext:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

def test_anti_corr_tuning_override():
    scorer = ConfidenceScorer()
    
    # baseline
    ctx_base = MockContext(
        market_mode="momentum",
        main_z=3.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"rsi_agree": 1}
    )
    _, parts_base = scorer.score(kind="breakout", side="LONG", ctx=ctx_base)
    base_osc = parts_base["bonus_osc_applied"]
    
    # with tuning overriding rsi_agree specifically for trend
    tuning = {
        "by_regime": {
            "trend": {
                "anti_corr": {"rsi_agree": 0.50}
            }
        }
    }
    
    ctx_tuned = MockContext(
        market_mode="momentum",
        main_z=3.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"rsi_agree": 1},
        conf_score_weight_tuning=tuning
    )
    
    _, parts_tuned = scorer.score(kind="breakout", side="LONG", ctx=ctx_tuned)
    tuned_osc = parts_tuned["bonus_osc_applied"]
    
    assert tuned_osc < base_osc
    assert tuned_osc > 0

def test_synergy_tuning_override():
    scorer = ConfidenceScorer()
    
    # baseline
    ctx_base = MockContext(
        market_mode="neutral",
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"sweep": 1, "reclaim": 1}
    )
    _, parts_base = scorer.score(kind="standard", side="UNKNOWN", ctx=ctx_base)
    base_syn = parts_base["bonus_synergy_applied"]
    
    # with tuning defining global synergy
    tuning = {
        "synergy": {
            "sweep+reclaim": 0.05
        }
    }
    
    ctx_tuned = MockContext(
        market_mode="neutral",
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"sweep": 1, "reclaim": 1},
        conf_score_weight_tuning=tuning
    )
    
    _, parts_tuned = scorer.score(kind="standard", side="UNKNOWN", ctx=ctx_tuned)
    tuned_syn = parts_tuned["bonus_synergy_applied"]
    
    assert tuned_syn > base_syn
    
def test_synergy_by_regime_override():
    scorer = ConfidenceScorer()
    
    ctx_neutral = MockContext(
        market_mode="neutral",
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"ice_strict": 1, "fp_edge_absorb": 1},
        conf_score_weight_tuning={
            "synergy_by_regime": {
                "neutral": {"iceberg_strict+fp_edge_absorb": 0.04},
                "trend": {"iceberg_strict+fp_edge_absorb": 0.01}
            }
        }
    )
    _, parts_neutral = scorer.score(kind="standard", side="LONG", ctx=ctx_neutral)
    
    ctx_trend = MockContext(
        market_mode="momentum",
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"ice_strict": 1, "fp_edge_absorb": 1},
        conf_score_weight_tuning={
            "synergy_by_regime": {
                "neutral": {"iceberg_strict+fp_edge_absorb": 0.04},
                "trend": {"iceberg_strict+fp_edge_absorb": 0.01}
            }
        }
    )
    _, parts_trend = scorer.score(kind="standard", side="LONG", ctx=ctx_trend)
    
    assert parts_neutral["bonus_synergy_applied"] > parts_trend["bonus_synergy_applied"]
