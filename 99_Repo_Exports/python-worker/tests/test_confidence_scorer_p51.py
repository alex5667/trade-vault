import pytest
from handlers.crypto_orderflow.scoring.confidence_scorer import ConfidenceScorer

class MockContext:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

def test_sweep_bonus_alignment():
    scorer = ConfidenceScorer()
    
    # sweep_eqh + SHORT -> bonus is higher than sweep_eql + SHORT
    ctx1 = MockContext(
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        market_mode="neutral", evidence={"sweep_eqh": 1}
    )
    score1, parts1 = scorer.score(kind="standard", side="SHORT", ctx=ctx1)
    
    ctx2 = MockContext(
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        market_mode="neutral", evidence={"sweep_eql": 1}
    )
    score2, parts2 = scorer.score(kind="standard", side="SHORT", ctx=ctx2)
    
    assert parts1["bonus_total_applied"] > parts2["bonus_total_applied"]

def test_trend_mom_strength_reduces_osc():
    scorer = ConfidenceScorer()
    
    ctx1 = MockContext(
        main_z=3.0, atr_q_main=0.5, spread_bps=1.0, 
        market_mode="momentum", mom_strength=0.0, evidence={"rsi_agree": 1}
    )
    _, parts1 = scorer.score(kind="breakout", side="LONG", ctx=ctx1)
    
    ctx2 = MockContext(
        main_z=3.0, atr_q_main=0.5, spread_bps=1.0, 
        market_mode="momentum", mom_strength=0.9, evidence={"rsi_agree": 1}
    )
    _, parts2 = scorer.score(kind="breakout", side="LONG", ctx=ctx2)
    
    assert parts1["bonus_osc_applied"] > parts2["bonus_osc_applied"]

def test_synergy_sweep_reclaim():
    scorer = ConfidenceScorer()
    ctx = MockContext(
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        market_mode="neutral", evidence={"sweep_eql": 1, "reclaim": 1}
    )
    _, parts = scorer.score(kind="standard", side="LONG", ctx=ctx)
    assert parts["bonus_synergy_applied"] > 0.0

def test_allowlist_ignores_random():
    scorer = ConfidenceScorer()
    ctx1 = MockContext(
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"random_nonsense": 1, "reclaim": 1} 
    )
    ctx2 = MockContext(
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"reclaim": 1}
    )
    _, parts1 = scorer.score(kind="standard", side="LONG", ctx=ctx1)
    _, parts2 = scorer.score(kind="standard", side="LONG", ctx=ctx2)
    assert parts1["bonus_total_applied"] == parts2["bonus_total_applied"]

def test_alias_ice_strict():
    scorer = ConfidenceScorer()
    ctx1 = MockContext(
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"iceberg_strict": 1}
    )
    _, parts1 = scorer.score(kind="standard", side="LONG", ctx=ctx1)
    
    ctx2 = MockContext(
        main_z=2.0, atr_q_main=0.5, spread_bps=1.0, 
        evidence={"ice_strict": 1}
    )
    _, parts2 = scorer.score(kind="standard", side="LONG", ctx=ctx2)
    
    assert parts1["bonus_micro_applied"] == parts2["bonus_micro_applied"]
    assert parts1["bonus_micro_applied"] > 0

