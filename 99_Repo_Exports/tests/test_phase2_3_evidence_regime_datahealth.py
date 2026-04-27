import math
import sys
import os
import pytest
from types import SimpleNamespace

# Adjust path to find services
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker/services")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker")))

try:
    from signal_confidence import ConfidenceScorer, ConfidenceConfig, _ctx_confirm_value
except ImportError:
    try:
        from services.signal_confidence import ConfidenceScorer, ConfidenceConfig, _ctx_confirm_value
    except ImportError:
         pytest.skip("Could not import ConfidenceScorer", allow_module_level=True)

def make_ctx(**kwargs):
    if "confirmations" not in kwargs:
        kwargs["confirmations"] = []
    if "indicators" not in kwargs:
        kwargs["indicators"] = {}
    
    # Mock ConfCtx behavior (getattr priority: evidence > indicators > config > runtime)
    class MockCtx:
        def __init__(self, kw):
            self.__dict__.update(kw)
            self.evidence = kw.get("evidence", {})
            self.indicators = kw.get("indicators", {})
            self.config = kw.get("config", {})
        
        def __getattr__(self, name):
            if name in self.evidence: return self.evidence[name]
            if name in self.indicators: return self.indicators[name]
            if name in self.config: return self.config[name]
            return self.__dict__.get(name)

    return MockCtx(kwargs)

def test_phase2_evidence_only_works():
    """Verify that score is calculated correctly using ONLY structural evidence (empty confirmations string list)."""
    scorer = ConfidenceScorer()
    
    # Case A: Legacy strings
    ctx_legacy = make_ctx(
        delta_z=3.0,
        confirmations=["rsi=1.0", "div=0.5"],
        evidence={}
    )
    conf_legacy, parts_legacy = scorer.score(kind="custom", side="LONG", ctx=ctx_legacy)
    
    # Case B: Structural evidence ONLY
    ctx_struct = make_ctx(
        delta_z=3.0,
        confirmations=[], # Empty!
        evidence={"rsi": 1.0, "div": 0.5}
    )
    conf_struct, parts_struct = scorer.score(kind="custom", side="LONG", ctx=ctx_struct)
    
    assert parts_struct["bonus_generic"] > 0
    assert math.isclose(parts_legacy["bonus_generic"], parts_struct["bonus_generic"], abs_tol=1e-9)
    assert math.isclose(conf_legacy, conf_struct, abs_tol=1e-9)

def test_phase3_regime_aware_weights():
    """Verify different bonuses for Trend vs Range regimes."""
    scorer = ConfidenceScorer()
    
    # 1. Trend Regime: RSI bonus boosted, Div bonus reduced
    ctx_trend = make_ctx(
        delta_z=3.0,
        market_mode="trend",
        evidence={"rsi": 1.0, "div": 1.0},
        # Multipliers: rsi=1.35, div=0.65 (defaults)
    )
    _, parts_trend = scorer.score(kind="custom", side="LONG", ctx=ctx_trend)
    
    # Base RSI bonus = 0.03. In trend * 1.35 = 0.0405
    # Base Div bonus = 0.04. In trend * 0.65 = 0.026
    # Total ~ 0.0665
    
    # 2. Range Regime: RSI reduced, Div boosted
    ctx_range = make_ctx(
        delta_z=3.0,
        market_mode="range",
        evidence={"rsi": 1.0, "div": 1.0},
        # Multipliers: rsi=0.80, div=1.35
    )
    _, parts_range = scorer.score(kind="custom", side="LONG", ctx=ctx_range)
    
    # Base RSI bonus = 0.03. In range * 0.80 = 0.024
    # Base Div bonus = 0.04. In range * 1.35 = 0.054
    # Total ~ 0.078
    
    assert parts_trend["regime"] == "trend"
    assert parts_range["regime"] == "range"
    
    # Check logical direction (Range should favor div+rsi combo more than Trend due to high Div weight?)
    # Actually 0.04*1.35 > 0.04*0.65.
    # Let's check specific components if possible, but "bonus_generic" is aggregated.
    # We can rely on total bonus_generic.
    
    assert parts_range["bonus_generic"] > parts_trend["bonus_generic"]

def test_phase3_counter_trend_penalty():
    """Verify penalty when acting against a strong divergence in Trend mode."""
    scorer = ConfidenceScorer()
    
    # Long in Trend, but Bearish Div present (Counter-trend!)
    ctx_ct = make_ctx(
        delta_z=3.0,
        market_mode="trend",
        div_kind="bearish_div",
        div_strength=0.8,
        evidence={},
        confirmations=[]
    )
    _, parts_ct = scorer.score(kind="custom", side="LONG", ctx=ctx_ct)
    
    assert parts_ct.get("pen_div_countertrend", 0.0) > 0.0
    
    # Same scenario but in Range mode -> No penalty (Range loves fading)
    ctx_range = make_ctx(
        delta_z=3.0,
        market_mode="range",
        div_kind="bearish_div",
        div_strength=0.8,
        evidence={},
        confirmations=[]
    )
    _, parts_range = scorer.score(kind="custom", side="LONG", ctx=ctx_range)
    
    assert parts_range.get("pen_div_countertrend", 0.0) == 0.0

def test_phase3_data_health_calibration():
    """Verify confidence dampening when data_health is low."""
    scorer = ConfidenceScorer()
    
    # Perfect health
    ctx_good = make_ctx(
        delta_z=4.0, 
        data_health=1.0,
        data_health_power=1.0,
        evidence={}
    )
    conf_good, _ = scorer.score(kind="custom", side="LONG", ctx=ctx_good)
    
    # Bad health (0.5)
    ctx_bad = make_ctx(
        delta_z=4.0, 
        data_health=0.5,
        data_health_power=1.0,
        evidence={}
    )
    conf_bad, parts_bad = scorer.score(kind="custom", side="LONG", ctx=ctx_bad)
    
    assert parts_bad["data_health_mult"] == 0.5
    assert conf_bad < conf_good
    # Roughly half the confidence01 (normalized), final conf might be clamped
    
    # Check floor logic
    ctx_floor = make_ctx(
        delta_z=4.0,
        data_health=0.0,
        data_health_floor=0.2,
        evidence={}
    )
    _, parts_floor = scorer.score(kind="custom", side="LONG", ctx=ctx_floor)
    assert parts_floor["data_health_mult"] == 0.2
