
from services.signal_confidence import ConfidenceScorer


class MockContext:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

def test_sweep_fallback_score():
    scorer = ConfidenceScorer()

    # 1. Baseline (no sweep)
    ctx = MockContext(confirmations=[])
    score_base, parts_base = scorer.score(ctx=ctx)

    # 2. Specific EQH sweep (strong)
    # sweep_eqh should trigger high confidence boost
    ctx_eqh = MockContext(confirmations=["sweep_eqh=1"])
    score_eqh, parts_eqh = scorer.score(ctx=ctx_eqh)
    assert score_eqh >= score_base
    # bonus_generic comes from 0.03 * clamp(s_sweep). s_sweep for eqh is 0.8.
    # 0.03 * 0.8 = 0.024.
    assert parts_eqh.get("bonus_generic", 0) > 0.02

    # 3. Generic sweep (fallback default 0.5)
    ctx_fallback = MockContext(confirmations=["sweep=1"])
    score_fallback, parts_fallback = scorer.score(ctx=ctx_fallback)
    assert score_fallback >= score_base
    # it should be less than specific sweep
    assert parts_fallback.get("bonus_generic", 0) < parts_eqh.get("bonus_generic", 0)
    # s_sweep default 0.5 -> 0.03 * 0.5 = 0.015
    assert parts_fallback.get("bonus_generic", 0) > 0.01

    # 4. Configured fallback (weak)
    ctx_weak = MockContext(confirmations=["sweep=1"], sweep_simple_strength=0.1)
    score_weak, parts_weak = scorer.score(ctx=ctx_weak)
    # s_sweep = 0.1 -> 0.03 * 0.1 = 0.003
    assert parts_weak.get("bonus_generic", 0) < parts_fallback.get("bonus_generic", 0)

    # 5. Configured fallback (strong)
    # 0.9 -> 0.027
    ctx_strong = MockContext(confirmations=["sweep=1"], sweep_simple_strength=0.9)
    score_strong, parts_strong = scorer.score(ctx=ctx_strong)
    assert parts_strong.get("bonus_generic", 0) > parts_fallback.get("bonus_generic", 0)
    assert parts_strong.get("bonus_generic", 0) > parts_eqh.get("bonus_generic", 0)
