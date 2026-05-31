from __future__ import annotations

from core.ab_router import choose_arm_abc, splits_for_regime


def test_choose_arm_deterministic():
    """Verify deterministic arm selection."""
    a1 = choose_arm_abc(key="k", split_b=10, split_c=10, salt="s")
    a2 = choose_arm_abc(key="k", split_b=10, split_c=10, salt="s")
    assert a1 == a2


def test_choose_arm_distribution():
    """Verify rough distribution matches splits."""
    arms = [choose_arm_abc(key=f"k{i}", split_b=10, split_c=10, salt="s") for i in range(1000)]
    b_pct = arms.count("B") / 10.0
    c_pct = arms.count("C") / 10.0
    # Should be roughly 10% each (allow ±5% variance)
    assert 5 < b_pct < 15
    assert 5 < c_pct < 15


def test_splits_for_regime():
    """Verify contextual split selection by regime."""
    cfg = {"ab_split_b_default": 10, "ab_split_c_default": 10, "ab_split_b_thin": 20, "ab_split_c_thin": 20}
    s1 = splits_for_regime(regime="range", cfg=cfg)
    s2 = splits_for_regime(regime="thin", cfg=cfg)
    assert s1.group == "default"
    assert s2.group == "thin"
    assert s2.b == 20
    assert s2.c == 20
