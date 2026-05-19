import pytest
from services.entry_policy_ab_gate import decide_active_arm, regime_group


def test_active_arm_gate():
    """
    Verify decide_active_arm logic:
      - Compares cand_arm with active_arm_value
      - Returns is_active=True if match
      - Returns is_active=False if mismatch
      - Fail-open when active_arm_value is None/empty (apply=False, is_active=True)
    """
    # Case 1: Default (active=None -> fail open), Cand=A -> pass through
    res = decide_active_arm(cand_arm="A", active_arm_value=None)
    assert res.is_active is True
    assert res.apply is False

    # Case 2: active=A, Cand=A -> Active
    res = decide_active_arm(cand_arm="A", active_arm_value="A")
    assert res.is_active is True
    assert res.apply is True

    # Case 3: active=B, Cand=A -> Shadow
    res = decide_active_arm(cand_arm="A", active_arm_value="B")
    assert res.is_active is False
    assert res.apply is True

    # Case 4: active=B, Cand=B -> Active
    res = decide_active_arm(cand_arm="B", active_arm_value="B")
    assert res.is_active is True

    # Case 5: active=C, Cand=C -> Active
    res = decide_active_arm(cand_arm="C", active_arm_value="C")
    assert res.is_active is True

    # Case 6: Fail open on bad active value
    res = decide_active_arm(cand_arm="A", active_arm_value="")
    assert res.is_active is True
    assert res.apply is False


def test_regime_group_mapping():
    """Verify thin/trend/range/mixed groupings."""
    assert regime_group("thin") == "thin"
    assert regime_group("news") == "thin"
    assert regime_group("illiquid") == "thin"
    assert regime_group("trend") == "trend"
    assert regime_group("trending_bull") == "trend"
    assert regime_group("range") == "range"
    assert regime_group("chop") == "range"
    assert regime_group("sideways") == "range"
    assert regime_group("unknown") == "mixed"
    assert regime_group("default") == "mixed"
    assert regime_group(None) == "mixed"  # type: ignore
