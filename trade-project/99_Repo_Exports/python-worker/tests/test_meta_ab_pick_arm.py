from core.of_confirm_engine import _ab_pick_arm


def test_ab_pick_arm_share_zero():
    assert _ab_pick_arm("sid", 0.0, "salt") == "champion"


def test_ab_pick_arm_share_one():
    assert _ab_pick_arm("sid", 1.0, "salt") == "challenger"


def test_ab_pick_arm_deterministic():
    a = _ab_pick_arm("sid:123", 0.42, "salt_v1")
    b = _ab_pick_arm("sid:123", 0.42, "salt_v1")
    assert a == b


def test_ab_pick_arm_salt_changes_bucket():
    a = _ab_pick_arm("sid:123", 0.5, "salt_a")
    b = _ab_pick_arm("sid:123", 0.5, "salt_b")
    assert a in ("champion", "challenger")
    assert b in ("champion", "challenger")
