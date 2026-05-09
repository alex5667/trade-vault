from services.entry_policy_ab_gate import decide_active_arm, norm_arm, regime_group


def test_regime_group():
    assert regime_group("thin") == "thin"
    assert regime_group("news") == "thin"
    assert regime_group("illiquid") == "thin"
    assert regime_group("default") == "default"
    assert regime_group("trend") == "default"
    assert regime_group(None) == "default"
    assert regime_group("") == "default"

def test_norm_arm():
    assert norm_arm("A") == "A"
    assert norm_arm("b") == "B"
    assert norm_arm(" C ") == "C"
    assert norm_arm("D") == "A"
    assert norm_arm(None) == "A"

def test_decide_active_arm_match():
    # Candidates match active arm
    res = decide_active_arm(cand_arm="A", active_arm_value="A")
    assert res.apply is True
    assert res.active_arm == "A"
    assert res.is_active is True
    assert res.reason == "OK"

    res = decide_active_arm(cand_arm="B", active_arm_value="B")
    assert res.is_active is True

def test_decide_active_arm_mismatch():
    # Candidate B, Active A
    res = decide_active_arm(cand_arm="B", active_arm_value="A")
    assert res.apply is True
    assert res.active_arm == "A"
    assert res.is_active is False
    assert res.reason == "INACTIVE_ARM"

    # Candidate A, Active C
    res = decide_active_arm(cand_arm="A", active_arm_value="C")
    assert res.is_active is False

def test_decide_active_arm_fail_open():
    # No active arm config -> Pass through (is_active=True, apply=False)
    # The logic returns apply=False, active_arm="NA", is_active=True, reason="NO_ACTIVE_ARM_KEY"
    res = decide_active_arm(cand_arm="B", active_arm_value=None)
    assert res.apply is False
    assert res.is_active is True
    assert res.reason == "NO_ACTIVE_ARM_KEY"

    res = decide_active_arm(cand_arm="B", active_arm_value="")
    assert res.apply is False
    assert res.is_active is True

    res = decide_active_arm(cand_arm="B", active_arm_value="XYZ")
    assert res.apply is False
    assert res.is_active is True
