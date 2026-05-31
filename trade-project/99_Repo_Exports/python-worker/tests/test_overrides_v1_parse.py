from core.entry_policy_overrides_v1 import EntryPolicyOverridesV1


def test_overrides_v1_parse_new_fields():
    data = {
        "apply_kind": "overrides_v1",
        "group": "default",
        "abs_lvl_tier_trend": 0,
        "abs_lvl_tier_range": 1,
        "abs_lvl_tier_thin": 2,
        "abs_lvl_tier_mode": "exact",
        "ab_split_b": 20,
        "ab_split_c": 30,
        "ab_salt": "test_salt"
    }

    o, err = EntryPolicyOverridesV1.from_dict(data)
    assert o is not None, f"Failed to parse: {err}"
    assert o.abs_lvl_tier_trend == 0
    assert o.abs_lvl_tier_range == 1
    assert o.abs_lvl_tier_thin == 2
    assert o.abs_lvl_tier_mode == "exact"
    assert o.ab_split_b == 20
    assert o.ab_split_c == 30
    assert o.ab_salt == "test_salt"

    ok, err = o.validate()
    assert ok, f"Validation failed: {err}"

def test_overrides_v1_invalid_tier():
    data = {"abs_lvl_tier_trend": 5}
    o, _ = EntryPolicyOverridesV1.from_dict(data)
    ok, err = o.validate()
    assert not ok
    assert "bad_abs_lvl_tier_trend" in err

if __name__ == "__main__":
    test_overrides_v1_parse_new_fields()
    test_overrides_v1_invalid_tier()
    print("OK")
