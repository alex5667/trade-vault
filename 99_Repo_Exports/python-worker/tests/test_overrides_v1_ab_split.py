
from core.entry_policy_overrides_v1 import EntryPolicyOverridesV1


def test_overrides_v1_valid_split():
    o = EntryPolicyOverridesV1(ab_split_b=10, ab_split_c=10, ab_salt="v1")
    ok, _ = o.validate()
    assert ok


def test_overrides_v1_invalid_split_sum():
    o = EntryPolicyOverridesV1(ab_split_b=60, ab_split_c=40)
    ok, reason = o.validate()
    assert not ok
    assert "ab_split_sum" in reason
