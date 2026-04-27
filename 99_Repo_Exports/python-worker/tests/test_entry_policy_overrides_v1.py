# -*- coding: utf-8 -*-

from core.entry_policy_overrides_v1 import EntryPolicyOverridesV1


def test_overrides_v1_parses_ab_splits() -> None:
    raw = {
        "v": 1,
        "kind": "overrides_v1",
        "updated_ts_ms": 123,
        "enabled": 1,
        "symbol": "BTCUSDT",
        "regime": "thin",
        "scenario": "continuation",
        "group": "thin",
        "force_active_arm": "B",
        "ab_split_b": 12,
        "ab_split_c": 7,
        "ab_salt": "v2",
    }
    o, status = EntryPolicyOverridesV1.from_dict(raw)
    assert o is not None
    ok, why = o.validate()
    assert ok, why
    assert o.ab_split_b == 12
    assert o.ab_split_c == 7
    assert o.ab_salt == "v2"


def test_overrides_v1_rejects_bad_splits() -> None:
    raw = {"v": 1, "kind": "overrides_v1", "updated_ts_ms": 1, "ab_split_b": 60, "ab_split_c": 50}
    o, status = EntryPolicyOverridesV1.from_dict(raw)
    assert o is not None
    ok, why = o.validate()
    assert not ok
