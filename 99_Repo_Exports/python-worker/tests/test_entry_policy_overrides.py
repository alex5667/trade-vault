"""
Tests for EntryPolicyOverridesV1 schema validation

Expert review:
  - Senior Python: Comprehensive coverage of validation edge cases
  - Professor Statistics: Validates range constraints prevent statistical nonsense
  - DevOps/SRE: Tests backward compatibility for zero-downtime deployment
"""
from core.entry_policy_overrides import EntryPolicyOverridesV1


def test_overrides_v1_parse_and_validate_ok():
    """Valid override with all fields should parse and validate successfully"""
    raw = '{"ver":1,"entry_min_of_score":1.0,"entry_max_spread_z":2.0,"entry_near_zone_bp":12,"entry_obi_min_sec":1.5,"hold_down_ms":3600000,"hysteresis_impr":0.04}'
    o, err = EntryPolicyOverridesV1.from_json(raw)
    assert err == "", f"Expected no error, got: {err}"
    assert o is not None
    ok, err2 = o.validate()
    assert ok and err2 == "", f"Validation failed: {err2}"
    assert o.entry_min_of_score == 1.0
    assert o.hold_down_ms == 3600000


def test_overrides_v1_reject_bad_ranges():
    """Out-of-range values should be rejected"""
    # zone_dist_bp must be >= 1.0
    raw = '{"ver":1,"entry_near_zone_bp":0}'
    o, err = EntryPolicyOverridesV1.from_json(raw)
    assert o is None
    assert "bad_" in err


def test_overrides_v1_reject_bad_json():
    """Malformed JSON should return error"""
    raw = '{invalid json'
    o, err = EntryPolicyOverridesV1.from_json(raw)
    assert o is None
    assert err == "bad_json"


def test_overrides_v1_reject_bad_version():
    """Unsupported version should be rejected"""
    raw = '{"ver":99,"entry_min_of_score":1.0}'
    o, err = EntryPolicyOverridesV1.from_json(raw)
    assert o is None
    assert err == "bad_ver"


def test_overrides_v1_backward_compat_env_keys():
    """Should accept legacy ENV-style keys for backward compatibility"""
    raw = '{"ver":1,"ENTRY_MIN_OF_SCORE":0.8,"ENTRY_MAX_SPREAD_Z":2.5}'
    o, err = EntryPolicyOverridesV1.from_json(raw)
    assert err == ""
    assert o is not None
    assert o.entry_min_of_score == 0.8
    assert o.entry_max_spread_z == 2.5


def test_overrides_v1_hold_down_logic():
    """Hold-down mechanism should block suggestions during cooldown period"""
    o = EntryPolicyOverridesV1(
        applied_ts_ms=1000000,
        hold_down_ms=3600000,  # 1 hour
    )
    
    # Within hold-down period
    assert o.is_in_hold_down(1000000 + 1800000) == True  # 30 min later
    
    # After hold-down period
    assert o.is_in_hold_down(1000000 + 3600001) == False  # 1 hour + 1ms later
    
    # No hold-down set
    o2 = EntryPolicyOverridesV1(applied_ts_ms=1000000, hold_down_ms=0)
    assert o2.is_in_hold_down(1000001) == False


def test_overrides_v1_partial_overrides():
    """Should allow partial overrides (only some thresholds set)"""
    raw = '{"ver":1,"entry_min_of_score":0.9}'
    o, err = EntryPolicyOverridesV1.from_json(raw)
    assert err == ""
    assert o.entry_min_of_score == 0.9
    assert o.entry_max_spread_z is None  # Not overridden
    assert o.entry_near_zone_bp is None


def test_overrides_v1_serialization_roundtrip():
    """Serialization roundtrip should preserve data"""
    o1 = EntryPolicyOverridesV1(
        entry_min_of_score=1.0,
        entry_max_spread_z=2.0,
        hold_down_ms=7200000,
        hysteresis_impr=0.05,
        src="thresh_lcb",
        sid="abc123",
    )
    
    json_str = o1.to_json()
    o2, err = EntryPolicyOverridesV1.from_json(json_str)
    
    assert err == ""
    assert o2.entry_min_of_score == o1.entry_min_of_score
    assert o2.hold_down_ms == o1.hold_down_ms
    assert o2.src == o1.src
