"""
Tests for Entry Policy Freeze Schema

Expert review:
  - Senior Python: Validates schema parsing, validation, and is_active logic
  - DevOps/SRE: Tests fail-safe defaults and backward compatibility
"""
from core.entry_policy_freeze import EntryPolicyFreezeV1


def test_freeze_v1_parse_ok():
    """Valid freeze should parse successfully"""
    raw = '{"ver":1,"symbol":"BTCUSDT","group":"default","scenario":"reversal","until_ts_ms":9999999999999,"mode":"shadow","reason_code":"DATA_BAD","created_ts_ms":1}'
    o, err = EntryPolicyFreezeV1.from_json(raw)
    assert err == ""
    assert o is not None
    assert o.symbol == "BTCUSDT"
    assert o.scenario == "reversal"
    assert o.mode == "shadow"
    assert o.is_active(1) is True


def test_freeze_v1_inactive_after_expiry():
    """Freeze should be inactive after until_ts_ms"""
    raw = '{"ver":1,"symbol":"BTCUSDT","group":"default","scenario":"continuation","until_ts_ms":1000,"created_ts_ms":1}'
    o, err = EntryPolicyFreezeV1.from_json(raw)
    assert err == ""
    assert o.is_active(1001) is False


def test_freeze_v1_reject_bad_scenario():
    """Invalid scenario should be rejected"""
    raw = '{"ver":1,"symbol":"BTCUSDT","group":"default","scenario":"invalid","until_ts_ms":10}'
    o, err = EntryPolicyFreezeV1.from_json(raw)
    assert o is None
    assert err == "bad_scenario"


def test_freeze_v1_reject_bad_version():
    """Unsupported version should be rejected"""
    raw = '{"ver":99,"symbol":"BTCUSDT","group":"default","scenario":"reversal","until_ts_ms":10}'
    o, err = EntryPolicyFreezeV1.from_json(raw)
    assert o is None
    assert err == "bad_ver"


def test_freeze_v1_reject_no_symbol():
    """Missing symbol should be rejected"""
    raw = '{"ver":1,"symbol":"","group":"default","scenario":"reversal","until_ts_ms":10}'
    o, err = EntryPolicyFreezeV1.from_json(raw)
    assert o is None
    assert err == "no_symbol"


def test_freeze_v1_reject_bad_json():
    """Malformed JSON should return error"""
    raw = '{invalid'
    o, err = EntryPolicyFreezeV1.from_json(raw)
    assert o is None
    assert err == "bad_json"


def test_freeze_v1_reject_bad_mode():
    """Invalid mode should be rejected"""
    raw = '{"ver":1,"symbol":"BTCUSDT","group":"default","scenario":"reversal","until_ts_ms":10,"mode":"invalid"}'
    o, err = EntryPolicyFreezeV1.from_json(raw)
    assert o is None
    assert err == "bad_mode"


def test_freeze_v1_mode_defaults_to_hard():
    """Mode should default to hard for backward compatibility"""
    raw = '{"ver":1,"symbol":"ETHUSDT","group":"default","scenario":"continuation","until_ts_ms":999999,"created_ts_ms":1}'
    o, err = EntryPolicyFreezeV1.from_json(raw)
    assert err == ""
    assert o.mode == "hard"


def test_freeze_v1_serialization_roundtrip():
    """Serialization roundtrip should preserve data"""
    fz1 = EntryPolicyFreezeV1(
        ver=1,
        symbol="ETHUSDT",
        group="thin",
        scenario="continuation",
        until_ts_ms=999999,
        mode="shadow",
        reason_code="DATA_BAD",
        notes="test freeze",
        src="cb_v1",
        created_ts_ms=123456,
        metrics={"spread_z_p95": 3.5},
    )
    
    json_str = fz1.to_json()
    fz2, err = EntryPolicyFreezeV1.from_json(json_str)
    
    assert err == ""
    assert fz2.symbol == fz1.symbol
    assert fz2.mode == fz1.mode
    assert fz2.until_ts_ms == fz1.until_ts_ms
    assert fz2.metrics == fz1.metrics
