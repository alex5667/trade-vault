import pytest


def test_reason_code_to_u16_known():
    from signal_scoring.reason_registry import reason_code_to_u16
    v = reason_code_to_u16("VETO_WALL_NEAR", strict=True)
    assert isinstance(v, int)
    assert 0 < v <= 65535


def test_legacy_reason_maps_to_structured_code():
    from signal_scoring.reason_registry import legacy_reason_to_code
    assert legacy_reason_to_code("near_big_wall") == "VETO_WALL_NEAR"
    assert legacy_reason_to_code("wall_near") == "VETO_WALL_NEAR"


def test_unknown_reason_code_fail_open_by_default(monkeypatch):
    # default: STRICT_REASON_CODES=0 => fail-open => 0
    monkeypatch.delenv("STRICT_REASON_CODES", raising=False)
    from signal_scoring.reason_registry import reason_code_to_u16
    assert reason_code_to_u16("SOME_NEW_CODE_THAT_IS_NOT_REGISTERED") == 0


def test_unknown_reason_code_strict_raises(monkeypatch):
    monkeypatch.setenv("STRICT_REASON_CODES", "1")
    from signal_scoring.reason_registry import reason_code_to_u16
    with pytest.raises(ValueError):
        reason_code_to_u16("SOME_NEW_CODE_THAT_IS_NOT_REGISTERED")
