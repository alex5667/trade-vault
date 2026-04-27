from orderflow_services.auto_rollback_trigger_engine_v1 import _j


def test_json_parse_reason_codes():
    assert _j('["ERROR_RATE_SPIKE"]', []) == ["ERROR_RATE_SPIKE"]


def test_json_parse_default():
    assert _j(None, []) == []
