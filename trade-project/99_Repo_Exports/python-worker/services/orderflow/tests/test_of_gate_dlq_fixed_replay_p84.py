
from orderflow_services.of_gate_dlq_fix_hints_registry_p84 import hint_for, known_dq_codes
from orderflow_services.of_gate_dlq_fixed_replay_p84 import (
    _coerce_int01,
    _normalize_ts_ms,
    _parse_stream_payload_from_fields,
    _safe_fix_payload,
)


def test_fix_hints_registry():
    codes = known_dq_codes()
    assert "dq_schema_missing" in codes
    assert "dq_ok_invariant" in codes

    # Known dq_code
    hint1 = hint_for("dq_schema_missing", "Some random err")
    assert hint1.hint_code == "schema_missing"
    assert hint1.severity == "warn"

    # Fallback err parsing (JSONDecodeError)
    hint2 = hint_for("dq_unknown", "JSONDecodeError expecting value")
    assert hint2.hint_code == "payload_parse"

    # Fallback err parsing (timeout)
    hint3 = hint_for("dq_unknown", "Timeout connection reached")
    assert hint3.hint_code == "timeout"

    # Unknown
    hint4 = hint_for(None, None)
    assert hint4.hint_code == "unknown"


def test_coerce_int01():
    assert _coerce_int01(0) == 0
    assert _coerce_int01(1) == 1
    assert _coerce_int01(True) == 1
    assert _coerce_int01(False) == 0
    assert _coerce_int01("1") == 1
    assert _coerce_int01("0") == 0
    assert _coerce_int01(1.0) == 1
    assert _coerce_int01(0.0) == 0
    assert _coerce_int01(2) == 1
    assert _coerce_int01("yes") == 1
    assert _coerce_int01("no") == 0


def test_normalize_ts_ms():
    # seconds
    assert _normalize_ts_ms(1700000000) == 1700000000000
    # ms
    assert _normalize_ts_ms(1700000000000) == 1700000000000
    # us
    assert _normalize_ts_ms(1700000000000000) == 1700000000000
    # str
    assert _normalize_ts_ms("1700000000000") == 1700000000000


def test_parse_stream_payload_from_fields_payload_key():
    fields = {"payload": '{"symbol":"BTCUSDT","ok":1,"ok_soft":0,"ts_ms":1700000000000}'}
    p = _parse_stream_payload_from_fields(fields)
    assert p["symbol"] == "BTCUSDT"
    assert p["ok"] == 1


def test_safe_fix_payload_schema_ts_missing_legs():
    payload = {
        "symbol": "BTCUSDT",
        "ok": 1,
        "ok_soft": 0,
        "ts_ms": 1700000000,  # seconds
    }
    new_p, fixes = _safe_fix_payload(payload.copy(), "1700000000000-0")

    assert new_p["schema_name"] == "of_gate_metrics"
    assert int(new_p["schema_version"]) == 2
    assert new_p["ts_ms"] == 1700000000000
    assert new_p["missing_legs"] == "[]"

    assert "add_schema_name" in fixes
    assert "add_schema_version" in fixes
    assert "normalize_ts_ms" in fixes
    assert "default_missing_legs_empty" in fixes


def test_safe_fix_payload_missing_legs_coerce():
    payload = {
        "symbol": "BTCUSDT",
        "schema_name": "of_gate_metrics",
        "schema_version": "2",
        "ok": 0,
        "ok_soft": 0,
        "ts_ms": 1700000000000,
        "missing_legs": "not_json",
    }
    new_p, fixes = _safe_fix_payload(payload.copy(), "1700000000000-0")
    assert "coerce_schema_version_int" in fixes
    assert "coerce_missing_legs_to_json" in fixes
    # should be a JSON list string
    assert isinstance(new_p["missing_legs"], str)
    assert new_p["missing_legs"].startswith("[")
