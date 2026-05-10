import json

from services.dispatch.envelope_parser import parse_envelope_fields


def test_parse_envelope_data_json():
    env = {"sid": "s1", "trace_id": "t1"}
    fields = {"data": json.dumps(env)}
    out = parse_envelope_fields(fields)
    assert out["sid"] == "s1"
    assert out["trace_id"] == "t1"


def test_parse_envelope_payload_json_bytes():
    env = {"sid": "s2"}
    fields = {"payload": json.dumps(env).encode("utf-8")}
    out = parse_envelope_fields(fields)
    assert out["sid"] == "s2"


def test_parse_envelope_none():
    assert parse_envelope_fields({}) is None
