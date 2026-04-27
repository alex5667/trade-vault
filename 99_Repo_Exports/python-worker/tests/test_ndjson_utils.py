import pytest
from core.ndjson_utils import read_concatenated_json

def test_read_concatenated_json_standard_ndjson():
    content = '{"a":1}\n{"b":2}\n'
    res = list(read_concatenated_json(content))
    assert len(res) == 2
    assert res[0]["a"] == 1
    assert res[1]["b"] == 2

def test_read_concatenated_json_no_newlines():
    content = '{"a":1}{"b":2}{"c":3}'
    res = list(read_concatenated_json(content))
    assert len(res) == 3
    assert res[0]["a"] == 1
    assert res[2]["c"] == 3

def test_read_concatenated_json_mixed_whitespace():
    content = '  {"a": 1} \n {"b": 2}   {"c":3}\n'
    res = list(read_concatenated_json(content))
    assert len(res) == 3
    assert res[1]["b"] == 2

def test_read_concatenated_json_invalid_ignored():
    content = '{"a":1} GARBAGE {"b":2}'
    # The garbage might cause issues depending on where it is, 
    # but the parser attempts to skip until next valid json start char or fail gracefully.
    # Our simple implementation: raw_decode might raise, we catch and advance 1 char.
    res = list(read_concatenated_json(content))
    # It should at least recover the valid objects if separated enough
    assert len(res) >= 2
    assert res[0]["a"] == 1
    # Note: " GARBAGE " might be skipped char by char until "{"
    assert res[-1]["b"] == 2





















