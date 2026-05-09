from __future__ import annotations

# -*- coding: utf-8 -*-
"""Tests for orderflow_services._common shared helpers."""
import json
from pathlib import Path

import pytest

from orderflow_services._common import (
    _as_float,
    _as_int,
    _as_str,
    _load_json,
    _load_json_file,
    _now_ms,
    _parse_list,
)
from utils.time_utils import get_ny_time_millis

# ---------------------------------------------------------------------------
# _now_ms
# ---------------------------------------------------------------------------

class TestNowMs:
    def test_returns_int(self):
        assert isinstance(_now_ms(), int)

    def test_close_to_real_time(self):
        expected = get_ny_time_millis()
        got = _now_ms()
        assert abs(got - expected) < 500  # within 500 ms


# ---------------------------------------------------------------------------
# _as_str
# ---------------------------------------------------------------------------

class TestAsStr:
    def test_none_returns_default(self):
        assert _as_str(None) == ""
        assert _as_str(None, "x") == "x"

    def test_bytes_decoded(self):
        assert _as_str(b"hello") == "hello"

    def test_bytearray_decoded(self):
        assert _as_str(bytearray(b"hi")) == "hi"

    def test_str_passthrough(self):
        assert _as_str("foo") == "foo"

    def test_int_converted(self):
        assert _as_str(42) == "42"

    def test_bad_bytes_ignored(self):
        result = _as_str(bytes([0xFF, 0xFE, 0x61]))
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# _as_int
# ---------------------------------------------------------------------------

class TestAsInt:
    def test_none_returns_default(self):
        assert _as_int(None) == 0
        assert _as_int(None, 7) == 7

    def test_bool_returns_default(self):
        # bool is subclass of int — we treat it as missing/untrusted
        assert _as_int(True) == 0
        assert _as_int(False, 5) == 5

    def test_int_passthrough(self):
        assert _as_int(42) == 42

    def test_float_truncated(self):
        assert _as_int(3.9) == 3

    def test_str_float(self):
        assert _as_int("3.0") == 3

    def test_str_int(self):
        assert _as_int("100") == 100

    def test_invalid_str_returns_default(self):
        assert _as_int("abc", 99) == 99

    def test_empty_str_returns_default(self):
        assert _as_int("", 5) == 5


# ---------------------------------------------------------------------------
# _as_float
# ---------------------------------------------------------------------------

class TestAsFloat:
    def test_none_returns_default(self):
        assert _as_float(None) == 0.0
        assert _as_float(None, 1.5) == 1.5

    def test_bool_returns_default(self):
        assert _as_float(True) == 0.0

    def test_int_converted(self):
        assert _as_float(3) == 3.0

    def test_float_passthrough(self):
        assert _as_float(1.23) == pytest.approx(1.23)

    def test_str_float(self):
        assert _as_float("3.14") == pytest.approx(3.14)

    def test_invalid_returns_default(self):
        assert _as_float("nan_str", 2.0) == 2.0


# ---------------------------------------------------------------------------
# _parse_list
# ---------------------------------------------------------------------------

class TestParseList:
    def test_empty_string(self):
        assert _parse_list("") == []

    def test_none_like(self):
        assert _parse_list(None) == []  # type: ignore

    def test_single_item(self):
        assert _parse_list("FOO") == ["FOO"]

    def test_comma_separated(self):
        assert _parse_list("a,b,c") == ["A", "B", "C"]

    def test_semicolon_separated(self):
        assert _parse_list("a;b;c") == ["A", "B", "C"]

    def test_deduplicated(self):
        assert _parse_list("a,b,a") == ["A", "B"]

    def test_lower_case_preserved_when_upper_false(self):
        result = _parse_list("foo,bar", upper=False)
        assert result == ["foo", "bar"]

    def test_extra_spaces_trimmed(self):
        assert _parse_list("  A , B , C  ") == ["A", "B", "C"]

    def test_mixed_separators(self):
        assert _parse_list("A,B;C") == ["A", "B", "C"]


# ---------------------------------------------------------------------------
# _load_json
# ---------------------------------------------------------------------------

class TestLoadJson:
    def test_empty(self):
        assert _load_json("") == {}

    def test_none(self):
        assert _load_json(None) == {}

    def test_valid_dict(self):
        assert _load_json('{"a": 1}') == {"a": 1}

    def test_valid_nested(self):
        d = {"x": [1, 2, 3], "y": {"z": True}}
        assert _load_json(json.dumps(d)) == d

    def test_array_returns_empty(self):
        # We only accept dicts — arrays return {}
        assert _load_json('[1,2,3]') == {}

    def test_invalid_json_returns_empty(self):
        assert _load_json("{broken}") == {}


# ---------------------------------------------------------------------------
# _load_json_file
# ---------------------------------------------------------------------------

class TestLoadJsonFile:
    def test_missing_file_returns_none(self, tmp_path: Path):
        assert _load_json_file(str(tmp_path / "nonexistent.json")) is None

    def test_valid_json_file(self, tmp_path: Path):
        f = tmp_path / "data.json"
        f.write_text('{"key": "value"}')
        assert _load_json_file(str(f)) == {"key": "value"}

    def test_array_json_file_returns_none(self, tmp_path: Path):
        f = tmp_path / "arr.json"
        f.write_text('[1, 2]')
        assert _load_json_file(str(f)) is None

    def test_malformed_json_returns_none(self, tmp_path: Path):
        f = tmp_path / "bad.json"
        f.write_text("{broken}")
        assert _load_json_file(str(f)) is None
