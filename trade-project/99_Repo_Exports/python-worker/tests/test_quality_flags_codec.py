from __future__ import annotations

"""Contract tests for common.quality_flags_codec.

Covers the two legacy wire formats (Go comma-separated + Python JSON list)
plus defensive edge cases.
"""


import json

import pytest

from common.quality_flags_codec import (
    decode_quality_flags,
    encode_quality_flags,
    is_clean,
)


class TestDecodeGoStyle:
    def test_ok_sentinel_is_empty(self):
        assert decode_quality_flags("ok") == []

    def test_empty_string(self):
        assert decode_quality_flags("") == []

    def test_whitespace(self):
        assert decode_quality_flags("   ") == []

    def test_single_flag(self):
        assert decode_quality_flags("ts_fallback") == ["ts_fallback"]

    def test_comma_multi(self):
        got = decode_quality_flags("ok,tick_gap,ts_fallback")
        assert got == ["tick_gap", "ts_fallback"]

    def test_case_insensitive(self):
        got = decode_quality_flags("OK,TS_Fallback")
        assert got == ["ts_fallback"]


class TestDecodeJsonStyle:
    def test_empty_list(self):
        assert decode_quality_flags("[]") == []

    def test_single(self):
        assert decode_quality_flags('["hlc_fallback"]') == ["hlc_fallback"]

    def test_dedupe_and_sort(self):
        got = decode_quality_flags('["b","a","b","c"]')
        assert got == ["a", "b", "c"]

    def test_dict_truthy_keys(self):
        got = decode_quality_flags('{"a":true,"b":false,"c":1}')
        assert got == ["a", "c"]


class TestDecodeNativeTypes:
    def test_none(self):
        assert decode_quality_flags(None) == []

    def test_list(self):
        assert decode_quality_flags(["a", "b", "a"]) == ["a", "b"]

    def test_tuple(self):
        assert decode_quality_flags(("x", "y")) == ["x", "y"]

    def test_set(self):
        # sets are unordered input, codec sorts output
        assert decode_quality_flags({"z", "a"}) == ["a", "z"]

    def test_bytes(self):
        assert decode_quality_flags(b"ok,foo") == ["foo"]


class TestDefensive:
    def test_malformed_json_falls_back_to_csv(self):
        # Broken JSON → falls through to CSV path
        assert decode_quality_flags("[broken") == ["[broken"]

    def test_non_string_non_collection(self):
        assert decode_quality_flags(42) == []

    def test_nested_unhashable_ignored(self):
        # dict inside list → str() of the dict is hashable; we accept it
        got = decode_quality_flags([{"k": 1}, "a"])
        assert "a" in got


class TestEncode:
    def test_roundtrip_go_form(self):
        enc = encode_quality_flags(["b", "a", "b"])
        assert enc == '["a","b"]'
        assert decode_quality_flags(enc) == ["a", "b"]

    def test_encode_empty(self):
        assert encode_quality_flags([]) == "[]"

    def test_encode_filters_ok_sentinel(self):
        assert encode_quality_flags(["ok", "foo"]) == '["foo"]'

    def test_roundtrip_json(self):
        enc = encode_quality_flags(["ts_fallback", "hlc_fallback"])
        assert json.loads(enc) == ["hlc_fallback", "ts_fallback"]


class TestIsClean:
    @pytest.mark.parametrize(
        "raw",
        ["ok", "", None, "[]", [], "ok,ok", "OK"],
    )
    def test_clean(self, raw):
        assert is_clean(raw) is True

    @pytest.mark.parametrize(
        "raw",
        ["ts_fallback", '["foo"]', ["bar"], "ok,ts_fallback"],
    )
    def test_not_clean(self, raw):
        assert is_clean(raw) is False
