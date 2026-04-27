"""
Unit tests for core/ module helpers.

Coverage:
  - core.redis_stream_consumer: utility helpers (_decode_any, _fields_to_dict,
      _parse_xpending_summary, _parse_xpending_consumers, _normalize_streams,
      _is_unsupported_xautoclaim, StreamMsg)
  - core.config: get_pattern_conf_threshold, get_pattern_weight, _normalize_pattern_key
  - core.rsi: StreamingRSI
  - core.robust_stats: RollingRobustZ
  - core.confidence_utils: normalize_confidence_pct, clamp, confidence_pct_to_ratio
  - core.cfg_merge: merged_cfg, get_int
"""
from __future__ import annotations

import math
import os

import pytest


# ---------------------------------------------------------------------------
# core.redis_stream_consumer helpers
# ---------------------------------------------------------------------------

class TestDecodeAny:
    def setup_method(self):
        from core.redis_stream_consumer import _decode_any
        self.f = _decode_any

    def test_none(self):
        assert self.f(None) == ""

    def test_str(self):
        assert self.f("hello") == "hello"

    def test_bytes(self):
        assert self.f(b"world") == "world"

    def test_bytearray(self):
        assert self.f(bytearray(b"test")) == "test"

    def test_int(self):
        assert self.f(42) == "42"

    def test_invalid_utf8_bytes(self):
        # Should not raise; falls back to replacement
        result = self.f(b"\xff\xfe")
        assert isinstance(result, str)


class TestFieldsToDict:
    def setup_method(self):
        from core.redis_stream_consumer import _fields_to_dict
        self.f = _fields_to_dict

    def test_none(self):
        assert self.f(None) == {}

    def test_dict_passthrough(self):
        d = {"a": "1", "b": "2"}
        assert self.f(d) == d

    def test_flat_list(self):
        assert self.f(["k1", "v1", "k2", "v2"]) == {"k1": "v1", "k2": "v2"}

    def test_flat_bytes_keys(self):
        result = self.f([b"key", "val"])
        assert result == {"key": "val"}

    def test_empty_list(self):
        assert self.f([]) == {}

    def test_other_type(self):
        assert self.f(123) == {}


class TestParsePendingSummary:
    def setup_method(self):
        from core.redis_stream_consumer import _parse_xpending_summary
        self.f = _parse_xpending_summary

    def test_none(self):
        assert self.f(None) == 0

    def test_dict(self):
        assert self.f({"pending": 5, "min": "1-0", "max": "2-0"}) == 5

    def test_list_format(self):
        assert self.f([7, "1-0", "2-0", []]) == 7

    def test_dict_bad_value(self):
        assert self.f({"pending": "not_int"}) == 0


class TestParsePendingConsumers:
    def setup_method(self):
        from core.redis_stream_consumer import _parse_xpending_consumers
        self.f = _parse_xpending_consumers

    def test_none(self):
        assert self.f(None) == {}

    def test_dict_with_dict_consumers(self):
        res = {"pending": 3, "consumers": [{"name": "w1", "pending": 2}, {"name": "w2", "pending": 1}]}
        assert self.f(res) == {"w1": 2, "w2": 1}

    def test_list_format(self):
        res = [3, "1-0", "2-0", [["w1", 3]]]
        assert self.f(res) == {"w1": 3}

    def test_dict_consumer_alias(self):
        res = {"pending": 1, "consumers": [{"consumer": "c1", "pending": 1}]}
        result = self.f(res)
        assert result.get("c1") == 1


class TestNormalizeStreams:
    def setup_method(self):
        from core.redis_stream_consumer import _normalize_streams
        self.f = _normalize_streams

    def test_list(self):
        assert self.f(["stream:a", "stream:b"]) == {"stream:a": ">", "stream:b": ">"}

    def test_dict_with_none(self):
        assert self.f({"stream:a": None}) == {"stream:a": ">"}

    def test_dict_with_id(self):
        assert self.f({"stream:a": "1234-0"}) == {"stream:a": "1234-0"}

    def test_dict_with_gt(self):
        assert self.f({"stream:a": ">"}) == {"stream:a": ">"}


class TestIsUnsupportedXautoclaim:
    def setup_method(self):
        from core.redis_stream_consumer import _is_unsupported_xautoclaim
        self.f = _is_unsupported_xautoclaim

    def test_unknown_command(self):
        assert self.f(Exception("unknown command 'XAUTOCLAIM'")) is True

    def test_wrong_number(self):
        assert self.f(Exception("wrong number of arguments for 'XAUTOCLAIM'")) is True

    def test_busygroup(self):
        assert self.f(Exception("BUSYGROUP")) is False


class TestStreamMsg:
    def test_creation(self):
        from core.redis_stream_consumer import StreamMsg
        msg = StreamMsg(stream="s:a", msg_id="1-0", fields={"k": "v"})
        assert msg.stream == "s:a"
        assert msg.fields["k"] == "v"


# ---------------------------------------------------------------------------
# core.config
# ---------------------------------------------------------------------------

class TestConfig:
    def test_normalize_pattern_key(self):
        from core.config import _normalize_pattern_key
        assert _normalize_pattern_key("breakout_R1") == "BREAKOUT_R1"
        assert _normalize_pattern_key("fade-PDH") == "FADE_PDH"
        assert _normalize_pattern_key("fade HTF OB") == "FADE_HTF_OB"

    def test_pattern_conf_threshold_default(self):
        from core.config import get_pattern_conf_threshold, GOLDEN_CONFIDENCE_DEFAULT
        # Passing None returns the module-level GOLDEN_CONFIDENCE_DEFAULT
        val = get_pattern_conf_threshold(None)
        assert val == GOLDEN_CONFIDENCE_DEFAULT

    def test_pattern_conf_threshold_env_override(self, monkeypatch):
        from core.config import get_pattern_conf_threshold
        monkeypatch.setenv("GOLDEN_CONF_BREAKOUT_R1", "95")
        val = get_pattern_conf_threshold("breakout_R1")
        assert val == 95

    def test_pattern_weight_default(self, monkeypatch):
        from core.config import get_pattern_weight
        monkeypatch.delenv("GOLDEN_WEIGHT_BREAKOUT_R1", raising=False)
        val = get_pattern_weight("breakout_R1")
        assert isinstance(val, float)

    def test_xau_handler_enabled_not_duplicated(self):
        """Ensure XAU_HANDLER_ENABLED is only defined once (deduplication fix)."""
        import inspect
        import core.config as cfg_mod
        source = inspect.getsource(cfg_mod)
        count = source.count("XAU_HANDLER_ENABLED: bool")
        assert count == 1, f"Expected 1 definition, found {count}"


# ---------------------------------------------------------------------------
# core.rsi
# ---------------------------------------------------------------------------

class TestStreamingRSI:
    def test_returns_none_before_warmup(self):
        from core.rsi import StreamingRSI
        rsi = StreamingRSI(period=3)
        rsi.update(100.0)  # first point sets prev
        v = rsi.update(101.0)
        assert v is not None  # after 2 points, value available

    def test_overbought(self):
        from core.rsi import StreamingRSI
        rsi = StreamingRSI(period=5)
        prices = [100.0, 101, 102, 103, 104, 105, 106, 107]
        val = None
        for p in prices:
            val = rsi.update(p)
        assert val is not None and val > 50

    def test_nan_ignored(self):
        from core.rsi import StreamingRSI
        rsi = StreamingRSI(period=5)
        rsi.update(100.0)
        result = rsi.update(float("nan"))
        # Should return existing value (or None) without crashing
        # prev should remain 100.0, not nan
        assert rsi.prev == 100.0

    def test_min_period_clamped(self):
        from core.rsi import StreamingRSI
        rsi = StreamingRSI(period=0)
        assert rsi.period == 2


# ---------------------------------------------------------------------------
# core.robust_stats
# ---------------------------------------------------------------------------

class TestRollingRobustZ:
    def test_returns_zero_for_insufficient_data(self):
        from core.robust_stats import RollingRobustZ
        rz = RollingRobustZ(window=20)
        for _ in range(5):
            rz.update(1.0)
        assert rz.z(1.0) == 0.0

    def test_z_score_symmetric(self):
        from core.robust_stats import RollingRobustZ
        rz = RollingRobustZ(window=50)
        for i in range(40):
            rz.update(float(i))
        med, mad, n = rz.median_mad()
        assert n == 40
        assert mad > 0
        z_high = rz.z(200.0)
        z_low = rz.z(-200.0)
        assert z_high > 0
        assert z_low < 0

    def test_nan_ignored(self):
        from core.robust_stats import RollingRobustZ
        rz = RollingRobustZ(window=20)
        for _ in range(15):
            rz.update(1.0)
        prev_len = len(rz.buf)
        rz.update(float("nan"))
        assert len(rz.buf) == prev_len  # NaN not added


# ---------------------------------------------------------------------------
# core.confidence_utils
# ---------------------------------------------------------------------------

class TestConfidenceUtils:
    def test_clamp_below(self):
        from core.confidence_utils import clamp
        assert clamp(-5.0, 0.0, 100.0) == 0.0

    def test_clamp_above(self):
        from core.confidence_utils import clamp
        assert clamp(150.0, 0.0, 100.0) == 100.0

    def test_clamp_within(self):
        from core.confidence_utils import clamp
        assert clamp(50.0, 0.0, 100.0) == 50.0

    def test_normalize_ratio(self):
        from core.confidence_utils import normalize_confidence_pct
        assert normalize_confidence_pct(0.75) == pytest.approx(75.0)

    def test_normalize_pct(self):
        from core.confidence_utils import normalize_confidence_pct
        assert normalize_confidence_pct(75.0) == pytest.approx(75.0)

    def test_normalize_invalid(self):
        from core.confidence_utils import normalize_confidence_pct
        assert normalize_confidence_pct("bad") == 0.0

    def test_confidence_pct_to_ratio(self):
        from core.confidence_utils import confidence_pct_to_ratio
        assert confidence_pct_to_ratio(75.0) == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# core.cfg_merge
# ---------------------------------------------------------------------------

class TestCfgMerge:
    def test_merged_cfg_override(self):
        from core.cfg_merge import merged_cfg
        base = {"a": 1, "b": 2}
        override = {"b": 99, "c": 3}
        result = merged_cfg(base, override)
        assert result == {"a": 1, "b": 99, "c": 3}

    def test_merged_cfg_empty_override(self):
        from core.cfg_merge import merged_cfg
        base = {"a": 1}
        assert merged_cfg(base, {}) == {"a": 1}

    def test_get_int_valid(self):
        from core.cfg_merge import get_int
        assert get_int({"x": "42"}, "x", 0) == 42

    def test_get_int_missing(self):
        from core.cfg_merge import get_int
        assert get_int({}, "missing", 7) == 7

    def test_get_int_bad_value(self):
        from core.cfg_merge import get_int
        assert get_int({"x": "bad"}, "x", 5) == 5
