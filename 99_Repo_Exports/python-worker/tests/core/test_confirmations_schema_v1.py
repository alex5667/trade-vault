"""Tests for new v1 API added to confirmations_schema_v1.py (Commit 1).

Covers:
  - ConfirmationSignalV1 dataclass
  - _ALIASES canonicalization
  - parse_confirmations_v1: all edge cases
  - confirmations_to_indicator_keys_v1: key structure
  - apply_confirmations_to_indicators: in-place mutation, no-overwrite guard
  - summarize_confirmations_v1
  - Never-raise guarantee on garbage input
"""
import pytest

from core.confirmations_schema_v1 import (
    _CANONICAL_KEYS_11,
    # Legacy API must still work
    CONF_KEYS_V1,
    # New v1 API
    ConfirmationSignalV1,
    apply_confirmations_to_indicators,
    confirmations_to_indicator_keys_v1,
    extract_confirmation_flags,
    parse_confirmations_list,
    parse_confirmations_v1,
    summarize_confirmations_v1,
)


class TestConfirmationSignalV1:
    def test_frozen_dataclass(self):
        c = ConfirmationSignalV1(key="rsi_agree", value=1.0)
        with pytest.raises((AttributeError, TypeError)):
            c.key = "other"  # type: ignore[misc]

    def test_fields(self):
        c = ConfirmationSignalV1(key="sweep", value=0.5)
        assert c.key == "sweep"
        assert c.value == 0.5


class TestAliases:
    def test_fp_edge_alias(self):
        parsed = parse_confirmations_v1(["fp_edge=1"])
        assert len(parsed) == 1
        assert parsed[0].key == "fp_edge_absorb"

    def test_iceberg_alias(self):
        parsed = parse_confirmations_v1(["iceberg=1"])
        assert parsed[0].key == "iceberg_strict"

    def test_absorb_alias(self):
        parsed = parse_confirmations_v1(["absorb=1"])
        assert parsed[0].key == "absorption"

    def test_abs_level_ok_alias(self):
        parsed = parse_confirmations_v1(["abs_level_ok=1"])
        assert parsed[0].key == "abs_lvl_ok"


class TestParseConfirmationsV1:
    def test_basic_kv(self):
        parsed = parse_confirmations_v1(["rsi_agree=1"])
        assert len(parsed) == 1
        assert parsed[0].key == "rsi_agree"
        assert parsed[0].value == 1.0

    def test_bare_key_treated_as_one(self):
        parsed = parse_confirmations_v1(["sweep"])
        assert parsed[0].value == 1.0

    def test_empty_string_skipped(self):
        parsed = parse_confirmations_v1(["", "  "])
        assert len(parsed) == 0

    def test_none_input_returns_empty(self):
        parsed = parse_confirmations_v1(None)  # type: ignore[arg-type]
        assert parsed == []

    def test_numeric_value(self):
        parsed = parse_confirmations_v1(["absorption=123.45"])
        assert parsed[0].value == pytest.approx(123.45)

    def test_boolean_true_tokens(self):
        for tok in ("true", "yes", "y", "ok"):
            parsed = parse_confirmations_v1([f"rsi_agree={tok}"])
            assert parsed[0].value == 1.0, f"failed for {tok}"

    def test_unknown_bool_token_defaults_zero(self):
        parsed = parse_confirmations_v1(["rsi_agree=garbage"])
        assert parsed[0].value == 0.0

    def test_all_11_canonical_keys(self):
        items = [f"{k}=1" for k in _CANONICAL_KEYS_11]
        parsed = parse_confirmations_v1(items)
        keys = {c.key for c in parsed}
        assert keys == _CANONICAL_KEYS_11

    def test_garbage_entry_yields_no_crash(self):
        # Must not raise
        parsed = parse_confirmations_v1(["!!!", "=value", "key=", "rsi_agree=1"])
        # rsi_agree=1 must appear even amid garbage
        assert any(c.key == "rsi_agree" for c in parsed)


class TestConfirmationsToIndicatorKeys:
    def test_conf_prefix_always_written(self):
        parsed = parse_confirmations_v1(["rsi_agree=1"])
        ind = confirmations_to_indicator_keys_v1(parsed)
        assert "conf:rsi_agree" in ind
        assert ind["conf:rsi_agree"] == 1.0

    def test_bool_key_written_for_canonical(self):
        parsed = parse_confirmations_v1(["rsi_agree=1"])
        ind = confirmations_to_indicator_keys_v1(parsed)
        assert "b:rsi_agree" in ind
        assert ind["b:rsi_agree"] == 1.0

    def test_raw_key_written_for_canonical(self):
        parsed = parse_confirmations_v1(["absorption=1"])
        ind = confirmations_to_indicator_keys_v1(parsed)
        assert "absorption" in ind
        assert ind["absorption"] == 1.0

    def test_zero_value_gives_zero_binary(self):
        parsed = parse_confirmations_v1(["rsi_agree=0"])
        ind = confirmations_to_indicator_keys_v1(parsed)
        assert ind["b:rsi_agree"] == 0.0
        assert ind["rsi_agree"] == 0.0

    def test_non_canonical_key_only_gets_conf_prefix(self):
        parsed = parse_confirmations_v1(["some_custom_signal=1"])
        ind = confirmations_to_indicator_keys_v1(parsed)
        assert "conf:some_custom_signal" in ind
        assert "b:some_custom_signal" not in ind


class TestApplyConfirmationsToIndicators:
    def test_in_place_mutation(self):
        indicators: dict = {}
        result = apply_confirmations_to_indicators(
            confirmations=["rsi_agree=1"],
            indicators=indicators,
            also_write_raw_keys=True,
        )
        assert result is indicators  # Same object
        assert "rsi_agree" in indicators

    def test_no_overwrite(self):
        """apply_confirmations_to_indicators must not overwrite existing keys."""
        indicators: dict = {"rsi_agree": 99.0}
        apply_confirmations_to_indicators(
            confirmations=["rsi_agree=1"],
            indicators=indicators,
            also_write_raw_keys=True,
        )
        assert indicators["rsi_agree"] == 99.0  # not overwritten

    def test_also_write_raw_keys_false(self):
        indicators: dict = {}
        apply_confirmations_to_indicators(
            confirmations=["rsi_agree=1"],
            indicators=indicators,
            also_write_raw_keys=False,
        )
        # conf:<key> and b:<key> should still be written
        assert "conf:rsi_agree" in indicators
        assert "b:rsi_agree" in indicators
        # raw key should NOT be written when also_write_raw_keys=False
        assert "rsi_agree" not in indicators

    def test_never_raises_on_garbage(self):
        indicators: dict = {"x": 1}
        # Must not raise regardless of input
        apply_confirmations_to_indicators(
            confirmations=["!!!", None, 42],  # type: ignore[list-item]
            indicators=indicators,
            also_write_raw_keys=True,
        )
        assert indicators["x"] == 1  # Existing data intact

    def test_empty_confirmations_noop(self):
        indicators: dict = {"pre": 1}
        apply_confirmations_to_indicators(
            confirmations=[],
            indicators=indicators,
        )
        assert indicators == {"pre": 1}


class TestSummarizeConfirmationsV1:
    def test_count_and_keys(self):
        count, keys = summarize_confirmations_v1(["rsi_agree=1", "sweep=1", "sweep=0"])
        # Both sweeps parse to the same key → 2 items but 1 unique key
        assert count == 3
        assert "rsi_agree" in keys
        assert "sweep" in keys

    def test_empty(self):
        count, keys = summarize_confirmations_v1([])
        assert count == 0
        assert keys == []


class TestLegacyAPIUnchanged:
    """Ensure old 4-key API still works unchanged."""

    def test_conf_keys_v1_has_4_items(self):
        assert len(CONF_KEYS_V1) == 4

    def test_parse_confirmations_list_basic(self):
        result = parse_confirmations_list(["rsi_agree=1", "div_match=1"])
        assert result["rsi_agree"] == 1
        assert result["div_match"] == 1

    def test_extract_confirmation_flags_basic(self):
        result = extract_confirmation_flags(confirmations=["rsi_agree=1"])
        assert result["rsi_agree"] == 1
