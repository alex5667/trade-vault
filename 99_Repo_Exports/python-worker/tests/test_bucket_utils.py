"""Tests for bucket_utils module."""

from core.bucket_utils import bucket_from_scenario


def test_bucket_range():
    """Test range bucket classification."""
    assert bucket_from_scenario("range_meanrev") == "range"
    assert bucket_from_scenario("range") == "range"
    assert bucket_from_scenario("chop") == "range"
    assert bucket_from_scenario("meanrev") == "range"


def test_bucket_trend():
    """Test trend bucket classification."""
    assert bucket_from_scenario("continuation") == "trend"
    assert bucket_from_scenario("trend") == "trend"
    assert bucket_from_scenario("bull") == "trend"
    assert bucket_from_scenario("bear") == "trend"
    assert bucket_from_scenario("cont") == "trend"


def test_bucket_reversal():
    """Test reversal scenarios default to trend."""
    assert bucket_from_scenario("reversal") == "trend"
    assert bucket_from_scenario("rev") == "trend"


def test_bucket_other():
    """Test unknown scenarios default to other."""
    assert bucket_from_scenario("none") == "other"
    assert bucket_from_scenario("unknown") == "other"
    assert bucket_from_scenario("") == "other"
    assert bucket_from_scenario(None) == "other"


def test_bucket_case_insensitive():
    """Test case-insensitive matching."""
    assert bucket_from_scenario("RANGE_MEANREV") == "range"
    assert bucket_from_scenario("CONTINUATION") == "trend"
    assert bucket_from_scenario("Reversal") == "trend"

