"""Unit tests for Telegram compact confirmations formatter.

Tests cover:
- CVD reclaim ratio extraction from indicators and confirmations
- OBI stability with quality score
- Footprint evidence
- Weak progress indicators
- Fail-open behavior with missing fields
"""

from core.telegram_confirmations import build_compact_confirmations


def test_telegram_compact_contains_cvdR_and_obi():
    """Test that CVD reclaim and OBI stability are formatted correctly."""
    indicators = {
        "obi_stable_secs": 2.5,
        "obi_stability_score": 0.92,
        "weak_range_atr": 0.27
    }
    confirmations = ["cvdR=1.43", "fp_absorb=1.20"]
    s = build_compact_confirmations(indicators=indicators, confirmations=confirmations)

    assert "obi=2.5s" in s, f"Expected OBI duration in output: {s}"
    assert "q=0.92" in s, f"Expected OBI quality in output: {s}"
    assert "cvdR=1.43" in s, f"Expected CVD reclaim ratio in output: {s}"
    # Note: fp_absorb is not in default formatter, but could be added


def test_telegram_cvdR_from_indicators():
    """Test CVD reclaim extraction from indicators dict."""
    indicators = {
        "cvd_reclaim_ratio": 1.67,
        "cvd_reclaim_ok": 1
    }
    s = build_compact_confirmations(indicators=indicators, confirmations=[])

    assert "cvdR=1.67" in s, f"Expected cvdR from indicators: {s}"


def test_telegram_cvdR_from_confirmations():
    """Test CVD reclaim extraction from confirmations list."""
    confirmations = ["cvdR=1.25", "reclaim"]
    s = build_compact_confirmations(indicators={}, confirmations=confirmations)

    assert "cvdR=1.25" in s, f"Expected cvdR from confirmations: {s}"


def test_telegram_obi_without_quality():
    """Test OBI formatting when quality score is missing (legacy)."""
    indicators = {"obi_stable_secs": 3.2}
    s = build_compact_confirmations(indicators=indicators, confirmations=[])

    assert "obi=3.2s" in s, f"Expected OBI without quality: {s}"
    assert "q=" not in s, f"Should not show quality when missing: {s}"


def test_telegram_weak_progress():
    """Test weak progress formatting."""
    indicators = {
        "weak_range_atr": 0.35,
        "weak_eff": 0.02
    }
    s = build_compact_confirmations(indicators=indicators, confirmations=[])

    assert "weakP=0.35" in s, f"Expected weak progress ratio: {s}"


def test_telegram_empty_inputs():
    """Test fail-open behavior with empty inputs."""
    s = build_compact_confirmations(indicators=None, confirmations=None)

    # Should return empty string or minimal output without crashing
    assert isinstance(s, str), "Expected string output"


def test_telegram_reclaim_flag():
    """Test reclaim boolean flag formatting."""
    indicators = {"reclaim": 1}
    s = build_compact_confirmations(indicators=indicators, confirmations=[])

    assert "reclaim" in s, f"Expected reclaim flag: {s}"


def test_telegram_iceberg_strict():
    """Test iceberg strict flag formatting."""
    indicators = {"iceberg_strict": 1}
    s = build_compact_confirmations(indicators=indicators, confirmations=[])

    assert "ice" in s, f"Expected iceberg flag: {s}"


def test_telegram_fp_edge_with_strength():
    """Test footprint edge absorb with strength value."""
    indicators = {
        "fp_edge_absorb": 1,
        "fp_edge_absorb_strength": 1.42
    }
    s = build_compact_confirmations(indicators=indicators, confirmations=[])

    assert "fp=" in s, f"Expected footprint evidence: {s}"
    assert "1.42" in s, f"Expected strength value: {s}"


def test_telegram_combined_evidence():
    """Test comprehensive evidence string with multiple confirmations."""
    indicators = {
        "reclaim": 1,
        "obi_stable_secs": 2.1,
        "obi_stability_score": 0.88,
        "cvd_reclaim_ratio": 1.52,
        "iceberg_strict": 1,
        "fp_edge_absorb": 1,
        "fp_edge_absorb_strength": 1.18,
        "weak_range_atr": 0.27
    }
    confirmations = []
    s = build_compact_confirmations(indicators=indicators, confirmations=confirmations)

    # Should contain multiple pieces of evidence
    assert "reclaim" in s, f"Missing reclaim: {s}"
    assert "obi=" in s, f"Missing OBI: {s}"
    assert "cvdR=" in s, f"Missing CVD reclaim: {s}"
    assert "ice" in s, f"Missing iceberg: {s}"
    assert "fp=" in s, f"Missing footprint: {s}"
    assert "weakP=" in s, f"Missing weak progress: {s}"


def test_telegram_invalid_values():
    """Test robustness with invalid/malformed values."""
    indicators = {
        "obi_stable_secs": "invalid",
        "cvd_reclaim_ratio": None,
        "weak_range_atr": "not_a_number"
    }
    confirmations = ["cvdR=bad_value", "malformed"]

    # Should not crash
    s = build_compact_confirmations(indicators=indicators, confirmations=confirmations)
    assert isinstance(s, str), "Expected string output even with invalid data"
