"""Unit tests for common.market_mode (canonical regime normaliser)."""

import pytest

from common.market_mode import (
    REGIME_MIXED,
    REGIME_RANGE,
    REGIME_TREND,
    REGIME_UNKNOWN,
    is_range_regime,
    is_trend_regime,
    normalize_regime,
)


class TestNormalizeRegime:
    """Exhaustive alias → canonical mapping."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            # ── range family ──
            ("range", REGIME_RANGE),
            ("ranging", REGIME_RANGE),
            ("meanrev", REGIME_RANGE),
            ("mean_reversion", REGIME_RANGE),
            ("mr", REGIME_RANGE),
            ("chop", REGIME_RANGE),
            ("sideways", REGIME_RANGE),
            ("range_bound", REGIME_RANGE),
            ("range_bullish", REGIME_RANGE),
            ("range_bearish", REGIME_RANGE),
            ("RANGE", REGIME_RANGE),
            ("  Ranging  ", REGIME_RANGE),
            ("MEANREV", REGIME_RANGE),
            # ── squeeze → range ──
            ("squeeze", REGIME_RANGE),
            ("squeeze_bullish", REGIME_RANGE),
            ("squeeze_bearish", REGIME_RANGE),
            # ── trend family ──
            ("trend", REGIME_TREND),
            ("trending", REGIME_TREND),
            ("momentum", REGIME_TREND),
            ("breakout", REGIME_TREND),
            ("TREND", REGIME_TREND),
            # ── directional trend (preserved) ──
            ("trending_bull", "trending_bull"),
            ("trending_bear", "trending_bear"),
            ("expansion_bull", "expansion_bull"),
            ("expansion_bear", "expansion_bear"),
            # ── mixed / unknown ──
            ("mixed", REGIME_MIXED),
            ("unknown", REGIME_UNKNOWN),
            ("", REGIME_UNKNOWN),
            ("na", REGIME_UNKNOWN),
            ("none", REGIME_UNKNOWN),
            ("random_string_xyz", REGIME_UNKNOWN),
        ],
    )
    def test_normalize(self, raw: str, expected: str) -> None:
        assert normalize_regime(raw) == expected


class TestIsRangeRegime:
    @pytest.mark.parametrize("raw", ["range", "ranging", "meanrev", "mean_reversion", "mr", "chop", "sideways", "squeeze"])
    def test_true_cases(self, raw: str) -> None:
        assert is_range_regime(raw) is True

    @pytest.mark.parametrize("raw", ["trend", "trending_bull", "mixed", "unknown", ""])
    def test_false_cases(self, raw: str) -> None:
        assert is_range_regime(raw) is False


class TestIsTrendRegime:
    @pytest.mark.parametrize("raw", ["trend", "trending", "momentum", "trending_bull", "trending_bear"])
    def test_true_cases(self, raw: str) -> None:
        assert is_trend_regime(raw) is True

    @pytest.mark.parametrize("raw", ["range", "mixed", "unknown", "meanrev", ""])
    def test_false_cases(self, raw: str) -> None:
        assert is_trend_regime(raw) is False
