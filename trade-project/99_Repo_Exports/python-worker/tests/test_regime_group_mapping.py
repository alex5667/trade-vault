"""
tests/test_regime_group_mapping.py
===================================
Проверка нового маппинга режимов в regime_group().
Оба источника (abc_router и entry_policy_ab_gate) должны быть синхронизированы.
"""
from __future__ import annotations

import pytest

from services.abc_router import regime_group as rg_abc
from services.entry_policy_ab_gate import regime_group as rg_gate


# ---------------------------------------------------------------------------
# Параметризованные проверки для ОБОИХ источников
# ---------------------------------------------------------------------------

IMPLEMENTATIONS = [rg_abc, rg_gate]
IDS = ["abc_router", "entry_policy_ab_gate"]


@pytest.mark.parametrize("fn", IMPLEMENTATIONS, ids=IDS)
class TestRegimeGroup:
    """Проверяем оба impl на одинаковое поведение."""

    # --- thin ---
    def test_thin(self, fn):
        assert fn("thin") == "thin"

    def test_news(self, fn):
        assert fn("news") == "thin"

    def test_illiquid(self, fn):
        assert fn("illiquid") == "thin"

    # --- trend ---
    def test_trend(self, fn):
        assert fn("trend") == "trend"

    def test_trending(self, fn):
        assert fn("trending") == "trend"

    def test_trending_bull(self, fn):
        assert fn("trending_bull") == "trend"

    def test_trending_bear(self, fn):
        assert fn("trending_bear") == "trend"

    def test_momentum(self, fn):
        assert fn("momentum") == "trend"

    def test_expansion(self, fn):
        assert fn("expansion") == "trend"

    # --- range ---
    def test_range(self, fn):
        assert fn("range") == "range"

    def test_chop(self, fn):
        assert fn("chop") == "range"

    def test_meanrev(self, fn):
        assert fn("meanrev") == "range"

    def test_sideways(self, fn):
        assert fn("sideways") == "range"

    # --- mixed / fallback ---
    def test_unknown(self, fn):
        assert fn("unknown") == "mixed"

    def test_empty_string(self, fn):
        assert fn("") == "mixed"

    def test_na(self, fn):
        assert fn("na") == "mixed"

    def test_high_vol(self, fn):
        # high_vol не в явном списке → mixed
        assert fn("high_vol") == "mixed"

    # --- case insensitivity ---
    def test_uppercase_thin(self, fn):
        assert fn("THIN") == "thin"

    def test_uppercase_trend(self, fn):
        assert fn("TRENDING_BULL") == "trend"

    def test_mixed_case_range(self, fn):
        assert fn("Range") == "range"

    # --- whitespace ---
    def test_leading_space(self, fn):
        assert fn("  trend  ") == "trend"


# ---------------------------------------------------------------------------
# Симметрия: оба impl дают одинаковый результат
# ---------------------------------------------------------------------------

REGIMES = [
    "thin", "news", "illiquid",
    "trend", "trending", "trending_bull", "trending_bear", "momentum", "expansion",
    "range", "chop", "meanrev", "sideways",
    "unknown", "", "na", "high_vol", "BTCUSDT", "volatility",
]


@pytest.mark.parametrize("regime", REGIMES)
def test_both_impls_symmetric(regime: str):
    """abc_router и entry_policy_ab_gate должны давать одинаковый результат."""
    assert rg_abc(regime) == rg_gate(regime), \
        f"Несоответствие для '{regime}': abc_router={rg_abc(regime)!r}, gate={rg_gate(regime)!r}"
