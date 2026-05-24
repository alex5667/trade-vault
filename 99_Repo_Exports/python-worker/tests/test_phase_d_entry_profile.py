"""Phase D regression tests: EntryProfileClassifier.

Покрытие:
  - порядок приоритета: adverse > news_shock > trend > expansion > range > unknown;
  - конфидентность растёт с smt_coh и og_score;
  - не падает на отсутствующих/мусорных полях ctx;
  - всегда возвращает корректную метку (никаких None).
"""

from __future__ import annotations

import pytest

from services.entry_profile_classifier import (
    EntryProfile,
    EntryProfileResult,
    classify_entry_profile,
)


def _ctx(**overrides):
    base = {
        "kind": "breakout",
        "vol_regime": "shock",
        "trend_regime": "trending",
        "side": "LONG",
        "og_score": 0.6,
        "smt_coh": 0.7,
        "news_shock": False,
        "adverse_cross": False,
    }
    base.update(overrides)
    return base


def test_adverse_cross_wins_over_everything():
    res = classify_entry_profile(_ctx(adverse_cross=True, news_shock=True))
    assert res.profile == EntryProfile.NO_TRADE_ADVERSE
    assert res.confidence == 1.0


def test_news_shock_protective_when_no_adverse():
    res = classify_entry_profile(_ctx(adverse_cross=False, news_shock=True))
    assert res.profile == EntryProfile.NEWS_SHOCK_PROTECTIVE
    assert 0.8 <= res.confidence <= 1.0


def test_trend_continuation_with_high_smt_coh():
    res = classify_entry_profile(_ctx(trend_regime="trending", smt_coh=0.8, og_score=0.7))
    assert res.profile == EntryProfile.TREND_CONTINUATION
    # Конфидентность должна быть высокой (smt_coh + og bonus).
    assert res.confidence > 0.85
    assert any("smt_coh" in r for r in res.reasons)


def test_trend_continuation_blocked_by_low_smt_coh_falls_to_expansion():
    """trending + shock + low smt_coh → не TREND_CONTINUATION, попадает в EXPANSION."""
    res = classify_entry_profile(_ctx(trend_regime="trending", vol_regime="shock", smt_coh=0.1))
    assert res.profile == EntryProfile.EXPANSION_BREAKOUT


def test_expansion_breakout_label():
    res = classify_entry_profile(_ctx(trend_regime="expansion", vol_regime="normal", smt_coh=0.0))
    assert res.profile == EntryProfile.EXPANSION_BREAKOUT


def test_range_scalp_calm_reclaim():
    res = classify_entry_profile(_ctx(
        kind="reclaim", vol_regime="calm", trend_regime="range",
        smt_coh=0.0, og_score=0.0,
    ))
    assert res.profile == EntryProfile.REVERSAL_RANGE_SCALP


def test_range_scalp_requires_supported_kind():
    res = classify_entry_profile(_ctx(
        kind="custom", vol_regime="calm", trend_regime="range",
        smt_coh=0.0,
    ))
    # custom kind НЕ попадает в scalp list.
    assert res.profile == EntryProfile.UNKNOWN


def test_unknown_fallback_for_mixed_regime():
    res = classify_entry_profile(_ctx(
        trend_regime="mixed", vol_regime="normal", smt_coh=0.0,
    ))
    assert res.profile == EntryProfile.UNKNOWN
    assert res.confidence < 0.5


def test_missing_fields_no_crash():
    """Все поля могут быть None / отсутствовать."""
    res = classify_entry_profile({})
    assert isinstance(res, EntryProfileResult)
    assert res.profile == EntryProfile.UNKNOWN


def test_string_smt_coh_parsed():
    res = classify_entry_profile(_ctx(smt_coh="0.8"))
    assert res.profile == EntryProfile.TREND_CONTINUATION


def test_bad_string_smt_coh_does_not_crash():
    res = classify_entry_profile(_ctx(trend_regime="trending", smt_coh="not-a-number"))
    # smt_coh→0.0 → не проходит threshold → не trend_continuation
    assert res.profile != EntryProfile.TREND_CONTINUATION


@pytest.mark.parametrize("trend", ["trending", "trending_bear"])
def test_both_trend_directions_accepted(trend):
    res = classify_entry_profile(_ctx(trend_regime=trend, smt_coh=0.6))
    assert res.profile == EntryProfile.TREND_CONTINUATION
