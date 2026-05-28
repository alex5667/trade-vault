"""
WR stop-bleed 2026-05-27 — regression for regime fallback в _normalize_signal.

Before fix: virtual reports показывали regime=unknown в 99% сделок →
trail_profile=range_protective везде → mgmt edge мёртв.

Root cause: `_normalize_signal` читал только `data.regime / data.entry_regime
/ data.regime_bucket`, но publish_of_inputs пишет regime в `indicators.regime`
(top-level может быть None для kinds с late regime resolution).

Fix: extracted helper `resolve_regime_from_payload` with indicators.regime
fallback. Same priority chain as create_position (L3184).
"""
from __future__ import annotations

from services.trade_monitor._monolith import resolve_regime_from_payload


def test_regime_top_level_takes_priority():
    """Если data.regime есть — игнорируем indicators."""
    assert resolve_regime_from_payload(
        {"regime": "expansion", "indicators": {"regime": "range"}}
    ) == "expansion"


def test_regime_entry_regime_top_level_priority():
    """data.entry_regime — тоже top-level."""
    assert resolve_regime_from_payload(
        {"entry_regime": "high_vol", "indicators": {"regime": "range"}}
    ) == "high_vol"


def test_regime_falls_back_to_indicators_when_top_missing():
    """data.regime отсутствует, но indicators.regime='expansion'."""
    assert resolve_regime_from_payload(
        {"indicators": {"regime": "expansion"}}
    ) == "expansion"


def test_regime_falls_back_to_indicators_when_top_unknown():
    """data.regime='unknown' (self-fallback значение) — смотрим indicators."""
    assert resolve_regime_from_payload(
        {"regime": "unknown", "indicators": {"regime": "expansion"}}
    ) == "expansion"


def test_regime_falls_back_to_indicators_when_top_na():
    """data.regime='na' — частое значение из orchestrator default."""
    assert resolve_regime_from_payload(
        {"regime": "na", "indicators": {"regime": "squeeze"}}
    ) == "squeeze"


def test_regime_falls_back_to_indicators_when_top_none_string():
    """data.regime='none' (строка) — тоже считаем как отсутствующее."""
    assert resolve_regime_from_payload(
        {"regime": "none", "indicators": {"regime": "trend"}}
    ) == "trend"


def test_regime_unknown_when_both_missing():
    """Нет ни top-level, ни indicators → unknown."""
    assert resolve_regime_from_payload({}) == "unknown"


def test_regime_unknown_when_indicators_also_unknown():
    """indicators.regime='unknown' — тоже как отсутствующее."""
    assert resolve_regime_from_payload(
        {"indicators": {"regime": "unknown"}}
    ) == "unknown"


def test_regime_unknown_when_indicators_not_dict():
    """indicators=None/list/string — fail-open."""
    assert resolve_regime_from_payload({"indicators": None}) == "unknown"
    assert resolve_regime_from_payload({"indicators": []}) == "unknown"
    assert resolve_regime_from_payload({"indicators": "range"}) == "unknown"


def test_regime_handles_non_dict_input():
    """Defensive: non-dict data → unknown."""
    assert resolve_regime_from_payload(None) == "unknown"  # type: ignore[arg-type]
    assert resolve_regime_from_payload("range") == "unknown"  # type: ignore[arg-type]


def test_regime_lowercases_and_strips():
    """ ' EXPANSION ' → 'expansion'."""
    assert resolve_regime_from_payload({"regime": " EXPANSION "}) == "expansion"


def test_regime_bucket_field_also_supported():
    """data.regime_bucket — третий top-level alias."""
    assert resolve_regime_from_payload({"regime_bucket": "high_vol"}) == "high_vol"
