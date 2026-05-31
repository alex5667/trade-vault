"""Plan 3 / Step 1 — entry_px explicit contract tests."""
from __future__ import annotations

from core.triple_barrier import pick_entry_price_v2


def test_prefers_explicit_when_positive():
    px, reason = pick_entry_price_v2(
        entry_px_expected=100.5,
        path=[(1000, 99.0), (2000, 101.0)],
    )
    assert px == 100.5
    assert reason == ""


def test_falls_back_to_first_tick_when_explicit_zero():
    flags: list[str] = []
    px, reason = pick_entry_price_v2(
        entry_px_expected=0.0,
        path=[(1000, 99.5), (2000, 101.0)],
        reason_flags=flags,
    )
    assert px == 99.5
    assert reason == "entry_px_fallback_first_tick"
    assert "entry_px_fallback_first_tick" in flags


def test_falls_back_when_explicit_none():
    px, reason = pick_entry_price_v2(
        entry_px_expected=None,
        path=[(1000, 50.0)],
    )
    assert px == 50.0
    assert reason == "entry_px_fallback_first_tick"


def test_falls_back_when_explicit_negative():
    px, reason = pick_entry_price_v2(
        entry_px_expected=-1.0,
        path=[(1000, 42.0)],
    )
    assert px == 42.0
    assert reason == "entry_px_fallback_first_tick"


def test_falls_back_when_explicit_string_garbage():
    px, reason = pick_entry_price_v2(
        entry_px_expected="not_a_number",
        path=[(1000, 7.0)],
    )
    assert px == 7.0
    assert reason == "entry_px_fallback_first_tick"


def test_no_path_no_explicit_returns_zero_with_reason():
    flags: list[str] = []
    px, reason = pick_entry_price_v2(
        entry_px_expected=0.0,
        path=[],
        reason_flags=flags,
    )
    assert px == 0.0
    assert reason == "entry_px_fallback_no_path"
    assert "entry_px_fallback_no_path" in flags


def test_explicit_preferred_over_path_even_if_path_present():
    """Sanity: never silently override explicit with tick."""
    px, reason = pick_entry_price_v2(
        entry_px_expected=200.0,
        path=[(1000, 99.0)],
    )
    assert px == 200.0
    assert reason == ""


def test_reason_flags_optional():
    """Caller may omit reason_flags list — function still returns (px, reason)."""
    px, reason = pick_entry_price_v2(
        entry_px_expected=0.0,
        path=[(1, 1.0)],
    )
    assert px == 1.0
    assert reason == "entry_px_fallback_first_tick"


def test_reason_flags_accumulate_across_calls():
    flags: list[str] = ["prior_flag"]
    pick_entry_price_v2(
        entry_px_expected=0.0,
        path=[(1, 1.0)],
        reason_flags=flags,
    )
    pick_entry_price_v2(
        entry_px_expected=0.0,
        path=[],
        reason_flags=flags,
    )
    assert flags == [
        "prior_flag",
        "entry_px_fallback_first_tick",
        "entry_px_fallback_no_path",
    ]
