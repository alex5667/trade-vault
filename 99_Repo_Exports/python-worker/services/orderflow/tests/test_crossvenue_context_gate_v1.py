"""Tests for cross-venue context gate policy.

Covers: monitor/tighten/veto profiles, direction disagree, dislocation
mid spread wide, trade imbalance (long/short), stale venues, veto conditions.
"""
import pytest
from services.orderflow.crossvenue_context_gate import (
    CrossVenueDecision
    evaluate_crossvenue_context
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_KWARGS = dict(
    min_agree=0.67
    max_dislocation_z=3.0
    max_mid_spread_bps=8.0
    max_stale_count=1
    tighten_mult=1.0
    tighten_cap_bps=6.0
)


def _eval(
    profile="monitor"
    side="BUY"
    direction_agree=1.0
    trade_imbalance=0.0
    dislocation_z=0.0
    mid_spread_bps=0.5
    stale_count=0
    **kwargs
) -> CrossVenueDecision:
    merged = {**_BASE_KWARGS, **kwargs}
    return evaluate_crossvenue_context(
        profile=profile
        side=side
        direction_agree=direction_agree
        trade_imbalance=trade_imbalance
        dislocation_z=dislocation_z
        mid_spread_bps=mid_spread_bps
        stale_count=stale_count
        **merged
    )


# ---------------------------------------------------------------------------
# Monitor mode — no tighten, no veto regardless of flags
# ---------------------------------------------------------------------------

def test_monitor_mode_no_adverse_no_flags():
    dec = _eval(profile="monitor")
    assert dec.hit is False
    assert dec.tighten_add_bps == 0.0
    assert dec.veto is False
    assert dec.mode == "monitor"


def test_monitor_mode_with_flags_still_no_tighten():
    dec = _eval(
        profile="monitor"
        direction_agree=0.3,   # below threshold
        dislocation_z=4.0,     # above threshold
    )
    assert dec.hit is True
    assert "venue_direction_disagree" in dec.flags
    assert "venue_dislocation" in dec.flags
    assert dec.tighten_add_bps == 0.0  # monitor → never tighten
    assert dec.veto is False


# ---------------------------------------------------------------------------
# Strict/tighten mode — tighten on adverse, no veto
# ---------------------------------------------------------------------------

def test_strict_tightens_on_direction_disagree():
    dec = _eval(
        profile="strict"
        side="BUY"
        direction_agree=0.33,  # below min_agree=0.67
    )
    assert "venue_direction_disagree" in dec.flags
    assert dec.tighten_add_bps > 0.0
    assert dec.veto is False
    assert dec.mode == "tighten"


def test_strict_tighten_capped_at_cap():
    dec = _eval(
        profile="strict"
        direction_agree=0.0,   # disagree
        dislocation_z=5.0,     # dislocation
        mid_spread_bps=15.0,   # wide spread
        tighten_mult=10.0,     # would exceed cap
        tighten_cap_bps=6.0
    )
    assert dec.tighten_add_bps == pytest.approx(6.0)


def test_strict_no_tighten_when_no_adverse():
    dec = _eval(profile="strict", direction_agree=1.0, dislocation_z=0.0)
    assert dec.tighten_add_bps == 0.0
    assert dec.veto is False


# ---------------------------------------------------------------------------
# Veto mode — veto on ≥2 adverse + not stale
# ---------------------------------------------------------------------------

def test_veto_mode_two_adverse_flags():
    dec = _eval(
        profile="hard"
        side="BUY"
        direction_agree=0.3
        dislocation_z=4.0
        stale_count=0
    )
    assert dec.veto is True
    assert "crossvenue_ctx:" in dec.veto_reason
    assert "venue_direction_disagree" in dec.veto_reason
    assert "venue_dislocation" in dec.veto_reason


def test_veto_mode_one_adverse_no_veto():
    dec = _eval(
        profile="veto"
        direction_agree=0.3,     # only one adverse flag
        dislocation_z=0.5
        stale_count=0
    )
    assert dec.veto is False
    assert dec.tighten_add_bps > 0.0  # tighten still applies


def test_veto_blocked_by_stale_count():
    """Even with 3 adverse flags, veto is blocked when stale_count > max_stale_count."""
    dec = _eval(
        profile="veto"
        direction_agree=0.0
        dislocation_z=5.0
        mid_spread_bps=20.0
        stale_count=2,           # > max_stale_count=1
    )
    assert dec.veto is False
    assert "venue_stale" in dec.flags


# ---------------------------------------------------------------------------
# Side-specific trade imbalance
# ---------------------------------------------------------------------------

def test_trade_imbalance_against_long():
    dec = _eval(
        profile="strict"
        side="BUY"
        trade_imbalance=-0.3,  # < -0.15 → adverse for LONG
    )
    assert "trade_imbalance_against_long" in dec.flags
    assert "trade_imbalance_against_short" not in dec.flags


def test_trade_imbalance_against_short():
    dec = _eval(
        profile="strict"
        side="SELL"
        trade_imbalance=0.4,  # > 0.15 → adverse for SHORT
    )
    assert "trade_imbalance_against_short" in dec.flags
    assert "trade_imbalance_against_long" not in dec.flags


def test_trade_imbalance_wrong_side_ignored():
    """Positive imbalance (buy pressure) should NOT flag a LONG signal."""
    dec = _eval(
        profile="strict"
        side="BUY"
        trade_imbalance=0.8,   # buy pressure — favorable for LONG
    )
    assert "trade_imbalance_against_long" not in dec.flags
    assert "trade_imbalance_against_short" not in dec.flags


# ---------------------------------------------------------------------------
# Venue stale flag
# ---------------------------------------------------------------------------

def test_venue_stale_detected():
    dec = _eval(stale_count=2, profile="strict")
    assert "venue_stale" in dec.flags


def test_venue_stale_alone_never_vetoes():
    """venue_stale alone must not trigger veto (it's data quality, not price evidence)."""
    dec = _eval(
        profile="veto"
        stale_count=2
        direction_agree=0.9,   # no price adverse flags
    )
    assert dec.veto is False


# ---------------------------------------------------------------------------
# Full adverse scenario (from spec)
# ---------------------------------------------------------------------------

def test_crossvenue_disagree_tightens_long():
    """Example from spec section 5."""
    dec = evaluate_crossvenue_context(
        profile="strict"
        side="BUY"
        direction_agree=0.33
        trade_imbalance=-0.2
        dislocation_z=3.5
        mid_spread_bps=9.0
        stale_count=0
        min_agree=0.67
        max_dislocation_z=3.0
        max_mid_spread_bps=8.0
        max_stale_count=1
        tighten_mult=1.0
        tighten_cap_bps=6.0
    )
    assert dec.veto is False               # strict mode → no veto
    assert dec.tighten_add_bps > 0.0
    assert "venue_direction_disagree" in dec.flags
    assert "venue_dislocation" in dec.flags
    assert "venue_mid_spread_wide" in dec.flags
    assert "trade_imbalance_against_long" in dec.flags


def test_profile_aliases():
    """All profile aliases should resolve to the correct internal mode."""
    for alias in ("default", "soft", "monitor"):
        dec = _eval(profile=alias)
        assert dec.mode == "monitor"
    for alias in ("strict", "tighten"):
        dec = _eval(profile=alias)
        assert dec.mode == "tighten"
    for alias in ("hard", "veto"):
        dec = _eval(profile=alias)
        assert dec.mode == "veto"
