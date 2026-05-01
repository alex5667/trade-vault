from __future__ import annotations
"""Tests for core.hawkes_like_intensity.update_hawkes_like."""


import math
import pytest
from core.hawkes_like_intensity import update_hawkes_like, _decay


# ---------------------------------------------------------------------------
# Tests: _decay
# ---------------------------------------------------------------------------

def test_decay_zero_dt():
    """dt_s=0 should return 1.0 (no decay)."""
    assert _decay(1.0, 0.0) == 1.0


def test_decay_positive():
    """decay should be in (0,1) for positive dt."""
    d = _decay(1.0, 1.0)
    assert 0.0 < d < 1.0


def test_decay_large_negative_clamped():
    """Very large dt causes underflow → 0.0."""
    d = _decay(1.0, 1000.0)
    assert d == 0.0


# ---------------------------------------------------------------------------
# Tests: update_hawkes_like
# ---------------------------------------------------------------------------

def test_initial_state_none():
    """State=None should initialize without crash and return zeroed lambdas."""
    st, snap = update_hawkes_like(
        None,
        now_ts_ms=1000,
        dt_s=1.0,
        rates={"taker_buy_rate": 1.0, "taker_sell_rate": 0.5, "cancel_bid_rate": 0.2,
               "cancel_ask_rate": 0.1, "limit_add_rate": 0.3},
    )
    assert isinstance(st, dict)
    assert isinstance(snap, dict)
    assert "hawkes_taker_buy_lam" in snap
    assert "hawkes_taker_sell_lam" in snap
    assert "hawkes_cancel_bid_lam" in snap
    assert "hawkes_cancel_ask_lam" in snap
    assert "hawkes_limit_add_lam" in snap
    # Legacy keys
    assert "hawkes_taker_lam" in snap
    assert "hawkes_cancel_lam" in snap
    assert "hawkes_churn_lam" in snap


def test_all_snapshot_finite():
    """All snapshot values must be finite."""
    st, snap = update_hawkes_like(
        {},
        now_ts_ms=1000,
        dt_s=1.0,
        rates={"taker_buy_rate": 5.0, "taker_sell_rate": 3.0, "cancel_bid_rate": 0.5,
               "cancel_ask_rate": 0.4, "limit_add_rate": 1.0},
    )
    for k, v in snap.items():
        assert math.isfinite(float(v)), f"snap[{k!r}] = {v} is not finite"


def test_state_ts_updated():
    """State ts_ms should be updated to now_ts_ms."""
    st, _ = update_hawkes_like(
        {"ts_ms": 0},
        now_ts_ms=5000,
        dt_s=1.0,
        rates={},
    )
    assert st["ts_ms"] == 5000


def test_state_accumulates():
    """Repeated calls should accumulate intensity (S > 0) when rate is positive."""
    st = {}
    for i in range(5):
        st, snap = update_hawkes_like(
            st,
            now_ts_ms=1000 * (i + 1),
            dt_s=1.0,
            rates={"taker_buy_rate": 2.0, "taker_sell_rate": 1.0, "cancel_bid_rate": 0.0,
                   "cancel_ask_rate": 0.0, "limit_add_rate": 0.0},
        )
    # After 5 steps with rate > 0, S_taker_buy should be > 0
    assert float(st.get("S_taker_buy", 0.0)) > 0.0
    assert float(snap["hawkes_taker_buy_lam"]) >= 0.0


def test_dt_zero_no_state_change():
    """dt_s=0 means no time elapsed → state accumulation is zero (add = rate*0)."""
    st = {"S_taker_buy": 1.0, "S_taker_sell": 0.5}
    st_new, snap = update_hawkes_like(
        st,
        now_ts_ms=1000,
        dt_s=0.0,
        rates={"taker_buy_rate": 100.0, "taker_sell_rate": 50.0},
    )
    # After dt=0 with decay=1.0: S_new = 1.0 * prev + rate*0 = prev
    # S_taker_buy → still 1.0
    assert abs(float(st_new.get("S_taker_buy", 0.0)) - 1.0) < 1e-6


def test_nan_rate_treated_as_zero():
    """NaN/inf rates should be treated as 0.0 (safe)."""
    st, snap = update_hawkes_like(
        {},
        now_ts_ms=1000,
        dt_s=1.0,
        rates={"taker_buy_rate": float("nan"), "taker_sell_rate": float("inf")},
    )
    for k, v in snap.items():
        assert math.isfinite(float(v)), f"snap[{k!r}] should be finite but got {v}"


def test_cfg_override_beta():
    """cfg override for hawkes_beta changes decay."""
    rates = {"taker_buy_rate": 1.0}
    _, snap_slow = update_hawkes_like({}, now_ts_ms=1000, dt_s=1.0, rates=rates,
                                       cfg={"hawkes_beta": 0.1})
    _, snap_fast = update_hawkes_like({}, now_ts_ms=1000, dt_s=1.0, rates=rates,
                                       cfg={"hawkes_beta": 5.0})
    # slower decay → more accumulation → higher lambda
    assert float(snap_slow["hawkes_taker_buy_lam"]) >= float(snap_fast["hawkes_taker_buy_lam"])


def test_split_rates_separated():
    """Buy and sell should produce independent lambdas."""
    rates_buy = {"taker_buy_rate": 5.0, "taker_sell_rate": 0.0}
    rates_sell = {"taker_buy_rate": 0.0, "taker_sell_rate": 5.0}

    _, snap_buy = update_hawkes_like({}, now_ts_ms=1000, dt_s=1.0, rates=rates_buy)
    _, snap_sell = update_hawkes_like({}, now_ts_ms=1000, dt_s=1.0, rates=rates_sell)

    assert float(snap_buy["hawkes_taker_buy_lam"]) > float(snap_buy["hawkes_taker_sell_lam"])
    assert float(snap_sell["hawkes_taker_sell_lam"]) > float(snap_sell["hawkes_taker_buy_lam"])


def test_returns_raw_states():
    """Snapshot contains raw S_* fields for observability."""
    st, snap = update_hawkes_like(
        {},
        now_ts_ms=1000,
        dt_s=1.0,
        rates={"cancel_bid_rate": 3.0, "cancel_ask_rate": 1.0},
    )
    assert "hawkes_S_cancel_bid" in snap
    assert "hawkes_S_cancel_ask" in snap
    assert float(snap["hawkes_S_cancel_bid"]) > float(snap["hawkes_S_cancel_ask"])
