"""Phase 2.2 — unit tests for horizon-aware shadow DQ gate."""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from services.atr_horizon_shadow_gate import compute_horizon_dq_shadow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _fresh_ctx(**kwargs):
    """Build a minimal ctx with sane defaults that passes all checks."""
    now = _now_ms()
    defaults = dict(
        hold_target_ms=300_000,
        alpha_half_life_ms=180_000,
        max_signal_age_ms=90_000,
        atr_value=250.0,
        atr_tf_ms=60_000,
        atr_age_ms=1_000,
        selector_reason_code="ATR_SEL_EXACT",
        book_ts_ms=now - 500,   # 500 ms old → comfortably inside book budget
        ts_ms=now - 1_000,      # 1 s old → comfortably inside signal budget
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_horizon_dq_shadow_allows_fresh_selected_atr():
    ctx = _fresh_ctx()
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is True
    assert out["shadow_reason_code"] == "DQ_HZ_OK"
    assert out["atr_selected_value"] == 250.0
    assert out["atr_selected_tf_ms"] == 60_000
    assert out["atr_selected_age_ms"] == 1_000
    # Budget must be derived from hold_target_ms
    assert out["atr_age_budget_ms"] == int(300_000 * 0.25)  # 75_000


# ---------------------------------------------------------------------------
# ATR unavailable
# ---------------------------------------------------------------------------

def test_horizon_dq_shadow_blocks_zero_atr():
    ctx = _fresh_ctx(atr_value=0.0)
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is False
    assert out["shadow_reason_code"] == "DQ_ATR_UNAVAILABLE_SELECTED"


def test_horizon_dq_shadow_blocks_negative_atr():
    ctx = _fresh_ctx(atr_value=-1.0)
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is False
    assert out["shadow_reason_code"] == "DQ_ATR_UNAVAILABLE_SELECTED"


# ---------------------------------------------------------------------------
# ATR stale for horizon
# ---------------------------------------------------------------------------

def test_horizon_dq_shadow_blocks_stale_selected_atr():
    ctx = _fresh_ctx(
        hold_target_ms=60_000,
        alpha_half_life_ms=30_000,
        max_signal_age_ms=30_000,
        atr_tf_ms=15_000,
        atr_age_ms=120_000,   # >> 60000*0.25 = 15000
    )
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is False
    assert out["shadow_reason_code"] == "DQ_ATR_STALE_FOR_HORIZON"


def test_horizon_dq_shadow_fresh_atr_within_budget():
    ctx = _fresh_ctx(
        hold_target_ms=60_000,
        atr_age_ms=10_000,  # < 15_000 budget → should pass
    )
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is True
    assert out["shadow_reason_code"] == "DQ_HZ_OK"


# ---------------------------------------------------------------------------
# Book stale for horizon
# ---------------------------------------------------------------------------

def test_horizon_dq_shadow_blocks_stale_book():
    now = _now_ms()
    ctx = _fresh_ctx(
        hold_target_ms=60_000,   # book budget = max(500, 60000*0.05)=3000 ms
        book_ts_ms=now - 10_000, # 10 s old >> 3 s budget
    )
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is False
    assert out["shadow_reason_code"] == "DQ_BOOK_STALE_FOR_HORIZON"


def test_horizon_dq_shadow_no_book_ts_does_not_block():
    """book_ts_ms=0 → skip book check (fail-open)."""
    ctx = _fresh_ctx(book_ts_ms=0)
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is True


# ---------------------------------------------------------------------------
# Signal too old for horizon
# ---------------------------------------------------------------------------

def test_horizon_dq_shadow_blocks_old_signal():
    now = _now_ms()
    ctx = _fresh_ctx(
        max_signal_age_ms=30_000,
        ts_ms=now - 60_000,  # 60 s >> 30 s budget
    )
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is False
    assert out["shadow_reason_code"] == "DQ_SIGNAL_TOO_OLD_FOR_HORIZON"


def test_horizon_dq_shadow_no_signal_ts_does_not_block():
    """ts_ms=0 → skip signal-age check (fail-open)."""
    ctx = _fresh_ctx(ts_ms=0)
    out = compute_horizon_dq_shadow(ctx)
    assert out["allow_shadow"] is True


# ---------------------------------------------------------------------------
# No horizon context → falls back to ENV caps
# ---------------------------------------------------------------------------

def test_horizon_dq_shadow_no_horizon_uses_caps():
    ctx = _fresh_ctx(
        hold_target_ms=0,
        alpha_half_life_ms=0,
        max_signal_age_ms=0,
        atr_age_ms=100,
    )
    out = compute_horizon_dq_shadow(ctx)
    # With no hold_target_ms the ATR budget = cap (300_000 by default) → pass
    assert out["allow_shadow"] is True
    assert out["atr_age_budget_ms"] == int(os.getenv("ATR_HORIZON_DQ_ATR_AGE_CAP_MS", "300000"))


# ---------------------------------------------------------------------------
# Fail-open: any bad ctx must never raise
# ---------------------------------------------------------------------------

def test_horizon_dq_shadow_handles_none_ctx():
    out = compute_horizon_dq_shadow(None)
    # Must not raise; fail-open means allow or internal error flag
    assert isinstance(out, dict)
    assert "allow_shadow" in out


def test_horizon_dq_shadow_fields_present():
    ctx = _fresh_ctx()
    out = compute_horizon_dq_shadow(ctx)
    required = {
        "allow_shadow", "shadow_reason_code",
        "atr_selected_value", "atr_selected_tf_ms", "atr_selected_age_ms",
        "atr_age_budget_ms", "book_age_budget_ms", "signal_age_budget_ms",
        "selector_reason_code", "reason_details",
    }
    assert required <= set(out.keys())


import os  # noqa: E402 – used in test above, must be after imports block
