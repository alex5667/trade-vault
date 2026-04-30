"""Phase 2.2 — unit tests for shadow stop/entry risk surface builder."""
from __future__ import annotations

import pytest

from services.atr_horizon_shadow_surface import build_risk_surface_shadow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sig(
    side: str = "BUY"
    entry_price: float = 100.0
    sl_atr_mult: float | None = None
    tp1_atr_mult: float | None = None
    atr_value: float = 4.0
    atr_tf_ms: int = 60_000
    atr_pct: float = 0.04
    hold_target_ms: int = 300_000
    alpha_half_life_ms: int = 180_000
    max_signal_age_ms: int = 90_000
) -> dict:
    meta: dict = {
        "horizon": {
            "hold_target_ms": hold_target_ms
            "alpha_half_life_ms": alpha_half_life_ms
            "max_signal_age_ms": max_signal_age_ms
        }
        "atr_profile": {
            "atr_value": atr_value
            "atr_tf_ms": atr_tf_ms
            "atr_pct": atr_pct
        }
    }
    if sl_atr_mult is not None:
        meta["sl_atr_mult"] = sl_atr_mult
    if tp1_atr_mult is not None:
        meta["tp1_atr_mult"] = tp1_atr_mult
    return {"side": side, "entry_price": entry_price, "meta": meta}


# ---------------------------------------------------------------------------
# BUY / LONG side
# ---------------------------------------------------------------------------

def test_shadow_surface_builds_buy_levels():
    sig = _sig(side="BUY", entry_price=100.0, sl_atr_mult=1.5, tp1_atr_mult=2.0, atr_value=4.0)
    out = build_risk_surface_shadow(sig)
    assert out["selected_stop_dist_px"] == pytest.approx(6.0)
    assert out["selected_tp1_dist_px"] == pytest.approx(8.0)
    assert out["selected_sl_price_shadow"] == pytest.approx(94.0)
    assert out["selected_tp1_price_shadow"] == pytest.approx(108.0)
    assert out["risk_reason_code"] == "RS_SHADOW_OK"
    assert out["mode"] == "shadow"


def test_shadow_surface_buy_long_alias():
    """LONG alias must produce same result as BUY."""
    sig_buy = _sig(side="BUY", entry_price=100.0, sl_atr_mult=1.5, tp1_atr_mult=2.0, atr_value=4.0)
    sig_long = _sig(side="LONG", entry_price=100.0, sl_atr_mult=1.5, tp1_atr_mult=2.0, atr_value=4.0)
    out_buy = build_risk_surface_shadow(sig_buy)
    out_long = build_risk_surface_shadow(sig_long)
    assert out_buy["selected_sl_price_shadow"] == out_long["selected_sl_price_shadow"]
    assert out_buy["selected_tp1_price_shadow"] == out_long["selected_tp1_price_shadow"]


# ---------------------------------------------------------------------------
# SELL / SHORT side
# ---------------------------------------------------------------------------

def test_shadow_surface_builds_sell_levels():
    sig = _sig(side="SELL", entry_price=100.0, sl_atr_mult=1.5, tp1_atr_mult=2.0, atr_value=4.0)
    out = build_risk_surface_shadow(sig)
    assert out["selected_stop_dist_px"] == pytest.approx(6.0)
    assert out["selected_tp1_dist_px"] == pytest.approx(8.0)
    # SELL: SL above entry, TP1 below entry
    assert out["selected_sl_price_shadow"] == pytest.approx(106.0)
    assert out["selected_tp1_price_shadow"] == pytest.approx(92.0)


def test_shadow_surface_sell_short_alias():
    sig_sell = _sig(side="SELL", entry_price=100.0, sl_atr_mult=1.5, tp1_atr_mult=2.0, atr_value=4.0)
    sig_short = _sig(side="SHORT", entry_price=100.0, sl_atr_mult=1.5, tp1_atr_mult=2.0, atr_value=4.0)
    out_sell = build_risk_surface_shadow(sig_sell)
    out_short = build_risk_surface_shadow(sig_short)
    assert out_sell["selected_sl_price_shadow"] == out_short["selected_sl_price_shadow"]


# ---------------------------------------------------------------------------
# Missing / incomplete data
# ---------------------------------------------------------------------------

def test_shadow_surface_zero_atr_produces_incomplete():
    sig = _sig(atr_value=0.0, entry_price=100.0)
    out = build_risk_surface_shadow(sig)
    assert out["risk_reason_code"] == "RS_SHADOW_INCOMPLETE"
    assert out["selected_stop_dist_px"] == 0.0
    assert out["selected_tp1_dist_px"] == 0.0


def test_shadow_surface_zero_entry_price_produces_incomplete():
    sig = _sig(atr_value=4.0, entry_price=0.0)
    out = build_risk_surface_shadow(sig)
    assert out["risk_reason_code"] == "RS_SHADOW_INCOMPLETE"


def test_shadow_surface_unknown_side_zeroes_prices():
    sig = _sig(side="UNKNOWN", entry_price=100.0, atr_value=4.0)
    out = build_risk_surface_shadow(sig)
    assert out["selected_sl_price_shadow"] == 0.0
    assert out["selected_tp1_price_shadow"] == 0.0


# ---------------------------------------------------------------------------
# Multipliers from ENV fallback
# ---------------------------------------------------------------------------

def test_shadow_surface_env_multiplier_fallback(monkeypatch):
    monkeypatch.setenv("ATR_HORIZON_SHADOW_SL_ATR_MULT", "2.0")
    monkeypatch.setenv("ATR_HORIZON_SHADOW_TP1_ATR_MULT", "3.0")
    # No multipliers in meta → should read from ENV
    sig = {"side": "BUY", "entry_price": 100.0, "meta": {
        "atr_profile": {"atr_value": 4.0, "atr_tf_ms": 60_000, "atr_pct": 0.04}
        "horizon": {"hold_target_ms": 300_000, "alpha_half_life_ms": 180_000, "max_signal_age_ms": 90_000}
    }}
    out = build_risk_surface_shadow(sig)
    assert out["sl_atr_mult"] == pytest.approx(2.0)
    assert out["tp1_atr_mult"] == pytest.approx(3.0)
    assert out["selected_stop_dist_px"] == pytest.approx(8.0)   # 4.0 * 2.0
    assert out["selected_tp1_dist_px"] == pytest.approx(12.0)   # 4.0 * 3.0


# ---------------------------------------------------------------------------
# Meta fields propagated
# ---------------------------------------------------------------------------

def test_shadow_surface_carries_horizon_fields():
    sig = _sig(
        atr_value=4.0
        atr_tf_ms=15_000
        hold_target_ms=60_000
        alpha_half_life_ms=30_000
        max_signal_age_ms=30_000
    )
    out = build_risk_surface_shadow(sig)
    assert out["atr_tf_ms"] == 15_000
    assert out["hold_target_ms"] == 60_000
    assert out["alpha_half_life_ms"] == 30_000
    assert out["max_signal_age_ms"] == 30_000
    assert out["entry_ttl_ms_shadow"] == 30_000


# ---------------------------------------------------------------------------
# Fail-open: empty / None signal must not raise
# ---------------------------------------------------------------------------

def test_shadow_surface_empty_signal():
    out = build_risk_surface_shadow({})
    assert isinstance(out, dict)
    assert out["risk_reason_code"] == "RS_SHADOW_INCOMPLETE"


def test_shadow_surface_none_signal():
    # Should not crash — _ensure_dict converts None to {}
    out = build_risk_surface_shadow(None)
    assert isinstance(out, dict)
