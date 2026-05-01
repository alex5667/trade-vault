from __future__ import annotations
"""Phase 2.4B unit tests: build_live_risk_surface."""

import pytest

from services.atr_horizon_live_surface import build_live_risk_surface


def _make_signal(side, entry, atr_value, atr_tf_ms=60_000, atr_pct=0.04,
                 sl_mult=1.5, tp1_mult=2.0, max_age_ms=90_000):
    return {
        "side": side,
        "entry_price": entry,
        "meta": {
            "sl_atr_mult": sl_mult,
            "tp1_atr_mult": tp1_mult,
            "horizon": {"max_signal_age_ms": max_age_ms},
            "atr_profile": {
                "atr_value": atr_value,
                "atr_tf_ms": atr_tf_ms,
                "atr_pct": atr_pct,
            }
        }
    }


class TestBuildLiveRiskSurfaceBuy:
    def test_sl_and_tp1_correct_for_buy(self):
        sig = _make_signal("BUY", 100.0, 4.0)
        out = build_live_risk_surface(sig)
        # sl = entry - atr * sl_mult = 100 - 4*1.5 = 94
        # tp1 = entry + atr * tp1_mult = 100 + 4*2.0 = 108
        assert out["selected_sl_price"] == pytest.approx(94.0)
        assert out["selected_tp1_price"] == pytest.approx(108.0)

    def test_stop_and_tp_distances_correct(self):
        sig = _make_signal("BUY", 100.0, 4.0)
        out = build_live_risk_surface(sig)
        assert out["selected_stop_dist_px"] == pytest.approx(6.0)   # 4 * 1.5
        assert out["selected_tp1_dist_px"] == pytest.approx(8.0)    # 4 * 2.0

    def test_max_signal_age_ms_passed_through(self):
        sig = _make_signal("BUY", 100.0, 4.0, max_age_ms=90_000)
        out = build_live_risk_surface(sig)
        assert out["selected_max_signal_age_ms"] == 90_000

    def test_reason_ok_when_complete(self):
        sig = _make_signal("BUY", 100.0, 4.0)
        assert build_live_risk_surface(sig)["reason_code"] == "LIVE_SURFACE_OK"

    def test_atr_passthrough_fields(self):
        sig = _make_signal("BUY", 100.0, 4.0, atr_tf_ms=300_000, atr_pct=0.05)
        out = build_live_risk_surface(sig)
        assert out["atr_tf_ms"] == 300_000
        assert out["atr_pct"] == pytest.approx(0.05)
        assert out["atr_value"] == pytest.approx(4.0)

    def test_mode_field(self):
        sig = _make_signal("BUY", 100.0, 4.0)
        assert build_live_risk_surface(sig)["mode"] == "live_canary_candidate"


class TestBuildLiveRiskSurfaceSell:
    def test_sl_and_tp1_correct_for_sell(self):
        sig = _make_signal("SELL", 100.0, 4.0)
        out = build_live_risk_surface(sig)
        # sl = entry + atr * sl_mult = 100 + 6 = 106
        # tp1 = entry - atr * tp1_mult = 100 - 8 = 92
        assert out["selected_sl_price"] == pytest.approx(106.0)
        assert out["selected_tp1_price"] == pytest.approx(92.0)


class TestBuildLiveRiskSurfaceSideAliases:
    def test_long_treated_as_buy(self):
        sig_buy = _make_signal("BUY", 100.0, 4.0)
        sig_long = _make_signal("LONG", 100.0, 4.0)
        assert build_live_risk_surface(sig_long)["selected_sl_price"] == \
               build_live_risk_surface(sig_buy)["selected_sl_price"]

    def test_short_treated_as_sell(self):
        sig_sell = _make_signal("SELL", 100.0, 4.0)
        sig_short = _make_signal("SHORT", 100.0, 4.0)
        assert build_live_risk_surface(sig_short)["selected_sl_price"] == \
               build_live_risk_surface(sig_sell)["selected_sl_price"]


class TestBuildLiveRiskSurfaceIncomplete:
    def test_missing_atr_gives_incomplete(self):
        # atr_value=0 → stop_dist=0 → sl=entry, tp1=entry (zero distances)
        # but reason_code must flag INCOMPLETE (atr_value not > 0)
        sig = _make_signal("BUY", 100.0, 0.0)
        out = build_live_risk_surface(sig)
        assert out["reason_code"] == "LIVE_SURFACE_INCOMPLETE"
        # stop_dist and tp1_dist must both be zero
        assert out["selected_stop_dist_px"] == pytest.approx(0.0)
        assert out["selected_tp1_dist_px"] == pytest.approx(0.0)

    def test_missing_entry_gives_incomplete(self):
        sig = _make_signal("BUY", 0.0, 4.0)  # entry=0
        out = build_live_risk_surface(sig)
        assert out["reason_code"] == "LIVE_SURFACE_INCOMPLETE"

    def test_missing_side_gives_incomplete(self):
        sig = _make_signal("", 100.0, 4.0)
        out = build_live_risk_surface(sig)
        assert out["reason_code"] == "LIVE_SURFACE_INCOMPLETE"
        assert out["selected_sl_price"] == pytest.approx(0.0)

    def test_empty_signal_does_not_raise(self):
        out = build_live_risk_surface({})
        assert out["reason_code"] == "LIVE_SURFACE_INCOMPLETE"

    def test_non_dict_signal_does_not_raise(self):
        out = build_live_risk_surface(None)  # type: ignore[arg-type]
        assert out["reason_code"] == "LIVE_SURFACE_INCOMPLETE"


class TestBuildLiveRiskSurfaceMultipliers:
    def test_env_sl_mult_fallback(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SL_ATR_MULT", "2.0")
        monkeypatch.setenv("ATR_HORIZON_LIVE_TP1_ATR_MULT", "3.0")
        sig = {
            "side": "BUY",
            "entry_price": 100.0,
            "meta": {
                "horizon": {"max_signal_age_ms": 60_000},
                "atr_profile": {"atr_value": 4.0, "atr_tf_ms": 60_000, "atr_pct": 0.04},
            }
        }
        out = build_live_risk_surface(sig)
        assert out["sl_atr_mult"] == pytest.approx(2.0)
        assert out["tp1_atr_mult"] == pytest.approx(3.0)
        assert out["selected_sl_price"] == pytest.approx(92.0)   # 100 - 4*2
        assert out["selected_tp1_price"] == pytest.approx(112.0)  # 100 + 4*3

    def test_meta_mult_overrides_env(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SL_ATR_MULT", "9.0")
        sig = _make_signal("BUY", 100.0, 4.0, sl_mult=1.0)
        out = build_live_risk_surface(sig)
        # meta.sl_atr_mult=1.0 wins over env 9.0
        assert out["sl_atr_mult"] == pytest.approx(1.0)
