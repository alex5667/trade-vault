# -*- coding: utf-8 -*-
"""Unit tests for liqmap TP/SL overlay (D1).

Focus:
- Deterministic behavior for LONG/SHORT.
- Supports current injected key style (dist_*_bps + peak_*1_usd) and explicit peak price style.
- Enforces SL widening cap.

These tests are pure and do not touch Redis or time.
"""


import sys
from pathlib import Path


# Ensure we import the SoT copy (tick_flow_full/services/...) first.
_PW_ROOT = Path(__file__).resolve().parents[4]
_TF_ROOT = _PW_ROOT / "tick_flow_full"
sys.path.insert(0, str(_TF_ROOT))

from services.orderflow.liqmap_features import apply_liqmap_tp_sl_adjustment


def _approx(a: float, b: float, tol: float = 1e-9) -> bool:
    return abs(a - b) <= tol


class TestLiqMapTpSlOverlayV1:
    def test_long_tp1_before_peak_derived_price(self):
        # entry=100, base_tp1=105, peak_up at +300 bps => 103, strong enough
        ind = {
            "liqmap_1h_dist_up_bps": 300.0,
            "liqmap_1h_peak_up1_usd": 200_000.0,
        }
        new_sl, new_tp1, out = apply_liqmap_tp_sl_adjustment(
            entry=100.0,
            side="LONG",
            base_sl=99.0,
            base_tp1=105.0,
            indicators=ind,
            window="1h",
            min_usd=50_000.0,
            buffer_bps=10.0,
            max_sl_widen_bps=50.0,
            enable_tp1=True,
            enable_sl=False,
        )

        assert _approx(new_sl, 99.0)
        # peak_up_price = 103.0 => tp1 = 103*(1-0.001)=102.897
        assert abs(new_tp1 - 102.897) < 1e-6
        assert out["liqmap_levels_applied"] == 1.0
        assert out["liqmap_levels_reason"] == "tp1_before_peak"
        assert abs(out["liqmap_tp1_anchor_price"] - 103.0) < 1e-12
        assert out["liqmap_tp1_anchor_usd"] == 200_000.0

        base_tp_bps = (105.0 - 100.0) / 100.0 * 10000.0
        new_tp_bps = (new_tp1 - 100.0) / 100.0 * 10000.0
        assert abs(out["liqmap_tp1_adj_bps"] - (new_tp_bps - base_tp_bps)) < 1e-6

    def test_long_sl_behind_peak_cap(self):
        # entry=100, base_sl=99 (100 bps), dn peak at -150 bps => 98.5, strong enough
        ind = {
            "liqmap_1h_dist_dn_bps": 150.0,
            "liqmap_1h_peak_dn1_usd": 150_000.0,
        }
        new_sl, new_tp1, out = apply_liqmap_tp_sl_adjustment(
            entry=100.0,
            side="LONG",
            base_sl=99.0,
            base_tp1=105.0,
            indicators=ind,
            window="1h",
            min_usd=50_000.0,
            buffer_bps=10.0,
            max_sl_widen_bps=50.0,  # cap widening to +50 bps (150 total)
            enable_tp1=False,
            enable_sl=True,
        )

        assert _approx(new_tp1, 105.0)

        # Proposed: 98.5*(1-0.001)=98.4015 => stop_bps ~159.85, would widen by ~59.85 -> capped to 150 bps.
        assert abs(new_sl - 98.5) < 1e-6
        assert out["liqmap_levels_applied"] == 1.0
        assert out["liqmap_levels_reason"] == "cap_sl_widen"
        assert abs(out["liqmap_sl_anchor_price"] - 98.5) < 1e-12
        assert out["liqmap_sl_anchor_usd"] == 150_000.0

        base_stop_bps = (100.0 - 99.0) / 100.0 * 10000.0
        new_stop_bps = (100.0 - new_sl) / 100.0 * 10000.0
        assert abs(out["liqmap_sl_adj_bps"] - (new_stop_bps - base_stop_bps)) < 1e-6
        assert abs(out["liqmap_sl_adj_bps"] - 50.0) < 1e-6

    def test_short_tp1_after_peak_explicit_price(self):
        ind = {
            "liqmap_1h_peak_dn_price": 97.0,
            "liqmap_1h_peak_dn_usd": 200_000.0,
        }
        new_sl, new_tp1, out = apply_liqmap_tp_sl_adjustment(
            entry=100.0,
            side="SHORT",
            base_sl=101.0,
            base_tp1=95.0,
            indicators=ind,
            window="1h",
            min_usd=50_000.0,
            buffer_bps=10.0,
            max_sl_widen_bps=50.0,
            enable_tp1=True,
            enable_sl=False,
        )

        assert _approx(new_sl, 101.0)
        # SHORT: tp1 = peak_dn*(1-0.001)=96.903
        assert abs(new_tp1 - 96.903) < 1e-6
        assert out["liqmap_levels_reason"] == "tp1_after_peak"
        assert abs(out["liqmap_tp1_anchor_price"] - 97.0) < 1e-12
        assert out["liqmap_tp1_anchor_usd"] == 200_000.0

    def test_short_sl_behind_peak_tighten_ok(self):
        # entry=100, base_sl=101, up peak at 100.8 => proposed sl 100.9008 (tightens)
        ind = {
            "liqmap_1h_peak_up_price": 100.8,
            "liqmap_1h_peak_up_usd": 120_000.0,
        }
        new_sl, new_tp1, out = apply_liqmap_tp_sl_adjustment(
            entry=100.0,
            side="SELL",  # alias
            base_sl=101.0,
            base_tp1=95.0,
            indicators=ind,
            window="1h",
            min_usd=50_000.0,
            buffer_bps=10.0,
            max_sl_widen_bps=50.0,
            enable_tp1=False,
            enable_sl=True,
        )

        assert _approx(new_tp1, 95.0)
        assert abs(new_sl - 100.9008) < 1e-6
        assert out["liqmap_levels_reason"] in ("sl_behind_peak", "tp1_sl")
        assert out["liqmap_sl_adj_bps"] < 0.0  # tightening

    def test_no_peak_returns_base(self):
        ind = {}
        new_sl, new_tp1, out = apply_liqmap_tp_sl_adjustment(
            entry=100.0,
            side="LONG",
            base_sl=99.0,
            base_tp1=105.0,
            indicators=ind,
            window="1h",
            min_usd=50_000.0,
            buffer_bps=10.0,
            max_sl_widen_bps=50.0,
            enable_tp1=True,
            enable_sl=True,
        )
        assert _approx(new_sl, 99.0)
        assert _approx(new_tp1, 105.0)
        assert out["liqmap_levels_applied"] == 0.0
        assert out["liqmap_levels_reason"] == "no_peak"
