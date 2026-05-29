"""Tests for v14_of alias-bridging additions in feature_enricher_v1."""
from __future__ import annotations

from core.feature_enricher_v1 import (
    _enrich_atr_aliases,
    _enrich_misc_aliases,
    _enrich_vol_features,
    _enrich_microbar,
    _enrich_momentum,
    _enrich_derived,
)


# ─── ATR aliases ─────────────────────────────────────────────────────────────


class TestAtrAliases:

    def test_bps_exec_from_atr_bps(self):
        out = _enrich_atr_aliases({"atr_bps": 12.5})
        assert out["atr_bps_exec"] == 12.5

    def test_unified_th_from_floor_picked(self):
        out = _enrich_atr_aliases({"atr_floor_picked_bps": 20.0})
        assert out["atr_unified_th_bps"] == 20.0

    def test_floor_tiers_synthesised_from_picked(self):
        out = _enrich_atr_aliases({"atr_floor_picked_bps": 10.0})
        assert out["atr_floor_t0_bps"] == 5.0
        assert out["atr_floor_t1_bps"] == 10.0
        assert out["atr_floor_t2_bps"] == 15.0

    def test_sanity_ok_from_atr_bad(self):
        out = _enrich_atr_aliases({"atr_bad": True})
        assert out["atr_sanity_ok"] == 0.0
        out2 = _enrich_atr_aliases({"atr_bad": False})
        assert out2["atr_sanity_ok"] == 1.0

    def test_consistency_from_jump_count(self):
        out = _enrich_atr_aliases({"atr_jump_count_window": 2})
        # 1.0 - 2/10 = 0.8 → consistency
        assert abs(out["atr_consistency"] - 0.8) < 1e-9
        assert out["atr_cons_ok"] == 1.0

    def test_consistency_high_jumps_caps(self):
        out = _enrich_atr_aliases({"atr_jump_count_window": 15})
        # min(1.0, 15/10) = 1.0 → consistency = 0
        assert out["atr_consistency"] == 0.0
        assert out["atr_cons_ok"] == 0.0

    def test_candidates_n_from_atr_src(self):
        out = _enrich_atr_aliases({"atr_src": "1m"})
        assert out["atr_candidates_n"] == 1.0

    def test_fees_defaults(self):
        out = _enrich_atr_aliases({"atr_floor_picked_bps": 10.0})
        assert out["atr_fees_th_bps"] == 10.0 + 2 * 2.0  # floor + 2 × fees_one_side
        assert out["atr_fees_rocket_mult"] == 1.5
        assert out["atr_fees_tp1_share"] == 0.5

    def test_fees_th_zero_when_floor_unknown(self):
        out = _enrich_atr_aliases({})
        assert out["atr_fees_th_bps"] == 0.0


# ─── Misc aliases ────────────────────────────────────────────────────────────


class TestMiscAliases:

    def test_iceberg_avg_qty_direct(self):
        out = _enrich_misc_aliases({"iceberg_qty": 5.0})
        assert out["iceberg_avg_qty"] == 5.0

    def test_iceberg_avg_qty_window_derive(self):
        out = _enrich_misc_aliases({
            "iceberg_count_window": 3.0,
            "iceberg_total_qty_window": 30.0,
        })
        assert out["iceberg_avg_qty"] == 10.0

    def test_liqmap_age_alias(self):
        out = _enrich_misc_aliases({"liqmap_1h_stale_ms": 5000.0})
        assert out["liqmap_1h_age_ms"] == 5000.0

    def test_liqmap_sl_aliases_from_gate(self):
        out = _enrich_misc_aliases({
            "liqmap_gate_risk_bps": 30.0,
            "liqmap_gate_reward_bps": 60.0,
            "liqmap_gate_rr": 2.0,
        })
        assert out["liqmap_sl_base_bps"] == 30.0
        assert out["liqmap_sl_reco_bps"] == 30.0
        # rr=2.0 > 1.5 → widen NOT needed
        assert out["liqmap_sl_widen_needed"] == 0.0
        assert out["liqmap_sl_widen_ratio"] == 1.0

    def test_liqmap_sl_widen_needed_when_rr_low(self):
        out = _enrich_misc_aliases({
            "liqmap_gate_risk_bps": 30.0,
            "liqmap_gate_rr": 1.0,
        })
        # rr=1.0 < 1.5 → widen needed, ratio = 1.5/1.0 = 1.5
        assert out["liqmap_sl_widen_needed"] == 1.0
        assert out["liqmap_sl_widen_ratio"] == 1.5

    def test_liq_score_x_spread(self):
        out = _enrich_misc_aliases({
            "liqmap_gate_risk_bps": 30.0,
            "spread_bps": 2.0,
        })
        assert out["liq_score_x_spread"] == 60.0

    def test_health_veto_defaults_zero(self):
        out = _enrich_misc_aliases({})
        assert out["book_health_veto_book_evidence"] == 0.0
        assert out["data_health_veto_book_evidence"] == 0.0

    def test_health_veto_set_when_present(self):
        out = _enrich_misc_aliases({"book_health_veto": True})
        assert out["book_health_veto_book_evidence"] == 1.0

    def test_confidence_ema_bridge(self):
        out = _enrich_misc_aliases({"confidence": 0.75})
        assert out["confidence_ema"] == 0.75

    def test_confidence_ema_skipped_when_present(self):
        # If confidence_ema already in dict, don't override
        out = _enrich_misc_aliases({"confidence": 0.75, "confidence_ma": 0.5})
        assert "confidence_ema" not in out

    def test_amihud_proxy_derivation(self):
        out = _enrich_misc_aliases({
            "microprice_shift_bps_20": 4.0,
            "trade_qty_window": 2.0,
        })
        assert out["amihud_illiq"] == 2.0  # |4| / 2 = 2


# ─── Vol features expanded ───────────────────────────────────────────────────


class TestVolFeaturesExpanded:

    def test_vol_fast_from_compression(self):
        out = _enrich_vol_features({"vol_compression_score": 0.5})
        assert out["vol_fast_bps"] == 0.5

    def test_vol_fast_from_atr_q(self):
        out = _enrich_vol_features({"atr_q": 1.2})
        # vol_compression takes priority over atr_q (defined first in alias list)
        # but with no vol_compression, atr_q wins
        assert out["vol_fast_bps"] == 1.2

    def test_regime_code_from_deribit(self):
        out = _enrich_vol_features({"deribit_vol_regime_code": 2})
        assert out["vol_regime_code"] == 2.0

    def test_vol_slow_derived_from_ratio(self):
        # vol_fast=5, ratio=2 → vol_slow = 5/2 = 2.5
        out = _enrich_vol_features({
            "vol_compression_score": 5.0,
            "vol_ratio_fast_slow": 2.0,
        })
        assert out["vol_slow_bps"] == 2.5


# ─── Microbar fallbacks ──────────────────────────────────────────────────────


class TestMicrobarFallback:

    def test_body_bps_from_price_and_vwap(self):
        # No explicit microbar_*_px — derive from price + vwap_1m
        out = _enrich_microbar({"price": 100.1, "vwap_1m": 100.0, "decision_mid": 100.05})
        # close=100.1 (price), open=100.0 (vwap_1m) → body = 10bps
        assert abs(out["microbar_body_bps"] - 10.0) < 1e-6

    def test_no_data_no_emission(self):
        out = _enrich_microbar({})
        assert "microbar_body_bps" not in out
        assert "microbar_range_bps" not in out


# ─── Momentum fallbacks ──────────────────────────────────────────────────────


class TestMomentumFallback:

    def test_direct_momentum_used_when_present(self):
        out = _enrich_momentum({"momentum_5s": 0.003})
        assert out["momentum_10s"] == 0.003

    def test_microprice_shift_proxy(self):
        out = _enrich_momentum({"microprice_shift_bps_20": 5.0})
        # 5 bps / 10000 = 0.0005
        assert abs(out["momentum_10s"] - 0.0005) < 1e-9

    def test_price_to_ema_from_vwap(self):
        # No ema_short → fall back to vwap_1m
        out = _enrich_momentum({"price": 100.1, "vwap_1m": 100.0})
        # (100.1 - 100) / 100 × 10000 = 10 bps
        assert abs(out["price_to_ema_bps"] - 10.0) < 1e-6


# ─── Derived mae/mfe placeholders ────────────────────────────────────────────


class TestMaeMfePlaceholders:

    def test_mae_r_zero_default(self):
        out = _enrich_derived({"regime": "range"}, {})
        assert out["mae_r"] == 0.0
        assert out["mfe_r"] == 0.0

    def test_mae_r_not_overridden_when_present(self):
        # When labels backfilled (rare at serve), don't override
        out = _enrich_derived({"regime": "range", "mae_r": 0.5}, {})
        # Our function emits 0.0 only when key NOT in indicators
        # setdefault later in enrich_indicators will keep existing value
        assert "mae_r" not in out  # we only emit when missing


# ─── conf_rsi_agree (moved from strategy.py to enricher) ─────────────────────


class TestConfRsiAgree:
    """conf_rsi_agree is now computed in _enrich_derived so that rsi_cvd
    from _enrich_rsi_cvd (book_rates Redis key) is available before the check.
    strategy.py used to compute it with rc=50.0 fallback which made rc>50 always False."""

    def test_long_both_above_50_is_1(self):
        inds = {"direction": "LONG", "rsi_price": 70.0}
        out = _enrich_derived(inds, {"rsi_cvd": 65.0})
        assert out["conf_rsi_agree"] == 1.0

    def test_short_both_below_50_is_1(self):
        inds = {"direction": "SHORT", "rsi_price": 30.0}
        out = _enrich_derived(inds, {"rsi_cvd": 35.0})
        assert out["conf_rsi_agree"] == 1.0

    def test_long_rsi_cvd_below_50_is_0(self):
        inds = {"direction": "LONG", "rsi_price": 70.0}
        out = _enrich_derived(inds, {"rsi_cvd": 45.0})
        assert out["conf_rsi_agree"] == 0.0

    def test_short_rsi_price_above_50_is_0(self):
        inds = {"direction": "SHORT", "rsi_price": 60.0}
        out = _enrich_derived(inds, {"rsi_cvd": 30.0})
        assert out["conf_rsi_agree"] == 0.0

    def test_rsi_cvd_from_indicators_fallback(self):
        # rsi_cvd already in indicators (from strategy.py runtime) — should work
        inds = {"direction": "LONG", "rsi_price": 77.7, "rsi_cvd": 73.5}
        out = _enrich_derived(inds, {})
        assert out["conf_rsi_agree"] == 1.0

    def test_missing_rsi_cvd_no_key_emitted(self):
        # Without rsi_cvd, conf_rsi_agree must NOT be emitted (avoids frozen-0 in schema)
        inds = {"direction": "LONG", "rsi_price": 77.7}
        out = _enrich_derived(inds, {})
        assert "conf_rsi_agree" not in out

    def test_exact_boundary_50_not_triggered(self):
        # rc == 50 is NOT > 50 — boundary check
        inds = {"direction": "LONG", "rsi_price": 50.0}
        out = _enrich_derived(inds, {"rsi_cvd": 50.0})
        assert out["conf_rsi_agree"] == 0.0

    def test_direction_case_insensitive(self):
        inds = {"direction": "long", "rsi_price": 70.0}
        out = _enrich_derived(inds, {"rsi_cvd": 65.0})
        assert out["conf_rsi_agree"] == 1.0
