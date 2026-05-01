from __future__ import annotations
"""
Unit tests for core/v13_of_features.py

Tests: all compute groups (NA–NX), inject_v13_of_features master function,
       fail-open on missing attributes, NX interaction derivations,
       toxicity composite (NC), z-score (NF), schema key validation.
"""

from utils.time_utils import get_ny_time_millis

import math
import time
import pytest
from types import SimpleNamespace
from typing import Any, Dict

from core.v13_of_features import (
    compute_group_na, compute_group_nb, compute_group_nc,
    compute_group_nd, compute_group_ne, compute_group_nf,
    compute_group_nx,
    inject_v13_of_features,
    _V13_OF_NEW_KEY_SET,
)
from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _runtime(**kwargs) -> Any:
    return SimpleNamespace(**kwargs)


def _now_ms() -> int:
    return get_ny_time_millis()


# ---------------------------------------------------------------------------
# Group NA — Advanced Volatility
# ---------------------------------------------------------------------------

class TestGroupNA:
    def test_reads_runtime_attrs(self):
        rt = _runtime(garman_klass_vol=0.015, parkinson_vol=0.012,
                       yang_zhang_vol=0.018, vol_of_vol=0.003)
        out = compute_group_na(rt, _now_ms(), {})
        assert out["garman_klass_vol"] == pytest.approx(0.015)
        assert out["parkinson_vol"] == pytest.approx(0.012)
        assert out["yang_zhang_vol"] == pytest.approx(0.018)
        assert out["vol_of_vol"] == pytest.approx(0.003)

    def test_defaults_to_zero_on_missing(self):
        rt = _runtime()
        out = compute_group_na(rt, _now_ms(), {})
        for k in ("garman_klass_vol", "parkinson_vol", "yang_zhang_vol", "vol_of_vol"):
            assert out[k] == 0.0, f"expected 0.0 for {k}"

    def test_no_panic_on_none_attrs(self):
        rt = _runtime(garman_klass_vol=None, parkinson_vol=None)
        out = compute_group_na(rt, _now_ms(), {})
        assert out["garman_klass_vol"] == 0.0


# ---------------------------------------------------------------------------
# Group NB — Academic Liquidity
# ---------------------------------------------------------------------------

class TestGroupNB:
    def test_reads_all_keys(self):
        rt = _runtime(amihud_illiquidity=0.0025, corwin_schultz_spread=1.5,
                       hasbrouck_info_share=0.35, depth_resilience_half_life=500.0)
        out = compute_group_nb(rt, _now_ms(), {})
        assert out["amihud_illiquidity"] == pytest.approx(0.0025)
        assert out["corwin_schultz_spread"] == pytest.approx(1.5)
        assert out["hasbrouck_info_share"] == pytest.approx(0.35)
        assert out["depth_resilience_half_life"] == pytest.approx(500.0)

    def test_defaults_zero(self):
        out = compute_group_nb(_runtime(), _now_ms(), {})
        for k in ("amihud_illiquidity", "corwin_schultz_spread", "hasbrouck_info_share", "depth_resilience_half_life"):
            assert out[k] == 0.0


# ---------------------------------------------------------------------------
# Group NC — Flow Toxicity
# ---------------------------------------------------------------------------

class TestGroupNC:
    def test_pin_from_runtime(self):
        rt = _runtime(pin_estimate=0.25)
        out = compute_group_nc(rt, _now_ms(), {})
        assert out["pin_estimate"] == pytest.approx(0.25)

    def test_lambda_asym_computation(self):
        rt = _runtime(kyle_lambda_buy=0.8, kyle_lambda_sell=0.2)
        out = compute_group_nc(rt, _now_ms(), {})
        # |0.8 - 0.2| / avg(0.8, 0.2) = 0.6 / 0.5 = 1.2
        assert out["lambda_asym"] == pytest.approx(1.2)

    def test_lambda_asym_symmetric(self):
        rt = _runtime(kyle_lambda_buy=0.5, kyle_lambda_sell=0.5)
        out = compute_group_nc(rt, _now_ms(), {})
        assert out["lambda_asym"] == pytest.approx(0.0)

    def test_toxicity_regime_score_composite(self):
        rt = _runtime(pin_estimate=0.5)
        indicators = {"vpin_rolling": 0.8, "adverse_drift_ms": 25.0, "info_flow": 0.6}
        out = compute_group_nc(rt, _now_ms(), indicators)
        # 0.3 * 0.8 + 0.3 * 0.5 + 0.2 * (25/50) + 0.2 * 0.6 = 0.24 + 0.15 + 0.1 + 0.12 = 0.61
        assert out["toxicity_regime_score"] == pytest.approx(0.61, abs=0.01)

    def test_toxicity_clamped_to_unit(self):
        rt = _runtime(pin_estimate=1.0)
        indicators = {"vpin_rolling": 1.0, "adverse_drift_ms": 100.0, "info_flow": 1.0}
        out = compute_group_nc(rt, _now_ms(), indicators)
        assert 0.0 <= out["toxicity_regime_score"] <= 1.0

    def test_aggressive_sweep_ratio(self):
        rt = _runtime(aggressive_sweep_ratio=0.15)
        out = compute_group_nc(rt, _now_ms(), {})
        assert out["aggressive_sweep_ratio"] == pytest.approx(0.15)

    def test_defaults_zero(self):
        out = compute_group_nc(_runtime(), _now_ms(), {})
        for k in ("pin_estimate", "lambda_asym", "toxicity_regime_score", "aggressive_sweep_ratio"):
            assert out[k] == 0.0


# ---------------------------------------------------------------------------
# Group ND — Cross-Asset Macro Extended (go-worker, fail-open)
# ---------------------------------------------------------------------------

class TestGroupND:
    def test_reads_crossasset_attrs(self):
        rt = _runtime(btc_dominance_momentum=-0.5, oi_weighted_funding=0.03,
                       total_market_oi_delta=1000.0, liq_heatmap_distance_bps=150.0,
                       long_short_ratio=1.3)
        out = compute_group_nd(rt, _now_ms(), {})
        assert out["btc_dominance_momentum"] == pytest.approx(-0.5)
        assert out["oi_weighted_funding"] == pytest.approx(0.03)
        assert out["total_market_oi_delta"] == pytest.approx(1000.0)
        assert out["liq_heatmap_distance_bps"] == pytest.approx(150.0)
        assert out["long_short_ratio"] == pytest.approx(1.3)

    def test_defaults_zero_without_goworker(self):
        out = compute_group_nd(_runtime(), _now_ms(), {})
        assert all(v == 0.0 for v in out.values())


# ---------------------------------------------------------------------------
# Group NE — Entropy / Information Theory
# ---------------------------------------------------------------------------

class TestGroupNE:
    def test_reads_entropy_attrs(self):
        rt = _runtime(price_entropy_50=2.1, order_size_gini=0.65,
                       mutual_info_price_volume=0.3)
        out = compute_group_ne(rt, _now_ms(), {})
        assert out["price_entropy_50"] == pytest.approx(2.1)
        assert out["order_size_gini"] == pytest.approx(0.65)
        assert out["mutual_info_price_volume"] == pytest.approx(0.3)

    def test_defaults_zero(self):
        out = compute_group_ne(_runtime(), _now_ms(), {})
        assert all(v == 0.0 for v in out.values())


# ---------------------------------------------------------------------------
# Group NF — Mean Reversion / Stationarity
# ---------------------------------------------------------------------------

class TestGroupNF:
    def test_reads_mean_reversion_attrs(self):
        rt = _runtime(half_life_mean_reversion=12.5, adf_pvalue_50=0.03)
        out = compute_group_nf(rt, _now_ms(), {})
        assert out["half_life_mean_reversion"] == pytest.approx(12.5)
        assert out["adf_pvalue_50"] == pytest.approx(0.03)

    def test_zscore_mid_to_vwap_computed(self):
        rt = _runtime(last_book_mid=100.0, mid_vwap_diff_std=2.0)
        indicators = {"roll_vwap_px": 96.0}
        out = compute_group_nf(rt, _now_ms(), indicators)
        # (100 - 96) / 2.0 = 2.0
        assert out["zscore_mid_to_vwap"] == pytest.approx(2.0)

    def test_zscore_zero_when_no_data(self):
        rt = _runtime()
        out = compute_group_nf(rt, _now_ms(), {})
        assert out["zscore_mid_to_vwap"] == 0.0

    def test_zscore_zero_sigma(self):
        """Zero sigma must not divide by zero."""
        rt = _runtime(last_book_mid=100.0, mid_vwap_diff_std=0.0)
        indicators = {"roll_vwap_px": 96.0}
        out = compute_group_nf(rt, _now_ms(), indicators)
        assert out["zscore_mid_to_vwap"] == 0.0


# ---------------------------------------------------------------------------
# Group NX — Advanced Interactions
# ---------------------------------------------------------------------------

class TestGroupNX:
    def test_vpin_x_funding_positive(self):
        indicators = {"vpin_rolling": 0.8, "funding_rate_bps": 5.0}
        out = compute_group_nx(_runtime(), _now_ms(), indicators)
        assert out["vpin_x_funding"] == pytest.approx(0.8)  # 0.8 * sign(5) = 0.8

    def test_vpin_x_funding_negative(self):
        indicators = {"vpin_rolling": 0.8, "funding_rate_bps": -3.0}
        out = compute_group_nx(_runtime(), _now_ms(), indicators)
        assert out["vpin_x_funding"] == pytest.approx(-0.8)  # 0.8 * sign(-3) = -0.8

    def test_vpin_x_funding_zero(self):
        indicators = {"vpin_rolling": 0.8, "funding_rate_bps": 0.0}
        out = compute_group_nx(_runtime(), _now_ms(), indicators)
        assert out["vpin_x_funding"] == pytest.approx(0.0)  # 0.8 * 0 = 0

    def test_hurst_x_vol_regime(self):
        rt = _runtime(hurst_exp_50=0.65, vol_regime_code=3)
        indicators = {}
        out = compute_group_nx(rt, _now_ms(), indicators)
        assert out["hurst_x_vol_regime"] == pytest.approx(0.65 * 3)

    def test_entropy_x_spread(self):
        indicators = {"price_entropy_50": 2.0, "spread_bps": 3.0}
        out = compute_group_nx(_runtime(), _now_ms(), indicators)
        assert out["entropy_x_spread"] == pytest.approx(6.0)

    def test_depth_resil_x_sweep(self):
        indicators = {"depth_resilience_half_life": 100.0, "aggressive_sweep_ratio": 0.2}
        out = compute_group_nx(_runtime(), _now_ms(), indicators)
        assert out["depth_resil_x_sweep"] == pytest.approx(20.0)

    def test_amihud_x_oi_delta(self):
        indicators = {"amihud_illiquidity": 0.005, "open_interest_delta": 2000.0}
        out = compute_group_nx(_runtime(), _now_ms(), indicators)
        assert out["amihud_x_oi_delta"] == pytest.approx(10.0)

    def test_defaults_zero(self):
        out = compute_group_nx(_runtime(), _now_ms(), {})
        for k in ("vpin_x_funding", "hurst_x_vol_regime", "entropy_x_spread",
                   "depth_resil_x_sweep", "amihud_x_oi_delta"):
            assert out[k] == 0.0


# ---------------------------------------------------------------------------
# inject_v13_of_features (master entry point)
# ---------------------------------------------------------------------------

class TestInjectV13OfFeatures:
    def test_all_keys_present(self):
        rt = _runtime()
        indicators: Dict[str, Any] = {}
        inject_v13_of_features(runtime=rt, now_ms=_now_ms(), indicators=indicators)

        for k in _V13_OF_NEW_KEY_SET:
            assert k in indicators, f"key {k!r} missing from indicators"

    def test_no_nan_on_empty_runtime(self):
        rt = _runtime()
        indicators: Dict[str, Any] = {}
        inject_v13_of_features(runtime=rt, now_ms=_now_ms(), indicators=indicators)
        for k, v in indicators.items():
            if isinstance(v, float):
                assert not math.isnan(v), f"NaN for key {k!r}"

    def test_does_not_overwrite_existing_indicators(self):
        """inject_ must not erase existing indicator values set before it runs."""
        rt = _runtime(garman_klass_vol=0.02)
        indicators: Dict[str, Any] = {"cvd_slope": 1.5, "vpin_rolling": 0.5}
        inject_v13_of_features(runtime=rt, now_ms=_now_ms(), indicators=indicators)
        assert indicators["cvd_slope"] == 1.5  # must be preserved
        assert indicators["garman_klass_vol"] == pytest.approx(0.02)

    def test_schema_keys_subset(self):
        """All new v13_of keys must be a subset of V13_OF_NUMERIC_KEYS."""
        expected = frozenset(V13_OF_NUMERIC_KEYS)
        missing = _V13_OF_NEW_KEY_SET - expected
        assert not missing, f"Keys not in V13_OF_NUMERIC_KEYS: {missing}"

    def test_key_count(self):
        """Exactly 28 new keys."""
        assert len(_V13_OF_NEW_KEY_SET) == 28

    def test_nx_interactions_use_injected_values(self):
        """NX group should use values from earlier groups (NA/NB/NC) injected in same call."""
        rt = _runtime(
            pin_estimate=0.4,
            aggressive_sweep_ratio=0.3,
            depth_resilience_half_life=200.0,
            amihud_illiquidity=0.01,
        )
        indicators: Dict[str, Any] = {
            "vpin_rolling": 0.6,
            "funding_rate_bps": 10.0,
            "open_interest_delta": 500.0,
        }
        inject_v13_of_features(runtime=rt, now_ms=_now_ms(), indicators=indicators)
        # After inject, NB/NC keys populate indicators, then NX uses them
        # depth_resil_x_sweep = depth_resilience_half_life × aggressive_sweep_ratio
        assert indicators["depth_resil_x_sweep"] == pytest.approx(200.0 * 0.3)
        # amihud_x_oi_delta = amihud_illiquidity × open_interest_delta
        assert indicators["amihud_x_oi_delta"] == pytest.approx(0.01 * 500.0)
        # vpin_x_funding = 0.6 * sign(10) = 0.6
        assert indicators["vpin_x_funding"] == pytest.approx(0.6)
