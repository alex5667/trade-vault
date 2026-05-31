from __future__ import annotations

"""
Unit tests for core/v12_of_features.py

Tests: all compute groups, inject_v12_of_features master function,
       temporal determinism (MC), fail-open on missing attributes, MX derivation.
"""

import math
from types import SimpleNamespace
from typing import Any

import pytest

from core.ml_feature_schema_v12_of import V12_OF_NUMERIC_KEYS
from core.v12_of_features import (
    compute_group_ma,
    compute_group_mb,
    compute_group_mc,
    compute_group_md,
    compute_group_me,
    compute_group_mx,
    inject_v12_of_features,
)
from utils.time_utils import get_ny_time_millis

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _runtime(**kwargs) -> Any:
    return SimpleNamespace(**kwargs)


def _now_ms() -> int:
    return get_ny_time_millis()


# ---------------------------------------------------------------------------
# Group MA
# ---------------------------------------------------------------------------

class TestGroupMA:
    def test_reads_runtime_attrs(self):
        rt = _runtime(trade_arrival_rate_hz=12.5, large_trade_ratio=0.15,
                      tick_direction_run=7, trade_size_entropy=2.3)
        out = compute_group_ma(rt, _now_ms(), {})
        assert out["trade_arrival_rate_hz"] == pytest.approx(12.5)
        assert out["large_trade_ratio"] == pytest.approx(0.15)
        assert out["tick_direction_run"] == pytest.approx(7.0)
        assert out["trade_size_entropy"] == pytest.approx(2.3)

    def test_defaults_to_zero_on_missing(self):
        rt = _runtime()
        out = compute_group_ma(rt, _now_ms(), {})
        for k in ("trade_arrival_rate_hz", "large_trade_ratio", "tick_direction_run", "trade_size_entropy"):
            assert out[k] == 0.0, f"expected 0.0 for {k}"

    def test_no_panic_on_none_attrs(self):
        rt = _runtime(trade_arrival_rate_hz=None, large_trade_ratio=None)
        out = compute_group_ma(rt, _now_ms(), {})
        assert out["trade_arrival_rate_hz"] == 0.0


# ---------------------------------------------------------------------------
# Group MB
# ---------------------------------------------------------------------------

class TestGroupMB:
    def test_reads_all_keys(self):
        rt = _runtime(quote_stuffing_score=0.8, depth_migration_bps=5.0,
                      level2_wap_divergence=-2.1, bid_ask_queue_imbalance=0.3)
        out = compute_group_mb(rt, _now_ms(), {})
        assert out["quote_stuffing_score"] == pytest.approx(0.8)
        assert out["depth_migration_bps"] == pytest.approx(5.0)
        assert out["level2_wap_divergence"] == pytest.approx(-2.1)
        assert out["bid_ask_queue_imbalance"] == pytest.approx(0.3)

    def test_defaults_zero(self):
        out = compute_group_mb(_runtime(), _now_ms(), {})
        for k in ("quote_stuffing_score", "depth_migration_bps", "level2_wap_divergence", "bid_ask_queue_imbalance"):
            assert out[k] == 0.0


# ---------------------------------------------------------------------------
# Group MC (temporal)
# ---------------------------------------------------------------------------

class TestGroupMC:
    def test_minutes_to_funding_deterministic(self):
        # Use a fixed ts: 2024-01-01 00:00:00 UTC = 1704067200000 ms
        # This is on a funding boundary → next is +8h = 28800000 ms = 480 min
        ts_ms = 1704067200000
        rt = _runtime()
        out = compute_group_mc(rt, ts_ms, {})
        # Next funding is exactly 8h away → 480 minutes
        assert out["minutes_to_funding"] == pytest.approx(480.0, abs=1.0)

    def test_minutes_to_funding_non_zero(self):
        # Use offset ts_ms so it's not on boundary
        ts_ms = 1704067200000 + 3600000  # +1h into funding cycle
        rt = _runtime()
        out = compute_group_mc(rt, ts_ms, {})
        # Should be 7h = 420 min remaining
        assert out["minutes_to_funding"] == pytest.approx(420.0, abs=1.0)

    def test_session_overlap_flag_in_overlap(self):
        # 14:00 UTC = hour 14 → NY∩London overlap (13–17)
        ts_14utc = 1704067200000 + 14 * 3600000  # 14:00 UTC
        rt = _runtime()
        out = compute_group_mc(rt, ts_14utc, {})
        assert out["session_overlap_flag"] == 1.0

    def test_session_overlap_flag_outside(self):
        # 20:00 UTC → no overlap
        ts_20utc = 1704067200000 + 20 * 3600000
        rt = _runtime()
        out = compute_group_mc(rt, ts_20utc, {})
        assert out["session_overlap_flag"] == 0.0

    def test_time_since_last_liq_ms(self):
        now = _now_ms()
        rt = _runtime(liq_last_ts_ms=now - 5000)
        out = compute_group_mc(rt, now, {})
        assert out["time_since_last_liq_ms"] == pytest.approx(5000.0, abs=50.0)

    def test_time_since_last_liq_ms_no_data(self):
        rt = _runtime()
        out = compute_group_mc(rt, _now_ms(), {})
        assert out["time_since_last_liq_ms"] == 0.0


# ---------------------------------------------------------------------------
# Group MD (cross-asset, requires go-worker)
# ---------------------------------------------------------------------------

class TestGroupMD:
    def test_reads_crossasset_attrs(self):
        rt = _runtime(eth_btc_corr_5m=0.75, perp_spot_basis_bps=15.3, stable_coin_flow_delta=-0.04)
        out = compute_group_md(rt, _now_ms(), {})
        assert out["eth_btc_corr_5m"] == pytest.approx(0.75)
        assert out["perp_spot_basis_bps"] == pytest.approx(15.3)
        assert out["stable_coin_flow_delta"] == pytest.approx(-0.04)

    def test_defaults_zero_without_goworker(self):
        out = compute_group_md(_runtime(), _now_ms(), {})
        assert all(v == 0.0 for v in out.values())


# ---------------------------------------------------------------------------
# Group ME (meta-signal)
# ---------------------------------------------------------------------------

class TestGroupME:
    def test_reads_meta_attrs(self):
        now = _now_ms()
        rt = _runtime(signal_count_1h=5, last_trade_pnl_bps=12.5, abs_lvl_calib_last_ts_ms=now - 30000)
        out = compute_group_me(rt, now, {})
        assert out["signal_frequency_1h"] == pytest.approx(5.0)
        assert out["last_trade_outcome_raw"] == pytest.approx(12.5)
        assert out["calibration_age_ms"] == pytest.approx(30000.0, abs=100.0)

    def test_zero_calib_ts(self):
        rt = _runtime(abs_lvl_calib_last_ts_ms=0)
        out = compute_group_me(rt, _now_ms(), {})
        assert out["calibration_age_ms"] == 0.0


# ---------------------------------------------------------------------------
# Group MX (derived)
# ---------------------------------------------------------------------------

class TestGroupMX:
    def test_cvd_divergence_detected(self):
        # cvd_slope positive, momentum negative → divergence
        indicators = {"cvd_slope": 1.0, "momentum_10s": -0.5, "ofi": 3.0}
        out = compute_group_mx(_runtime(), _now_ms(), indicators)
        assert out["cvd_divergence_from_price"] == 1.0

    def test_cvd_divergence_absent(self):
        # Both positive → no divergence
        indicators = {"cvd_slope": 1.0, "momentum_10s": 0.5, "ofi": 3.0}
        out = compute_group_mx(_runtime(), _now_ms(), indicators)
        assert out["cvd_divergence_from_price"] == 0.0

    def test_order_imbalance_momentum(self):
        # ofi now=5.0, ofi_prev=3.0 → delta=2.0
        rt = _runtime(ofi_prev_tick=3.0)
        indicators = {"ofi": 5.0, "cvd_slope": 1.0, "momentum_10s": 1.0}
        out = compute_group_mx(rt, _now_ms(), indicators)
        assert out["order_imbalance_momentum"] == pytest.approx(2.0)

    def test_percentile_ranks(self):
        rt = _runtime(spread_bps_rank_1d=0.7, atr_bps_rank_30d=0.4)
        out = compute_group_mx(rt, _now_ms(), {})
        assert out["spread_percentile_rank_1d"] == pytest.approx(0.7)
        assert out["atr_percentile_rank_30d"] == pytest.approx(0.4)


# ---------------------------------------------------------------------------
# inject_v12_of_features (master entry point)
# ---------------------------------------------------------------------------

class TestInjectV12OfFeatures:
    def test_all_keys_present(self):
        rt = _runtime()
        indicators: dict[str, Any] = {}
        inject_v12_of_features(runtime=rt, now_ms=_now_ms(), indicators=indicators)

        # The 21 new group MA/MB/MC/MD/ME/MX keys must all be present
        new_keys = {
            "trade_arrival_rate_hz", "large_trade_ratio", "tick_direction_run", "trade_size_entropy",
            "quote_stuffing_score", "depth_migration_bps", "level2_wap_divergence", "bid_ask_queue_imbalance",
            "minutes_to_funding", "session_overlap_flag", "time_since_last_liq_ms",
            "eth_btc_corr_5m", "perp_spot_basis_bps", "stable_coin_flow_delta",
            "signal_frequency_1h", "last_trade_outcome_raw", "calibration_age_ms",
            "spread_percentile_rank_1d", "cvd_divergence_from_price", "order_imbalance_momentum", "atr_percentile_rank_30d",
        }
        for k in new_keys:
            assert k in indicators, f"key {k!r} missing from indicators"

    def test_no_nan_on_empty_runtime(self):
        rt = _runtime()
        indicators: dict[str, Any] = {}
        inject_v12_of_features(runtime=rt, now_ms=_now_ms(), indicators=indicators)
        for k, v in indicators.items():
            if isinstance(v, float):
                assert not math.isnan(v), f"NaN for key {k!r}"

    def test_does_not_overwrite_existing_indicators(self):
        """inject_ must not erase existing indicator values set before it runs."""
        rt = _runtime(eth_btc_corr_5m=0.8)
        # Pre-set some keys that exist in v10_of too
        indicators: dict[str, Any] = {"cvd_slope": 1.5, "momentum_10s": 1.0, "ofi": 2.0}
        inject_v12_of_features(runtime=rt, now_ms=_now_ms(), indicators=indicators)
        assert indicators["cvd_slope"] == 1.5   # must be preserved
        assert indicators["eth_btc_corr_5m"] == pytest.approx(0.8)

    def test_minutes_to_funding_always_positive(self):
        rt = _runtime()
        for ts in [1704067200000, 1704067200000 + 7200000, _now_ms()]:
            indicators: dict[str, Any] = {}
            inject_v12_of_features(runtime=rt, now_ms=ts, indicators=indicators)
            assert indicators["minutes_to_funding"] >= 0.0

    def test_schema_keys_subset(self):
        """All new v12_of keys must be a subset of V12_OF_NUMERIC_KEYS."""
        expected = frozenset(V12_OF_NUMERIC_KEYS)
        new_keys = {
            "trade_arrival_rate_hz", "large_trade_ratio", "tick_direction_run", "trade_size_entropy",
            "quote_stuffing_score", "depth_migration_bps", "level2_wap_divergence", "bid_ask_queue_imbalance",
            "minutes_to_funding", "session_overlap_flag", "time_since_last_liq_ms",
            "eth_btc_corr_5m", "perp_spot_basis_bps", "stable_coin_flow_delta",
            "signal_frequency_1h", "last_trade_outcome_raw", "calibration_age_ms",
            "spread_percentile_rank_1d", "cvd_divergence_from_price", "order_imbalance_momentum", "atr_percentile_rank_30d",
        }
        missing = new_keys - expected
        assert not missing, f"Keys not in V12_OF_NUMERIC_KEYS: {missing}"
