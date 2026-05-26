"""Tests for symbol balancing and slice reporting in train_ml_scorer_v3."""
from __future__ import annotations

import numpy as np

from scripts.train_ml_scorer_v3 import (
    _apply_symbol_balance_policy,
    _build_feature_row,
    _build_slice_report,
    _drop_feature_columns,
    _parse_feature_drop_list,
    _parse_symbol_feature_masks,
)


class TestSymbolBalancePolicy:
    def test_none_mode_keeps_rows(self):
        cols = ["ts", "symbol", "r"]
        rows = [(i, "BTCUSDT", 0.1) for i in range(10)]
        out, weights, meta = _apply_symbol_balance_policy(
            rows,
            cols,
            mode="none",
            max_samples_per_symbol=5,
            target_count=5,
        )
        assert out == rows
        assert weights is None
        assert meta["mode"] == "none"

    def test_cap_limits_each_symbol_and_keeps_tail(self):
        cols = ["ts", "symbol"]
        rows = [(i, "ETHUSDT") for i in range(20)]
        out, weights, meta = _apply_symbol_balance_policy(
            rows,
            cols,
            mode="cap",
            max_samples_per_symbol=5,
            target_count=5,
        )
        assert weights is None
        assert len(out) == 5
        assert [r[0] for r in out] == [15, 16, 17, 18, 19]
        assert meta["capped_counts"]["ETHUSDT"] == 5

    def test_weight_mode_downweights_dominant_symbol(self):
        cols = ["ts", "symbol"]
        rows = []
        rows.extend((i, "BTCUSDT") for i in range(4))
        rows.extend((i + 100, "ETHUSDT") for i in range(2))
        out, weights, meta = _apply_symbol_balance_policy(
            rows,
            cols,
            mode="weight",
            max_samples_per_symbol=0,
            target_count=2,
        )
        assert out == rows
        assert weights is not None
        assert np.allclose(weights[:4], np.array([0.5, 0.5, 0.5, 0.5]))
        assert np.allclose(weights[4:], np.array([1.0, 1.0]))
        assert meta["mode"] == "weight"
        assert meta["symbol_weights"]["BTCUSDT"] == 0.5
        assert meta["symbol_weights"]["ETHUSDT"] == 1.0


class TestSliceReport:
    def test_slice_report_contains_symbol_regime_and_session(self):
        rows = [
            {"symbol": "BTCUSDT", "regime_bucket": "trend", "session_bucket": "ny"},
            {"symbol": "BTCUSDT", "regime_bucket": "trend", "session_bucket": "ny"},
            {"symbol": "ETHUSDT", "regime_bucket": "range", "session_bucket": "asia"},
            {"symbol": "ETHUSDT", "regime_bucket": "range", "session_bucket": "asia"},
        ]
        y_true = np.array([1.0, 0.0, 1.0, 0.0], dtype=np.float64)
        y_score = np.array([0.9, 0.1, 0.8, 0.2], dtype=np.float64)
        report = _build_slice_report(rows, y_true, y_score)

        assert set(report.keys()) == {"symbol", "regime", "session"}
        assert report["symbol"][0]["key"] == "BTCUSDT"
        assert report["symbol"][0]["n"] == 2
        assert report["regime"][1]["key"] == "range"
        assert report["session"][0]["top5_hit_rate"] in (0.0, 1.0)


class TestSymbolFeatureMasks:
    def test_parse_symbol_feature_masks(self):
        parsed = _parse_symbol_feature_masks("BTCUSDT:direction_long;ETHUSDT:obi_spread,is_extreme_outlier")
        assert parsed == {
            "BTCUSDT": {"direction_long"},
            "ETHUSDT": {"obi_spread", "is_extreme_outlier"},
        }

    def test_build_feature_row_masks_symbol_specific_features(self):
        row = {
            "symbol": "BTCUSDT",
            "direction": 1,
            "atr_14": 1.0,
            "obi_avg_20": 0.25,
            "weak_progress_ratio": 0.5,
            "l3_spread_bps": 0.1,
            "l3_microprice_shift_bps_20": 0.2,
            "l3_microprice_velocity_bps": 0.3,
            "l3_obi_5": 1.2,
            "l3_obi_20": 0.4,
            "l3_obi_50": 0.7,
            "l3_obi_persistence_score": 0.5,
            "l3_cancel_to_trade_bid_5s": 0.1,
            "l3_cancel_to_trade_ask_5s": 0.2,
            "l3_cancel_to_trade_bid_20s": 0.3,
            "l3_cancel_to_trade_ask_20s": 0.4,
            "l3_queue_pressure_bid": 0.9,
            "l3_queue_pressure_ask": 0.6,
            "l3_market_depth_imbalance": 0.05,
            "ind_delta_z": 0.7,
            "ind_exec_risk_bps": 0.2,
            "ind_ofi_z": 0.8,
            "ind_spread_bps": 0.3,
            "ind_burst_z": 0.1,
            "ind_data_health": 1.0,
            "ind_fill_prob_proxy": 0.2,
        }
        features = _build_feature_row(
            row,
            symbol_feature_masks={"BTCUSDT": {"direction_long", "obi_spread", "is_extreme_outlier"}},
        )
        assert features[24] == 0.0  # direction_long
        assert features[26] == 0.0  # obi_spread
        assert features[31] == 0.0  # is_extreme_outlier


class TestGlobalFeatureDrop:
    def test_parse_feature_drop_list(self):
        parsed = _parse_feature_drop_list("is_extreme_outlier, obi_spread ,,")
        assert parsed == {"is_extreme_outlier", "obi_spread"}

    def test_drop_feature_columns_removes_named_features(self):
        X = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float64)
        names = ["a", "is_extreme_outlier", "c"]
        out, kept = _drop_feature_columns(X, names, {"is_extreme_outlier"})
        assert kept == ["a", "c"]
        assert out.shape == (2, 2)
        assert np.allclose(out, np.array([[1.0, 3.0], [4.0, 6.0]]))
