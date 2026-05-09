from __future__ import annotations

"""
Tests for world-practice LOB pressure metrics (v22) in tick_processor:
- trade_micro_mid_div_bps, trade_micro_shift_bps
- trade_qi_mean, trade_qi_max_abs, trade_qi_slope
- trade_depth_slope_bid, trade_depth_slope_ask, trade_depth_slope_imb, trade_depth_slope_imb_norm
- trade_depth_convexity_bid, trade_depth_convexity_ask, trade_depth_convexity_imb
- trade_dw_obi, trade_dw_obi_z, trade_dw_obi_stability_score, trade_dw_obi_stable_secs, trade_dw_obi_stable

Tests validate:
1. All 17 metrics are importable from services.orderflow.metrics.
2. Correct gauge registration (labels=[sym, bucket]).
3. Metric emission logic from indicators dict — including depth_slope_imb_norm normalization formula.
4. Fail-safe: None / missing values default to 0.0.
5. depth_slope_imb_norm = (bid - ask) / (|bid| + |ask| + 1e-9).
"""


import importlib
import math

import pytest

# ──────────────────────────────────────────────────────────────────────────────
# 1. Import smoke: all 17 new metrics must be importable
# ──────────────────────────────────────────────────────────────────────────────

LOB_PRESSURE_METRICS = [
    "trade_qi_mean",
    "trade_qi_max_abs",
    "trade_qi_slope",
    "trade_micro_mid_div_bps",
    "trade_micro_shift_bps",
    "trade_depth_slope_bid",
    "trade_depth_slope_ask",
    "trade_depth_slope_imb",
    "trade_depth_slope_imb_norm",
    "trade_depth_convexity_bid",
    "trade_depth_convexity_ask",
    "trade_depth_convexity_imb",
    "trade_dw_obi",
    "trade_dw_obi_z",
    "trade_dw_obi_stability_score",
    "trade_dw_obi_stable_secs",
    "trade_dw_obi_stable",
]


@pytest.mark.parametrize("metric_name", LOB_PRESSURE_METRICS)
def test_metric_importable(metric_name: str) -> None:
    """All LOB pressure v22 metrics must be importable from services.orderflow.metrics."""
    mod = importlib.import_module("services.orderflow.metrics")
    assert hasattr(mod, metric_name), f"Metric '{metric_name}' missing from services.orderflow.metrics"


@pytest.mark.parametrize("metric_name", LOB_PRESSURE_METRICS)
def test_metric_has_sym_bucket_labels(metric_name: str) -> None:
    """Each gauge must have ['sym', 'bucket'] label names."""
    mod = importlib.import_module("services.orderflow.metrics")
    gauge = getattr(mod, metric_name)
    # prometheus_client Gauge stores _labelnames
    labels = list(gauge._labelnames) if hasattr(gauge, "_labelnames") else []
    assert "sym" in labels, f"'{metric_name}' missing 'sym' label"
    assert "bucket" in labels, f"'{metric_name}' missing 'bucket' label"


# ──────────────────────────────────────────────────────────────────────────────
# 2. Emission logic unit tests (pure Python, no Redis / no TickProcessor)
# ──────────────────────────────────────────────────────────────────────────────

def _emit_lob_pressure(indicators: dict, sym: str = "BTCUSDT", bk: str = "NORMAL") -> dict:
    """
    Replicate the exact emission logic from tick_processor.py
    so we can unit-test it in isolation.
    Returns a dict of {metric_name: value_set}.
    """
    results: dict = {}

    results["lob_qi_mean"] = float(indicators.get("lob_qi_mean", 0.0) or 0.0)
    results["lob_qi_max_abs"] = float(indicators.get("lob_qi_max_abs", 0.0) or 0.0)
    results["lob_qi_slope"] = float(indicators.get("lob_qi_slope", 0.0) or 0.0)

    results["lob_micro_mid_div_bps"] = float(indicators.get("lob_micro_mid_div_bps", 0.0) or 0.0)
    results["lob_micro_shift_bps"] = float(indicators.get("lob_micro_shift_bps", 0.0) or 0.0)

    _dsb = float(indicators.get("lob_depth_slope_bid", 0.0) or 0.0)
    _dsa = float(indicators.get("lob_depth_slope_ask", 0.0) or 0.0)
    _dsi = float(indicators.get("lob_depth_slope_imb", (_dsb - _dsa)) or 0.0)
    results["lob_depth_slope_bid"] = _dsb
    results["lob_depth_slope_ask"] = _dsa
    results["lob_depth_slope_imb"] = _dsi
    _den = abs(_dsb) + abs(_dsa) + 1e-9
    results["lob_depth_slope_imb_norm"] = float(_dsi) / float(_den)

    results["lob_depth_convexity_bid"] = float(indicators.get("lob_depth_convexity_bid", 0.0) or 0.0)
    results["lob_depth_convexity_ask"] = float(indicators.get("lob_depth_convexity_ask", 0.0) or 0.0)
    results["lob_depth_convexity_imb"] = float(indicators.get("lob_depth_convexity_imb", 0.0) or 0.0)

    results["lob_dw_obi"] = float(indicators.get("lob_dw_obi", 0.0) or 0.0)
    results["lob_dw_obi_z"] = float(indicators.get("lob_dw_obi_z", 0.0) or 0.0)
    results["lob_dw_obi_stability_score"] = float(indicators.get("lob_dw_obi_stability_score", 0.0) or 0.0)
    results["lob_dw_obi_stable_secs"] = float(indicators.get("lob_dw_obi_stable_secs", 0.0) or 0.0)
    results["lob_dw_obi_stable"] = float(indicators.get("lob_dw_obi_stable", 0) or 0)

    return results


class TestEmissionFromIndicators:
    def test_all_keys_present_full_indicators(self) -> None:
        """All 17 output keys must be present when indicators is fully populated."""
        ind = {
            "lob_qi_mean": 0.12,
            "lob_qi_max_abs": 0.45,
            "lob_qi_slope": -0.03,
            "lob_micro_mid_div_bps": 1.5,
            "lob_micro_shift_bps": 0.2,
            "lob_depth_slope_bid": 50.0,
            "lob_depth_slope_ask": 30.0,
            "lob_depth_slope_imb": 20.0,
            "lob_depth_convexity_bid": 0.15,
            "lob_depth_convexity_ask": 0.10,
            "lob_depth_convexity_imb": 0.05,
            "lob_dw_obi": 0.35,
            "lob_dw_obi_z": 2.8,
            "lob_dw_obi_stability_score": 0.72,
            "lob_dw_obi_stable_secs": 180.0,
            "lob_dw_obi_stable": 1,
        }
        res = _emit_lob_pressure(ind)
        assert len(res) == 17

    def test_values_match_indicators(self) -> None:
        ind = {"lob_dw_obi": 0.5, "lob_dw_obi_z": 2.1, "lob_micro_mid_div_bps": 3.1}
        res = _emit_lob_pressure(ind)
        assert res["lob_dw_obi"] == pytest.approx(0.5)
        assert res["lob_dw_obi_z"] == pytest.approx(2.1)
        assert res["lob_micro_mid_div_bps"] == pytest.approx(3.1)

    def test_missing_indicators_default_to_zero(self) -> None:
        """Missing keys must default to 0.0, never raise."""
        res = _emit_lob_pressure({})
        for k, v in res.items():
            assert v == pytest.approx(0.0), f"Expected 0.0 for missing {k}, got {v}"

    def test_none_values_default_to_zero(self) -> None:
        """None values in indicators must default to 0.0 (or 0 for flags)."""
        ind = dict.fromkeys(["lob_qi_mean", "lob_dw_obi", "lob_dw_obi_z", "lob_micro_mid_div_bps", "lob_dw_obi_stable"])
        res = _emit_lob_pressure(ind)
        assert res["lob_qi_mean"] == 0.0
        assert res["lob_dw_obi"] == 0.0
        assert res["lob_dw_obi_stable"] == 0.0

    def test_no_nan_or_inf(self) -> None:
        """No metric should produce NaN or Inf."""
        ind = {"lob_depth_slope_bid": 1e15, "lob_depth_slope_ask": -1e15}
        res = _emit_lob_pressure(ind)
        for k, v in res.items():
            assert not math.isnan(v), f"{k} is NaN"
            assert not math.isinf(v), f"{k} is Inf"


class TestDepthSlopeImbNorm:
    """Verify normalization formula: imb / (|bid| + |ask| + 1e-9)."""

    def test_normalization_formula_positive(self) -> None:
        bid, ask = 80.0, 20.0
        ind = {"lob_depth_slope_bid": bid, "lob_depth_slope_ask": ask}
        # imb = bid - ask = 60; norm = 60 / (80 + 20 + 1e-9) ≈ 0.6
        res = _emit_lob_pressure(ind)
        expected_imb = bid - ask  # 60.0 (imb fallback used since no explicit key)
        expected_norm = expected_imb / (abs(bid) + abs(ask) + 1e-9)
        assert res["lob_depth_slope_imb"] == pytest.approx(expected_imb)
        assert res["lob_depth_slope_imb_norm"] == pytest.approx(expected_norm, abs=1e-9)

    def test_normalization_bounded_minus_one_to_one(self) -> None:
        # any bid/ask combination should produce norm in [-1, 1]
        cases = [
            (0.0, 0.0),
            (100.0, 0.0),
            (0.0, 100.0),
            (50.0, 50.0),
            (-30.0, 70.0),
        ]
        for bid, ask in cases:
            ind = {"lob_depth_slope_bid": bid, "lob_depth_slope_ask": ask}
            res = _emit_lob_pressure(ind)
            norm = res["lob_depth_slope_imb_norm"]
            assert -1.0 - 1e-9 <= norm <= 1.0 + 1e-9, f"norm={norm} out of bounds for bid={bid} ask={ask}"

    def test_zero_zero_produces_zero_norm(self) -> None:
        res = _emit_lob_pressure({})
        # 0 / (0 + 0 + 1e-9) = 0
        assert res["lob_depth_slope_imb_norm"] == pytest.approx(0.0, abs=1e-6)

    def test_explicit_imb_key_overrides_bid_minus_ask(self) -> None:
        """If lob_depth_slope_imb is explicitly set, it takes precedence over bid-ask."""
        ind = {
            "lob_depth_slope_bid": 80.0,
            "lob_depth_slope_ask": 20.0,
            "lob_depth_slope_imb": 99.0,  # explicit override
        }
        res = _emit_lob_pressure(ind)
        assert res["lob_depth_slope_imb"] == pytest.approx(99.0)


class TestDWOBIStableFlag:
    def test_stable_flag_one(self) -> None:
        res = _emit_lob_pressure({"lob_dw_obi_stable": 1})
        assert res["lob_dw_obi_stable"] == 1.0

    def test_stable_flag_zero(self) -> None:
        res = _emit_lob_pressure({"lob_dw_obi_stable": 0})
        assert res["lob_dw_obi_stable"] == 0.0

    def test_stable_flag_bool_true(self) -> None:
        res = _emit_lob_pressure({"lob_dw_obi_stable": True})
        assert res["lob_dw_obi_stable"] == 1.0


class TestAlertFilePresence:
    """Sanity checks for the Prometheus alert YAML file."""

    def _load_yaml(self, path: str) -> dict:
        import yaml
        with open(path) as f:
            return yaml.safe_load(f)

    def test_alerts_file_parseable(self) -> None:
        import os
        path = "orderflow_services/prometheus_alerts_world_practice_lob_pressure_v1.yml"
        if not os.path.exists(path):
            pytest.skip(f"Alert file not found at {path}")
        data = self._load_yaml(path)
        assert data is not None

    def test_all_expected_alert_names_present(self) -> None:
        import os

        import yaml
        path = "orderflow_services/prometheus_alerts_world_practice_lob_pressure_v1.yml"
        if not os.path.exists(path):
            pytest.skip(f"Alert file not found at {path}")
        with open(path) as f:
            data = yaml.safe_load(f)
        # groups: key at root → data is a dict with key "groups" (list), or a plain list
        groups = data.get("groups", data) if isinstance(data, dict) else data
        # Collect all alert names
        alert_names = set()
        for group in groups:
            for rule in group.get("rules", []):
                if "alert" in rule:
                    alert_names.add(rule["alert"])
        expected = {
            "OF_WP_MicroMidDivHigh_Warn",
            "OF_WP_MicroShiftSpike_Warn",
            "OF_WP_DWOBIStableHigh_Warn",
            "OF_WP_DepthConvexityImbHigh_Warn",
            "OF_WP_LobPressureSnapshotsStuckZero_Crit",
        }
        missing = expected - alert_names
        assert not missing, f"Missing alert names: {missing}"

    def test_alert_severities_valid(self) -> None:
        import os

        import yaml
        path = "orderflow_services/prometheus_alerts_world_practice_lob_pressure_v1.yml"
        if not os.path.exists(path):
            pytest.skip(f"Alert file not found at {path}")
        with open(path) as f:
            data = yaml.safe_load(f)
        groups = data.get("groups", data) if isinstance(data, dict) else data
        for group in groups:
            for rule in group.get("rules", []):
                if "alert" in rule:
                    sev = rule.get("labels", {}).get("severity", "")
                    assert sev in ("warning", "critical"), (
                        f"Alert {rule['alert']} has unexpected severity: {sev}"
                    )
