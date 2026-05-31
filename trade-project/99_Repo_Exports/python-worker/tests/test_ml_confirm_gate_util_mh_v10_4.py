from utils.time_utils import get_ny_time_millis

"""
Unit test for MLConfirmGate util_mh_v1 (v10.4 champion JSON compatibility).

Tests:
- Floor selection (global/by_bucket)
- Best horizon selection (score_h = util_h - unc_k*unc_h)
- Missing critical features handling (ENFORCE vs SHADOW)
- exec_risk_norm derivation
- Scenario normalization (|, space, :, @ separators)
"""

import numpy as np
import pytest

from services.ml_confirm import MLConfirmGate, _scenario_norm


class DummyUtilMH:
    """Mock model compatible with v10.4 util_mh_v1."""
    feature_cols = [
        "f_spread_bps",
        "f_expected_slippage_bps",
        "f_exec_risk_norm",
        "direction_LONG",
        "scenario_v4_range_meanrev",
    ]
    horizons = [60000, 180000]
    unc_k = 0.5

    def predict_util(self, X):
        # util at 60s: 0.01, at 180s: 0.05
        return {60000: np.array([0.01]), 180000: np.array([0.05])}

    def predict_unc(self, X):
        # uncertainty at 60s: 0.02, at 180s: 0.01
        return {60000: np.array([0.02]), 180000: np.array([0.01])}


def test_util_mh_floor_and_best_h():
    """Test that best horizon is selected and floor check works."""
    from unittest.mock import Mock

    import redis

    # Mock Redis
    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)

    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )

    # Set up config and model
    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_run_001",
        "model_path": "/tmp/test_model.pkl",
        "util_floors": {
            "global": {"floor": 0.03},
            "by_bucket": {},
            "unc_k": 0.5,
        },
    }
    gate._model = DummyUtilMH()

    # Test decision
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 2.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert dec.kind == "util_mh_v1"
    assert dec.mode == "SHADOW"
    assert dec.best_h_ms == 180000  # score = 0.05 - 0.5*0.01 = 0.045 > 0.01 - 0.5*0.02 = 0.0
    assert dec.score == pytest.approx(0.045, abs=1e-4)  # 0.05 - 0.5*0.01
    assert dec.floor == 0.03  # global floor
    assert dec.bucket == "range"
    assert dec.allow is True  # 0.045 >= 0.03
    assert dec.model_run_id == "test_run_001"
    assert "util_mh" in dec.reason


def test_util_mh_by_bucket_floor():
    """Test that bucket-specific floor is used when available."""
    from unittest.mock import Mock

    import redis

    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)

    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )

    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_run_002",
        "model_path": "/tmp/test_model.pkl",
        "util_floors": {
            "global": {"floor": 0.05},
            "by_bucket": {
                "range": {"floor": 0.02},  # lower floor for range
                "trend": {"floor": 0.08},
            },
            "unc_k": 0.5,
        },
    }
    gate._model = DummyUtilMH()

    dec = gate.check(
        symbol="ETHUSDT",
        ts_ms=2000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 1.5, "expected_slippage_bps": 1.5},
        rule_score=0.6,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert dec.bucket == "range"
    assert dec.floor == 0.02  # bucket-specific floor, not global
    assert dec.allow is True  # 0.045 >= 0.02


def test_util_mh_missing_critical_enforce():
    """Test that ENFORCE mode blocks when critical features are missing."""
    from unittest.mock import Mock

    import redis

    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)

    gate = MLConfirmGate(
        r=r,
        mode="ENFORCE",
        fail_policy="CLOSED",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )

    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_run_003",
        "model_path": "/tmp/test_model.pkl",
        "util_floors": {"global": {"floor": 0.01}, "unc_k": 0.5},
    }
    gate._model = DummyUtilMH()

    # Missing spread_bps and expected_slippage_bps
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=3000000,
        direction="LONG",
        scenario="trend_continuation",
        indicators={},  # missing critical features
        rule_score=0.8,
        rule_have=3,
        rule_need=3,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert dec.mode == "ENFORCE"
    assert dec.allow is False  # blocked due to missing critical features
    assert "missing_critical" in dec.reason
    assert dec.score == 0.0
    assert dec.floor == 0.0


def test_util_mh_missing_critical_shadow():
    """Test that SHADOW mode allows even when critical features are missing."""
    from unittest.mock import Mock

    import redis

    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)

    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )

    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_run_004",
        "model_path": "/tmp/test_model.pkl",
        "util_floors": {"global": {"floor": 0.01}, "unc_k": 0.5},
    }
    gate._model = DummyUtilMH()

    # Missing spread_bps and expected_slippage_bps
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=4000000,
        direction="SHORT",
        scenario="trend_reversal",
        indicators={},  # missing critical features
        rule_score=0.9,
        rule_have=4,
        rule_need=4,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert dec.mode == "SHADOW"
    # In SHADOW, missing features don't block, but model may still compute
    # (depends on implementation - if model can't run, it would be ERR mode)
    assert dec.missing is not None
    assert len(dec.missing) > 0


def test_util_mh_exec_risk_norm_derivation():
    """Test that exec_risk_norm is derived when missing."""
    import os
    from unittest.mock import Mock

    import redis

    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)

    # Set EXEC_RISK_REF_BPS for test
    os.environ["EXEC_RISK_REF_BPS"] = "10"

    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )

    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_run_005",
        "model_path": "/tmp/test_model.pkl",
        "util_floors": {"global": {"floor": 0.01}, "unc_k": 0.5},
    }
    gate._model = DummyUtilMH()

    indicators = {"spread_bps": 3.0, "expected_slippage_bps": 2.0}
    # exec_risk_norm not in indicators

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=5000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators=indicators,
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # exec_risk_norm should be derived: (3.0 + 2.0) / 10.0 = 0.5
    assert "exec_risk_norm" in indicators
    assert indicators["exec_risk_norm"] == pytest.approx(0.5, abs=1e-4)
    assert indicators["exec_risk_bps"] == pytest.approx(5.0, abs=1e-4)


def test_scenario_norm_unit():
    """Unit test for _scenario_norm() function covering all separator formats."""
    test_cases = [
        # Basic cases
        ("range_meanrev", "range_meanrev"),
        ("RANGE_MEANREV", "range_meanrev"),  # lowercase
        ("  range_meanrev  ", "range_meanrev"),  # strip whitespace

        # Pipe separator (existing)
        ("range_meanrev|extra_info", "range_meanrev"),
        ("range_meanrev|", "range_meanrev"),
        ("continuation|v1|extra", "continuation"),

        # Space separator (existing)
        ("range_meanrev extra", "range_meanrev"),
        ("range_meanrev ", "range_meanrev"),
        ("reversal v2", "reversal"),

        # Colon separator (new)
        ("range_meanrev:v2", "range_meanrev"),
        ("range_meanrev:v3:extra", "range_meanrev"),
        ("continuation:latest", "continuation"),
        ("vol_shock_news_proxy:v1", "vol_shock_news_proxy"),

        # At symbol separator (new)
        ("range_meanrev@X", "range_meanrev"),
        ("range_meanrev@canary", "range_meanrev"),
        ("saw_chop_spoof_proxy@test", "saw_chop_spoof_proxy"),

        # Combined separators (should process in order: |, space, :, @)
        ("range_meanrev|info:v2", "range_meanrev"),  # | takes precedence
        ("range_meanrev info:v2", "range_meanrev"),  # space takes precedence
        ("range_meanrev:v2@X", "range_meanrev"),  # : takes precedence
        ("range_meanrev@X:v2", "range_meanrev"),  # @ takes precedence

        # Edge cases
        ("", ""),
        (None, ""),
        ("|", ""),
        (":", ""),
        ("@", ""),
        (" :@| ", ""),
        ("a|b:c@d", "a"),  # all separators, | wins
    ]

    for input_scenario, expected in test_cases:
        result = _scenario_norm(input_scenario)
        assert result == expected, f"Input: {input_scenario!r}, Expected: {expected!r}, Got: {result!r}"


def test_util_mh_scenario_normalization():
    """Test that scenario normalization works for one-hot encoding in ML gate."""
    from unittest.mock import Mock

    import redis

    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)

    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )

    # Set config and model after initialization to avoid cache refresh overwriting
    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_run_006",
        "model_path": "/tmp/test_model.pkl",
        "util_floors": {"global": {"floor": 0.01}, "unc_k": 0.5},
    }
    gate._model = DummyUtilMH()
    # Set cache timestamp to prevent refresh from overwriting our test config
    gate._cache_loaded_ms = get_ny_time_millis() + 100000  # far future

    # Test all separator formats
    test_scenarios = [
        "range_meanrev|extra_info",  # pipe (existing)
        "range_meanrev extra",       # space (existing)
        "range_meanrev:v2",          # colon (new)
        "range_meanrev@X",           # at symbol (new)
        "range_meanrev",             # clean format
    ]

    for scenario in test_scenarios:
        dec = gate.check(
            symbol="BTCUSDT",
            ts_ms=6000000,
            direction="LONG",
            scenario=scenario,
            indicators={"spread_bps": 2.0, "expected_slippage_bps": 2.0},
            rule_score=0.7,
            rule_have=2,
            rule_need=2,
            cancel_spike_veto=0,
            ok_rule=1,
        )

        # All should normalize to "range_meanrev" and recognize as "range" bucket
        assert dec.bucket == "range", f"Failed for scenario: {scenario!r}, got bucket: {dec.bucket!r}, reason: {dec.reason!r}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

