"""Tests for MLConfirmGate util_mh v10.4 support."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import Dict, Any

import pytest
import numpy as np

from services.ml_confirm_gate import MLConfirmGate, MLConfirmDecision


class DummyUtilMH:
    """Mock util_mh model for testing."""
    feature_cols = ["f_spread_bps", "f_expected_slippage_bps", "f_exec_risk_norm", "direction_LONG", "direction_SHORT", "scenario_v4_range_meanrev"]
    horizons = [60000, 180000]
    unc_k = 0.5

    def predict_util(self, X):
        # prefer 180000 (higher utility)
        return {60000: np.array([0.01]), 180000: np.array([0.05])}

    def predict_unc(self, X):
        return {60000: np.array([0.02]), 180000: np.array([0.01])}


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    r = MagicMock()
    r.get.return_value = None
    return r


@pytest.fixture
def util_mh_cfg():
    """Config for util_mh v10.4 model."""
    return {
        "kind": "util_mh_v1",
        "run_id": "test_run_001",
        "model_path": "/tmp/test_model.joblib",
        "util_floors": {
            "global": {"floor": 0.03},
            "by_bucket": {
                "range": {"floor": 0.02},
                "trend": {"floor": 0.04}
            },
            "unc_k": 0.5
        }
    }


def test_util_mh_best_h_and_floor(mock_redis, util_mh_cfg):
    """Test util_mh decision: best horizon selection and floor comparison."""
    # Setup Redis mock to return config
    mock_redis.get.return_value = json.dumps(util_mh_cfg)
    
    gate = MLConfirmGate(
        r=mock_redis,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    # Mock model loading
    gate._cfg = util_mh_cfg
    gate._model = DummyUtilMH()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={
            "spread_bps": 2.0,
            "expected_slippage_bps": 2.0,
            "exec_risk_norm": 0.3
        },
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert dec.kind == "util_mh_v1"
    assert dec.best_h_ms == 180000  # Should prefer 180000 (score = 0.05 - 0.5*0.01 = 0.045)
    assert dec.score == pytest.approx(0.045, abs=0.001)  # 0.05 - 0.5*0.01
    assert dec.floor == pytest.approx(0.02, abs=0.001)  # range bucket floor
    assert dec.bucket == "range"
    assert dec.allow is True  # 0.045 >= 0.02
    assert "util_mh" in dec.reason
    
    # p_edge is sigmoid of (score * scale_factor)
    # 0.045 * 2.5 = 0.1125 -> sigmoid(0.1125) = 0.52809...
    assert dec.p_edge == pytest.approx(0.528, abs=0.001)
    
    assert dec.util_pred is not None
    assert dec.unc is not None


def test_util_mh_block_below_floor(mock_redis, util_mh_cfg):
    """Test util_mh blocks when score < floor."""
    # Set high floor to force block
    util_mh_cfg["util_floors"]["global"]["floor"] = 0.10
    util_mh_cfg["util_floors"]["by_bucket"]["range"]["floor"] = 0.10
    
    mock_redis.get.return_value = json.dumps(util_mh_cfg)
    
    gate = MLConfirmGate(
        r=mock_redis,
        mode="ENFORCE",
        fail_policy="CLOSED",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    gate._cfg = util_mh_cfg
    gate._model = DummyUtilMH()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={
            "spread_bps": 2.0,
            "expected_slippage_bps": 2.0,
            "exec_risk_norm": 0.3
        },
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert dec.allow is False  # 0.045 < 0.10
    assert dec.score < dec.floor


def test_util_mh_missing_critical_features_enforce(mock_redis, util_mh_cfg):
    """Test ENFORCE mode blocks when critical features are missing."""
    mock_redis.get.return_value = json.dumps(util_mh_cfg)
    
    gate = MLConfirmGate(
        r=mock_redis,
        mode="ENFORCE",
        fail_policy="CLOSED",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    gate._cfg = util_mh_cfg
    gate._model = DummyUtilMH()
    
    # Missing critical features
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={},  # Missing spread_bps, expected_slippage_bps, exec_risk_norm
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert dec.allow is False
    assert "missing_critical" in dec.reason
    assert dec.missing is not None
    assert len(dec.missing) > 0


def test_util_mh_missing_critical_features_shadow(mock_redis, util_mh_cfg):
    """Test SHADOW mode logs missing features but doesn't block."""
    mock_redis.get.return_value = json.dumps(util_mh_cfg)
    
    gate = MLConfirmGate(
        r=mock_redis,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    gate._cfg = util_mh_cfg
    gate._model = DummyUtilMH()
    
    # Missing critical features - but SHADOW should still compute
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={},  # Missing critical features
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # SHADOW mode should still allow (just logs)
    assert dec.mode == "SHADOW"
    assert dec.missing is not None
    # Note: in SHADOW, we still compute but log missing features


def test_util_mh_bucket_classification(mock_redis, util_mh_cfg):
    """Test bucket classification from scenario."""
    mock_redis.get.return_value = json.dumps(util_mh_cfg)
    
    gate = MLConfirmGate(
        r=mock_redis,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    gate._cfg = util_mh_cfg
    gate._model = DummyUtilMH()
    
    # Test range bucket
    dec_range = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={
            "spread_bps": 2.0,
            "expected_slippage_bps": 2.0,
            "exec_risk_norm": 0.3
        },
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    assert dec_range.bucket == "range"
    assert dec_range.floor == pytest.approx(0.02, abs=0.001)
    
    # Test trend bucket
    dec_trend = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="trend_continuation",
        indicators={
            "spread_bps": 2.0,
            "expected_slippage_bps": 2.0,
            "exec_risk_norm": 0.3
        },
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    assert dec_trend.bucket == "trend"
    assert dec_trend.floor == pytest.approx(0.04, abs=0.001)


def test_util_mh_compatibility_fields(mock_redis, util_mh_cfg):
    """Test that p_edge/p_min are set for backward compatibility."""
    mock_redis.get.return_value = json.dumps(util_mh_cfg)
    
    gate = MLConfirmGate(
        r=mock_redis,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    gate._cfg = util_mh_cfg
    gate._model = DummyUtilMH()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={
            "spread_bps": 2.0,
            "expected_slippage_bps": 2.0,
            "exec_risk_norm": 0.3
        },
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # Compatibility: p_edge should map to sigmoid-scaled score, p_min to floor
    # 0.045 * 2.5 = 0.1125 -> sigmoid(0.1125) = 0.52809...
    assert dec.p_edge == pytest.approx(0.528, abs=0.001)
    # p_min is initialized to floor, but selective prediction can increase it based on exec_risk_norm
    assert dec.p_min >= dec.floor


def test_util_mh_off_mode(mock_redis):
    """Test OFF mode returns allow=True."""
    gate = MLConfirmGate(
        r=mock_redis,
        mode="OFF",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert dec.mode == "OFF"
    assert dec.allow is True
    assert dec.reason == "mode_off"


def test_util_mh_no_cfg(mock_redis):
    """Test behavior when no config is loaded."""
    mock_redis.get.return_value = None
    
    gate = MLConfirmGate(
        r=mock_redis,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert dec.mode == "ERR"
    assert dec.reason == "no_cfg"
    assert dec.allow is True  # FAIL_OPEN policy


def test_util_mh_no_model(mock_redis, util_mh_cfg):
    """Test behavior when model is None."""
    util_mh_cfg["model_path"] = ""  # No model path
    
    mock_redis.get.return_value = json.dumps(util_mh_cfg)
    
    gate = MLConfirmGate(
        r=mock_redis,
        mode="ENFORCE",
        fail_policy="CLOSED",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    gate._cfg = util_mh_cfg
    gate._model = None  # No model loaded
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert dec.mode == "ERR"
    assert dec.reason == "no_model_loaded"
    assert dec.allow is False  # FAIL_CLOSED policy in ENFORCE

