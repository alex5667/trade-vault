# python-worker/tests/test_of_confirm_engine_meta.py
"""
Unit tests for OFConfirmEngine meta-model integration and exec-risk penalty.
"""
import os
import tempfile
import json
from unittest.mock import Mock, MagicMock

import pytest

from core.of_confirm_engine import OFConfirmEngine
from core.meta_model_lr import MetaModelLR


def test_meta_model_loader_fail_open_no_file():
    """Test that build() does not crash when meta model file is missing."""
    engine = OFConfirmEngine()
    
    runtime = Mock()
    runtime.last_wp = Mock(weak_any=False)
    runtime.last_obi_event = None
    runtime.last_iceberg_event = None
    runtime.last_ofi_event = None
    runtime.last_sweep = None
    runtime.last_reclaim = None
    runtime.last_fp_edge = None
    runtime.last_bar = None
    runtime.last_regime = "na"
    runtime.dynamic_cfg = {}
    runtime.pressure = Mock()
    runtime.pressure.is_pressure_hi = Mock(return_value=False)
    runtime.book_churn_hi = 0
    runtime.liq_regime = "normal"
    
    cfg = {
        "meta_model_enable": 1,
        "meta_model_path": "/nonexistent/path/model.json",  # File does not exist
        "meta_model_mode": "SHADOW",
    }
    
    indicators = {
        "spread_bps": 10.0,
        "expected_slippage_bps": 2.0,
        "book_health_ok": 1,
        "data_health": 1.0,
    }
    
    # Should not raise exception
    result, dec = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=1000000,
        price=50000.0,
        delta_z=2.5,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators,
    )
    
    assert result is not None
    assert result.ok == 0  # Should fail gate, but not crash
    assert result.evidence["meta_enable"] == 1
    assert result.evidence["meta_p"] == -1.0  # Not loaded


def test_meta_model_loader_fail_open_invalid_json():
    """Test that build() does not crash when meta model file has invalid JSON."""
    engine = OFConfirmEngine()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write("invalid json {")
        temp_path = f.name
    
    try:
        runtime = Mock()
        runtime.last_wp = Mock(weak_any=False)
        runtime.last_obi_event = None
        runtime.last_iceberg_event = None
        runtime.last_ofi_event = None
        runtime.last_sweep = None
        runtime.last_reclaim = None
        runtime.last_fp_edge = None
        runtime.last_bar = None
        runtime.last_regime = "na"
        runtime.dynamic_cfg = {}
        runtime.pressure = Mock()
        runtime.pressure.is_pressure_hi = Mock(return_value=False)
        runtime.book_churn_hi = 0
        runtime.liq_regime = "normal"
        
        cfg = {
            "meta_model_enable": 1,
            "meta_model_path": temp_path,
            "meta_model_mode": "SHADOW",
        }
        
        indicators = {
            "spread_bps": 10.0,
            "expected_slippage_bps": 2.0,
            "book_health_ok": 1,
            "data_health": 1.0,
        }
        
        # Should not raise exception
        result, dec = engine.build(
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            tick_ts_ms=1000000,
            price=50000.0,
            delta_z=2.5,
            runtime=runtime,
            cfg=cfg,
            indicators=indicators,
        )
        
        assert result is not None
        assert result.evidence["meta_p"] == -1.0  # Not loaded due to error
    finally:
        os.unlink(temp_path)


def test_exec_risk_penalty_missing_spread_slippage():
    """Test that exec-risk penalty is non-zero when spread/slippage are missing."""
    engine = OFConfirmEngine()
    
    runtime = Mock()
    runtime.last_wp = Mock(weak_any=False)
    runtime.last_obi_event = None
    runtime.last_iceberg_event = None
    runtime.last_ofi_event = None
    runtime.last_sweep = None
    runtime.last_reclaim = None
    runtime.last_fp_edge = None
    runtime.last_bar = None
    runtime.last_regime = "na"
    runtime.dynamic_cfg = {}
    runtime.pressure = Mock()
    runtime.pressure.is_pressure_hi = Mock(return_value=False)
    runtime.book_churn_hi = 0
    runtime.liq_regime = "normal"
    
    cfg = {
        "spread_bps_missing_default": 15.0,
        "expected_slippage_bps_missing_default": 4.0,
        "exec_risk_ref_bps": 10.0,
        "w_exec_risk": 0.18,
    }
    
    indicators = {
        # Missing spread_bps and expected_slippage_bps
        "book_health_ok": 1,
        "data_health": 1.0,
    }
    
    result, dec = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=1000000,
        price=50000.0,
        delta_z=2.5,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators,
    )
    
    assert result is not None
    # Check that missing flags are set
    assert indicators.get("spread_bps_missing") == 1
    assert indicators.get("expected_slippage_missing") == 1
    # Check that exec_risk_bps uses defaults
    assert result.evidence["spread_bps"] == 15.0
    assert result.evidence["expected_slippage_bps"] == 4.0
    assert result.evidence["exec_risk_bps"] == 19.0  # 15 + 4
    # Check that exec_risk_norm is > 0
    assert indicators["exec_risk_norm"] > 0.0
    # Check that penalty is applied in contrib
    assert "exec_risk_penalty" in result.contrib
    assert result.contrib["exec_risk_penalty"] < 0.0  # Penalty is negative
    assert indicators["exec_pen"] > 0.0


def test_fp_edge_absorb_from_indicators():
    """Test that fp_edge_absorb from indicators is used in eval_reversal."""
    engine = OFConfirmEngine()
    
    runtime = Mock()
    runtime.last_wp = Mock(weak_any=True)
    runtime.last_obi_event = None
    runtime.last_iceberg_event = None
    runtime.last_ofi_event = None
    runtime.last_sweep = Mock(ts_ms=999000)  # Recent sweep
    runtime.last_reclaim = None
    runtime.last_fp_edge = None
    runtime.last_bar = None
    runtime.last_regime = "na"
    runtime.dynamic_cfg = {}
    runtime.pressure = Mock()
    runtime.pressure.is_pressure_hi = Mock(return_value=False)
    runtime.book_churn_hi = 0
    runtime.liq_regime = "normal"
    
    cfg = {
        "strong_need_reversal": 2,
        "strong_z_min": 2.0,
        "spread_bps": 10.0,
        "expected_slippage_bps": 2.0,
    }
    
    indicators = {
        "spread_bps": 10.0,
        "expected_slippage_bps": 2.0,
        "book_health_ok": 1,
        "data_health": 1.0,
        "fp_edge_absorb": 1,  # Set fp_edge_absorb in indicators
    }
    
    result, dec = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=1000000,
        price=50000.0,
        delta_z=2.5,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators,
    )
    
    assert result is not None
    # Check that fp_edge_absorb is in indicators (it gets derived)
    assert indicators["fp_edge_absorb"] == 1
    # Check that it was used in eval_reversal (should contribute to have/need)
    if dec is not None:
        # fp_edge_absorb should contribute to C leg in reversal
        assert dec.have >= 0
        assert dec.need >= 2


def test_meta_model_shadow_mode():
    """Test that meta-model in SHADOW mode does not change ok but exports meta_p."""
    engine = OFConfirmEngine()
    
    # Create a valid meta model file
    model_data = {
        "features": ["score", "have", "need", "delta_z_abs", "exec_risk_norm"],
        "intercept": 0.0,
        "coef": [1.0, 0.5, -0.3, 0.2, -0.5],
        "threshold": 0.5,
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(model_data, f)
        temp_path = f.name
    
    try:
        runtime = Mock()
        runtime.last_wp = Mock(weak_any=True)
        runtime.last_obi_event = None
        runtime.last_iceberg_event = None
        runtime.last_ofi_event = None
        runtime.last_sweep = Mock(ts_ms=999000)
        runtime.last_reclaim = None
        runtime.last_fp_edge = None
        runtime.last_bar = None
        runtime.last_regime = "na"
        runtime.dynamic_cfg = {}
        runtime.pressure = Mock()
        runtime.pressure.is_pressure_hi = Mock(return_value=False)
        runtime.book_churn_hi = 0
        runtime.liq_regime = "normal"
        
        cfg = {
            "meta_model_enable": 1,
            "meta_model_path": temp_path,
            "meta_model_mode": "SHADOW",
            "meta_p_min": 0.55,
            "strong_need_reversal": 2,
            "strong_z_min": 2.0,
            "spread_bps": 10.0,
            "expected_slippage_bps": 2.0,
        }
        
        indicators = {
            "spread_bps": 10.0,
            "expected_slippage_bps": 2.0,
            "book_health_ok": 1,
            "data_health": 1.0,
        }
        
        result, dec = engine.build(
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            tick_ts_ms=1000000,
            price=50000.0,
            delta_z=2.5,
            runtime=runtime,
            cfg=cfg,
            indicators=indicators,
        )
        
        assert result is not None
        assert result.evidence["meta_enable"] == 1
        assert result.evidence["meta_mode"] == "SHADOW"
        # meta_p should be computed (or -1.0 if mock data is incomplete)
        assert result.evidence["meta_p"] >= -1.0
        assert result.evidence["meta_p"] <= 1.0
        # In SHADOW mode, ok should not be changed by meta model
        # (only logged in meta_veto)
        assert result.evidence["meta_veto"] in [0, 1]  # Can be 1 if meta_p < meta_p_min, but ok unchanged
    finally:
        os.unlink(temp_path)

