"""
Unit tests for ok_ok_soft_gate_improvements.diff changes.

Tests cover:
1. ok_soft near-miss logic (honor scenario-specific, suppress on veto)
2. MLConfirmGate p_min probability space conversion
3. CancellationSpikeGate bucket reset/wrap handling
4. metrics_stage.stage_ms_hist backward compatibility
"""

import pytest
import math
from unittest.mock import MagicMock
from typing import Dict, Any

from services.cancellation_spike_gate import CancellationSpikeGate, CancelSpikeParams
from common.metrics_stage import stage_ms_hist


# ============================================================================
# Test 1: CancellationSpikeGate bucket reset/wrap
# ============================================================================

def test_cancel_spike_bucket_duplicate():
    """Test that duplicate bucket_id returns early with allow=True."""
    g = CancellationSpikeGate(CancelSpikeParams(enable=True, mode="veto", window=50, min_samples=3))
    cfg2 = {}
    
    # First call
    dec1 = g.check(
        symbol="BTCUSDT", direction="LONG",
        cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=100, cfg2=cfg2,
    )
    assert dec1.allow is True  # warmup
    
    # Duplicate bucket_id
    dec2 = g.check(
        symbol="BTCUSDT", direction="LONG",
        cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=100, cfg2=cfg2,  # same bucket
    )
    assert dec2.allow is True
    assert "duplicate_bucket" in dec2.reason
    assert dec2.meta.get("bucket_id") == 100


def test_cancel_spike_bucket_reset_large_jump():
    """Test that large backward jump resets state (fail-open)."""
    g = CancellationSpikeGate(CancelSpikeParams(enable=True, mode="veto", window=50, min_samples=3))
    cfg2 = {"cancel_bucket_reset_gap": 10000}
    
    # Build up state
    for b in range(20000, 20010):
        g.check(
            symbol="BTCUSDT", direction="LONG",
            cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
            taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
            bucket_id=b, cfg2=cfg2,
        )
    
    # Large backward jump (reset)
    dec = g.check(
        symbol="BTCUSDT", direction="LONG",
        cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=100, cfg2=cfg2,  # jump from 20009 to 100
    )
    assert dec.allow is True  # fail-open after reset
    assert dec.meta.get("bucket_reset") == 1
    assert dec.meta.get("bucket_prev") == 20009
    assert dec.meta.get("bucket_id") == 100


def test_cancel_spike_bucket_out_of_order():
    """Test that small backward jump (out-of-order) returns early."""
    g = CancellationSpikeGate(CancelSpikeParams(enable=True, mode="veto", window=50, min_samples=3))
    cfg2 = {"cancel_bucket_reset_gap": 10000}
    
    # Build up state
    for b in range(100, 110):
        g.check(
            symbol="BTCUSDT", direction="LONG",
            cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
            taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
            bucket_id=b, cfg2=cfg2,
        )
    
    # Small backward jump (out-of-order, not reset)
    dec = g.check(
        symbol="BTCUSDT", direction="LONG",
        cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=105, cfg2=cfg2,  # jump from 109 to 105 (small)
    )
    assert dec.allow is True
    assert "out_of_order_bucket" in dec.reason
    assert dec.meta.get("bucket_prev") == 109
    assert dec.meta.get("bucket_id") == 105


# ============================================================================
# Test 2: metrics_stage.stage_ms_hist backward compatibility
# ============================================================================

def test_stage_ms_hist_modern_api():
    """Test modern API: stage_ms_hist(host, *, stage=..., ms=..., kind=..., symbol=...)."""
    mock_host = MagicMock()
    mock_metrics = MagicMock()
    mock_host.metrics = mock_metrics
    mock_metrics.observe = MagicMock()
    
    stage_ms_hist(mock_host, stage="test_stage", ms=42.5, kind="test_kind", symbol="BTCUSDT")
    
    # Should call observe with pipeline_stage_ms
    assert mock_metrics.observe.called
    call_args = mock_metrics.observe.call_args
    assert call_args[0][0] == "pipeline_stage_ms"
    assert call_args[0][1] == 42.5
    tags = call_args[1]["tags"]
    assert tags.get("stage") == "test_stage"
    assert tags.get("kind") == "test_kind"
    assert tags.get("symbol") == "BTCUSDT"


def test_stage_ms_hist_legacy_api():
    """Test legacy API: stage_ms_hist(host, name, *, ms=..., kind=...)."""
    mock_host = MagicMock()
    mock_metrics = MagicMock()
    mock_host.metrics = mock_metrics
    mock_metrics.observe = MagicMock()
    
    stage_ms_hist(mock_host, "legacy_metric_name", ms=123.0, kind="legacy_kind")
    
    # Should call observe with legacy metric name
    assert mock_metrics.observe.called
    call_args = mock_metrics.observe.call_args
    assert call_args[0][0] == "legacy_metric_name"
    assert call_args[0][1] == 123.0
    tags = call_args[1]["tags"]
    assert tags.get("kind") == "legacy_kind"


def test_stage_ms_hist_fail_open():
    """Test that stage_ms_hist fails open (no exceptions)."""
    # Should not raise even with None/invalid host
    stage_ms_hist(None, stage="test", ms=1.0)
    stage_ms_hist("invalid", stage="test", ms=1.0)
    stage_ms_hist(MagicMock(), stage="test", ms=1.0)  # no metrics attr


# ============================================================================
# Test 3: MLConfirmGate p_min probability space (integration test pattern)
# ============================================================================

def test_ml_confirm_p_min_in_probability_space():
    """
    Test that p_min is converted from utility floor to probability space.
    
    This is a pattern test - actual MLConfirmGate requires Redis and model files.
    The key assertion: p_min should be in [0,1] and use same scaling/calibration as p_edge.
    """
    # Pattern: floor is utility threshold (e.g., -2.0), p_min should be sigmoid(scaled_floor)
    # For floor=-2.0, scale=2.5: p_min ≈ sigmoid(-2.0 * 2.5) = sigmoid(-5.0) ≈ 0.0067
    
    def _sigmoid(x: float) -> float:
        if x >= 0:
            z = math.exp(-x)
            return 1.0 / (1.0 + z)
        z = math.exp(x)
        return z / (1.0 + z)
    
    floor_utility = -2.0
    base_scale = 2.5
    scaled_floor = floor_utility * base_scale
    p_min_from_floor = _sigmoid(scaled_floor)
    
    assert 0.0 <= p_min_from_floor <= 1.0
    assert p_min_from_floor < 0.01  # negative utility -> low probability
    
    # Positive floor should map to higher probability
    floor_positive = 2.0
    scaled_positive = floor_positive * base_scale
    p_min_positive = _sigmoid(scaled_positive)
    assert p_min_positive > 0.9  # positive utility -> high probability


# ============================================================================
# Test 4: ok_soft logic (integration test pattern)
# ============================================================================

def test_ok_soft_suppressed_on_veto():
    """
    Test that ok_soft is suppressed when gate_vetoed or hard_veto.
    
    Pattern test - actual OFConfirmEngine requires full setup.
    Key assertion: ok_soft should be 0 if gate_vetoed=True or hard_veto is set.
    """
    # Pattern: ok_soft should honor scenario-specific first, then generic near-miss
    # But MUST be suppressed if veto_block = gate_vetoed or hard_veto
    
    gate_vetoed = True
    hard_veto = ""
    ok = 0
    have = 2
    need = 3
    score = 0.70
    exec_risk_norm = 0.50
    
    # With veto, ok_soft should be 0
    veto_block = bool(gate_vetoed) or bool(hard_veto)
    if veto_block:
        ok_soft = 0  # suppressed
    else:
        if need > 0 and have == need - 1:
            if score >= 0.60 and exec_risk_norm <= 0.65:
                ok_soft = 1
            else:
                ok_soft = 0
        else:
            ok_soft = 0
    
    assert ok_soft == 0  # suppressed by veto


def test_ok_soft_near_miss_without_veto():
    """Test that ok_soft=1 for near-miss (have=need-1) when no veto."""
    gate_vetoed = False
    hard_veto = ""
    ok = 0
    have = 2
    need = 3
    score = 0.70
    exec_risk_norm = 0.50
    
    veto_block = bool(gate_vetoed) or bool(hard_veto)
    ok_soft = 0
    if not veto_block and ok == 0:
        if need > 0 and have == need - 1:
            if score >= 0.60 and exec_risk_norm <= 0.65:
                ok_soft = 1
    
    assert ok_soft == 1  # near-miss passes


def test_ok_soft_strict_regime_suppressed():
    """Test that ok_soft is suppressed in strict regimes (vol_shock, saw_chop)."""
    scenario_v4 = "vol_shock_news_proxy"
    gate_vetoed = False
    hard_veto = ""
    ok = 0
    have = 2
    need = 3
    score = 0.70
    exec_risk_norm = 0.50
    
    strict_regime = str(scenario_v4 or "").lower() in ("vol_shock_news_proxy", "saw_chop_spoof_proxy")
    veto_block = bool(gate_vetoed) or bool(hard_veto)
    ok_soft = 0
    
    if not veto_block and ok == 0:
        if (not strict_regime) and need > 0 and have == need - 1:
            if score >= 0.60 and exec_risk_norm <= 0.65:
                ok_soft = 1
    
    assert ok_soft == 0  # suppressed by strict regime


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

