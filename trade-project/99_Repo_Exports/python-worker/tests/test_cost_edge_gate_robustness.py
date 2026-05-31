

import pytest

from handlers.crypto_orderflow.core.cost_edge_gate import CostEdgeConfig, CostEdgeGate


@pytest.fixture
def base_config():
    return CostEdgeConfig(
        enabled=True,
        default_cost_k=4.0,
        fees_bps=4.0,
        slippage_bps=4.0,
        slippage_use_spread_half=False,
        buffer_bps=0.0
    )

@pytest.fixture
def gate(base_config):
    return CostEdgeGate(base_config)

class MockContext:
    def __init__(self, tp1=None, rr=None, atr=None, spread_bps=None):
        self.tp1 = tp1
        self.rr = rr
        self.atr = atr
        self.spread_bps = spread_bps
        self.side = "LONG"

def test_bad_inputs_fail_safe(gate):
    """Test that NaN/Inf/Negative inputs don't crash and are treated safely."""
    # NaN price
    res_nan = gate.evaluate(MockContext(tp1=100), "BTCUSDT", float('nan'))
    assert res_nan.passed is False
    assert res_nan.veto_reason == "no_edge_estimate_available"
    assert res_nan.expected_edge_bps == 0.0

    # Negative price
    res_neg = gate.evaluate(MockContext(tp1=100), "BTCUSDT", -100.0)
    assert res_neg.expected_edge_bps == 0.0

def test_clamping_bad_config(base_config):
    """Test robustness against invalid K and Buffer."""
    # Case 1: K <= 0 (should reset to default)
    base_config.default_cost_k = -5.0
    gate = CostEdgeGate(base_config)
    res = gate.evaluate(MockContext(tp1=110), "BTCUSDT", 100) # Edge 10%
    assert res.cost_multiplier == 4.0 # Should fallback to 4.0 (hardcoded safety) NOT default if default is bad?
    # Actually code doubles check: if config.default_cost_k is bad, it might fallback to 4.0?
    # Let's check logic: cost_k = symbol_k.get(..., default_cost_k). If result <=0 => default_cost_k. If still <=0 => 4.0.

    # Case 2: Buffer < 0 (should sanitize to 0)
    base_config.buffer_bps = -50.0
    gate = CostEdgeGate(base_config)
    res = gate.evaluate(MockContext(tp1=110), "BTCUSDT", 100)
    assert res.buffer_bps == 0.0

def test_required_zero_ratio_inf(base_config):
    """Test behavior when required edge is 0 (zero costs)."""
    base_config.fees_bps = 0.0
    base_config.slippage_bps = 0.0
    base_config.default_cost_k = 1.0
    gate = CostEdgeGate(base_config)

    # Edge > 0 => Ratio should be Inf
    ctx = MockContext(tp1=105) # 5% edge
    res = gate.evaluate(ctx, "BTCUSDT", 100)
    assert res.required_edge_bps == 0.0
    assert res.passed is True
    assert res.edge_ratio == float("inf")

    # Edge = 0 => Ratio 0
    ctx_no_edge = MockContext(tp1=100)
    res0 = gate.evaluate(ctx_no_edge, "BTCUSDT", 100)
    assert res0.edge_ratio == 0.0

def test_boundary_eps(base_config):
    """Test epsilon logic at the boundary."""
    # Costs = 8.0, K=1.0 => Req = 8.0
    base_config.fees_bps = 4.0
    base_config.slippage_bps = 4.0
    base_config.default_cost_k = 1.0
    gate = CostEdgeGate(base_config)

    # Required = 8.0 bps
    # Case 1: Edge = 7.8 (fail)
    # 7.8 + 0.1 = 7.9 < 8.0 -> Fail
    ctx_fail = MockContext(tp1=100.078)
    res_fail = gate.evaluate(ctx_fail, "BTCUSDT", 100.0)
    assert res_fail.passed is False

    # Case 2: Edge = 7.95 (pass due to EPS if logic were looser? No, EPS=0.1)
    # 7.95 + 0.1 = 8.05 >= 8.0 -> Pass
    # Price for 7.95 bps: 100 * (1 + 7.95/10000) = 100.0795
    ctx_pass = MockContext(tp1=100.0795)
    res_pass = gate.evaluate(ctx_pass, "BTCUSDT", 100.0)

    # Calculate actual edge internally to confirm
    # (100.0795 - 100)/100 * 10000 = 7.95 bps
    assert res_pass.expected_edge_bps == pytest.approx(7.95, 0.001)
    assert res_pass.passed is True

def test_veto_reason_format(base_config):
    """Verify detailed breakdown in veto reasons."""
    base_config.buffer_bps = 2.5
    gate = CostEdgeGate(base_config)

    # Req = (4+4+2.5)*4 = 42 bps. Edge = 10 bps.
    ctx = MockContext(tp1=100.10) # 10bps
    res = gate.evaluate(ctx, "BTCUSDT", 100.0)

    assert res.passed is False
    # New format: CostEdge VETO symbol=BTCUSDT exp_bps=10.0 req_bps=42.0 k=4.0 fees_bps=4.0 slip_bps=4.0 buf_bps=2.5 total_costs_bps=10.5 ratio=...
    assert "fees_bps=4.0" in res.veto_reason
    assert "slip_bps=4.0" in res.veto_reason
    assert "buf_bps=2.5" in res.veto_reason
    assert "k=4.0" in res.veto_reason

