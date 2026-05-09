"""Tests for risk_policy_engine P4.5 metrics pack: latency and clamp_ratio in snapshot."""
import importlib.util
import sys
from pathlib import Path

# Load module standalone without needing the full services package installed
mod_path = Path(__file__).resolve().parent / 'risk_policy_engine.py'
spec = importlib.util.spec_from_file_location('risk_policy_engine', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_decision_snapshot_contains_latency_and_clamp_ratio():
    """evaluate_risk_policy snapshot must include decision_latency_ms and clamp_ratio (P4.5)."""
    inp = mod.RiskPolicyInput(
        symbol='BTCUSDT',
        cluster='majors',
        tier='A',
        requested_notional_usd=1000,
        equity_usd=1000,
        stop_distance_bps=50,
        volatility_bps=25,
        spread_bps=1,
        expected_slippage_bps=1,
        confidence=0.8,
    )
    dec = mod.evaluate_risk_policy(inp, mod.RiskPolicyLimits.from_env())
    assert 'decision_latency_ms' in dec.snapshot, "snapshot must contain decision_latency_ms"
    assert 'clamp_ratio' in dec.snapshot, "snapshot must contain clamp_ratio"
    assert dec.snapshot['decision_latency_ms'] >= 0
    assert 0.0 <= dec.snapshot['clamp_ratio'] <= 1.0 or dec.snapshot['clamp_ratio'] == 1.0


def test_decision_latency_is_positive_float():
    """decision_latency_ms must be a non-negative float."""
    inp = mod.RiskPolicyInput(
        symbol='ETHUSDT',
        cluster='majors',
        tier='A',
        requested_notional_usd=500,
        equity_usd=2000,
        stop_distance_bps=30,
        volatility_bps=20,
        spread_bps=1,
        expected_slippage_bps=1,
        confidence=0.75,
    )
    dec = mod.evaluate_risk_policy(inp, mod.RiskPolicyLimits.from_env())
    latency = dec.snapshot['decision_latency_ms']
    assert isinstance(latency, float)
    assert latency >= 0.0


def test_clamp_ratio_is_one_when_not_clamped():
    """When notional is NOT clamped, clamp_ratio should be 1.0 (full notional awarded)."""
    # Use small requested notional to avoid clamping
    inp = mod.RiskPolicyInput(
        symbol='BTCUSDT',
        cluster='majors',
        tier='A',
        requested_notional_usd=10,  # tiny: should not be clamped
        equity_usd=10000,
        stop_distance_bps=50,
        volatility_bps=25,
        spread_bps=1,
        expected_slippage_bps=1,
        confidence=0.8,
    )
    dec = mod.evaluate_risk_policy(inp, mod.RiskPolicyLimits.from_env())
    # If allow, clamp_ratio should be 1.0 (no clamp)
    if dec.allow_trade_publish:
        assert dec.snapshot['clamp_ratio'] == 1.0
