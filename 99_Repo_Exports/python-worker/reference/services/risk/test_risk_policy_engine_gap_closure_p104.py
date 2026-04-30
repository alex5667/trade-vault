from pathlib import Path
import importlib.util
import sys

mod_path = Path(__file__).with_name('risk_policy_engine.py')
spec = importlib.util.spec_from_file_location('risk_policy_engine_p104', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_news_blackout_denies_hard():
    dec = mod.evaluate_risk_policy(mod.RiskPolicyInput(
        symbol='DOGEUSDT'
        cluster='alts'
        tier='C'
        requested_notional_usd=100
        equity_usd=1000
        stop_distance_bps=50
        news_blackout=True
    ))
    assert dec.level == mod.RISK_DENY_HARD
    assert 'news_blackout' in dec.reasons


def test_edge_negative_after_cost_denies_soft():
    dec = mod.evaluate_risk_policy(mod.RiskPolicyInput(
        symbol='SOLUSDT'
        cluster='alts'
        tier='B'
        requested_notional_usd=100
        equity_usd=1000
        stop_distance_bps=50
        expected_edge_bps=3
        spread_bps=1
        expected_slippage_bps=1.5
        fee_bps=1
    ))
    assert dec.level == mod.RISK_DENY_SOFT
    assert 'edge_negative_after_cost' in dec.reasons


def test_leader_override_tightens_alts():
    dec = mod.evaluate_risk_policy(mod.RiskPolicyInput(
        symbol='DOGEUSDT'
        cluster='alts'
        tier='C'
        requested_notional_usd=100
        equity_usd=1000
        stop_distance_bps=50
        expected_edge_bps=20
        spread_bps=1
        expected_slippage_bps=1
        fee_bps=1
        leader_drawdown_bps=400
    ))
    assert dec.risk_multiplier < 1.0
