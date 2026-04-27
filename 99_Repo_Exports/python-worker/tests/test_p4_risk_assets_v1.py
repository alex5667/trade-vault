"""Smoke tests: verify that P4 risk engine assets exist in the expected locations."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def test_env_example_exists():
    """config/env.execution-p4-risk.example must exist."""
    p = PROJECT_ROOT / "config" / "env.execution-p4-risk.example"
    assert p.exists(), f"Missing: {p}"
    content = p.read_text()
    assert "RISK_KILL_SWITCH" in content
    assert "RISK_TIER_A_MAX_LEVERAGE" in content
    assert "RISK_TIER_B_BASE_RISK_PCT" in content
    assert "RISK_TIER_C_MAKER_ALLOWED" in content


def test_prometheus_rules_exists():
    """monitoring/prometheus_rules_execution_p4_risk.yml must exist."""
    p = PROJECT_ROOT / "monitoring" / "prometheus_rules_execution_p4_risk.yml"
    assert p.exists(), f"Missing: {p}"
    content = p.read_text()
    assert "TradeRiskForceFlatten" in content
    assert "TradeRiskDenySpike" in content
    assert "TradePortfolioExposureHigh" in content


def test_risk_policy_engine_exists():
    """services/risk/risk_policy_engine.py must exist."""
    p = PROJECT_ROOT / "python-worker" / "services" / "risk" / "risk_policy_engine.py"
    assert p.exists(), f"Missing: {p}"


def test_portfolio_risk_engine_is_wrapper():
    """services/risk/portfolio_risk_engine.py must be a backward-compat wrapper."""
    p = PROJECT_ROOT / "python-worker" / "services" / "risk" / "portfolio_risk_engine.py"
    assert p.exists(), f"Missing: {p}"
    content = p.read_text()
    assert "risk_policy_engine" in content
