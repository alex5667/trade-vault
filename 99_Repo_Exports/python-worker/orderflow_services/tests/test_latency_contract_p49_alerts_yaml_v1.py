"""P4.9 test: prometheus alerts YAML contains policy alert rules."""
from pathlib import Path


def test_p49_alerts_have_policy_rules() -> None:
    txt = Path(__file__).resolve().parents[2].joinpath('orderflow_services/prometheus_alerts_latency_contract_p49_v1.yml').read_text(encoding='utf-8')
    assert 'OF_LatencyDeployLint_PolicyBlocked_Crit' in txt
    assert 'latency_contract_deploy_lint_summary_policy_blocked_gate_active_total' in txt
    assert 'OF_LatencyDeployLint_PolicyOverrideActive_Warn' in txt
