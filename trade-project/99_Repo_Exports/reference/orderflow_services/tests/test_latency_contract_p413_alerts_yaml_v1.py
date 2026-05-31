"""P4.13 Prometheus alerts YAML presence tests."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_alerts_yaml_contains_p413_rules() -> None:
    txt = (ROOT / 'orderflow_services/prometheus_alerts_latency_contract_p413_v1.yml').read_text(encoding='utf-8')
    assert 'OF_LatencyDeployLint_DualControlSemanticBindingMismatch_Warn' in txt
    assert 'latency_contract_deploy_lint_summary_dual_control_semantic_binding_mismatch_total' in txt
    assert 'OF_LatencyDeployLint_DetailsFingerprintMismatch_Crit' in txt
    assert 'latency_contract_deploy_lint_silence_approval_details_fingerprint_match' in txt
