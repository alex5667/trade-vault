"""P4.9 exporter attribute presence test."""
from orderflow_services import latency_contract_deploy_lint_exporter_v1 as mod


def test_exporter_has_p49_policy_metrics() -> None:
    """Exporter module must expose all P4.9 policy gauge attributes."""
    assert hasattr(mod, 'G_POLICY_LIMIT_HIT_TOTAL')
    assert hasattr(mod, 'G_POLICY_DENIED_TOTAL')
    assert hasattr(mod, 'G_SUMMARY_POLICY_BLOCKED_GATE_ACTIVE_TOTAL')
    assert hasattr(mod, 'G_SUMMARY_POLICY_OVERRIDE_GATE_ACTIVE_TOTAL')
