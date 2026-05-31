"""P4.13 exporter metric presence tests."""
from orderflow_services import latency_contract_deploy_lint_exporter_v1 as mod


def test_exporter_has_p413_semantic_binding_metrics() -> None:
    assert hasattr(mod, 'G_APPROVAL_DETAILS_FINGERPRINT_MATCH')
    assert hasattr(mod, 'G_APPROVAL_BINDING_SCHEMA_VERSION')
    assert hasattr(mod, 'G_SUMMARY_DUAL_CONTROL_SEMANTIC_BINDING_MISMATCH_TOTAL')
    # P4.12 metrics still present
    assert hasattr(mod, 'G_APPROVAL_INVALIDATED')
    assert hasattr(mod, 'G_APPROVAL_BINDING_MATCH')
    assert hasattr(mod, 'G_SUMMARY_DUAL_CONTROL_INVALIDATED_GATE_ACTIVE_TOTAL')
    assert hasattr(mod, 'G_SUMMARY_DUAL_CONTROL_BINDING_MISMATCH_TOTAL')
    # P4.11 freshness metrics still present
    assert hasattr(mod, 'G_APPROVAL_EXPIRED')
    assert hasattr(mod, 'G_APPROVAL_CANCELLED')
    assert hasattr(mod, 'G_APPROVAL_FRESHNESS_REMAINING')
