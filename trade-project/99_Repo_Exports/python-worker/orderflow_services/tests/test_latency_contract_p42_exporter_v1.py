"""Tests for P4.2 exporter rollout gate Prometheus gauges."""
import orderflow_services.latency_contract_exporter_v1 as mod


def test_p42_exporter_has_rollout_gate_metrics():
    """Exporter module must expose P4.2 rollout gate Prometheus gauges."""
    assert hasattr(mod, 'latency_contract_rollout_gate_active')
    assert hasattr(mod, 'latency_contract_rollout_gate_external_missing_total')
    assert hasattr(mod, 'latency_contract_rollout_gate_budget_hold_seconds')
    assert hasattr(mod, 'latency_contract_rollout_gate_budget_breach_total')
    assert hasattr(mod, 'latency_contract_rollout_gate_budget_hold_reached')
