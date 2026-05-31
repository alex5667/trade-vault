from __future__ import annotations

"""P4.10 exporter module dual-control metrics presence tests."""



def test_exporter_has_dual_control_metrics():
    import orderflow_services.latency_contract_deploy_lint_exporter_v1 as exp
    assert hasattr(exp, 'G_DUAL_CONTROL_REQUIRED')
    assert hasattr(exp, 'G_DUAL_CONTROL_DENIED_TOTAL')
    assert hasattr(exp, 'G_DUAL_CONTROL_OVERRIDE_ACTIVE')
    assert hasattr(exp, 'G_APPROVAL_PENDING')
    assert hasattr(exp, 'G_APPROVAL_READY')
    assert hasattr(exp, 'G_APPROVAL_AGE')
    assert hasattr(exp, 'G_SUMMARY_DUAL_CONTROL_PENDING_TOTAL')
    assert hasattr(exp, 'G_SUMMARY_DUAL_CONTROL_READY_TOTAL')
    assert hasattr(exp, 'G_SUMMARY_DUAL_CONTROL_OVERRIDE_GATE_ACTIVE_TOTAL')


def test_exporter_cfg_has_approval_prefix():
    import dataclasses

    from orderflow_services.latency_contract_deploy_lint_exporter_v1 import Cfg
    fields = {f.name for f in dataclasses.fields(Cfg)}
    assert 'approval_prefix' in fields
