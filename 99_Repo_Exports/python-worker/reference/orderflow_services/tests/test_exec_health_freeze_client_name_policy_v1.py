from services.orderflow.exec_health_freeze_client_name_policy import evaluate_client_name_policy


def test_client_name_policy_ok_subset():
    raw = "\n".join([
        'id=1 user=exec_health_freeze_writer addr=10.0.0.1:1111 name=exec-health-freeze-override-v1 lib-name=exec-health-freeze-writer',
        'id=2 user=exec_health_freeze_audit addr=10.0.0.2:2222 name=exec-health-freeze-acl-drift-exporter-v1 lib-name=exec-health-freeze-audit',
        'id=3 user=exec_health_freeze_audit addr=10.0.0.3:3333 name=exec-health-freeze-client-name-audit-exporter-v1 lib-name=exec-health-freeze-audit',
    ])
    res = evaluate_client_name_policy(raw, required_services=['exec_health_freeze_override_v1', 'exec_health_freeze_acl_drift_exporter_v1', 'exec_health_freeze_client_name_audit_exporter_v1'])
    assert res['ok'] is True
    assert res['services']['exec_health_freeze_override_v1']['lib_name_match'] == 1


def test_client_name_policy_detects_unnamed_wrong_lib_and_duplicate():
    raw = "\n".join([
        'id=1 user=exec_health_freeze_writer addr=10.0.0.1:1111 name=exec-health-freeze-override-v1 lib-name=wrong-lib',
        'id=2 user=exec_health_freeze_writer addr=10.0.0.2:2222 name=exec-health-freeze-override-v1 lib-name=exec-health-freeze-writer',
        'id=3 user=exec_health_freeze_writer addr=10.0.0.3:3333 name= lib-name=exec-health-freeze-writer',
    ])
    res = evaluate_client_name_policy(raw, required_services=['exec_health_freeze_override_v1'])
    kinds = [v['kind'] for v in res['violations']]
    assert 'wrong_lib_name_after_reconnect' in kinds
    assert 'duplicate_trusted_client_name' in kinds
    assert 'service_started_unnamed_client' in kinds
