from services.orderflow.exec_health_freeze_service_identity import build_service_identity_contract, evaluate_client_list_against_contract, normalize_client_entry, verify_entry_against_expected


def test_contract_contains_expected_services():
    c = build_service_identity_contract()
    assert 'exec_health_freeze_override_v1' in c
    assert c['exec_health_freeze_override_v1'].redis_user == 'exec_health_freeze_writer'


def test_evaluate_client_list_against_contract_ok_subset():
    raw = "\n".join([
        'id=1 user=exec_health_freeze_writer name=exec-health-freeze-override-v1 lib-name=exec-health-freeze-writer',
        'id=2 user=exec_health_freeze_audit name=exec-health-freeze-acl-drift-exporter-v1 lib-name=exec-health-freeze-audit',
    ])
    res = evaluate_client_list_against_contract(raw, required_services=['exec_health_freeze_override_v1', 'exec_health_freeze_acl_drift_exporter_v1'])
    assert res['ok'] is True
    assert res['services']['exec_health_freeze_override_v1']['user_match'] == 1


def test_evaluate_client_list_against_contract_detects_wrong_user_and_missing():
    raw = 'id=1 user=default name=exec-health-freeze-override-v1 lib-name=exec-health-freeze-writer'
    res = evaluate_client_list_against_contract(raw, required_services=['exec_health_freeze_override_v1', 'exec_health_freeze_acl_drift_exporter_v1'])
    kinds = {(v['kind'], v['service']) for v in res['violations']}
    assert ('wrong_user', 'exec_health_freeze_override_v1') in kinds
    assert ('service_missing', 'exec_health_freeze_acl_drift_exporter_v1') in kinds


def test_verify_entry_against_expected():
    expected = build_service_identity_contract()['exec_health_freeze_override_v1']
    chk = verify_entry_against_expected(normalize_client_entry('id=1 user=exec_health_freeze_writer name=exec-health-freeze-override-v1 lib-name=exec-health-freeze-writer'), expected)
    assert chk['ok'] is True
