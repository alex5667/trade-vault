from pathlib import Path


def test_sensitive_compose_wrappers_reference_preflight():
    compose_apply = Path('orderflow_services/deploy/docker-compose.exec-health-freeze-acl-policy-apply-v1.yml').read_text()
    compose_commit = Path('orderflow_services/deploy/docker-compose.exec-health-freeze-override-commit-thaw-v1.yml').read_text()
    wrapper_apply = Path('orderflow_services/deploy/systemd/run-exec-health-freeze-acl-policy-apply-v1.sh').read_text()
    wrapper_commit = Path('orderflow_services/deploy/systemd/run-exec-health-freeze-override-commit-thaw-v1.sh').read_text()
    wrapper_generic = Path('orderflow_services/deploy/systemd/run-exec-health-freeze-with-rollout-preflight-v1.sh').read_text()

    assert 'exec_health_freeze_acl_policy_v1 apply' in compose_apply
    assert 'exec_health_freeze_override_v1 commit-thaw' in compose_commit
    assert 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_PURPOSE="exec_health_freeze_acl_policy_apply"' in wrapper_apply
    assert 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_PURPOSE="exec_health_freeze_override_commit_thaw"' in wrapper_commit
    assert 'exec_health_freeze_rollout_preflight_v1' in wrapper_generic


def test_sensitive_systemd_units_use_guarded_wrappers():
    svc_apply = Path('orderflow_services/deploy/systemd/exec-health-freeze-acl-policy-apply.service').read_text()
    svc_commit = Path('orderflow_services/deploy/systemd/exec-health-freeze-override-commit-thaw.service').read_text()

    assert 'run-exec-health-freeze-acl-policy-apply-v1.sh' in svc_apply
    assert 'run-exec-health-freeze-override-commit-thaw-v1.sh' in svc_commit


def test_sensitive_compose_env_contract_and_examples_present():
    """P20: compose files must declare all required env contract fields, env examples must be present."""
    compose_apply = Path('orderflow_services/deploy/docker-compose.exec-health-freeze-acl-policy-apply-v1.yml').read_text()
    compose_commit = Path('orderflow_services/deploy/docker-compose.exec-health-freeze-override-commit-thaw-v1.yml').read_text()
    env_apply = Path('orderflow_services/deploy/env/exec-health-freeze-acl-policy-apply.env.example').read_text()
    env_commit = Path('orderflow_services/deploy/env/exec-health-freeze-override-commit-thaw.env.example').read_text()

    # P20 required fields must be present in compose files
    for txt in (compose_apply, compose_commit):
        assert 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_SCHEMA_VERSION' in txt
        assert 'EXEC_HEALTH_DEPLOY_CONTRACT_PURPOSE' in txt
        assert 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_CLIENT_NAME' in txt
        assert 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_LIB_NAME' in txt
        assert 'EXEC_HEALTH_REDIS_CLIENT_NAME' in txt
        assert 'EXEC_HEALTH_REDIS_LIB_NAME' in txt

    # Env examples must contain the canonical PURPOSE and CLIENT_NAME values
    assert 'EXEC_HEALTH_DEPLOY_CONTRACT_PURPOSE=exec_health_freeze_acl_policy_apply' in env_apply
    assert 'EXEC_HEALTH_DEPLOY_CONTRACT_PURPOSE=exec_health_freeze_override_commit_thaw' in env_commit

