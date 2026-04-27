from __future__ import annotations

"""Tests for exec_health_freeze_deploy_contract (P20).

Covers:
- render_sensitive_deploy_env_templates — returns correct keys and values for both purposes.
- validate_sensitive_deploy_env_contract — happy path (ok=True) and violation detection.
- assert_runtime_service_env_contract — happy path (ok=True).
"""

from services.orderflow.exec_health_freeze_deploy_contract import (
    ROLLOUT_PREFLIGHT_SCHEMA_VERSION,
    assert_runtime_service_env_contract,
    render_sensitive_deploy_env_templates,
    validate_sensitive_deploy_env_contract,
)


def test_render_sensitive_deploy_env_templates_contains_sensitive_purposes() -> None:
    """render_sensitive_deploy_env_templates returns both purposes with expected keys."""
    tpl = render_sensitive_deploy_env_templates()
    assert 'exec_health_freeze_acl_policy_apply' in tpl
    assert 'exec_health_freeze_override_commit_thaw' in tpl

    acl = tpl['exec_health_freeze_acl_policy_apply']
    # Preflight identity must be present (exec-health-freeze-rollout-preflight-v1)
    assert acl['EXEC_HEALTH_ROLLOUT_PREFLIGHT_CLIENT_NAME'] == 'exec-health-freeze-rollout-preflight-v1'
    assert acl['EXEC_HEALTH_ROLLOUT_PREFLIGHT_LIB_NAME'] == 'exec-health-freeze-audit'
    # Schema version
    assert acl['EXEC_HEALTH_ROLLOUT_PREFLIGHT_SCHEMA_VERSION'] == ROLLOUT_PREFLIGHT_SCHEMA_VERSION

    commit = tpl['exec_health_freeze_override_commit_thaw']
    assert commit['EXEC_HEALTH_REDIS_LIB_NAME'] == 'exec-health-freeze-writer'


def test_validate_sensitive_deploy_env_contract_ok_acl_policy_apply() -> None:
    """validate_sensitive_deploy_env_contract returns ok=True for a valid env (acl_policy_apply)."""
    env = render_sensitive_deploy_env_templates()['exec_health_freeze_acl_policy_apply']
    chk = validate_sensitive_deploy_env_contract('exec_health_freeze_acl_policy_apply', env=env)
    assert chk['ok'] is True
    assert chk['schema_version'] == ROLLOUT_PREFLIGHT_SCHEMA_VERSION
    assert not chk['violations']


def test_validate_sensitive_deploy_env_contract_ok_override_commit_thaw() -> None:
    """validate_sensitive_deploy_env_contract returns ok=True for override_commit_thaw."""
    env = render_sensitive_deploy_env_templates()['exec_health_freeze_override_commit_thaw']
    chk = validate_sensitive_deploy_env_contract('exec_health_freeze_override_commit_thaw', env=env)
    assert chk['ok'] is True


def test_validate_sensitive_deploy_env_contract_detects_wrong_users_and_names() -> None:
    """validate_sensitive_deploy_env_contract detects schema mismatch, wrong user, wrong client name."""
    env = render_sensitive_deploy_env_templates()['exec_health_freeze_override_commit_thaw'].copy()
    env['EXEC_HEALTH_REDIS_AUDIT_URL'] = 'redis://default:bad@redis-worker-1:6379/0'
    env['EXEC_HEALTH_REDIS_CLIENT_NAME'] = 'wrong-name'
    env['EXEC_HEALTH_ROLLOUT_PREFLIGHT_SCHEMA_VERSION'] = 'older-v0'
    chk = validate_sensitive_deploy_env_contract('exec_health_freeze_override_commit_thaw', env=env)
    assert chk['ok'] is False
    kinds = {(row['kind'], row['field']) for row in chk['violations'] if 'field' in row}
    assert ('preflight_schema_version_mismatch', 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_SCHEMA_VERSION') in kinds
    assert ('wrong_redis_user', 'EXEC_HEALTH_REDIS_AUDIT_URL') in kinds
    assert ('wrong_client_name', 'EXEC_HEALTH_REDIS_CLIENT_NAME') in kinds


def test_assert_runtime_service_env_contract_ok() -> None:
    """assert_runtime_service_env_contract returns ok=True for a valid env (override_v1)."""
    env = render_sensitive_deploy_env_templates()['exec_health_freeze_override_commit_thaw'].copy()
    chk = assert_runtime_service_env_contract('exec_health_freeze_override_v1', env=env)
    assert chk['ok'] is True


def test_assert_runtime_service_env_contract_ok_acl_policy() -> None:
    """assert_runtime_service_env_contract returns ok=True for a valid env (acl_policy_v1)."""
    env = render_sensitive_deploy_env_templates()['exec_health_freeze_acl_policy_apply'].copy()
    chk = assert_runtime_service_env_contract('exec_health_freeze_acl_policy_v1', env=env)
    assert chk['ok'] is True


def test_validate_sensitive_deploy_env_contract_missing_urls() -> None:
    """validate_sensitive_deploy_env_contract detects missing required URLs."""
    env = render_sensitive_deploy_env_templates()['exec_health_freeze_acl_policy_apply'].copy()
    del env['EXEC_HEALTH_REDIS_AUDIT_URL']
    del env['EXEC_HEALTH_REDIS_BOOTSTRAP_URL']
    chk = validate_sensitive_deploy_env_contract('exec_health_freeze_acl_policy_apply', env=env)
    assert chk['ok'] is False
    fields = {row.get('field') for row in chk['violations']}
    assert 'EXEC_HEALTH_REDIS_AUDIT_URL' in fields
    assert 'EXEC_HEALTH_REDIS_BOOTSTRAP_URL' in fields
