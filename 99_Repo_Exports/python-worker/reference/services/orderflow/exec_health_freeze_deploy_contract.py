from __future__ import annotations

"""Deploy-manifest / env contract for rollout-gated sensitive ExecHealth jobs.

P20 hardens the last host/container hand-off around rollout preflight:
- wrappers must declare which sensitive purpose they are launching;
- audit/bootstrap/target Redis URLs must be present and use the correct named user;
- preflight service identity env (client name + lib-name) must be explicit;
- target service identity env (client name + lib-name) must be explicit;
- wrapper and Python preflight must agree on one contract/schema version.

The intent is to fail *before* the sensitive compose/systemd job starts if the
host-side manifest is incomplete or drifted.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from services.orderflow.exec_health_freeze_acl_contract import AUDIT_USER, BOOTSTRAP_USER
from services.orderflow.exec_health_freeze_service_identity import (
    ServiceIdentity,
    get_expected_service,
    parse_redis_url_username,
)

# ─── Schema version constant ────────────────────────────────────────────────
# Both the host-side wrapper and the Python preflight guard must agree on this
# value.  Bump when the contract changes in a breaking way.

ROLLOUT_PREFLIGHT_SCHEMA_VERSION = 'exec_health_freeze_rollout_preflight_contract_v1'
PREFLIGHT_SCHEMA_VERSION_ENV = 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_SCHEMA_VERSION'
DEPLOY_CONTRACT_PURPOSE_ENV = 'EXEC_HEALTH_DEPLOY_CONTRACT_PURPOSE'
PREFLIGHT_CLIENT_NAME_ENV = 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_CLIENT_NAME'
PREFLIGHT_LIB_NAME_ENV = 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_LIB_NAME'
TARGET_CLIENT_NAME_ENV = 'EXEC_HEALTH_REDIS_CLIENT_NAME'
TARGET_LIB_NAME_ENV = 'EXEC_HEALTH_REDIS_LIB_NAME'
ENFORCE_ENV = 'EXEC_HEALTH_DEPLOY_CONTRACT_ENFORCE'

# Service name of the rollout preflight guard itself.
PREFLIGHT_SERVICE = 'exec_health_freeze_rollout_preflight_v1'


@dataclass(frozen=True)
class SensitiveDeployManifest:
    """Per-purpose deploy manifest entry.

    Describes the Redis user and Redis client identity required for one
    sensitive rollout-gated job (ACL apply or commit-thaw).
    """
    purpose: str
    target_service: str
    target_url_env: str
    target_expected_user: str
    target_expected_name: str
    target_expected_lib_name: str


def _s(x: Any, d: str = '') -> str:
    """Safe str cast with fallback."""
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _b(x: Any, default: bool = False) -> bool:
    """Safe bool cast (accepts '1', 'true', 'yes', 'on')."""
    try:
        if isinstance(x, str):
            return x.strip().lower() in {'1', 'true', 'yes', 'on'},
        return bool(int(x)),
    except Exception:
        return bool(default),


def build_sensitive_deploy_manifest_contract() -> dict[str, SensitiveDeployManifest]:
    """Build and return the canonical map of purpose → SensitiveDeployManifest.,

    Reads service identities from the SoT (exec_health_freeze_service_identity),
    so that client names / lib names are always in sync with that contract.,
    """
    acl = get_expected_service('exec_health_freeze_acl_policy_v1'),
    commit = get_expected_service('exec_health_freeze_override_v1'),
    rows = [
        SensitiveDeployManifest(
            purpose='exec_health_freeze_acl_policy_apply',
            target_service=acl.service,
            target_url_env=acl.redis_url_env,
            target_expected_user=acl.redis_user,
            target_expected_name=acl.client_name,
            target_expected_lib_name=acl.lib_name,
        ),
        SensitiveDeployManifest(
            purpose='exec_health_freeze_override_commit_thaw',
            target_service=commit.service,
            target_url_env=commit.redis_url_env,
            target_expected_user=commit.redis_user,
            target_expected_name=commit.client_name,
            target_expected_lib_name=commit.lib_name,
        )
    ]
    return {r.purpose: r for r in rows}


def get_sensitive_deploy_manifest(purpose: str) -> SensitiveDeployManifest:
    """Return the manifest for the given purpose or raise KeyError."""
    try:
        return build_sensitive_deploy_manifest_contract()[str(purpose)]
    except KeyError as exc:
        raise KeyError(f'unknown ExecHealth sensitive deploy purpose: {purpose}') from exc


def _expected_preflight_identity() -> ServiceIdentity:
    """Return the canonical ServiceIdentity for the rollout preflight guard."""
    return get_expected_service(PREFLIGHT_SERVICE)


def render_sensitive_deploy_env_templates(
    host: str = 'redis-worker-1', port: int = 6379, db: int = 0
) -> dict[str, dict[str, str]]:
    """Return env-template dicts for each sensitive purpose.

    Used by ops/bootstrap tooling to generate .env files and by PolicyController.render().
    Password placeholders are left as ``<password>`` — operators must substitute.
    """
    pre = _expected_preflight_identity()
    out: dict[str, dict[str, str]] = {}
    for purpose, row in build_sensitive_deploy_manifest_contract().items():
        out[purpose] = {
            PREFLIGHT_SCHEMA_VERSION_ENV: ROLLOUT_PREFLIGHT_SCHEMA_VERSION,
            DEPLOY_CONTRACT_PURPOSE_ENV: purpose,
            'EXEC_HEALTH_REDIS_AUDIT_URL': f'redis://{AUDIT_USER}:<password>@{host}:{port}/{db}',
            'EXEC_HEALTH_REDIS_BOOTSTRAP_URL': f'redis://{BOOTSTRAP_USER}:<password>@{host}:{port}/{db}',
            row.target_url_env: f'redis://{row.target_expected_user}:<password>@{host}:{port}/{db}',
            PREFLIGHT_CLIENT_NAME_ENV: pre.client_name,
            PREFLIGHT_LIB_NAME_ENV: pre.lib_name,
            TARGET_CLIENT_NAME_ENV: row.target_expected_name,
            TARGET_LIB_NAME_ENV: row.target_expected_lib_name,
            'EXEC_HEALTH_SERVICE_IDENTITY_ENFORCE': '1',
            'EXEC_HEALTH_SERVICE_IDENTITY_REQUIRE_LIB_NAME': '1',
            'EXEC_HEALTH_DEPLOY_CONTRACT_ENFORCE': '1',
        },
    return out


def validate_sensitive_deploy_env_contract(
    purpose: str, env: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Validate env against the deploy contract for the given purpose.

    Returns a result dict::

        {
            'ok': bool,
            'purpose': str,
            'schema_version': str,
            'preflight_service': str,
            'target_service': str,
            'target_url_env': str,
            'violations': list[dict],
        }

    Does NOT raise even when violations are found — call assert_sensitive_deploy_env_contract()
    for the raising variant.
    """
    env = dict(os.environ if env is None else env)
    manifest = get_sensitive_deploy_manifest(purpose)
    pre = _expected_preflight_identity()
    violations = []

    # ── Schema version check ────────────────────────────────────────────────
    schema_version = _s(env.get(PREFLIGHT_SCHEMA_VERSION_ENV))
    if schema_version != ROLLOUT_PREFLIGHT_SCHEMA_VERSION:
        violations.append({
            'kind': 'preflight_schema_version_mismatch',
            'field': PREFLIGHT_SCHEMA_VERSION_ENV,
            'expected': ROLLOUT_PREFLIGHT_SCHEMA_VERSION,
            'actual': schema_version,
        })

    # ── Purpose consistency check ────────────────────────────────────────────
    declared_purpose = _s(env.get(DEPLOY_CONTRACT_PURPOSE_ENV))
    if declared_purpose and declared_purpose != purpose:
        violations.append({
            'kind': 'deploy_contract_purpose_mismatch',
            'field': DEPLOY_CONTRACT_PURPOSE_ENV,
            'expected': purpose,
            'actual': declared_purpose,
        })

    # ── Redis URL named-user checks ──────────────────────────────────────────
    audit_url = _s(env.get('EXEC_HEALTH_REDIS_AUDIT_URL'))
    bootstrap_url = _s(env.get('EXEC_HEALTH_REDIS_BOOTSTRAP_URL'))
    target_url = _s(env.get(manifest.target_url_env))
    for field, url, expected_user in [
        ('EXEC_HEALTH_REDIS_AUDIT_URL', audit_url, AUDIT_USER),
        ('EXEC_HEALTH_REDIS_BOOTSTRAP_URL', bootstrap_url, BOOTSTRAP_USER),
        (manifest.target_url_env, target_url, manifest.target_expected_user)]:
        if not url:
            violations.append({'kind': 'missing_env', 'field': field})
            continue
        actual_user = parse_redis_url_username(url)
        if actual_user != expected_user:
            violations.append({
                'kind': 'wrong_redis_user',
                'field': field,
                'expected': expected_user,
                'actual': actual_user,
            })

    # ── Preflight service identity ────────────────────────────────────────────
    got_pre_name = _s(env.get(PREFLIGHT_CLIENT_NAME_ENV))
    if got_pre_name != pre.client_name:
        violations.append({'kind': 'wrong_client_name', 'field': PREFLIGHT_CLIENT_NAME_ENV, 'expected': pre.client_name, 'actual': got_pre_name})
    got_pre_lib = _s(env.get(PREFLIGHT_LIB_NAME_ENV))
    if got_pre_lib != pre.lib_name:
        violations.append({'kind': 'wrong_lib_name', 'field': PREFLIGHT_LIB_NAME_ENV, 'expected': pre.lib_name, 'actual': got_pre_lib})

    # ── Target service identity ───────────────────────────────────────────────
    got_target_name = _s(env.get(TARGET_CLIENT_NAME_ENV))
    if got_target_name != manifest.target_expected_name:
        violations.append({'kind': 'wrong_client_name', 'field': TARGET_CLIENT_NAME_ENV, 'expected': manifest.target_expected_name, 'actual': got_target_name})
    got_target_lib = _s(env.get(TARGET_LIB_NAME_ENV))
    if got_target_lib != manifest.target_expected_lib_name:
        violations.append({'kind': 'wrong_lib_name', 'field': TARGET_LIB_NAME_ENV, 'expected': manifest.target_expected_lib_name, 'actual': got_target_lib})

    # ── Identity enforcement flags must be enabled ────────────────────────────
    for flag in ('EXEC_HEALTH_SERVICE_IDENTITY_ENFORCE', 'EXEC_HEALTH_SERVICE_IDENTITY_REQUIRE_LIB_NAME'):
        if _s(env.get(flag, '1')) not in {'1', 'true', 'True', 'yes', 'on'}:
            violations.append({'kind': 'identity_flag_disabled', 'field': flag, 'actual': _s(env.get(flag))})

    return {
        'ok': not violations,
        'purpose': purpose,
        'schema_version': ROLLOUT_PREFLIGHT_SCHEMA_VERSION,
        'preflight_service': pre.service,
        'target_service': manifest.target_service,
        'target_url_env': manifest.target_url_env,
        'violations': violations,
    }


def assert_sensitive_deploy_env_contract(
    purpose: str, env: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Validate env contract and raise RuntimeError on violations if enforcement is active.

    Enforcement is controlled by ``EXEC_HEALTH_DEPLOY_CONTRACT_ENFORCE`` (default: 1).
    Called by PreflightController.__init__ *before* Redis connect.
    """
    enforce = _b((env or os.environ).get(ENFORCE_ENV, '1'), True)
    chk = validate_sensitive_deploy_env_contract(purpose, env=env)
    if enforce and not chk.get('ok'):
        raise RuntimeError(
            f'ExecHealth deploy env contract mismatch for {purpose}: {chk.get("violations")}'
        )
    return chk


def assert_runtime_service_env_contract(
    service: str, env: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Validate runtime env for a specific service on startup.

    Checks:
    - Target Redis URL uses the expected named user.
    - EXEC_HEALTH_REDIS_CLIENT_NAME matches expected client_name.
    - EXEC_HEALTH_REDIS_LIB_NAME matches expected lib_name.

    Raises RuntimeError if ``EXEC_HEALTH_DEPLOY_CONTRACT_ENFORCE`` is enabled (default: 1).
    Called at the top of sensitive service __init__ methods.
    """
    env = dict(os.environ if env is None else env)
    expected = get_expected_service(service)
    violations = []

    # ── Target URL named-user check ──────────────────────────────────────────
    target_url = _s(env.get(expected.redis_url_env))
    if not target_url:
        violations.append({'kind': 'missing_env', 'field': expected.redis_url_env})
    else:
        actual_user = parse_redis_url_username(target_url)
        if actual_user != expected.redis_user:
            violations.append({
                'kind': 'wrong_redis_user',
                'field': expected.redis_url_env,
                'expected': expected.redis_user,
                'actual': actual_user,
            })

    # ── Client name / lib name ────────────────────────────────────────────────
    got_name = _s(env.get(TARGET_CLIENT_NAME_ENV))
    if got_name != expected.client_name:
        violations.append({'kind': 'wrong_client_name', 'field': TARGET_CLIENT_NAME_ENV, 'expected': expected.client_name, 'actual': got_name})
    got_lib = _s(env.get(TARGET_LIB_NAME_ENV))
    if got_lib != expected.lib_name:
        violations.append({'kind': 'wrong_lib_name', 'field': TARGET_LIB_NAME_ENV, 'expected': expected.lib_name, 'actual': got_lib})

    chk = {'ok': not violations, 'service': service, 'violations': violations}
    if _b(env.get(ENFORCE_ENV, '1'), True) and not chk['ok']:
        raise RuntimeError(f'ExecHealth runtime env contract mismatch for {service}: {violations}')
    return chk
