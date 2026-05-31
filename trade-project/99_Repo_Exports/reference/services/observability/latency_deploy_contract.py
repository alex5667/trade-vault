from __future__ import annotations

"""Canonical deploy contract for latency-contract sensitive rollout jobs.

This module keeps the file layout, wrapper binding and runtime environment
requirements in one place so host-side wrappers and CI/pre-deploy linting use the
same source of truth.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable
import json
import os
import re

COMMON_RUNTIME_ENV = (
    'TRADE_REPO_ROOT',
    'TRADE_ORDERFLOW_IMAGE',
    'REDIS_URL',
    'LATENCY_CONTRACT_ROLLOUT_GATE_STATE_KEY',
    'LATENCY_CONTRACT_ROLLOUT_GATE_KEY',
)


@dataclass(frozen=True)
class SensitiveJobContract:
    purpose: str
    compose_rel: str
    service_name: str
    wrapper_rel: str
    unit_rel: str
    timer_rel: str | None
    expected_env_file: str
    required_runtime_env: tuple[str, ...]


CONTRACTS: dict[str, SensitiveJobContract] = {
    'conf_score_guardrails_apply': SensitiveJobContract(
        purpose='conf_score_guardrails_apply',
        compose_rel='python-worker/orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-apply-v1.yml',
        service_name='conf-score-guardrails-apply',
        wrapper_rel='python-worker/orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_apply_v1.sh',
        unit_rel='python-worker/orderflow_services/deploy/systemd/trade-conf-score-guardrails-apply.service',
        timer_rel=None,
        expected_env_file='/etc/default/trade-latency-sensitive-jobs-staging',
        required_runtime_env=(
            'CONF_SCORE_GUARD_BUNDLE_DIR',
            'CONF_SCORE_GUARD_BUNDLE_STAGED_POINTER',
            'CONF_SCORE_GUARD_LOCK_PATH',
            'CONF_SCORE_GUARD_DRIFT_REPORT_PATH',
            'CONF_SCORE_GUARD_APPLY',
            'CONF_SCORE_GUARD_STAGE',
        ),
    ),
    'conf_score_guardrails_promote': SensitiveJobContract(
        purpose='conf_score_guardrails_promote',
        compose_rel='python-worker/orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-promote-v1.yml',
        service_name='conf-score-guardrails-promote',
        wrapper_rel='python-worker/orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_promote_v1.sh',
        unit_rel='python-worker/orderflow_services/deploy/systemd/trade-conf-score-guardrails-promote.service',
        timer_rel=None,
        expected_env_file='/etc/default/trade-latency-sensitive-jobs-staging',
        required_runtime_env=(
            'CONF_SCORE_GUARD_BUNDLE_DIR',
            'CONF_SCORE_GUARD_HEALTH_STATE_PATH',
        ),
    ),
    'meta_cov_rollout_controller': SensitiveJobContract(
        purpose='meta_cov_rollout_controller',
        compose_rel='python-worker/orderflow_services/deploy/compose/docker-compose.meta-cov-rollout-controller-v1.yml',
        service_name='meta-cov-rollout-controller',
        wrapper_rel='python-worker/orderflow_services/deploy/systemd/run_trade_meta_cov_rollout_controller_v1.sh',
        unit_rel='python-worker/orderflow_services/deploy/systemd/trade-meta-cov-rollout-controller.service',
        timer_rel='python-worker/orderflow_services/deploy/systemd/trade-meta-cov-rollout-controller.timer',
        expected_env_file='/etc/default/trade-latency-sensitive-jobs-staging',
        required_runtime_env=(
            'META_COV_ROLLOUT_LOOKBACK_MIN',
            'META_COV_ROLLOUT_MIN_HOLD_SEC',
        ),
    ),
    'conf_score_guardrails_autopromo_controller': SensitiveJobContract(
        purpose='conf_score_guardrails_autopromo_controller',
        compose_rel='python-worker/orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-autopromo-controller-v1.yml',
        service_name='conf-score-guardrails-autopromo-controller',
        wrapper_rel='python-worker/orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_autopromo_controller_v1.sh',
        unit_rel='python-worker/orderflow_services/deploy/systemd/trade-conf-score-guardrails-autopromo-controller.service',
        timer_rel='python-worker/orderflow_services/deploy/systemd/trade-conf-score-guardrails-autopromo-controller.timer',
        expected_env_file='/etc/default/trade-latency-sensitive-jobs-staging',
        required_runtime_env=(
            'CONF_SCORE_GUARD_BUNDLE_DIR',
            'CONF_SCORE_GUARD_BUNDLE_STAGED_POINTER',
            'CONF_SCORE_GUARD_HEALTH_STATE_PATH',
            'CONF_SCORE_GUARD_LOCK_PATH',
            'CONF_SCORE_GUARD_AUTOPROMO_APPLY',
        ),
    ),
}


def contract_for_purpose(purpose: str) -> SensitiveJobContract:
    if purpose not in CONTRACTS:
        raise KeyError(f'unknown purpose: {purpose}')
    return CONTRACTS[purpose]


def parse_env_file_text(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def parse_env_file(path: Path) -> dict[str, str]:
    return parse_env_file_text(path.read_text(encoding='utf-8'))


def _contains_environmentfile(text: str) -> list[str]:
    vals: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith('EnvironmentFile='):
            vals.append(line.split('=', 1)[1].strip())
    return vals


def lint_deploy_contract(
    *,
    repo_root: str | Path,
    purpose: str,
    env: dict[str, str] | None = None,
    compose_file: str | Path | None = None,
    wrapper_file: str | Path | None = None,
    unit_file: str | Path | None = None,
    env_file: str | Path | None = None,
) -> dict[str, Any]:
    repo_root = Path(repo_root)
    contract = contract_for_purpose(purpose)
    runtime_env = dict(os.environ if env is None else env)

    compose_path = repo_root / (str(compose_file) if compose_file else contract.compose_rel)
    wrapper_path = repo_root / (str(wrapper_file) if wrapper_file else contract.wrapper_rel)
    unit_path = repo_root / (str(unit_file) if unit_file else contract.unit_rel)
    timer_path = repo_root / contract.timer_rel if contract.timer_rel else None

    errors: list[str] = []
    warnings: list[str] = []
    checks: dict[str, Any] = {
        'purpose': purpose,
        'compose_file': str(compose_path),
        'wrapper_file': str(wrapper_path),
        'unit_file': str(unit_path),
        'timer_file': str(timer_path) if timer_path else '',
        'env_file': str(env_file) if env_file else '',
    }

    # Compose file
    if not compose_path.exists():
        errors.append(f'missing_compose_file:{contract.compose_rel}')
    else:
        txt = compose_path.read_text(encoding='utf-8')
        checks['compose_has_service'] = contract.service_name in txt
        checks['compose_has_preflight_wrapper'] = 'run_with_latency_contract_rollout_preflight_v1.sh' in txt
        checks['compose_has_purpose'] = f'LATENCY_CONTRACT_PREFLIGHT_PURPOSE: {purpose}' in txt
        if not checks['compose_has_service']:
            errors.append(f'compose_wrong_service:{contract.service_name}')
        if not checks['compose_has_preflight_wrapper']:
            errors.append('compose_missing_preflight_wrapper')
        if not checks['compose_has_purpose']:
            errors.append(f'compose_wrong_purpose:{purpose}')

    # Specific wrapper
    if not wrapper_path.exists():
        errors.append(f'missing_wrapper_file:{contract.wrapper_rel}')
    else:
        txt = wrapper_path.read_text(encoding='utf-8')
        checks['wrapper_calls_generic'] = 'run_trade_latency_gated_compose_job_v1.sh' in txt
        checks['wrapper_has_compose_path'] = Path(contract.compose_rel).name in txt
        checks['wrapper_has_service_name'] = contract.service_name in txt
        checks['wrapper_has_purpose'] = f'"{purpose}"' in txt or f"'{purpose}'" in txt
        if not checks['wrapper_calls_generic']:
            errors.append('wrapper_missing_generic_delegate')
        if not checks['wrapper_has_compose_path']:
            errors.append('wrapper_wrong_compose_file')
        if not checks['wrapper_has_service_name']:
            errors.append('wrapper_wrong_service_name')
        if not checks['wrapper_has_purpose']:
            errors.append('wrapper_wrong_purpose')

    # Systemd unit
    env_file_values: list[str] = []
    if not unit_path.exists():
        errors.append(f'missing_unit_file:{contract.unit_rel}')
    else:
        txt = unit_path.read_text(encoding='utf-8')
        env_file_values = _contains_environmentfile(txt)
        checks['unit_has_environmentfile'] = bool(env_file_values)
        checks['unit_exec_calls_wrapper'] = Path(contract.wrapper_rel).name in txt
        if not checks['unit_has_environmentfile']:
            errors.append('unit_missing_environmentfile')
        if not checks['unit_exec_calls_wrapper']:
            errors.append('unit_wrong_execstart_wrapper')
        if env_file_values and contract.expected_env_file not in env_file_values:
            warnings.append(f'unexpected_environmentfile:{env_file_values}')

    if timer_path:
        checks['timer_exists'] = timer_path.exists()
        if not checks['timer_exists']:
            errors.append(f'missing_timer_file:{contract.timer_rel}')

    # Runtime env
    required_env = list(COMMON_RUNTIME_ENV) + list(contract.required_runtime_env)
    missing_env = [k for k in required_env if not str(runtime_env.get(k, '')).strip()]
    checks['required_env'] = required_env
    checks['missing_runtime_env'] = missing_env
    if missing_env:
        errors.append('missing_runtime_env:' + ','.join(missing_env))

    # Optional env file parse for machine-readable diff.
    env_file_data: dict[str, str] = {}
    if env_file:
        p = Path(env_file)
        if not p.exists():
            errors.append(f'missing_env_file:{p}')
        else:
            env_file_data = parse_env_file(p)
            checks['env_file_present'] = True
            missing_in_file = [k for k in required_env if not str(env_file_data.get(k, '')).strip()]
            checks['missing_env_file_vars'] = missing_in_file
            if missing_in_file:
                errors.append('missing_env_file_vars:' + ','.join(missing_in_file))
    else:
        checks['env_file_present'] = False
        checks['unit_environmentfiles'] = env_file_values

    ok = not errors
    return {
        'ok': ok,
        'purpose': purpose,
        'errors': errors,
        'warnings': warnings,
        'checks': checks,
        'contract': {
            'compose_rel': contract.compose_rel,
            'service_name': contract.service_name,
            'wrapper_rel': contract.wrapper_rel,
            'unit_rel': contract.unit_rel,
            'timer_rel': contract.timer_rel or '',
            'expected_env_file': contract.expected_env_file,
            'required_runtime_env': list(required_env),
        },
    }


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
