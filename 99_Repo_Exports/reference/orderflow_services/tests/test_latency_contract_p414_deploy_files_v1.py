"""P4.14 tests: verify deploy env example files contain the new P4.14 ENV vars."""
from __future__ import annotations

import os

import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ENV_VARS = [
    'LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_WARN_CSV',
    'LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_CRIT_CSV',
    'LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_PAGE_CSV',
]
_ENV_FILES = [
    os.path.join(_REPO, 'deploy', 'env', 'latency-contract-sensitive-jobs.staging.env.example'),
    os.path.join(_REPO, 'deploy', 'env', 'latency-contract-sensitive-jobs.prod.env.example'),
]


@pytest.mark.parametrize('env_file', _ENV_FILES)
@pytest.mark.parametrize('var', _ENV_VARS)
def test_p414_env_var_present_in_file(env_file: str, var: str) -> None:
    assert os.path.exists(env_file), f"Env file not found: {env_file}"
    content = open(env_file).read()
    assert var in content, f"{var} not found in {env_file}"
