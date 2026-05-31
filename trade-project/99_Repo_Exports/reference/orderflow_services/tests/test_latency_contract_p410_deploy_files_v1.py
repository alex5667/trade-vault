"""P4.10 deploy env example files contain dual-control variables."""
from __future__ import annotations

import os

_DIR = os.path.join(os.path.dirname(__file__), '..', 'deploy', 'env')

_P410_VARS = (
    'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_PREFIX',
    'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_TTL_S',
    'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_DUAL_CONTROL_MINUTES',
)


def _read(name: str) -> str:
    with open(os.path.join(_DIR, name)) as f:
        return f.read()


def test_prod_env_example_has_p410_vars():
    content = _read('latency-contract-sensitive-jobs.prod.env.example')
    for var in _P410_VARS:
        assert var in content, f"Missing {var} in prod example"


def test_staging_env_example_has_p410_vars():
    content = _read('latency-contract-sensitive-jobs.staging.env.example')
    for var in _P410_VARS:
        assert var in content, f"Missing {var} in staging example"
