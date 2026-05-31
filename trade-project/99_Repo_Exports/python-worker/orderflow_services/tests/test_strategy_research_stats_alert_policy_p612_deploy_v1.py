from __future__ import annotations

import os

_DIR = os.path.join(os.path.dirname(__file__), '..', 'deploy', 'env')

_P612_VARS = (
    'STRATEGY_RESEARCH_STATS_ALERT_POLICY_DUAL_CONTROL_APPROVED_FRESHNESS_S',
)


def _read(name: str) -> str:
    with open(os.path.join(_DIR, name)) as f:
        return f.read()


def test_prod_env_example_has_p612_vars() -> None:
    content = _read('latency-contract-sensitive-jobs.prod.env.example')
    for var in _P612_VARS:
        assert var in content, f'Missing {var} in prod example'


def test_staging_env_example_has_p612_vars() -> None:
    content = _read('latency-contract-sensitive-jobs.staging.env.example')
    for var in _P612_VARS:
        assert var in content, f'Missing {var} in staging example'
