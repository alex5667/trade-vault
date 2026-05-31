"""P4.9 test: env example files contain all required policy vars."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_env_examples_include_p49_policy_vars() -> None:
    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        txt = (ROOT / rel).read_text(encoding='utf-8')
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_WINDOW_HOURS=' in txt
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_MAX_BUDGET_MINUTES=' in txt
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_MAX_ACKS=' in txt
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_DENIED_EXIT_CODE=' in txt
