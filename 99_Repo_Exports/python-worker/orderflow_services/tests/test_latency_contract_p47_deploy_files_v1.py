from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_env_examples_include_silence_vars() -> None:
    for rel in ['orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example', 'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example']:
        txt = (ROOT / rel).read_text(encoding='utf-8')
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_PREFIX=' in txt
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_DEFAULT_MINUTES=' in txt


def test_wrapper_exists() -> None:
    assert (ROOT / 'orderflow_services/deploy/systemd/run_trade_latency_contract_deploy_lint_silence_v1.sh').exists()
