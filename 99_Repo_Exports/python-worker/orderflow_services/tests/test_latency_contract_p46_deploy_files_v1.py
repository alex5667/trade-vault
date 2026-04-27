from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_systemd_files_exist() -> None:
    for rel in [
        'orderflow_services/deploy/systemd/trade-latency-contract-deploy-lint-exporter.service',
        'orderflow_services/deploy/systemd/trade-latency-contract-deploy-lint-notifier.service',
        'orderflow_services/deploy/systemd/trade-latency-contract-deploy-lint-notifier.timer',
    ]:
        assert (ROOT / rel).exists(), rel


def test_env_examples_include_notifier_vars() -> None:
    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        txt = (ROOT / rel).read_text(encoding='utf-8')
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_NOTIFIER_STATE_KEY=' in txt
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_STREAM=' in txt
