from orderflow_services import latency_contract_deploy_lint_exporter_v1 as mod


def test_exporter_has_p45_metrics() -> None:
    assert hasattr(mod, 'G_GATE_ACTIVE')
    assert hasattr(mod, 'G_SUMMARY_GATE_ACTIVE_TOTAL')
    assert hasattr(mod, 'G_FAIL_AGE')
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_env_examples_include_p45_vars() -> None:
    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        txt = (ROOT / rel).read_text(encoding='utf-8')
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_STATE_PREFIX=' in txt
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_PERSIST_HOLD_S=' in txt
