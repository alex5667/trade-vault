from __future__ import annotations

import importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding='utf-8')


def test_generic_wrapper_calls_deploy_lint_before_preflight() -> None:
    txt = _read('orderflow_services/deploy/systemd/run_trade_latency_gated_compose_job_v1.sh')
    assert 'latency_contract_deploy_lint_v1' in txt
    assert txt.index('latency_contract_deploy_lint_v1') < txt.index('latency_contract_rollout_preflight_v1')


def test_specific_wrappers_export_wrapper_and_unit_paths() -> None:
    files = [
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_apply_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_promote_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_meta_cov_rollout_controller_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_autopromo_controller_v1.sh',
    ]
    for rel in files:
        txt = _read(rel)
        assert 'LATENCY_CONTRACT_DEPLOY_WRAPPER_FILE' in txt
        assert 'LATENCY_CONTRACT_DEPLOY_UNIT_FILE' in txt


def test_systemd_units_export_env_file_and_lint_report_path() -> None:
    files = [
        'orderflow_services/deploy/systemd/trade-conf-score-guardrails-apply.service',
        'orderflow_services/deploy/systemd/trade-conf-score-guardrails-promote.service',
        'orderflow_services/deploy/systemd/trade-meta-cov-rollout-controller.service',
        'orderflow_services/deploy/systemd/trade-conf-score-guardrails-autopromo-controller.service',
    ]
    for rel in files:
        txt = _read(rel)
        assert 'EnvironmentFile=' in txt
        assert 'Environment=LATENCY_CONTRACT_ENV_FILE=' in txt
        assert 'Environment=LATENCY_CONTRACT_DEPLOY_LINT_REPORT_PATH=' in txt


def test_env_examples_include_deploy_lint_report_path() -> None:
    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        txt = _read(rel)
        assert 'LATENCY_CONTRACT_DEPLOY_LINT_REPORT_PATH=' in txt


def test_deploy_lint_cli_module_loads() -> None:
    mod = importlib.import_module('orderflow_services.latency_contract_deploy_lint_v1')
    assert hasattr(mod, 'main')
