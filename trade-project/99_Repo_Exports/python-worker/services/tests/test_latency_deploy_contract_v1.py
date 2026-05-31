from __future__ import annotations

from pathlib import Path

from services.observability.latency_deploy_contract import lint_deploy_contract, parse_env_file_text

ROOT = Path(__file__).resolve().parents[2]


def test_parse_env_file_text_skips_comments() -> None:
    env = parse_env_file_text("#x\nA=1\nB='two'\n")
    assert env['A'] == '1'
    assert env['B'] == 'two'


def test_lint_contract_ok_against_repo_examples() -> None:
    report = lint_deploy_contract(
        repo_root=ROOT,
        purpose='conf_score_guardrails_apply',
        env={
            'TRADE_REPO_ROOT': '/opt/trade/staging/repo',
            'TRADE_ORDERFLOW_IMAGE': 'trade-orderflow:staging',
            'REDIS_URL': 'redis://redis-worker-1:6379/0',
            'LATENCY_CONTRACT_ROLLOUT_GATE_STATE_KEY': 'metrics:latency_contract:rollout_gate:last',
            'LATENCY_CONTRACT_ROLLOUT_GATE_KEY': 'cfg:orderflow:latency_contract:rollout_gate:v1',
            'CONF_SCORE_GUARD_BUNDLE_DIR': '/var/lib/trade/conf_score_guard_bundles',
            'CONF_SCORE_GUARD_BUNDLE_STAGED_POINTER': '/var/lib/trade/conf_score_guard_bundles/staged.json',
            'CONF_SCORE_GUARD_LOCK_PATH': '/var/lib/trade/conf_score_guardrails.lock',
            'CONF_SCORE_GUARD_DRIFT_REPORT_PATH': '/var/lib/trade/reports/conf_parts_drift.json',
            'CONF_SCORE_GUARD_APPLY': '1',
            'CONF_SCORE_GUARD_STAGE': '0',
        },
    )
    assert report['ok'], report


def test_lint_contract_missing_env_fails() -> None:
    report = lint_deploy_contract(repo_root=ROOT, purpose='meta_cov_rollout_controller', env={'TRADE_REPO_ROOT': '/x'})
    assert not report['ok']
    assert any('missing_runtime_env:' in e for e in report['errors'])
