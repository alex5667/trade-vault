from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding='utf-8')


def test_preflight_wrapper_uses_env_purpose() -> None:
    txt = _read('orderflow_services/integrations/run_with_latency_contract_rollout_preflight_v1.sh')
    assert 'LATENCY_CONTRACT_PREFLIGHT_PURPOSE' in txt
    assert 'latency_contract_rollout_preflight_v1' in txt


def test_specific_systemd_wrappers_delegate_to_generic_host_wrapper() -> None:
    files = [
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_apply_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_promote_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_meta_cov_rollout_controller_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_autopromo_controller_v1.sh',
    ]
    for rel in files:
        txt = _read(rel)
        assert 'run_trade_latency_gated_compose_job_v1.sh' in txt
        assert 'TRADE_REPO_ROOT' in txt


def test_compose_jobs_use_in_container_preflight_wrapper() -> None:
    mapping = {
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-apply-v1.yml': 'conf_score_guardrails_apply_v1',
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-promote-v1.yml': 'conf_score_guardrails_promote_v1',
        'orderflow_services/deploy/compose/docker-compose.meta-cov-rollout-controller-v1.yml': 'meta_cov_rollout_controller_v1',
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-autopromo-controller-v1.yml': 'conf_score_guardrails_autopromo_controller_v1',
    }
    for rel, module in mapping.items():
        txt = _read(rel)
        assert 'run_with_latency_contract_rollout_preflight_v1.sh' in txt
        assert module in txt
        assert 'LATENCY_CONTRACT_PREFLIGHT_PURPOSE' in txt


def test_systemd_units_call_specific_wrappers() -> None:
    mapping = {
        'orderflow_services/deploy/systemd/trade-conf-score-guardrails-apply.service': 'run_trade_conf_score_guardrails_apply_v1.sh',
        'orderflow_services/deploy/systemd/trade-conf-score-guardrails-promote.service': 'run_trade_conf_score_guardrails_promote_v1.sh',
        'orderflow_services/deploy/systemd/trade-meta-cov-rollout-controller.service': 'run_trade_meta_cov_rollout_controller_v1.sh',
        'orderflow_services/deploy/systemd/trade-conf-score-guardrails-autopromo-controller.service': 'run_trade_conf_score_guardrails_autopromo_controller_v1.sh',
    }
    for rel, wrapper in mapping.items():
        txt = _read(rel)
        assert 'EnvironmentFile=' in txt
        assert wrapper in txt
        assert 'docker.service' in txt


def test_timer_units_present_for_recurring_rollout_jobs() -> None:
    for rel in [
        'orderflow_services/deploy/systemd/trade-meta-cov-rollout-controller.timer',
        'orderflow_services/deploy/systemd/trade-conf-score-guardrails-autopromo-controller.timer',
    ]:
        txt = _read(rel)
        assert 'OnCalendar=' in txt
        assert '.service' in txt


def test_env_examples_cover_required_runtime_fields() -> None:
    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        txt = _read(rel)
        for needle in [
            'TRADE_REPO_ROOT=',
            'TRADE_ORDERFLOW_IMAGE=',
            'REDIS_URL=',
            'CONF_SCORE_GUARD_BUNDLE_DIR=',
            'CONF_SCORE_GUARD_HEALTH_STATE_PATH=',
        ]:
            assert needle in txt
