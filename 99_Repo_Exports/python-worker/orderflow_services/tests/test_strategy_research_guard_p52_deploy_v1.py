from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding='utf-8')


def test_host_and_container_wrappers_include_research_guard_preflight() -> None:
    for rel in [
        'orderflow_services/deploy/systemd/run_trade_latency_gated_compose_job_v1.sh',
        'orderflow_services/integrations/run_with_latency_contract_rollout_preflight_v1.sh',
    ]:
        txt = _read(rel)
        assert 'strategy_research_guard_rollout_preflight_v1' in txt
        assert 'ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE' in txt


def test_sensitive_compose_jobs_propagate_research_guard_env() -> None:
    for rel in [
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-apply-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-promote-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-autopromo-controller-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.meta-cov-rollout-controller-v1.yml',
    ]:
        txt = _read(rel)
        for needle in [
            'ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE:',
            'STRATEGY_RESEARCH_GUARD_BLOCKER_KEY:',
            'STRATEGY_RESEARCH_GUARD_SUMMARY_KEY:',
            'STRATEGY_RESEARCH_GUARD_MAX_AGE_SEC:',
        ]:
            assert needle in txt, (rel, needle)


def test_env_examples_include_research_guard_hard_gate_vars() -> None:
    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        txt = _read(rel)
        for needle in [
            'ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE=',
            'STRATEGY_RESEARCH_GUARD_BLOCKER_KEY=',
            'STRATEGY_RESEARCH_GUARD_SUMMARY_KEY=',
            'STRATEGY_RESEARCH_GUARD_MAX_AGE_SEC=',
            'STRATEGY_RESEARCH_GUARD_FAIL_CLOSED_MISSING=',
        ]:
            assert needle in txt, (rel, needle)
