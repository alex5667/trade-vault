from __future__ import annotations

from orderflow_services.orchestration_composite_preflight_exporter_v1 import (
    compute_purpose_state,
    normalize_reason_code,
    summarize,
)


def test_normalize_reason_code_bounds_unknown_values() -> None:
    assert normalize_reason_code('latency_contract', 'latency_contract:external_missing') == 'latency_contract:external_missing'
    assert normalize_reason_code('strategy_research_stats', 'pbo_high') == 'strategy_research_stats:pbo_high'
    assert normalize_reason_code('strategy_research_stats', 'weird_runtime_failure') == 'strategy_research_stats:other'
    assert normalize_reason_code('none', '') == 'none:ok'


def test_compute_purpose_state_uses_present_status_source_reason() -> None:
    raw = {
        'updated_ts_ms': '1000',
        'status': 'block',
        'selected_source': 'deploy_lint',
        'selected_reason_code': 'deploy_lint:persistent_config_drift',
        'selected_priority_rank': '0',
        'strategy_research_stats_status': 'soft',
    }
    state = compute_purpose_state('conf_score_guardrails_promote', raw, now_ms=31_000)
    assert state.present == 1.0
    assert state.age_seconds == 30.0
    assert state.decision_status == 'block'
    assert state.selected_source == 'deploy_lint'
    assert state.selected_reason_code == 'deploy_lint:persistent_config_drift'
    assert state.selected_priority_rank == 0.0
    assert state.strategy_research_stats_status == 'soft'


def test_summarize_counts_ok_block_invalid() -> None:
    states = [
        compute_purpose_state('a', {'status': 'ok', 'selected_source': 'none', 'selected_reason_code': 'ok'}),
        compute_purpose_state('b', {'status': 'block', 'selected_source': 'latency_contract', 'selected_reason_code': 'latency_contract:external_missing'}),
        compute_purpose_state('c', {}),
    ]
    summary = summarize(states)
    assert summary.purposes_total == 3.0
    assert summary.present_total == 2.0
    assert summary.ok_total == 1.0
    assert summary.block_total == 1.0
    assert summary.invalid_total == 1.0
