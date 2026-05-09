from __future__ import annotations

"""Integration tests for P6.3 composite orchestration preflight.

Tests verify that:
- compose files use the single unified composite wrapper
- systemd scripts delegate to run_trade_orchestration_composite_gated_compose_job_v1.sh
- the core orchestration_composite_preflight_v1 module works end-to-end
- soft strategy_research_stats does NOT block orchestration decision
- stage-mode bypass only affects strategy_research_stats gate
- deploy-lint and latency-contract block decisions independently
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

OS_SERVICES = ROOT / "orderflow_services"


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding='utf-8')


# ── File/wrapper consistency checks ──────────────────────────────────────────

def test_compose_uses_single_composite_wrapper() -> None:
    """Compose files must use run_with_orchestration_composite_rollout_preflight_v1.sh
    and NOT the old two-wrapper chain."""
    for rel in [
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-apply-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-promote-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-autopromo-controller-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.meta-cov-rollout-controller-v1.yml',
    ]:
        txt = _read(rel)
        assert 'run_with_orchestration_composite_rollout_preflight_v1.sh' in txt, \
            f"{rel}: missing unified composite wrapper"
        assert 'run_with_strategy_research_stats_rollout_preflight_v1.sh' not in txt, \
            f"{rel}: still references old stats wrapper in command"


def test_compose_has_strategy_research_stats_vars() -> None:
    """Compose files must include ENABLE_STRATEGY_RESEARCH_STATS_COMPOSITE_PREFLIGHT."""
    for rel in [
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-apply-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-promote-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-autopromo-controller-v1.yml',
        'orderflow_services/deploy/compose/docker-compose.meta-cov-rollout-controller-v1.yml',
    ]:
        txt = _read(rel)
        assert 'ENABLE_STRATEGY_RESEARCH_STATS_COMPOSITE_PREFLIGHT' in txt, \
            f"{rel}: missing ENABLE_STRATEGY_RESEARCH_STATS_COMPOSITE_PREFLIGHT"


def test_systemd_uses_unified_composite_wrapper() -> None:
    """Systemd scripts must exec run_trade_orchestration_composite_gated_compose_job_v1.sh."""
    txt = _read('orderflow_services/deploy/systemd/run_trade_orchestration_composite_gated_compose_job_v1.sh')
    assert 'orchestration_composite_preflight_v1' in txt

    for rel in [
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_apply_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_promote_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_autopromo_controller_v1.sh',
    ]:
        body = _read(rel)
        assert 'run_trade_orchestration_composite_gated_compose_job_v1.sh' in body, \
            f"{rel}: must delegate to run_trade_orchestration_composite_gated_compose_job_v1.sh"


def test_legacy_wrappers_are_aliases() -> None:
    """Legacy wrapper scripts must be thin aliases to the unified composite wrapper."""
    for rel in [
        'orderflow_services/integrations/run_with_latency_contract_rollout_preflight_v1.sh',
        'orderflow_services/integrations/run_with_strategy_research_stats_rollout_preflight_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_latency_gated_compose_job_v1.sh',
        'orderflow_services/deploy/systemd/run_trade_strategy_research_stats_gated_compose_job_v1.sh',
    ]:
        body = _read(rel)
        # They may still exec the composite OR just forward to run_with_orchestration_composite
        has_composite_ref = (
            'run_with_orchestration_composite_rollout_preflight_v1.sh' in body
            or 'run_trade_orchestration_composite_gated_compose_job_v1.sh' in body
        )
        assert has_composite_ref, f"{rel}: must delegate to composite wrapper"


# ── Core module logic tests ───────────────────────────────────────────────────

class FakeRedis:
    """Minimal in-memory Redis stub for unit testing."""
    def __init__(self, hashes: dict | None = None) -> None:
        self.hashes: dict = dict(hashes or {})
        self.streams: list = []
        self.expires: dict = {}
        self._last_hset: dict = {}

    @classmethod
    def from_url(cls, url: str, **kw) -> FakeRedis:  # type: ignore
        return cls()

    def hgetall(self, key: str) -> dict:
        return dict(self.hashes.get(key) or {})

    def hget(self, key: str, field: str):
        h = self.hashes.get(key, {})
        return h.get(field)

    def hset(self, key: str, mapping: dict | None = None, **kw) -> None:
        if mapping:
            self.hashes.setdefault(key, {}).update(mapping)
        self._last_hset = {key: dict(mapping or {})}

    def expire(self, key: str, ttl: int) -> None:
        self.expires[key] = ttl

    def xadd(self, stream: str, fields: dict, **kw) -> str:
        self.streams.append({'stream': stream, 'fields': fields})
        return '1-0'


def test_priority_prefers_block_over_invalid_and_latency_over_stats(monkeypatch) -> None:
    from orderflow_services.orchestration_composite_preflight_v1 import evaluate_composite_gate
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_STATS_COMPOSITE_PREFLIGHT', '1')
    r = FakeRedis(
        {
            'metrics:latency_contract:rollout_gate:last': {'gate_active': '1', 'gate_reason_code': 'external_missing'},
            'cfg:strategy_research_stats:blocker:v1': {'blocked': '1', 'gate_mode': 'hard', 'reason': 'pbo_high', 'updated_ts_ms': '9999999999999'},
            'metrics:strategy_research_stats:last': {'updated_ts_ms': '9999999999999'},
        }
    )
    state = evaluate_composite_gate('redis://unused', purpose='conf_score_guardrails_promote', client=r)
    # latency_contract has higher priority (rank 1) than strategy_research_stats (rank 2)
    assert state['status'] == 'block'
    assert state['selected_source'] == 'latency_contract'


def test_priority_prefers_deploy_lint_block_when_multiple_blocks(monkeypatch) -> None:
    from orderflow_services.orchestration_composite_preflight_v1 import evaluate_composite_gate
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_STATS_COMPOSITE_PREFLIGHT', '1')
    r = FakeRedis(
        {
            'cfg:orderflow:latency_contract:deploy_lint_gate:conf_score_guardrails_promote:v1': {
                'gate_active': '1',
                'gate_reason_code': 'persistent_config_drift',
            },
            'metrics:latency_contract:rollout_gate:last': {'gate_active': '1', 'gate_reason_code': 'external_missing'},
            'cfg:strategy_research_stats:blocker:v1': {'blocked': '1', 'gate_mode': 'hard', 'reason': 'pbo_high', 'updated_ts_ms': '9999999999999'},
            'metrics:strategy_research_stats:last': {'updated_ts_ms': '9999999999999'},
        }
    )
    state = evaluate_composite_gate('redis://unused', purpose='conf_score_guardrails_promote', client=r)
    assert state['status'] == 'block'
    # deploy_lint wins (rank 0 < latency 1 < stats 2)
    assert state['selected_source'] == 'deploy_lint'
    assert state['selected_priority_rank'] == 0


def test_stage_mode_bypasses_strategy_research_stats_only(monkeypatch) -> None:
    from orderflow_services.orchestration_composite_preflight_v1 import evaluate_composite_gate
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_STATS_COMPOSITE_PREFLIGHT', '1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_PREFLIGHT_ALLOW_STAGE', '1')
    r = FakeRedis(
        {
            'metrics:latency_contract:deploy_lint:last:conf_score_guardrails_apply': {'gate_active': '0', 'ok': '1'},
            'metrics:latency_contract:rollout_gate:last': {'gate_active': '0'},
            'cfg:strategy_research_stats:blocker:v1': {'blocked': '1', 'gate_mode': 'hard', 'reason': 'pbo_high', 'updated_ts_ms': '9999999999999'},
            'metrics:strategy_research_stats:last': {'updated_ts_ms': '9999999999999'},
        }
    )
    state = evaluate_composite_gate(
        'redis://unused', purpose='conf_score_guardrails_apply', stage_mode=True, client=r
    )
    assert state['status'] == 'ok'
    assert state['strategy_research_stats_reason'] == 'stage_allowed'


def test_soft_strategy_research_stats_does_not_block(monkeypatch) -> None:
    from orderflow_services.orchestration_composite_preflight_v1 import evaluate_composite_gate
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_STATS_COMPOSITE_PREFLIGHT', '1')
    r = FakeRedis({
        'metrics:latency_contract:deploy_lint:last:conf_score_guardrails_promote': {'gate_active': '0', 'ok': '1'},
        'metrics:latency_contract:rollout_gate:last': {'gate_active': '0'},
        'cfg:strategy_research_stats:blocker:v1': {'soft_blocked': '1', 'reason': 'psr_low', 'gate_mode': 'soft', 'updated_ts_ms': '9999999999999'},
        'metrics:strategy_research_stats:last': {'updated_ts_ms': '9999999999999'},
    })
    state = evaluate_composite_gate('redis://unused', purpose='conf_score_guardrails_promote', client=r)
    assert state['status'] == 'ok'
    assert state['selected_source'] == 'none'
    # soft status is visible in per-source reporting
    assert state['strategy_research_stats_status'] == 'soft'
    assert state['strategy_research_stats_reason'] == 'psr_low'


def test_emit_audit_persists_state_and_event(monkeypatch) -> None:
    from orderflow_services.orchestration_composite_preflight_v1 import evaluate_composite_gate
    monkeypatch.setenv('ORCHESTRATION_PREFLIGHT_STATE_PREFIX', 'metrics:orchestration:preflight:last')
    monkeypatch.setenv('ORCHESTRATION_PREFLIGHT_OPS_EVENT_STREAM', 'ops:orchestration:preflight:v1')
    r = FakeRedis({
        'metrics:latency_contract:rollout_gate:last': {'gate_active': '0'},
    })
    state = evaluate_composite_gate(
        'redis://unused',
        purpose='conf_score_guardrails_promote',
        emit_audit=True,
        client=r,
    )
    assert state['decision_status'] == 'ok'
    skey = 'metrics:orchestration:preflight:last:conf_score_guardrails_promote'
    assert skey in r.hashes
    assert r.hashes[skey].get('status') == 'ok'
    assert len(r.streams) >= 1
