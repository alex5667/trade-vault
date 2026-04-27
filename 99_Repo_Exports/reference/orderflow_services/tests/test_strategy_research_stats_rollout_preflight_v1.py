from __future__ import annotations

import time

from orderflow_services.strategy_research_stats_rollout_preflight_v1 import evaluate_rollout_preflight


class FakeRedis:
    def __init__(self, mapping):
        self.mapping = mapping

    def hgetall(self, key):
        return dict(self.mapping.get(key, {}))


def test_preflight_blocks_on_hard_block(monkeypatch):
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_STATS_HARD_GATE', '1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_INVALID_AS_BLOCK', '1')
    ts = str(int(time.time() * 1000))
    client = FakeRedis({
        'cfg:strategy_research_stats:blocker:v1': {'blocked': '1', 'reason': 'pbo_high', 'gate_mode': 'hard', 'updated_ts_ms': ts},
        'metrics:strategy_research_stats:last': {'updated_ts_ms': ts},
    })
    out = evaluate_rollout_preflight(purpose='conf_score_guardrails_apply', client=client)
    assert out['allowed'] is False
    assert out['exit_code'] == 24
    assert out['reason'] == 'pbo_high'


def test_preflight_blocks_on_invalid_when_configured(monkeypatch):
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_STATS_HARD_GATE', '1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_INVALID_AS_BLOCK', '1')
    client = FakeRedis({})
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_FAIL_CLOSED_MISSING', '1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_GATE_MODE', 'hard')
    out = evaluate_rollout_preflight(purpose='conf_score_guardrails_promote', client=client)
    assert out['allowed'] is False
    assert out['exit_code'] == 25


def test_preflight_allows_soft_mode(monkeypatch):
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_STATS_HARD_GATE', '1')
    ts = str(int(time.time() * 1000))
    client = FakeRedis({
        'cfg:strategy_research_stats:blocker:v1': {'soft_blocked': '1', 'reason': 'psr_low', 'gate_mode': 'soft', 'updated_ts_ms': ts},
        'metrics:strategy_research_stats:last': {'updated_ts_ms': ts},
    })
    out = evaluate_rollout_preflight(purpose='conf_score_guardrails_autopromo_controller', client=client)
    assert out['allowed'] is True
    assert out['status'] == 'soft'


def test_preflight_disabled(monkeypatch):
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_STATS_HARD_GATE', '0')
    out = evaluate_rollout_preflight(purpose='conf_score_guardrails_apply', client=FakeRedis({}))
    assert out['allowed'] is True
    assert out['reason'] == 'hard_gate_disabled'
