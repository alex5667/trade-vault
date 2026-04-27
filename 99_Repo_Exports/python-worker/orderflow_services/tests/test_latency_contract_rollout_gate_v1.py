from utils.time_utils import get_ny_time_millis
"""Tests for latency_contract_rollout_gate_v1 (P4.2)."""
from orderflow_services.latency_contract_rollout_gate_v1 import Cfg, evaluate_once


class FakeRedis:
    """Minimal Redis stub for unit tests."""
    def __init__(self, data):
        self.data = data

    def hgetall(self, key):
        return dict(self.data.get(key, {}))


def _cfg(**kwargs) -> Cfg:
    defaults = dict(
        redis_url='redis://x',
        summary_key='metrics:latency_contract:slo:last',
        state_key='metrics:latency_contract:rollout_gate:last',
        gate_key='cfg:orderflow:latency_contract:rollout_gate:v1',
        interval_s=10.0,
        budget_hold_s=300,
        gate_ttl_s=900,
    )
    defaults.update(kwargs)
    return Cfg(**defaults)


def test_rollout_gate_blocks_on_external_missing():
    """Gate must activate when external stages are missing."""
    data = {
        'metrics:latency_contract:slo:last': {
            'external_missing_total': '2',
            'budget_breach_total': '0',
        },
        'metrics:latency_contract:rollout_gate:last': {},
    }
    m = evaluate_once(FakeRedis(data), _cfg())
    assert int(m['gate_active']) == 1
    assert m['gate_reason_code'] == 'external_missing'


def test_rollout_gate_allows_when_all_present():
    """Gate must be inactive when all stages are covered and no budget breach."""
    data = {
        'metrics:latency_contract:slo:last': {
            'external_missing_total': '0',
            'budget_breach_total': '0',
        },
        'metrics:latency_contract:rollout_gate:last': {},
    }
    m = evaluate_once(FakeRedis(data), _cfg())
    assert int(m['gate_active']) == 0
    assert m['gate_reason_code'] == 'ok'


def test_rollout_gate_blocks_on_sustained_budget_breach():
    """Gate must activate when budget breach has been sustained beyond hold threshold."""
    data = {
        'metrics:latency_contract:slo:last': {
            'external_missing_total': '0',
            'budget_breach_total': '1',
        },
        'metrics:latency_contract:rollout_gate:last': {
            # timestamp far in the past so hold_s > budget_hold_s=1
            'budget_breach_since_ts_ms': '1',
        },
    }
    m = evaluate_once(FakeRedis(data), _cfg(budget_hold_s=1))
    assert int(m['budget_hold_reached']) == 1
    assert int(m['gate_active']) == 1
    assert 'budget_breach_sustained' in m['gate_reason_codes']


def test_rollout_gate_does_not_block_fresh_budget_breach():
    """Gate must NOT activate on a fresh (under hold threshold) budget breach."""
    import time
    now_ms = get_ny_time_millis()
    data = {
        'metrics:latency_contract:slo:last': {
            'external_missing_total': '0',
            'budget_breach_total': '1',
        },
        'metrics:latency_contract:rollout_gate:last': {
            # timestamp NOW — hold not reached for budget_hold_s=300
            'budget_breach_since_ts_ms': str(now_ms),
        },
    }
    m = evaluate_once(FakeRedis(data), _cfg(budget_hold_s=300))
    assert int(m['budget_hold_reached']) == 0
    assert int(m['gate_active']) == 0


def test_rollout_gate_blocks_on_missing_slo_summary():
    """When SLO summary is missing entirely the gate must activate (safer-than-open)."""
    data = {}  # no keys in Redis
    m = evaluate_once(FakeRedis(data), _cfg())
    assert int(m['gate_active']) == 1
    assert m['gate_reason_code'] == 'summary_missing'
    assert m['summary_present'] == '0'


def test_rollout_gate_both_reasons():
    """Gate must include both reasons when both conditions are active."""
    data = {
        'metrics:latency_contract:slo:last': {
            'external_missing_total': '1',
            'budget_breach_total': '1',
        },
        'metrics:latency_contract:rollout_gate:last': {
            'budget_breach_since_ts_ms': '1',
        },
    }
    m = evaluate_once(FakeRedis(data), _cfg(budget_hold_s=1))
    codes = m['gate_reason_codes'].split(',')
    assert 'external_missing' in codes
    assert 'budget_breach_sustained' in codes
