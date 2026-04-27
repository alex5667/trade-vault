from __future__ import annotations

from orderflow_services.research_guard_blocker_v1 import evaluate_research_guard_gate


class FakeRedis:
    def __init__(self, hashes):
        self.hashes = hashes

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))


def test_research_guard_allows_report_only() -> None:
    r = FakeRedis(
        {
            'cfg:research_guard:blocker:v1': {'report_only': '1', 'blocked': '1', 'reason': 'pbo_high'},
            'metrics:strategy_research_guard:last': {'updated_ts_ms': '1700000000000'},
        }
    )
    state = evaluate_research_guard_gate(
        'redis://unused',
        'cfg:research_guard:blocker:v1',
        'metrics:strategy_research_guard:last',
        client=r,
        max_age_sec=0,
    )
    assert state['status'] == 'ok'
    assert state['reason'] == 'report_only'


def test_research_guard_blocks_when_blocker_active() -> None:
    r = FakeRedis(
        {
            'cfg:research_guard:blocker:v1': {'report_only': '0', 'blocked': '1', 'reason': 'pbo_high'},
            'metrics:strategy_research_guard:last': {'updated_ts_ms': '9999999999999'},
        }
    )
    state = evaluate_research_guard_gate(
        'redis://unused',
        'cfg:research_guard:blocker:v1',
        'metrics:strategy_research_guard:last',
        client=r,
        max_age_sec=999999999,
    )
    assert state['status'] == 'block'
    assert state['reason'] == 'pbo_high'


def test_research_guard_invalid_when_missing_and_fail_closed() -> None:
    r = FakeRedis({})
    state = evaluate_research_guard_gate(
        'redis://unused',
        'cfg:research_guard:blocker:v1',
        'metrics:strategy_research_guard:last',
        client=r,
        fail_closed_missing=1,
    )
    assert state['status'] == 'invalid'
    assert state['reason'] == 'state_missing'


def test_research_guard_blocks_stale_report() -> None:
    r = FakeRedis(
        {
            'cfg:research_guard:blocker:v1': {'report_only': '0', 'blocked': '0'},
            'metrics:strategy_research_guard:last': {'updated_ts_ms': '1'},
        }
    )
    state = evaluate_research_guard_gate(
        'redis://unused',
        'cfg:research_guard:blocker:v1',
        'metrics:strategy_research_guard:last',
        client=r,
        max_age_sec=60,
    )
    assert state['status'] == 'block'
    assert state['reason'] == 'report_stale'
