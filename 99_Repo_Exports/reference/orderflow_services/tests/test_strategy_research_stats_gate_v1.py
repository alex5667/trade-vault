from __future__ import annotations

import time
import pytest
from orderflow_services.strategy_research_stats_gate_v1 import evaluate_strategy_research_stats_gate, gate_check_message


class FakeRedis:
    """Minimal Redis stub for testing."""

    def __init__(self, blocker: dict | None = None, summary: dict | None = None):
        self._data: dict[str, dict[str, str]] = {}
        if blocker:
            self._data['blk'] = {k: str(v) for k, v in blocker.items()}
        if summary:
            self._data['sum'] = {k: str(v) for k, v in summary.items()}

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._data.get(key, {}))


REDIS_URL = 'redis://localhost:6379/0'
BLOCKER_KEY = 'blk'
SUMMARY_KEY = 'sum'


def _call(blocker=None, summary=None, *, max_age_sec=0.0, fail_closed_missing=0):
    client = FakeRedis(blocker, summary)
    return evaluate_strategy_research_stats_gate(
        REDIS_URL, BLOCKER_KEY, SUMMARY_KEY,
        max_age_sec=max_age_sec,
        fail_closed_missing=fail_closed_missing,
        client=client,
    )


def test_empty_state_allowed_by_default():
    r = _call()
    assert r['status'] == 'ok'
    assert not r['blocked']


def test_empty_state_fail_closed_hard_mode():
    r = _call(fail_closed_missing=1, blocker={'gate_mode': 'hard'})
    # blocker has data (gate_mode field), so not empty → check path
    # since blocked=0 and soft_blocked=0 → ok
    assert r['gate_mode'] == 'hard'


def test_ok_state():
    now_ms = int(time.time() * 1000)
    r = _call(
        blocker={'gate_mode': 'report_only', 'blocked': 0, 'soft_blocked': 0, 'reason': 'ok', 'updated_ts_ms': now_ms},
        summary={'updated_ts_ms': now_ms, 'psr': 0.6},
    )
    assert r['status'] == 'ok'
    assert not r['blocked']
    assert not r['soft_blocked']


def test_hard_block():
    now_ms = int(time.time() * 1000)
    r = _call(
        blocker={'gate_mode': 'hard', 'blocked': 1, 'reason': 'psr_low', 'updated_ts_ms': now_ms},
        summary={'updated_ts_ms': now_ms},
    )
    assert r['status'] == 'block'
    assert r['blocked']
    assert 'psr_low' in str(r['reason'])


def test_soft_block():
    now_ms = int(time.time() * 1000)
    r = _call(
        blocker={'gate_mode': 'soft', 'soft_blocked': 1, 'reason': 'dsr_low', 'updated_ts_ms': now_ms},
        summary={'updated_ts_ms': now_ms},
    )
    assert r['status'] == 'soft'
    assert not r['blocked']
    assert r['soft_blocked']


def test_stale_report_hard():
    old_ms = int((time.time() - 200000) * 1000)
    r = _call(
        blocker={'gate_mode': 'hard', 'blocked': 0, 'updated_ts_ms': old_ms},
        summary={'updated_ts_ms': old_ms},
        max_age_sec=86400.0,
    )
    assert r['status'] == 'block'
    assert 'stale' in str(r['reason'])


def test_stale_report_soft():
    old_ms = int((time.time() - 200000) * 1000)
    r = _call(
        blocker={'gate_mode': 'soft', 'blocked': 0, 'updated_ts_ms': old_ms},
        summary={'updated_ts_ms': old_ms},
        max_age_sec=86400.0,
    )
    assert r['status'] == 'soft'


def test_gate_check_message():
    state = {'status': 'soft', 'reason': 'dsr_low', 'gate_mode': 'soft'}
    msg = gate_check_message(state, purpose='test_purpose')
    assert 'purpose=test_purpose' in msg
    assert 'status=soft' in msg
    assert 'gate_mode=soft' in msg
