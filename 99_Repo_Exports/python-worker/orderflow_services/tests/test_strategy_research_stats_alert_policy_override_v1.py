from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import time
from typing import Any

from orderflow_services.strategy_research_stats_alert_policy_override_v1 import (
    clear_override,
    list_active_overrides,
    override_key,
    set_override,
)


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.expiry_s: dict[str, int] = {}
        self.events: list[tuple[str, dict[str, str]]] = []

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.hashes[key] = {**self.hashes.get(key, {}), **mapping}

    def expire(self, key: str, ttl_s: int) -> None:
        self.expiry_s[key] = ttl_s

    def delete(self, key: str) -> int:
        existed = 1 if key in self.hashes else 0
        self.hashes.pop(key, None)
        self.expiry_s.pop(key, None)
        return existed

    def xadd(self, stream: str, fields: dict[str, str], maxlen: int | None = None, approximate: bool = True) -> str:
        self.events.append((stream, dict(fields)))
        return '1-0'

    def keys(self, pattern: str) -> list[str]:
        prefix = pattern.rstrip('*')
        return [k for k in self.hashes if k.startswith(prefix)]


def test_set_override_writes_ttl_and_audit_event() -> None:
    r = FakeRedis()
    payload = set_override(
        r,
        purpose='conf_score_guardrails_promote',
        family='pbo_high',
        ticket='INC-123',
        operator='alex',
        reason='hold repeated false positives',
        ttl_s=7200,
    )
    key = override_key('conf_score_guardrails_promote', 'pbo_high')
    raw = r.hgetall(key)
    assert raw['ticket'] == 'INC-123'
    assert raw['operator'] == 'alex'
    assert raw['reason'] == 'hold repeated false positives'
    assert int(raw['expire_ts_ms']) > int(raw['created_ts_ms'])
    assert r.expiry_s[key] == 7200
    assert payload['suppress_active'] == '1'
    assert r.events and r.events[-1][1]['kind'] == 'strategy_research_stats_alert_policy_suppress_override_set'


def test_clear_override_removes_key_and_emits_audit_event() -> None:
    r = FakeRedis()
    set_override(
        r,
        purpose='conf_score_guardrails_apply',
        family='report_stale',
        ticket='OPS-1',
        operator='ops',
        reason='waiting for upstream dataset refresh',
        ttl_s=3600,
    )
    existing = clear_override(
        r,
        purpose='conf_score_guardrails_apply',
        family='report_stale',
        ticket='OPS-2',
        operator='ops2',
        reason='refresh completed',
    )
    assert existing['ticket'] == 'OPS-1'
    assert not r.hgetall(override_key('conf_score_guardrails_apply', 'report_stale'))
    assert r.events[-1][1]['kind'] == 'strategy_research_stats_alert_policy_suppress_override_cleared'


def test_list_active_overrides_filters_expired_rows() -> None:
    r = FakeRedis()
    now_ms = get_ny_time_millis()
    active_key = override_key('conf_score_guardrails_apply', 'pbo_high')
    expired_key = override_key('conf_score_guardrails_apply', 'report_stale')
    r.hset(active_key, {
        'purpose': 'conf_score_guardrails_apply',
        'family': 'pbo_high',
        'ticket': 'A',
        'operator': 'alice',
        'reason': 'investigate',
        'created_ts_ms': str(now_ms - 1000),
        'expire_ts_ms': str(now_ms + 60_000),
    })
    r.hset(expired_key, {
        'purpose': 'conf_score_guardrails_apply',
        'family': 'report_stale',
        'ticket': 'B',
        'operator': 'bob',
        'reason': 'stale',
        'created_ts_ms': str(now_ms - 120_000),
        'expire_ts_ms': str(now_ms - 1),
    })
    rows = list_active_overrides(r, purpose='conf_score_guardrails_apply')
    assert len(rows) == 1
    assert rows[0]['family'] == 'pbo_high'
    assert rows[0]['remaining_s'] >= 0
