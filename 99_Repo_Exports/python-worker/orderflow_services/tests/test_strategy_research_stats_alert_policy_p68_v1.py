from __future__ import annotations

from pathlib import Path

import yaml

from orderflow_services.strategy_research_stats_alert_policy_exporter_v1 import (
    POLICY_OVERRIDE_EXPIRED_RECENTLY,
    POLICY_OVERRIDE_EXPIRING_SOON,
    POLICY_OVERRIDE_LAST_EXPIRED_UNIX,
    POLICY_OVERRIDE_LAST_REMINDER_UNIX,
    publish,
)
from orderflow_services.strategy_research_stats_alert_policy_override_v1 import (
    clear_override,
    override_key,
    override_state_key,
    set_override,
)


class FakeRedis:
    def __init__(self, hashes: dict[str, dict[str, str]] | None = None) -> None:
        self.hashes = dict(hashes or {})
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


def test_p68_set_override_creates_state_hash() -> None:
    r = FakeRedis()
    set_override(
        r,
        purpose='conf_score_guardrails_promote',
        family='pbo_high',
        ticket='INC-123',
        operator='alex',
        reason='temporary suppression',
        ttl_s=600,
    )
    state = r.hgetall(override_state_key('conf_score_guardrails_promote', 'pbo_high'))
    assert state['lifecycle_state'] == 'active'
    assert state['active'] == '1'
    assert state['ticket'] == 'INC-123'


def test_p68_clear_override_marks_state_cleared() -> None:
    r = FakeRedis()
    set_override(
        r,
        purpose='conf_score_guardrails_apply',
        family='report_stale',
        ticket='OPS-1',
        operator='ops',
        reason='suppress until refresh',
        ttl_s=600,
    )
    clear_override(
        r,
        purpose='conf_score_guardrails_apply',
        family='report_stale',
        ticket='OPS-2',
        operator='ops2',
        reason='refresh done',
    )
    state = r.hgetall(override_state_key('conf_score_guardrails_apply', 'report_stale'))
    assert state['lifecycle_state'] == 'cleared'
    assert state['active'] == '0'


def test_p68_publish_emits_expiry_warning_once(monkeypatch) -> None:
    now_ms = 1_700_000_000_000
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', 'conf_score_guardrails_apply')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_REMINDER_WINDOW_S', '3600')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_override:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_STATE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_state:v1')
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as m
    monkeypatch.setattr(m, '_now_ms', lambda: now_ms)
    r = FakeRedis({
        override_key('conf_score_guardrails_apply', 'pbo_high'): {
            'purpose': 'conf_score_guardrails_apply',
            'family': 'pbo_high',
            'ticket': 'INC-1',
            'operator': 'alex',
            'reason': 'temp',
            'created_ts_ms': str(now_ms - 1000),
            'expire_ts_ms': str(now_ms + 1_200_000),
        }
    })
    publish(r)
    assert POLICY_OVERRIDE_EXPIRING_SOON.labels(purpose='conf_score_guardrails_apply', family='pbo_high')._value.get() == 1.0
    assert any(e[1]['kind'] == 'strategy_research_stats_alert_policy_suppress_override_expiry_warning' for e in r.events)
    before = len(r.events)
    publish(r)
    assert len(r.events) == before
    assert POLICY_OVERRIDE_LAST_REMINDER_UNIX.labels(purpose='conf_score_guardrails_apply', family='pbo_high')._value.get() > 0


def test_p68_publish_marks_expired_recently_even_if_override_hash_missing(monkeypatch) -> None:
    now_ms = 1_700_000_000_000
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', 'conf_score_guardrails_apply')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_EXPIRED_RECENT_WINDOW_S', '21600')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_STATE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_state:v1')
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as m
    monkeypatch.setattr(m, '_now_ms', lambda: now_ms)
    state_key = override_state_key('conf_score_guardrails_apply', 'report_stale')
    r = FakeRedis({
        state_key: {
            'purpose': 'conf_score_guardrails_apply',
            'family': 'report_stale',
            'ticket': 'INC-2',
            'operator': 'ops',
            'reason': 'wait for dataset',
            'created_ts_ms': str(now_ms - 7200_000),
            'expire_ts_ms': str(now_ms - 60_000),
            'active': '1',
            'lifecycle_state': 'active',
        }
    })
    publish(r)
    assert POLICY_OVERRIDE_EXPIRED_RECENTLY.labels(purpose='conf_score_guardrails_apply', family='report_stale')._value.get() == 1.0
    assert POLICY_OVERRIDE_LAST_EXPIRED_UNIX.labels(purpose='conf_score_guardrails_apply', family='report_stale')._value.get() > 0
    assert any(e[1]['kind'] == 'strategy_research_stats_alert_policy_suppress_override_expired' for e in r.events)


def test_p68_alert_bundle_references_expiry_and_resurfaced_metrics() -> None:
    base = Path(__file__).resolve().parents[1]
    doc = yaml.safe_load((base / 'prometheus_alerts_orchestration_composite_preflight_strategy_research_stats_p68.yml').read_text())
    text = str(doc)
    assert 'strategy_research_stats_alert_policy_override_expiring_soon' in text
    assert 'strategy_research_stats_alert_policy_override_expired_recently' in text
    assert 'orchestration_composite_preflight_history_strategy_research_stats_reason_family_total' in text
