from __future__ import annotations

from orderflow_services.strategy_research_stats_alert_policy_exporter_v1 import (
    POLICY_OVERRIDE_ACTIVE,
    POLICY_OVERRIDE_REMAINING,
    POLICY_SUPPRESS,
    publish,
    resolve_family_policy,
)


def test_resolve_family_policy_applies_defaults_then_purpose_override() -> None:
    defaults = {'min_events_24h_pbo_high': '5', 'enabled_pbo_high': '1'}
    purpose = {'min_events_24h_pbo_high': '9', 'suppress_active_pbo_high': '1'}
    policy = resolve_family_policy('pbo_high', defaults, purpose)
    assert policy['min_events_24h'] == 9.0
    assert policy['enabled'] == 1.0
    assert policy['suppress_active'] == 1.0


def test_resolve_family_policy_psr_dsr_low_keeps_numeric_thresholds() -> None:
    policy = resolve_family_policy('psr_dsr_low', {'share_threshold_24h_psr_dsr_low': '0.7'}, {'delta_vs_7d_psr_dsr_low': '0.2'})
    assert policy['share_threshold_24h'] == 0.7
    assert policy['delta_vs_7d'] == 0.2
    assert policy['min_events_24h'] >= 1.0


class _FakeRedis:
    def __init__(self, hashes: dict[str, dict[str, str]]) -> None:
        self.hashes = hashes
    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))
    def delete(self, key: str) -> int:
        self.hashes.pop(key, None)
        return 1


def test_publish_applies_ttl_override_to_effective_suppression(monkeypatch) -> None:
    now_ms = 1_700_000_000_000
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', 'conf_score_guardrails_apply')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_override:v1')
    hashes = {
        'cfg:strategy_research_stats:alert_policy:suppress_override:v1:conf_score_guardrails_apply:pbo_high': {
            'ticket': 'INC-9',
            'operator': 'alex',
            'reason': 'temporary noise suppression',
            'created_ts_ms': str(now_ms - 1000),
            'expire_ts_ms': str(now_ms + 120_000),
        }
    }
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as m
    monkeypatch.setattr(m, '_now_ms', lambda: now_ms)
    publish(_FakeRedis(hashes))
    assert POLICY_SUPPRESS.labels(purpose='conf_score_guardrails_apply', family='pbo_high')._value.get() == 1.0
    assert POLICY_OVERRIDE_ACTIVE.labels(purpose='conf_score_guardrails_apply', family='pbo_high')._value.get() == 1.0
    assert POLICY_OVERRIDE_REMAINING.labels(purpose='conf_score_guardrails_apply', family='pbo_high')._value.get() >= 100.0


def test_publish_clears_expired_override(monkeypatch) -> None:
    now_ms = 1_700_000_000_000
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', 'conf_score_guardrails_apply')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_override:v1')
    key = 'cfg:strategy_research_stats:alert_policy:suppress_override:v1:conf_score_guardrails_apply:report_stale'
    hashes = {
        key: {
            'ticket': 'INC-10',
            'operator': 'ops',
            'reason': 'expired',
            'created_ts_ms': str(now_ms - 1000),
            'expire_ts_ms': str(now_ms - 1),
        }
    }
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as m
    monkeypatch.setattr(m, '_now_ms', lambda: now_ms)
    r = _FakeRedis(hashes)
    publish(r)
    assert key not in r.hashes
    assert POLICY_OVERRIDE_ACTIVE.labels(purpose='conf_score_guardrails_apply', family='report_stale')._value.get() == 0.0
