from __future__ import annotations

import pytest

from orderflow_services.strategy_research_stats_alert_policy_override_v1 import (
    OverrideWorkflowError,
    acknowledge_renewal,
    override_key,
    override_state_key,
    renew_override,
    set_override,
)
from orderflow_services.strategy_research_stats_alert_policy_exporter_v1 import (
    POLICY_OVERRIDE_RENEW_ACK_PRESENT,
    POLICY_OVERRIDE_RENEW_ACK_REQUIRED,
    POLICY_OVERRIDE_RENEW_COUNT,
    publish,
)


class FakeRedis:
    """In-memory Redis stub sufficient for override/exporter tests."""

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

    def keys(self, pattern: str) -> list[str]:
        prefix = pattern.rstrip('*')
        return [k for k in self.hashes if k.startswith(prefix)]


# ---------------------------------------------------------------------------
# P6.9 test 1: renew without acknowledge-renew must be blocked
# ---------------------------------------------------------------------------

def test_p69_requires_ack_before_renew(monkeypatch: pytest.MonkeyPatch) -> None:
    """renew_override() must raise OverrideWorkflowError when no acknowledgement is present."""
    now_ms = 1_700_000_000_000
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', 'conf_score_guardrails_promote')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_override:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_STATE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_state:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_REMINDER_WINDOW_S', '3600')
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as exp
    monkeypatch.setattr(exp, '_now_ms', lambda: now_ms)
    from orderflow_services import strategy_research_stats_alert_policy_override_v1 as mod
    monkeypatch.setattr(mod, '_now_ms', lambda: now_ms)
    r = FakeRedis()
    # Create override within expiry window so exporter marks it as reminder
    set_override(
        r,
        purpose='conf_score_guardrails_promote',
        family='pbo_high',
        ticket='INC-1',
        operator='alex',
        reason='temp suppression',
        ttl_s=1200,
    )
    # Simulate exporter publish: emits reminder, sets renew_ack_required=1
    publish(r)
    # Attempting renew without ack must fail
    try:
        renew_override(r, purpose='conf_score_guardrails_promote', family='pbo_high', ttl_s=1800)
    except OverrideWorkflowError as exc:
        assert 'acknowledged' in str(exc)
    else:  # pragma: no cover
        raise AssertionError('renew_override must require prior acknowledgement')


# ---------------------------------------------------------------------------
# P6.9 test 2: same-identity ack rejected, valid ack+renew fully rotates ticket
# ---------------------------------------------------------------------------

def test_p69_ack_then_renew_requires_new_identity_and_rotates_ticket(monkeypatch: pytest.MonkeyPatch) -> None:
    """acknowledge_renewal() rejects same identity; valid ack followed by renew_override() rotates state."""
    now_ms = 1_700_000_000_000
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', 'conf_score_guardrails_promote')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_override:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_STATE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_state:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_REMINDER_WINDOW_S', '3600')
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as exp
    monkeypatch.setattr(exp, '_now_ms', lambda: now_ms)
    from orderflow_services import strategy_research_stats_alert_policy_override_v1 as mod
    monkeypatch.setattr(mod, '_now_ms', lambda: now_ms)
    r = FakeRedis()
    set_override(
        r,
        purpose='conf_score_guardrails_promote',
        family='pbo_high',
        ticket='INC-1',
        operator='alex',
        reason='temp suppression',
        ttl_s=1200,
    )
    # Exporter sets renew_ack_required=1 via expiry reminder
    publish(r)
    # Reject same-identity ack
    try:
        acknowledge_renewal(
            r,
            purpose='conf_score_guardrails_promote',
            family='pbo_high',
            ticket='INC-1',
            operator='alex',
            reason='temp suppression',
        )
    except OverrideWorkflowError as exc:
        assert 'new ticket' in str(exc)
    else:  # pragma: no cover
        raise AssertionError('acknowledge_renewal must reject same identity')
    # Accept new identity
    ack = acknowledge_renewal(
        r,
        purpose='conf_score_guardrails_promote',
        family='pbo_high',
        ticket='INC-2',
        operator='bob',
        reason='extend while root cause ticket updated',
    )
    assert ack['renew_ack_required'] == '1'
    assert ack['renew_ack_ticket'] == 'INC-2'
    # Renew with a new TTL — consumes the ack, activates fresh override
    renewed = renew_override(r, purpose='conf_score_guardrails_promote', family='pbo_high', ttl_s=1800)
    raw = r.hgetall(override_key('conf_score_guardrails_promote', 'pbo_high'))
    state = r.hgetall(override_state_key('conf_score_guardrails_promote', 'pbo_high'))
    # Override hash carries the new ticket
    assert renewed['ticket'] == 'INC-2'
    assert raw['ticket'] == 'INC-2'
    # State hash: renew_count incremented, audit trail populated, ack cleared
    assert state['renew_count'] == '1'
    assert state['renewed_from_ticket'] == 'INC-1'
    assert state['renew_ack_required'] == '0'
    assert state['renew_ack_ticket'] == ''
    # Audit stream carries the renewed event
    assert any(e[1]['kind'] == 'strategy_research_stats_alert_policy_suppress_override_renewed' for e in r.events)


# ---------------------------------------------------------------------------
# P6.9 test 3: exporter surfaces renew_ack_required, renew_ack_present, renew_count
# ---------------------------------------------------------------------------

def test_p69_exporter_surfaces_renew_ack_and_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """publish() correctly exposes renew_ack_required, renew_ack_present and renew_count gauges."""
    now_ms = 1_700_000_000_000
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', 'conf_score_guardrails_apply')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_override:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_STATE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_state:v1')
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as exp
    monkeypatch.setattr(exp, '_now_ms', lambda: now_ms)
    # Pre-seed lifecycle state as if already in renewal-pending state with a stored ack
    r = FakeRedis({
        override_state_key('conf_score_guardrails_apply', 'report_stale'): {
            'purpose': 'conf_score_guardrails_apply',
            'family': 'report_stale',
            'ticket': 'INC-1',
            'operator': 'ops1',
            'reason': 'wait for dataset',
            'created_ts_ms': str(now_ms - 7200_000),
            'expire_ts_ms': str(now_ms - 60_000),
            'active': '0',
            'lifecycle_state': 'expired',
            'expired_ts_ms': str(now_ms - 60_000),
            'renew_ack_required': '1',
            'renew_ack_ts_ms': str(now_ms - 30_000),
            'renew_ack_ticket': 'INC-2',
            'renew_ack_operator': 'ops2',
            'renew_ack_reason': 'renew while new incident open',
            'renew_count': '2',
        }
    })
    publish(r)
    # All three metrics must reflect the pre-seeded state
    assert POLICY_OVERRIDE_RENEW_ACK_REQUIRED.labels(purpose='conf_score_guardrails_apply', family='report_stale')._value.get() == 1.0
    assert POLICY_OVERRIDE_RENEW_ACK_PRESENT.labels(purpose='conf_score_guardrails_apply', family='report_stale')._value.get() == 1.0
    assert POLICY_OVERRIDE_RENEW_COUNT.labels(purpose='conf_score_guardrails_apply', family='report_stale')._value.get() == 2.0
