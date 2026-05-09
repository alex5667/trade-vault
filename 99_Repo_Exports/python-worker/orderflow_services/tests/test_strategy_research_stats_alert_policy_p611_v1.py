from __future__ import annotations

from orderflow_services.strategy_research_stats_alert_policy_exporter_v1 import (
    POLICY_OVERRIDE_DUAL_CONTROL_APPROVAL_AGE,
    POLICY_OVERRIDE_DUAL_CONTROL_APPROVER_PRESENT,
    POLICY_OVERRIDE_DUAL_CONTROL_REQUIRED,
    POLICY_OVERRIDE_DUAL_CONTROL_STATE,
    publish,
)
from orderflow_services.strategy_research_stats_alert_policy_override_v1 import (
    OverrideWorkflowError,
    acknowledge_renewal,
    approve_dual_control_renewal,
    override_key,
    override_state_key,
    renew_override,
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

    def keys(self, pattern: str) -> list[str]:
        prefix = pattern.rstrip('*')
        return [k for k in self.hashes if k.startswith(prefix)]


def _set_common_env(monkeypatch) -> None:
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', 'conf_score_guardrails_promote')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_override:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_STATE_PREFIX', 'cfg:strategy_research_stats:alert_policy:suppress_state:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_LIMITS_PREFIX', 'cfg:strategy_research_stats:alert_policy:override_limits:v1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_LIMITS_DEFAULTS_KEY', 'cfg:strategy_research_stats:alert_policy:override_limits:v1:defaults')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_REMINDER_WINDOW_S', '3600')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_DEFAULT_MAX_BUDGET_S', '1500')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_DEFAULT_MAX_RENEW_COUNT', '2')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_REQUIRE_ESCALATION_ON_LIMIT', '1')
    monkeypatch.setenv('STRATEGY_RESEARCH_STATS_ALERT_POLICY_DUAL_CONTROL_ON_LIMIT', '1')


def test_p611_limit_hit_requires_dual_control_before_renew(monkeypatch) -> None:
    now_ms = 1_700_000_000_000
    _set_common_env(monkeypatch)
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
        operator='alice',
        reason='initial suppression',
        ttl_s=1200,
    )
    publish(r)
    acknowledge_renewal(
        r,
        purpose='conf_score_guardrails_promote',
        family='pbo_high',
        ticket='INC-2',
        operator='bob',
        reason='renew with new ticket',
        escalation_ticket='SEV-1',
        escalation_operator='lead',
        escalation_reason='budget exceeded',
    )
    try:
        renew_override(r, purpose='conf_score_guardrails_promote', family='pbo_high', ttl_s=600)
    except OverrideWorkflowError as exc:
        assert 'dual-control approval' in str(exc)
    else:  # pragma: no cover
        raise AssertionError('renew_override must require dual-control approval on limit-hit escalation path')
    state = r.hgetall(override_state_key('conf_score_guardrails_promote', 'pbo_high'))
    assert state['dual_control_required'] == '1'
    assert state['dual_control_approval_state'] == 'pending'
    assert state['dual_control_request_ticket'] == 'SEV-1'
    assert any(ev[1]['kind'] == 'strategy_research_stats_alert_policy_suppress_override_dual_control_required' for ev in r.events)


def test_p611_second_approver_must_differ_and_consumed_on_renew(monkeypatch) -> None:
    now_ms = 1_700_000_000_000
    _set_common_env(monkeypatch)
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as exp
    monkeypatch.setattr(exp, '_now_ms', lambda: now_ms)
    from orderflow_services import strategy_research_stats_alert_policy_override_v1 as mod
    monkeypatch.setattr(mod, '_now_ms', lambda: now_ms)
    r = FakeRedis()
    set_override(
        r,
        purpose='conf_score_guardrails_promote',
        family='report_stale',
        ticket='INC-10',
        operator='alice',
        reason='wait for feed recovery',
        ttl_s=1200,
    )
    publish(r)
    acknowledge_renewal(
        r,
        purpose='conf_score_guardrails_promote',
        family='report_stale',
        ticket='INC-11',
        operator='bob',
        reason='renew while refresh underway',
        escalation_ticket='SEV-200',
        escalation_operator='lead',
        escalation_reason='budget exceeded and requested',
    )
    try:
        renew_override(r, purpose='conf_score_guardrails_promote', family='report_stale', ttl_s=600)
    except OverrideWorkflowError:
        pass
    else:  # pragma: no cover
        raise AssertionError('renew must block until dual-control approval exists')
    try:
        approve_dual_control_renewal(
            r,
            purpose='conf_score_guardrails_promote',
            family='report_stale',
            approval_ticket='APR-1',
            approver='lead',
            approval_reason='same operator should fail',
        )
    except OverrideWorkflowError as exc:
        assert 'differ' in str(exc)
    else:  # pragma: no cover
        raise AssertionError('approve_dual_control_renewal must reject same operator as escalation operator')
    approve_dual_control_renewal(
        r,
        purpose='conf_score_guardrails_promote',
        family='report_stale',
        approval_ticket='APR-2',
        approver='manager',
        approval_reason='approved by second operator',
    )
    renewed = renew_override(r, purpose='conf_score_guardrails_promote', family='report_stale', ttl_s=600)
    raw = r.hgetall(override_key('conf_score_guardrails_promote', 'report_stale'))
    state = r.hgetall(override_state_key('conf_score_guardrails_promote', 'report_stale'))
    assert renewed['ticket'] == 'INC-11'
    assert raw['ticket'] == 'INC-11'
    assert state['dual_control_approval_state'] == 'consumed'
    assert state['dual_control_approved_operator'] == 'manager'
    assert state['dual_control_required'] == '1'
    assert any(ev[1]['kind'] == 'strategy_research_stats_alert_policy_suppress_override_dual_control_approved' for ev in r.events)


def test_p611_exporter_surfaces_dual_control_state(monkeypatch) -> None:
    now_ms = 1_700_000_000_000
    _set_common_env(monkeypatch)
    from orderflow_services import strategy_research_stats_alert_policy_exporter_v1 as exp
    monkeypatch.setattr(exp, '_now_ms', lambda: now_ms)
    r = FakeRedis({
        override_state_key('conf_score_guardrails_promote', 'pbo_high'): {
            'purpose': 'conf_score_guardrails_promote',
            'family': 'pbo_high',
            'ticket': 'INC-1',
            'operator': 'alice',
            'reason': 'temp suppression',
            'created_ts_ms': str(now_ms - 1800_000),
            'expire_ts_ms': str(now_ms - 60_000),
            'active': '0',
            'lifecycle_state': 'expired',
            'expired_ts_ms': str(now_ms - 60_000),
            'renew_ack_required': '1',
            'renew_ack_ts_ms': str(now_ms - 30_000),
            'renew_ack_ticket': 'INC-2',
            'renew_ack_operator': 'bob',
            'renew_ack_reason': 'renew with new incident',
            'renew_escalation_ticket': 'SEV-9',
            'renew_escalation_operator': 'lead',
            'renew_escalation_reason': 'limit override requested',
            'policy_budget_used_s': '4200',
            'policy_max_budget_s': '3600',
            'policy_max_renew_count': '1',
            'policy_limit_hit_kind': 'budget',
            'policy_limit_hit_ts_ms': str(now_ms - 20_000),
            'policy_limit_requires_escalation': '1',
            'renew_count': '2',
            'dual_control_required': '1',
            'dual_control_approval_state': 'approved',
            'dual_control_request_ts_ms': str(now_ms - 25_000),
            'dual_control_request_ticket': 'SEV-9',
            'dual_control_request_operator': 'lead',
            'dual_control_request_reason': 'limit override requested',
            'dual_control_approved_ts_ms': str(now_ms - 15_000),
            'dual_control_approved_ticket': 'APR-5',
            'dual_control_approved_operator': 'director',
            'dual_control_approved_reason': 'second approver accepted',
        },
    })
    publish(r)
    assert POLICY_OVERRIDE_DUAL_CONTROL_REQUIRED.labels(purpose='conf_score_guardrails_promote', family='pbo_high')._value.get() == 1.0
    assert POLICY_OVERRIDE_DUAL_CONTROL_STATE.labels(purpose='conf_score_guardrails_promote', family='pbo_high', state='approved')._value.get() == 1.0
    assert POLICY_OVERRIDE_DUAL_CONTROL_APPROVER_PRESENT.labels(purpose='conf_score_guardrails_promote', family='pbo_high')._value.get() == 1.0
    assert POLICY_OVERRIDE_DUAL_CONTROL_APPROVAL_AGE.labels(purpose='conf_score_guardrails_promote', family='pbo_high')._value.get() == 15.0
