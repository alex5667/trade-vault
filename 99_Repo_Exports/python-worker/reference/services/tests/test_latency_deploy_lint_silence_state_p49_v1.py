"""P4.9 silence state tests: policy denial, override, and escalation ticket reuse."""
from services.observability import latency_deploy_lint_silence_state as mod


class FakeRedis:
    def __init__(self):
        self.h = {}
        self.exp = {}
        self.stream = []
    def hgetall(self, k):
        return dict(self.h.get(k, {}))
    def hset(self, k, mapping=None, **kwargs):
        cur = self.h.setdefault(k, {})
        cur.update(mapping or kwargs)
    def expire(self, k, ttl):
        self.exp[k] = ttl
    def xadd(self, stream, fields, maxlen=None, approximate=None):
        self.stream.append((stream, dict(fields)))
        return f"{len(self.stream)}-0"


def test_evaluate_ack_policy_requires_escalation_after_ack_limit() -> None:
    """Once policy_max_acks is reached, evaluate_ack_policy must require escalation."""
    raw = {
        'policy_window_start_ts_ms': '1000',
        'policy_window_end_ts_ms': str(1000 + 168 * 3600 * 1000),
        'policy_window_ack_count': '2',
        'policy_window_budget_minutes_used': '60',
    }
    out = mod.evaluate_ack_policy(raw, now_ms=2000, silence_minutes=30, policy_window_s=168 * 3600, max_budget_minutes=240, max_acks=2, ticket='INC-1')
    assert out.allowed is False
    assert out.requires_escalation is True
    assert out.limit_kind == 'ack_limit'
    assert out.denied_reason == 'escalation_ticket_required'


def test_upsert_ack_silence_policy_override_records_escalation_ticket() -> None:
    """With a valid escalation ticket, override is allowed and recorded."""
    r = FakeRedis()
    mod.upsert_ack_silence(r, prefix='cfg:x:silence', purpose='p', operator='alex', ticket='INC-1', reason='known drift', silence_minutes=30, ttl_s=3600, now_ms=1000, policy_window_s=168 * 3600, policy_max_budget_minutes=60, policy_max_acks=1)
    out = mod.upsert_ack_silence(r, prefix='cfg:x:silence', purpose='p', operator='alex', ticket='INC-1', reason='still known drift', silence_minutes=45, ttl_s=3600, ops_stream='ops:test', now_ms=2000, policy_window_s=168 * 3600, policy_max_budget_minutes=60, policy_max_acks=1, escalation_ticket='SEV-9')
    st = mod.parse_silence_state(out, now_ms=2001)
    assert st.policy_current_override_active is True
    assert st.policy_current_override_ticket == 'SEV-9'
    assert r.stream[-1][1]['kind'] == 'latency_deploy_lint_ack_silence_policy_override'


def test_upsert_ack_silence_policy_denied_rejects_reused_escalation_ticket() -> None:
    """The same escalation ticket cannot be reused within the same policy window."""
    r = FakeRedis()
    mod.upsert_ack_silence(r, prefix='cfg:x:silence', purpose='p', operator='alex', ticket='INC-1', reason='known drift', silence_minutes=30, ttl_s=3600, now_ms=1000, policy_window_s=168 * 3600, policy_max_budget_minutes=60, policy_max_acks=1)
    mod.upsert_ack_silence(r, prefix='cfg:x:silence', purpose='p', operator='alex', ticket='INC-1', reason='still known drift', silence_minutes=45, ttl_s=3600, ops_stream='ops:test', now_ms=2000, policy_window_s=168 * 3600, policy_max_budget_minutes=60, policy_max_acks=1, escalation_ticket='SEV-9')
    out = mod.upsert_ack_silence(r, prefix='cfg:x:silence', purpose='p', operator='alex', ticket='INC-1', reason='third extension', silence_minutes=45, ttl_s=3600, ops_stream='ops:test', now_ms=3000, policy_window_s=168 * 3600, policy_max_budget_minutes=60, policy_max_acks=1, escalation_ticket='SEV-9')
    assert out['last_action'] == 'ack_denied_policy'
    assert out['policy_last_deny_reason'] == 'escalation_ticket_reused'
    assert r.stream[-1][1]['kind'] == 'latency_deploy_lint_ack_silence_policy_denied'
