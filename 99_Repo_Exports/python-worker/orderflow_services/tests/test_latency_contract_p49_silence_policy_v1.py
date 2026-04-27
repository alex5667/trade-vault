"""P4.9 silence CLI policy tests: deny without escalation ticket, override with escalation ticket."""
from orderflow_services import latency_contract_deploy_lint_silence_v1 as mod


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


def _cfg() -> mod.Cfg:
    return mod.Cfg(
        redis_url='redis://unused',
        state_prefix='metrics:latency_contract:deploy_lint:last',
        silence_prefix='cfg:orderflow:latency_contract:deploy_lint:silence',
        ops_stream='ops:latency_contract:events:v1',
        silence_ttl_s=3600,
        default_minutes=60,
        policy_window_s=168 * 3600,
        policy_max_budget_minutes=60,
        policy_max_acks=1,
        policy_denied_exit_code=27,
    )


def test_cmd_ack_denies_when_policy_limit_hit_without_escalation_ticket() -> None:
    """Second ack without escalation ticket must be denied once max_acks=1 is exceeded."""
    r = FakeRedis()
    r.h['metrics:latency_contract:deploy_lint:last:meta_cov_rollout_controller'] = {'gate_active': '1'}
    first = mod.cmd_ack(r, _cfg(), purpose='meta_cov_rollout_controller', operator='alex', ticket='INC-1', reason='known', minutes=30, now_ms=1000)
    second = mod.cmd_ack(r, _cfg(), purpose='meta_cov_rollout_controller', operator='alex', ticket='INC-1', reason='still known', minutes=45, now_ms=2000)
    assert first['ok'] is True
    assert second['ok'] is False
    assert second['policy']['denied_reason'] == 'escalation_ticket_required'
    assert second['status']['last_action'] == 'ack_denied_policy'


def test_cmd_ack_allows_override_with_separate_escalation_ticket() -> None:
    """Second ack with a distinct escalation ticket must be allowed with override_active=True."""
    r = FakeRedis()
    r.h['metrics:latency_contract:deploy_lint:last:meta_cov_rollout_controller'] = {'gate_active': '1'}
    mod.cmd_ack(r, _cfg(), purpose='meta_cov_rollout_controller', operator='alex', ticket='INC-1', reason='known', minutes=30, now_ms=1000)
    out = mod.cmd_ack(r, _cfg(), purpose='meta_cov_rollout_controller', operator='alex', ticket='INC-1', escalation_ticket='SEV-9', reason='bridge approved', minutes=45, now_ms=2000)
    assert out['ok'] is True
    assert out['policy']['override_active'] is True
    assert out['status']['policy_current_override_ticket'] == 'SEV-9'
    assert r.stream[-1][1]['kind'] == 'latency_deploy_lint_ack_silence_policy_override'
