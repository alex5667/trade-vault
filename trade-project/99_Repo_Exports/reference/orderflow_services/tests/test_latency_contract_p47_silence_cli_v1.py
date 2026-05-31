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
    return mod.Cfg(redis_url='redis://unused', state_prefix='metrics:latency_contract:deploy_lint:last', silence_prefix='cfg:orderflow:latency_contract:deploy_lint:silence', ops_stream='ops:latency_contract:events:v1', silence_ttl_s=3600, default_minutes=60, policy_window_s=168*3600, policy_max_budget_minutes=1440, policy_max_acks=3, policy_denied_exit_code=27)


def test_cmd_ack_marks_notifier_silence_only() -> None:
    r = FakeRedis()
    r.h['metrics:latency_contract:deploy_lint:last:meta_cov_rollout_controller'] = {'gate_active': '1', 'gate_reason_code': 'persistent_config_drift', 'error_codes': 'missing_env', 'fail_age_s': '1200'}
    out = mod.cmd_ack(r, _cfg(), purpose='meta_cov_rollout_controller', operator='alex', ticket='INC-1', reason='known', minutes=30, now_ms=1000)
    assert out['status']['gate_active'] is True
    assert out['status']['silence_active'] is True
    assert r.stream[-1][1]['kind'] == 'latency_deploy_lint_ack_silence_set'


def test_cmd_unsilence_clears_suppression() -> None:
    r = FakeRedis()
    mod.cmd_ack(r, _cfg(), purpose='meta_cov_rollout_controller', operator='alex', ticket='INC-1', reason='known', minutes=30, now_ms=1000)
    out = mod.cmd_unsilence(r, _cfg(), purpose='meta_cov_rollout_controller', operator='alex', ticket='INC-1', reason='fixed', now_ms=2000)
    assert out['status']['silence_active'] is False
    assert r.stream[-1][1]['kind'] == 'latency_deploy_lint_ack_silence_cleared'
