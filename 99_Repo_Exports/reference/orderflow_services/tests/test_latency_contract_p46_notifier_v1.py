from orderflow_services import latency_contract_deploy_lint_notifier_v1 as mod


class FakeRedis:
    def __init__(self):
        self.h = {}
        self.streams = []
        self.exp = {}
    def hgetall(self, k):
        return dict(self.h.get(k, {}))
    def hset(self, k, mapping=None, **kwargs):
        cur = self.h.setdefault(k, {})
        cur.update(mapping or kwargs)
    def expire(self, k, ttl):
        self.exp[k] = ttl
    def xadd(self, stream, fields, maxlen=None, approximate=None):
        self.streams.append((stream, dict(fields)))
        return '1-0'


def test_summary_text_mentions_purpose() -> None:
    txt = mod._summary_text(['meta_cov_rollout_controller'], {'meta_cov_rollout_controller': {'error_codes': 'missing_env'}})
    assert 'meta_cov_rollout_controller' in txt


def test_active_state_payload_reads_gate_flags() -> None:
    r = FakeRedis()
    r.h['metrics:latency_contract:deploy_lint:last:conf_score_guardrails_apply'] = {'gate_active': '1'}
    active, details = mod._active_state_payload(r, 'metrics:latency_contract:deploy_lint:last')
    assert 'conf_score_guardrails_apply' in active
    assert 'conf_score_guardrails_apply' in details
