from services.observability import latency_deploy_lint_notify_state as mod


class FakeRedis:
    def __init__(self):
        self.h = {}
        self.exp = {}
    def hgetall(self, k):
        return dict(self.h.get(k, {}))
    def hset(self, k, mapping=None, **kwargs):
        cur = self.h.setdefault(k, {})
        cur.update(mapping or kwargs)
    def expire(self, k, ttl):
        self.exp[k] = ttl


def test_update_notifier_state_sets_fields() -> None:
    r = FakeRedis()
    out = mod.update_notifier_state(r, prefix='x:notifier:last', active_purposes=['a','b'], emitted=True, event_kind='drift', ttl_s=60, now_ms=1000)
    assert out['last_status'] == 'active'
    assert out['active_purposes_count'] == '2'
    assert r.h['x:notifier:last']['last_emit_ts_ms'] == '1000'
