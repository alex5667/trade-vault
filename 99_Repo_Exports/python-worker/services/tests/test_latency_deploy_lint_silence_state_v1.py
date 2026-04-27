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


def test_upsert_ack_silence_sets_active_and_audit() -> None:
    r = FakeRedis()
    out = mod.upsert_ack_silence(r, prefix='cfg:x:silence', purpose='meta_cov_rollout_controller', operator='alex', ticket='INC-1', reason='known drift', silence_minutes=30, ttl_s=3600, ops_stream='ops:test', gate_active=True, now_ms=1000)
    st = mod.parse_silence_state(out, now_ms=1001)
    assert st.silence_active is True
    assert st.ack_operator == 'alex'
    assert r.stream[-1][1]['kind'] == 'latency_deploy_lint_ack_silence_set'


def test_clear_ack_silence_deactivates_but_preserves_ack_context() -> None:
    r = FakeRedis()
    mod.upsert_ack_silence(r, prefix='cfg:x:silence', purpose='p', operator='alex', ticket='INC-1', reason='known drift', silence_minutes=5, ttl_s=3600, now_ms=1000)
    out = mod.clear_ack_silence(r, prefix='cfg:x:silence', purpose='p', operator='alex', ticket='INC-1', reason='fixed', ttl_s=3600, ops_stream='ops:test', now_ms=2000)
    st = mod.parse_silence_state(out, now_ms=2001)
    assert st.silence_active is False
    assert st.ack_ticket == 'INC-1'
    assert out['unsilence_reason'] == 'fixed'
    assert r.stream[-1][1]['kind'] == 'latency_deploy_lint_ack_silence_cleared'


def test_parse_silence_state_expires_by_time() -> None:
    raw = {'purpose': 'p', 'silence_active': '1', 'silence_until_ts_ms': '1500'}
    assert mod.parse_silence_state(raw, now_ms=1200).silence_active is True
    assert mod.parse_silence_state(raw, now_ms=1600).silence_active is False
