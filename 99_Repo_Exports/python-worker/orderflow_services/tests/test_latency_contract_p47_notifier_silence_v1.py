from orderflow_services import latency_contract_deploy_lint_notifier_v1 as mod


class FakeRedis:
    def __init__(self):
        self.h = {}
    def hgetall(self, k):
        return dict(self.h.get(k, {}))


def test_partition_active_by_silence_filters_silenced_purposes() -> None:
    r = FakeRedis()
    r.h['cfg:lt:silence:meta_cov_rollout_controller'] = {'purpose': 'meta_cov_rollout_controller', 'silence_active': '1', 'silence_until_ts_ms': '999999', 'ack_operator': 'alex', 'ack_ticket': 'INC-42'}
    active, silenced, details = mod._partition_active_by_silence(r, prefix='cfg:lt:silence', active=['meta_cov_rollout_controller', 'conf_score_guardrails_apply'], now_ms=1000)
    assert active == ['conf_score_guardrails_apply']
    assert silenced == ['meta_cov_rollout_controller']
    assert details['meta_cov_rollout_controller']['ack_ticket'] == 'INC-42'


def test_should_emit_does_not_treat_silence_as_recovery() -> None:
    emit, kind = mod._should_emit(prev_status='active', prev_hash='abc', current_status='silenced', current_hash='', raw_active=['meta_cov_rollout_controller'], last_emit_ts_ms=0, reminder_s=60, now_ms=1000)
    assert emit is False
    assert kind == 'noop'
