from __future__ import annotations
"""P4.13 semantic approval binding workflow tests.

Tests that the dual-control approval is invalidated when gate_reason_code,
errors_count, or details_json fingerprint change between prepare/approve and final ack.
"""

from orderflow_services import latency_contract_deploy_lint_silence_v1 as mod


class FakeRedis:
    def __init__(self):
        self.h = {}
        self.exp = {}
        self.stream = []
        self.kv = {}

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

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, ex=None):
        self.kv[k] = v
        if ex is not None:
            self.exp[k] = ex


def _cfg() -> mod.Cfg:
    return mod.Cfg(
        redis_url='redis://unused',
        state_prefix='metrics:latency_contract:deploy_lint:last',
        silence_prefix='cfg:orderflow:latency_contract:deploy_lint:silence',
        approval_prefix='cfg:orderflow:latency_contract:deploy_lint:silence_approval',
        ops_stream='ops:latency_contract:events:v1',
        silence_ttl_s=3600,
        approval_ttl_s=3600,
        approval_prepared_freshness_s=60,
        approval_approved_freshness_s=60,
        default_minutes=60,
        policy_window_s=168 * 3600,
        policy_max_budget_minutes=60,
        policy_max_acks=1,
        policy_denied_exit_code=27,
        dual_control_minutes=45,
    )


def _lint_key(purpose: str) -> str:
    return f'metrics:latency_contract:deploy_lint:last:{purpose}'


def _prepare_and_approve(r, cfg, purpose='meta_cov_rollout_controller'):
    """Helper: exhaust budget with short ack, then prepare+approve a long one."""
    r.h[_lint_key(purpose)] = {
        'gate_active': '1',
        'gate_reason_code': 'compose_missing_preflight_wrapper',
        'errors_count': '2',
        'error_codes': 'compose_missing_preflight_wrapper',
    }
    mod.cmd_ack(r, cfg, purpose=purpose, operator='alex', ticket='INC-1', reason='known', minutes=30, now_ms=1000)
    prep = mod.cmd_prepare_override(
        r,
        cfg,
        purpose=purpose,
        operator='alex',
        ticket='INC-1',
        escalation_ticket='SEV-9',
        reason='need long silence',
        minutes=45,
        now_ms=2000,
    )
    req_id = prep['request_id']
    mod.cmd_approve_override(r, cfg, request_id=req_id, operator='bob', reason='approved', now_ms=3000)
    return req_id


def test_ack_invalidates_approval_when_gate_reason_code_changes() -> None:
    """P4.13: approval invalidated when gate_reason_code changes after prepare."""
    r = FakeRedis()
    cfg = _cfg()
    purpose = 'meta_cov_rollout_controller'
    req_id = _prepare_and_approve(r, cfg, purpose)
    approval_key = f'cfg:orderflow:latency_contract:deploy_lint:silence_approval:req:{req_id}'
    assert r.h[approval_key]['bound_gate_reason_code'] == 'compose_missing_preflight_wrapper'
    # Simulate gate_reason_code change
    r.h[_lint_key(purpose)]['gate_reason_code'] = 'wrapper_wrong_purpose'
    out = mod.cmd_ack(
        r,
        cfg,
        purpose=purpose,
        operator='alex',
        ticket='INC-1',
        escalation_ticket='SEV-9',
        approval_request_id=req_id,
        reason='bridge approved',
        minutes=45,
        now_ms=4000,
    )
    assert out['ok'] is False
    assert 'gate_reason_code' in out['policy']['denied_reason']
    assert r.h[approval_key]['status'] == 'invalidated'
    assert 'gate_reason_code' in r.h[approval_key]['invalidated_reason']
    assert any(evt[1]['kind'] == 'latency_deploy_lint_override_approval_invalidated' for evt in r.stream)


def test_ack_invalidates_approval_when_errors_count_changes() -> None:
    """P4.13: approval invalidated when errors_count changes after prepare."""
    r = FakeRedis()
    cfg = _cfg()
    purpose = 'meta_cov_rollout_controller'
    req_id = _prepare_and_approve(r, cfg, purpose)
    approval_key = f'cfg:orderflow:latency_contract:deploy_lint:silence_approval:req:{req_id}'
    assert r.h[approval_key]['bound_errors_count'] == '2'
    # Simulate errors_count change
    r.h[_lint_key(purpose)]['errors_count'] = '5'
    out = mod.cmd_ack(
        r,
        cfg,
        purpose=purpose,
        operator='alex',
        ticket='INC-1',
        escalation_ticket='SEV-9',
        approval_request_id=req_id,
        reason='bridge approved',
        minutes=45,
        now_ms=4000,
    )
    assert out['ok'] is False
    assert 'errors_count' in out['policy']['denied_reason']
    assert r.h[approval_key]['status'] == 'invalidated'


def test_ack_invalidates_approval_when_details_fingerprint_changes() -> None:
    """P4.13: approval invalidated when details_json fingerprint changes after prepare."""
    r = FakeRedis()
    cfg = _cfg()
    purpose = 'meta_cov_rollout_controller'
    req_id = _prepare_and_approve(r, cfg, purpose)
    approval_key = f'cfg:orderflow:latency_contract:deploy_lint:silence_approval:req:{req_id}'
    orig_fingerprint = r.h[approval_key]['bound_details_fingerprint']
    assert orig_fingerprint  # fingerprint was captured
    # Simulate a change in compose_file (changes details_json fingerprint)
    r.h[_lint_key(purpose)]['compose_file'] = 'new_compose_file.yml'
    out = mod.cmd_ack(
        r,
        cfg,
        purpose=purpose,
        operator='alex',
        ticket='INC-1',
        escalation_ticket='SEV-9',
        approval_request_id=req_id,
        reason='bridge approved',
        minutes=45,
        now_ms=4000,
    )
    assert out['ok'] is False
    assert 'details_fingerprint' in out['policy']['denied_reason']
    assert r.h[approval_key]['status'] == 'invalidated'
    assert r.h[approval_key]['invalidated_details_fingerprint'] != orig_fingerprint


def test_ack_succeeds_when_drift_unchanged() -> None:
    """P4.13: approval accepted when all semantic fields match at ack time."""
    r = FakeRedis()
    cfg = _cfg()
    purpose = 'meta_cov_rollout_controller'
    req_id = _prepare_and_approve(r, cfg, purpose)
    # Do NOT change any drift fields; ack should succeed
    out = mod.cmd_ack(
        r,
        cfg,
        purpose=purpose,
        operator='alex',
        ticket='INC-1',
        escalation_ticket='SEV-9',
        approval_request_id=req_id,
        reason='bridge approved',
        minutes=45,
        now_ms=4000,
    )
    assert out['ok'] is True, f"Expected ok but got: {out.get('policy', {}).get('denied_reason')}"


def test_cmd_status_reports_binding_mismatch_fields() -> None:
    """P4.13: status reflects binding_mismatch_fields when gate_reason_code drifts."""
    r = FakeRedis()
    cfg = _cfg()
    purpose = 'meta_cov_rollout_controller'
    req_id = _prepare_and_approve(r, cfg, purpose)
    # Change gate_reason_code
    r.h[_lint_key(purpose)]['gate_reason_code'] = 'wrapper_wrong_purpose'
    status = mod.cmd_status(r, cfg, purpose=purpose, now_ms=5000)
    row = status['rows'][0]
    assert 'gate_reason_code' in row['latest_approval_binding_mismatch_fields']
    assert row['latest_approval_binding_match'] is False


def test_invalidated_approval_is_not_reused() -> None:
    """P4.13: once invalidated, trying ack again with same request_id fails with invalidated reason."""
    r = FakeRedis()
    cfg = _cfg()
    purpose = 'meta_cov_rollout_controller'
    req_id = _prepare_and_approve(r, cfg, purpose)
    # Change drift and ack → invalidates
    r.h[_lint_key(purpose)]['gate_reason_code'] = 'wrapper_wrong_purpose'
    mod.cmd_ack(
        r, cfg, purpose=purpose, operator='alex', ticket='INC-1',
        escalation_ticket='SEV-9', approval_request_id=req_id,
        reason='test', minutes=45, now_ms=4000,
    )
    # Try to reuse - restore original drift (simulate fix and revert) but req is now invalidated
    r.h[_lint_key(purpose)]['gate_reason_code'] = 'compose_missing_preflight_wrapper'
    out2 = mod.cmd_ack(
        r, cfg, purpose=purpose, operator='alex', ticket='INC-1',
        escalation_ticket='SEV-9', approval_request_id=req_id,
        reason='retry', minutes=45, now_ms=5000,
    )
    assert out2['ok'] is False
    assert 'invalidated' in out2['policy']['denied_reason']
