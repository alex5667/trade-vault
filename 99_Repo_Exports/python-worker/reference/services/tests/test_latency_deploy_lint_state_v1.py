from __future__ import annotations

from services.observability.latency_deploy_lint_state import update_deploy_lint_state, state_key, gate_key


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.deleted: set[str] = set()

    def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    def hset(self, key: str, mapping=None, **kwargs):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in dict(mapping or {}).items()})
        return 1

    def expire(self, key: str, ttl: int):
        return True

    def delete(self, key: str):
        self.deleted.add(key)
        self.hashes.pop(key, None)
        return 1


def _report(ok: bool, errors: list[str] | None = None):
    return {
        'ok': ok
        'errors': list(errors or [])
        'warnings': []
        'checks': {
            'compose_file': '/tmp/compose.yml'
            'wrapper_file': '/tmp/wrapper.sh'
            'unit_file': '/tmp/unit.service'
            'env_file': '/tmp/env'
            'missing_runtime_env': []
            'missing_env_file_vars': []
        }
    }


def test_transient_failure_does_not_activate_gate() -> None:
    r = FakeRedis()
    mapping = update_deploy_lint_state(
        r
        purpose='meta_cov_rollout_controller'
        report=_report(False, ['missing_runtime_env:X'])
        state_prefix='metrics:latency_contract:deploy_lint:last'
        gate_prefix='cfg:orderflow:latency_contract:deploy_lint_gate'
        hold_s=300
        ttl_s=3600
        now_ms=1_000
    )
    assert mapping['ok'] == '0'
    assert mapping['gate_active'] == '0'
    assert state_key('metrics:latency_contract:deploy_lint:last', 'meta_cov_rollout_controller') in r.hashes
    assert gate_key('cfg:orderflow:latency_contract:deploy_lint_gate', 'meta_cov_rollout_controller') not in r.hashes


def test_persistent_failure_activates_gate_and_success_clears_it() -> None:
    r = FakeRedis()
    update_deploy_lint_state(
        r
        purpose='meta_cov_rollout_controller'
        report=_report(False, ['wrapper_wrong_compose_file'])
        state_prefix='metrics:latency_contract:deploy_lint:last'
        gate_prefix='cfg:orderflow:latency_contract:deploy_lint_gate'
        hold_s=300
        ttl_s=3600
        now_ms=1_000
    )
    mapping = update_deploy_lint_state(
        r
        purpose='meta_cov_rollout_controller'
        report=_report(False, ['wrapper_wrong_compose_file'])
        state_prefix='metrics:latency_contract:deploy_lint:last'
        gate_prefix='cfg:orderflow:latency_contract:deploy_lint_gate'
        hold_s=300
        ttl_s=3600
        now_ms=305_000
    )
    gkey = gate_key('cfg:orderflow:latency_contract:deploy_lint_gate', 'meta_cov_rollout_controller')
    assert mapping['gate_active'] == '1'
    assert r.hashes[gkey]['gate_reason_code'] == 'persistent_config_drift'

    cleared = update_deploy_lint_state(
        r
        purpose='meta_cov_rollout_controller'
        report=_report(True)
        state_prefix='metrics:latency_contract:deploy_lint:last'
        gate_prefix='cfg:orderflow:latency_contract:deploy_lint_gate'
        hold_s=300
        ttl_s=3600
        now_ms=400_000
    )
    assert cleared['ok'] == '1'
    assert cleared['gate_active'] == '0'
    assert gkey in r.deleted
