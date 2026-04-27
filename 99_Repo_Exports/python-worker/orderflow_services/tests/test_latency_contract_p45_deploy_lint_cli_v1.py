from __future__ import annotations

import os
import sys
import types
from pathlib import Path

from orderflow_services import latency_contract_deploy_lint_v1 as mod


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}

    def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    def hset(self, key: str, mapping=None, **kwargs):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in dict(mapping or {}).items()})
        return 1

    def expire(self, key: str, ttl: int):
        return True

    def delete(self, key: str):
        self.hashes.pop(key, None)
        return 1


class _RedisModule:
    def __init__(self, client: FakeRedis) -> None:
        self._client = client

    class Redis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True):
            raise AssertionError('patched below')


def test_cli_returns_27_for_persistent_drift(monkeypatch, tmp_path: Path) -> None:
    fake = FakeRedis()
    redis_mod = types.SimpleNamespace()
    redis_mod.Redis = types.SimpleNamespace(from_url=lambda *a, **k: fake)
    monkeypatch.setitem(sys.modules, 'redis', redis_mod)

    report = {
        'ok': False,
        'errors': ['wrapper_wrong_compose_file'],
        'warnings': [],
        'checks': {
            'compose_file': 'x',
            'wrapper_file': 'y',
            'unit_file': 'z',
            'env_file': 'e',
            'missing_runtime_env': [],
            'missing_env_file_vars': [],
        },
    }
    monkeypatch.setattr(mod, 'lint_deploy_contract', lambda **kwargs: report)
    monkeypatch.setenv('REDIS_URL', 'redis://fake/0')
    monkeypatch.setenv('LATENCY_CONTRACT_DEPLOY_LINT_PERSIST_HOLD_S', '1')

    # Seed previous failure so the next failed run becomes persistent.
    fake.hset('metrics:latency_contract:deploy_lint:last:conf_score_guardrails_apply', mapping={
        'fail_since_ts_ms': '1',
        'last_ok_ts_ms': '0',
    })

    argv = sys.argv[:]
    try:
        sys.argv = [
            'latency_contract_deploy_lint_v1.py',
            '--purpose', 'conf_score_guardrails_apply',
            '--repo-root', str(tmp_path),
            '--json-out', str(tmp_path / 'lint.json'),
        ]
        rc = mod.main()
    finally:
        sys.argv = argv
        for k in ['REDIS_URL', 'LATENCY_CONTRACT_DEPLOY_LINT_PERSIST_HOLD_S']:
            os.environ.pop(k, None)
    assert rc == 27
