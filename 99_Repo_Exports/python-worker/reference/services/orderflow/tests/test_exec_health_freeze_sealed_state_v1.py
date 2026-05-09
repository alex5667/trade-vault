from __future__ import annotations

import os

from services.orderflow.exec_health_freeze_sealed_state import (
    prepare_sealed_mapping,
    sealed_set_sync,
    verify_sealed_hash,
)


class FakeRedis:
    def __init__(self):
        self.hashes = {}

    def execute_command(self, *args):
        if args[:2] == ('FUNCTION', 'LOAD'):
            return 'ok'
        if args and args[0] == 'FCALL':
            _, fn, numkeys, key, *argv = args
            cur = self.hashes.setdefault(key, {})
            if fn.endswith('sealed_set'):
                expected_seal = str(argv[0])
                expected_ver = int(argv[1])
                cur_seal = (cur.get('seal_digest', ''))
                cur_ver = int(cur.get('seal_version', 0) or 0)
                if cur_seal != expected_seal:
                    return 0
                if cur_ver != expected_ver:
                    return -2
            flat = argv[4:]
            cur.clear()
            for i in range(0, len(flat), 2):
                cur[str(flat[i])] = str(flat[i + 1])
            return 1
        raise AssertionError(f'unexpected command {args}')


def test_prepare_and_verify_sealed_mapping() -> None:
    os.environ['EXEC_HEALTH_FREEZE_SEAL_SECRET'] = 'seal-secret'
    obj = prepare_sealed_mapping(prev_raw={}, mapping={'effective_freeze_active': '1', 'control_source': 'autoguard'}, entrypoint='test', secret='seal-secret')
    assert obj['seal_digest']
    assert verify_sealed_hash(obj, secret='seal-secret') is True


def test_bootstrap_from_unsealed_prev_hash() -> None:
    os.environ['EXEC_HEALTH_FREEZE_SEAL_SECRET'] = 'seal-secret'
    r = FakeRedis()
    r.hashes['k'] = {'effective_freeze_active': '1', 'control_source': 'autoguard'}
    out = sealed_set_sync(r, key='k', prev_raw=r.hashes['k'], mapping={'effective_freeze_active': '1', 'control_source': 'autoguard'}, entrypoint='bootstrap', ttl_s=60)
    assert out['ok'] is True
    assert r.hashes['k']['seal_digest']
    assert verify_sealed_hash(r.hashes['k'], secret='seal-secret') is True


def test_invalid_prev_seal_is_rejected_without_force(monkeypatch) -> None:
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_SEAL_SECRET', 'seal-secret')
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_SEAL_ENFORCE', '1')
    r = FakeRedis()
    bad = prepare_sealed_mapping(prev_raw={}, mapping={'effective_freeze_active': '1'}, entrypoint='test', secret='seal-secret')
    bad['effective_freeze_active'] = '0'
    r.hashes['k'] = bad
    out = sealed_set_sync(r, key='k', prev_raw=r.hashes['k'], mapping={'effective_freeze_active': '1'}, entrypoint='test', ttl_s=60)
    assert out['ok'] is False
    assert out.get('error') == 'invalid_prev_seal'
