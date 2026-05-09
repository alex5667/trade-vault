#!/usr/bin/env python3
from __future__ import annotations

"""ExecHealth Redis reconnect chaos/integration harness.

P16 adds an operator-facing harness and deterministic integration tests for the
P15 self-healing contract. The goal is to validate the full path, not just the
local helper:

- a reconnect/drifted client is repaired back to the expected name/lib-name;
- a recovery event is emitted into the existing freeze event stream;
- the persisted heal state increments recovery_total;
- wrong_user remains a hard violation and is not self-healed.

The harness can run against a real Redis URL or against test doubles.
"""

import argparse
import json
import os
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from services.orderflow.exec_health_freeze_reconnect_healing import (
    get_heal_state_key,
    heal_service_identity_sync,
)
from services.orderflow.exec_health_freeze_service_identity import (
    ensure_service_identity_sync,
    get_expected_service,
)


def _s(x: Any, d: str = '') -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


class ChaosHarness:
    def __init__(self, redis_url: str, *, service: str, wrong_user_url: str = '') -> None:
        if redis is None:
            raise RuntimeError('redis dependency missing')
        self.redis_url = redis_url
        self.wrong_user_url = wrong_user_url
        self.service = service
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)

    def _read_heal_state(self) -> dict[str, Any]:
        try:
            return self.r.hgetall(get_heal_state_key(self.service)) or {}
        except Exception:
            return {}

    def _clear_state(self) -> None:
        try:
            self.r.delete(get_heal_state_key(self.service))
        except Exception:
            pass

    def _current_entry(self) -> dict[str, Any]:
        raw = self.r.execute_command('CLIENT', 'LIST', 'ID', self.r.execute_command('CLIENT', 'ID'))
        line = _s(raw).splitlines()[0]
        out: dict[str, Any] = {}
        for part in line.split():
            if '=' not in part:
                continue
            k, v = part.split('=', 1)
            out[k] = v
        return out

    def _force_disconnect(self) -> None:
        pool = getattr(self.r, 'connection_pool', None)
        if pool is not None:
            pool.disconnect()

    def _break_identity(self, mode: str) -> None:
        expected = get_expected_service(self.service)
        if mode in {'reconnect-name', 'reconnect-both'}:
            self.r.execute_command('CLIENT', 'SETNAME', f'chaos-{expected.client_name}-bad')
        if mode in {'reconnect-lib', 'reconnect-both'}:
            self.r.execute_command('CLIENT', 'SETINFO', 'LIB-NAME', f'chaos-{expected.lib_name}-bad')

    def run_repairable(self, mode: str) -> dict[str, Any]:
        self._clear_state()
        ensure_service_identity_sync(self.r, self.service)
        # Seed heal-state/cache on the healthy connection so the next client-id
        # change is treated as a reconnect, not just a first-time verification.
        heal_service_identity_sync(self.r, self.service, force=True)
        before_client_id = int(self.r.execute_command('CLIENT', 'ID'))
        self._force_disconnect()
        self.r.ping()
        after_reconnect_client_id = int(self.r.execute_command('CLIENT', 'ID'))
        self._break_identity(mode)
        before_entry = self._current_entry()
        out = heal_service_identity_sync(self.r, self.service, force=True)
        state = self._read_heal_state()
        after_entry = self._current_entry()
        return {
            'ok': bool(out.get('ok')),
            'recovered': bool(out.get('recovered')),
            'repair_attempted': bool(out.get('repair_attempted')),
            'event_id': _s(out.get('event_id')),
            'before_client_id': before_client_id,
            'after_reconnect_client_id': after_reconnect_client_id,
            'before_entry': before_entry,
            'after_entry': after_entry,
            'state': state,
        }

    def run_wrong_user(self) -> dict[str, Any]:
        url = self.wrong_user_url or self.redis_url
        r = redis.Redis.from_url(url, decode_responses=True)
        try:
            out = heal_service_identity_sync(r, self.service, force=True)
            return {'ok': bool(out.get('ok')), 'unexpected_success': True, 'result': out}
        except Exception as exc:
            try:
                state = r.hgetall(get_heal_state_key(self.service)) or {}
            except Exception:
                state = {}
            return {
                'ok': False,
                'unexpected_success': False,
                'error': str(exc),
                'state': state,
            }


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='ExecHealth reconnect chaos harness')
    p.add_argument('--service', default='exec_health_freeze_override_v1')
    p.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0'))
    p.add_argument('--wrong-user-url', default=os.getenv('EXEC_HEALTH_REDIS_WRONG_USER_URL', ''))
    p.add_argument('--mode', choices=['reconnect-name', 'reconnect-lib', 'reconnect-both', 'wrong-user'], default='reconnect-both')
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    h = ChaosHarness(args.redis_url, service=args.service, wrong_user_url=args.wrong_user_url)
    if args.mode == 'wrong-user':
        out = h.run_wrong_user()
    else:
        out = h.run_repairable(args.mode)
    print(json.dumps(out, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
