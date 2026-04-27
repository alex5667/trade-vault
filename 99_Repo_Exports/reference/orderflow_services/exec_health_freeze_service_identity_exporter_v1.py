#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from typing import Any, Dict, List

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Gauge, start_http_server

from services.orderflow.exec_health_freeze_service_identity import build_service_identity_contract, evaluate_client_list_against_contract, ensure_service_identity_sync

UP = Gauge('exec_health_freeze_service_identity_up', '1 if service identity exporter loop is healthy')
STATE_AGE_S = Gauge('exec_health_freeze_service_identity_state_age_seconds', 'Age of service identity exporter state in seconds')
LAST_TS_MS = Gauge('exec_health_freeze_service_identity_last_check_ts_ms', 'Last service identity check timestamp in epoch ms')
MATCH = Gauge('exec_health_freeze_service_identity_match', '1 if live CLIENT LIST matches expected identity field', ['service', 'field'])
ACTIVE_CONNECTIONS = Gauge('exec_health_freeze_service_identity_active_connections', 'Current CLIENT LIST connections for expected ExecHealth service', ['service'])
VIOLATION = Gauge('exec_health_freeze_service_identity_violation', 'One-hot service identity violation', ['kind', 'service'])


def _now_ms() -> int:
    return int(time.time() * 1000)


class Exporter:
    def __init__(self) -> None:
        if redis is None:
            raise RuntimeError('redis dependency missing')
        self.redis_url = os.getenv('EXEC_HEALTH_REDIS_AUDIT_URL') or os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')
        self.state_key = os.getenv('EXEC_HEALTH_FREEZE_SERVICE_IDENTITY_STATE_KEY', 'metrics:exec_health:freeze_service_identity:last')
        self.loop_s = max(5, int(os.getenv('EXEC_HEALTH_FREEZE_SERVICE_IDENTITY_INTERVAL_S', '30') or 30))
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        ensure_service_identity_sync(self.r, 'exec_health_freeze_service_identity_exporter_v1')

    def _read_state(self) -> Dict[str, Any]:
        try:
            return self.r.hgetall(self.state_key) or {}
        except Exception:
            return {}

    def _write_state(self, payload: Dict[str, Any]) -> None:
        try:
            self.r.hset(self.state_key, mapping={str(k): str(v) for k, v in payload.items()})
            self.r.expire(self.state_key, 86400 * 7)
        except Exception:
            pass

    def run_once(self) -> Dict[str, Any]:
        now = _now_ms()
        raw = self.r.execute_command('CLIENT', 'LIST') or ''
        res = evaluate_client_list_against_contract(raw)
        contract = build_service_identity_contract()
        for service in contract.keys():
            row = dict(res.get('services', {}).get(service) or {})
            ACTIVE_CONNECTIONS.labels(service=service).set(float(int(row.get('seen', 0) or 0)))
            MATCH.labels(service=service, field='user').set(float(int(row.get('user_match', 0) or 0)))
            MATCH.labels(service=service, field='name').set(float(int(row.get('name_match', 0) or 0)))
            MATCH.labels(service=service, field='lib_name').set(float(int(row.get('lib_name_match', 0) or 0)))
        active = {(str(v.get('kind')), str(v.get('service'))): 1 for v in list(res.get('violations', []) or [])}
        known_kinds = ['service_missing', 'duplicate_service_connection', 'wrong_user', 'wrong_name', 'wrong_lib_name', 'unexpected_exec_health_client']
        for kind in known_kinds:
            for service in list(contract.keys()) + ['unknown']:
                VIOLATION.labels(kind=kind, service=service).set(1.0 if (kind, service) in active else 0.0)
        st = self._read_state()
        st.update({'updated_ts_ms': int(now), 'last_check_ts_ms': int(now), 'violation_count': int(len(list(res.get('violations', []) or []))), 'ok': 1 if res.get('ok') else 0})
        self._write_state(st)
        LAST_TS_MS.set(float(now))
        STATE_AGE_S.set(0.0)
        UP.set(1.0)
        return {'ok': bool(res.get('ok')), 'violation_count': int(len(list(res.get('violations', []) or []))), 'services': res.get('services', {})}


def main() -> None:
    ex = Exporter()
    start_http_server(int(os.getenv('EXEC_HEALTH_FREEZE_SERVICE_IDENTITY_EXPORTER_PORT', '9833')))
    while True:
        try:
            ex.run_once()
        except Exception:
            UP.set(0.0)
        time.sleep(ex.loop_s)


if __name__ == '__main__':
    main()
