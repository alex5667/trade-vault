#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from typing import Any, Dict

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Gauge, start_http_server

from services.orderflow.exec_health_freeze_client_name_policy import evaluate_client_name_policy
from services.orderflow.exec_health_freeze_service_identity import build_service_identity_contract, ensure_service_identity_sync
from services.orderflow.exec_health_freeze_reconnect_healing import get_heal_state_key, heal_service_identity_sync

UP = Gauge('exec_health_freeze_client_name_policy_up', '1 if Redis-side ExecHealth client-name policy exporter loop is healthy')
STATE_AGE_S = Gauge('exec_health_freeze_client_name_policy_state_age_seconds', 'Age of ExecHealth client-name policy exporter state in seconds')
LAST_TS_MS = Gauge('exec_health_freeze_client_name_policy_last_check_ts_ms', 'Last client-name policy check timestamp in epoch ms')
MATCH = Gauge('exec_health_freeze_client_name_match', '1 if trusted client-name/lib-name field matches the ExecHealth contract', ['service', 'field'])
ACTIVE_CONNECTIONS = Gauge('exec_health_freeze_client_name_active_connections', 'Current CLIENT LIST connections per trusted ExecHealth client name', ['service'])
DISTINCT_ADDRS = Gauge('exec_health_freeze_client_name_distinct_addrs', 'Distinct addr count per trusted ExecHealth client name', ['service'])
VIOLATION = Gauge('exec_health_freeze_client_name_violation', 'One-hot Redis-side client-name policy violation', ['kind', 'service'])
RECOVERY_TOTAL = Gauge('exec_health_freeze_client_name_recovery_total', 'Self-healing recoveries after Redis reconnect identity drift', ['service'])
LAST_RECOVERY_TS_MS = Gauge('exec_health_freeze_client_name_last_recovery_ts_ms', 'Last successful client identity recovery timestamp', ['service'])
REPAIR_FAILED_TOTAL = Gauge('exec_health_freeze_client_name_repair_failed_total', 'Failed self-healing attempts after Redis reconnect identity drift', ['service'])

KNOWN_KINDS = [
    'service_started_unnamed_client',
    'wrong_lib_name_after_reconnect',
    'duplicate_trusted_client_name',
    'unexpected_trusted_client_name',
]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


class Exporter:
    def __init__(self) -> None:
        if redis is None:
            raise RuntimeError('redis dependency missing')
        self.redis_url = os.getenv('EXEC_HEALTH_REDIS_AUDIT_URL') or os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')
        self.state_key = os.getenv('EXEC_HEALTH_FREEZE_CLIENT_NAME_POLICY_STATE_KEY', 'metrics:exec_health:freeze_client_name_audit:last')
        self.loop_s = max(5, int(os.getenv('EXEC_HEALTH_FREEZE_CLIENT_NAME_POLICY_INTERVAL_S', '30') or 30))
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        ensure_service_identity_sync(self.r, 'exec_health_freeze_client_name_audit_exporter_v1')
        heal_service_identity_sync(self.r, 'exec_health_freeze_client_name_audit_exporter_v1', force=True)

    def _read_state(self) -> Dict[str, Any]:
        try:
            return self.r.hgetall(self.state_key) or {}
        except Exception:
            return {}

    def _read_heal_state(self, service: str) -> Dict[str, Any]:
        try:
            return self.r.hgetall(get_heal_state_key(service)) or {}
        except Exception:
            return {}

    def _write_state(self, payload: Dict[str, Any]) -> None:
        try:
            self.r.hset(self.state_key, mapping={str(k): str(v) for k, v in payload.items()})
            self.r.expire(self.state_key, 86400 * 7)
        except Exception:
            pass

    def run_once(self) -> Dict[str, Any]:
        heal_service_identity_sync(self.r, 'exec_health_freeze_client_name_audit_exporter_v1')
        now = _now_ms()
        raw = self.r.execute_command('CLIENT', 'LIST') or ''
        res = evaluate_client_name_policy(raw)
        contract = build_service_identity_contract()
        for service in contract.keys():
            row = dict(res.get('services', {}).get(service) or {})
            ACTIVE_CONNECTIONS.labels(service=service).set(float(int(row.get('seen', 0) or 0)))
            MATCH.labels(service=service, field='name').set(float(int(row.get('name_match', 0) or 0)))
            MATCH.labels(service=service, field='lib_name').set(float(int(row.get('lib_name_match', 0) or 0)))
            DISTINCT_ADDRS.labels(service=service).set(float(int(row.get('distinct_addrs', 0) or 0)))
            heal = self._read_heal_state(service)
            RECOVERY_TOTAL.labels(service=service).set(float(int(heal.get('recovery_total', 0) or 0)))
            LAST_RECOVERY_TS_MS.labels(service=service).set(float(int(heal.get('last_recovery_ts_ms', 0) or 0)))
            REPAIR_FAILED_TOTAL.labels(service=service).set(float(int(heal.get('repair_failed_total', 0) or 0)))
        active = {(str(v.get('kind')), str(v.get('service'))): 1 for v in list(res.get('violations', []) or [])}
        for kind in KNOWN_KINDS:
            for service in list(contract.keys()) + ['unknown']:
                VIOLATION.labels(kind=kind, service=service).set(1.0 if (kind, service) in active else 0.0)
        st = self._read_state()
        st.update({
            'updated_ts_ms': int(now),
            'last_check_ts_ms': int(now),
            'violation_count': int(len(list(res.get('violations', []) or []))),
            'ok': 1 if res.get('ok') else 0,
            'trusted_connection_count': int(res.get('trusted_connection_count', 0) or 0),
        })
        prev = _i(st.get('updated_ts_ms'), now)
        self._write_state(st)
        LAST_TS_MS.set(float(now))
        STATE_AGE_S.set(max(0.0, float(now - prev) / 1000.0))
        UP.set(1.0)
        return {'ok': bool(res.get('ok')), 'violation_count': int(len(list(res.get('violations', []) or []))), 'services': res.get('services', {})}


def main() -> None:
    ex = Exporter()
    start_http_server(int(os.getenv('EXEC_HEALTH_FREEZE_CLIENT_NAME_POLICY_EXPORTER_PORT', '9834')))
    while True:
        try:
            ex.run_once()
        except Exception:
            UP.set(0.0)
        time.sleep(ex.loop_s)


if __name__ == '__main__':
    main()
