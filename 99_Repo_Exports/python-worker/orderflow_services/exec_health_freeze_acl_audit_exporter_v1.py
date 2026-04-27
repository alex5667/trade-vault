#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
from services.orderflow.exec_health_freeze_service_identity import ensure_service_identity_sync
from services.orderflow.exec_health_freeze_reconnect_healing import heal_service_identity_sync

"""P11: ACL audit exporter — читает ACL LOG и экспортирует метрики о попытках прямых hash-записей.

Мониторит команды HSET/HDEL/DEL/UNLINK/EVAL/EVALSHA на freeze-control ключах.
Каждый denied attempt в ACL LOG считается policy violation.
"""

import os
import time
from typing import Any, Dict, List

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Counter, Gauge, start_http_server

FREEZE_KEYS = (
    'cfg:orderflow:exec_health:freeze_control:v1',
    'cfg:orderflow:exec_health:auto_freeze:v1',
    'metrics:exec_health:slo:autoguard:state',
)
WATCH_CMDS = {'HSET', 'HDEL', 'DEL', 'UNLINK', 'EVAL', 'EVALSHA'}

UP = Gauge('exec_health_freeze_acl_audit_up', '1 if ExecHealth ACL audit exporter loop is healthy')
STATE_AGE_S = Gauge('exec_health_freeze_acl_audit_state_age_seconds', 'Age of ACL audit exporter state in seconds')
LAST_TS_MS = Gauge('exec_health_freeze_acl_audit_last_event_ts_ms', 'Last matching ACL violation timestamp in epoch ms')
VIOLATION_TOTAL = Counter('exec_health_freeze_acl_violation_total', 'Redis ACL violations for ExecHealth freeze-control surfaces', ['user', 'reason', 'command'])
ACTIVE = Gauge('exec_health_freeze_acl_violation_active', 'One-hot recent ACL violation by command', ['command'])


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(x: Any, d: str = '') -> str:
    try:
        return str(x) if x is not None else str(d)
    except Exception:
        return str(d)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _norm_entry(raw: Any) -> Dict[str, Any]:
    """Нормализует запись ACL LOG (может быть dict или flat list) в словарь."""
    if isinstance(raw, dict):
        return {str(k): v for k, v in raw.items()}
    if isinstance(raw, (list, tuple)):
        out: Dict[str, Any] = {}
        flat = list(raw)
        for i in range(0, len(flat) - 1, 2):
            out[str(flat[i])] = flat[i + 1]
        return out
    return {}


def _matches(entry: Dict[str, Any]) -> bool:
    """True если запись ACL LOG касается freeze-control ключей и запрещённых команд."""
    cmd = _s(entry.get('command') or entry.get('cmd')).upper()
    obj = _s(entry.get('object'))
    key = _s(entry.get('key'))
    username = _s(entry.get('username'))
    target = '|'.join([obj, key, username])
    return cmd in WATCH_CMDS and any(k in target for k in FREEZE_KEYS)


class Exporter:
    def __init__(self) -> None:
        if redis is None:
            raise RuntimeError('redis dependency missing')
        self.redis_url = os.getenv('EXEC_HEALTH_REDIS_AUDIT_URL') or os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')
        self.state_key = os.getenv('EXEC_HEALTH_FREEZE_ACL_AUDIT_STATE_KEY', 'metrics:exec_health:freeze_acl_audit:last')
        self.loop_s = max(5, int(os.getenv('EXEC_HEALTH_FREEZE_ACL_AUDIT_INTERVAL_S', '30') or 30))
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        ensure_service_identity_sync(self.r, "exec_health_freeze_acl_audit_exporter_v1")
        heal_service_identity_sync(self.r, "exec_health_freeze_acl_audit_exporter_v1", force=True)
        self._seen_ids: set[str] = set()

    def _read_state(self) -> Dict[str, Any]:
        try:
            return self.r.hgetall(self.state_key) or {}
        except Exception:
            return {}

    def _write_state(self, mapping: Dict[str, Any]) -> None:
        try:
            self.r.hset(self.state_key, mapping={str(k): str(v) for k, v in mapping.items()})
            self.r.expire(self.state_key, 86400 * 7)
        except Exception:
            pass

    def run_once(self) -> Dict[str, Any]:
        """Один цикл: читает ACL LOG, фильтрует freeze-control violations, обновляет метрики."""
        try:
            heal_service_identity_sync(self.r, "exec_health_freeze_acl_audit_exporter_v1")
        except Exception:
            pass
        now = _now_ms()
        rows = self.r.execute_command('ACL', 'LOG', 64) or []
        matches: List[Dict[str, Any]] = []
        active_cmds = set()
        last_ts_ms = 0
        for raw in rows:
            e = _norm_entry(raw)
            if not _matches(e):
                continue
            entry_id = _s(e.get('entry-id') or e.get('id') or '')
            if entry_id and entry_id in self._seen_ids:
                continue
            if entry_id:
                self._seen_ids.add(entry_id)
            matches.append(e)
            cmd = _s(e.get('command') or e.get('cmd')).upper()
            active_cmds.add(cmd)
            last_ts_ms = max(last_ts_ms, _i(e.get('timestamp-created') or e.get('ts_ms') or now, now))
            VIOLATION_TOTAL.labels(user=_s(e.get('username') or 'unknown'), reason=_s(e.get('reason') or 'acl'), command=cmd or 'UNKNOWN').inc()
        for cmd in sorted(WATCH_CMDS):
            ACTIVE.labels(command=cmd).set(1.0 if cmd in active_cmds else 0.0)
        LAST_TS_MS.set(float(last_ts_ms))
        st = self._read_state()
        st.update({'updated_ts_ms': int(now), 'last_event_ts_ms': int(last_ts_ms), 'match_count': int(_i(st.get('match_count'), 0) + len(matches))})
        self._write_state(st)
        age = max(0.0, float(now - _i(st.get('updated_ts_ms'), now)) / 1000.0)
        STATE_AGE_S.set(age)
        UP.set(1.0)
        return {'ok': True, 'match_count': len(matches), 'last_event_ts_ms': int(last_ts_ms)}


def main() -> None:
    ex = Exporter()
    start_http_server(int(os.getenv('EXEC_HEALTH_FREEZE_ACL_AUDIT_EXPORTER_PORT', '9831')))
    while True:
        try:
            ex.run_once()
        except Exception:
            UP.set(0.0)
        time.sleep(ex.loop_s)


if __name__ == '__main__':
    main()
