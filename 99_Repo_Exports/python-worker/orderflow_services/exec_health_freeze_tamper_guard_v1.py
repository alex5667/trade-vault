#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
"""P10 ExecHealth Freeze Tamper Guard.

Мониторит целостность freeze_control/state хешей и автоматически
повторно замораживает систему если обнаружена прямая подмена хешей
без соответствующей записи в append-only request log.
""",
import json
import os
import secrets
import time
from typing import Any, Dict, List, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Counter, Gauge, start_http_server

from services.orderflow.exec_health_freeze_control import build_autoguard_latch_update, stringify_mapping
from services.orderflow.exec_health_freeze_sealed_state import sealed_set_sync
from services.orderflow.exec_health_freeze_integrity import evaluate_freeze_integrity
from services.orderflow.exec_health_freeze_request_log import DEFAULT_REQUEST_STREAM
from services.orderflow.exec_health_freeze_service_identity import ensure_service_identity_sync
from services.orderflow.exec_health_freeze_reconnect_healing import heal_service_identity_sync


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _read_events(r: Any, key: str, count: int) -> List[Tuple[str, Dict[str, Any]]]:
    try:
        rows = r.xrevrange(key, count=max(1, int(count))) or []
        return [(str(eid), dict(payload or {})) for eid, payload in rows]
    except Exception:
        return []


# Виды нарушений, при которых требуется авто-повторная заморозка
# P11: добавлены seal-invalid нарушения — прямая запись мимо FCALL entrypoints тоже триггерит re-freeze
TAMPER_REFREEZE_KINDS = {
    'control_state_missing_without_valid_ack',
    'thaw_without_valid_ack_event',
    'invalid_control_ack_signature',
    'control_seal_invalid',
    'state_seal_invalid',
    'request_log_prepare_missing',
    'request_log_approve_missing',
    'request_log_commit_missing',
    'request_log_same_operator_violation',
    'request_log_out_of_order',
    'request_log_nonce_mismatch',
    'control_request_mismatch',
}


def should_refreeze(violation_kinds: List[str]) -> bool:
    return any(k in TAMPER_REFREEZE_KINDS for k in list(violation_kinds or []))


UP = Gauge('exec_health_freeze_tamper_guard_up', '1 if tamper guard loop is healthy')
TAMPER_ACTIVE = Gauge('exec_health_freeze_tamper_guard_tamper_active', '1 if current integrity evaluation indicates tamper requiring refreeze')
STATE_AGE_S = Gauge('exec_health_freeze_tamper_guard_state_age_seconds', 'Age of tamper guard state hash in seconds')
LAST_REFREEZE_TS_MS = Gauge('exec_health_freeze_tamper_guard_last_refreeze_ts_ms', 'Last automatic tamper refreeze timestamp')
VIOLATION = Gauge('exec_health_freeze_tamper_guard_violation', 'One-hot tamper violations seen by guard', ['kind'])
REFREEZE_TOTAL = Counter('exec_health_freeze_tamper_guard_refreeze_total', 'Automatic re-freeze actions due to tamper', ['reason'])


class Guard:
    def __init__(self) -> None:
        if redis is None:
            raise RuntimeError('redis dependency missing')
        self.redis_url = os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')
        self.control_key = os.getenv('EXEC_HEALTH_FREEZE_CONTROL_KEY', 'cfg:orderflow:exec_health:freeze_control:v1')
        self.state_key = os.getenv('EXEC_HEALTH_SLO_AUTOGUARD_STATE_KEY', 'metrics:exec_health:slo:autoguard:state')
        self.freeze_key = os.getenv('EXEC_HEALTH_AUTO_FREEZE_KEY', 'cfg:orderflow:exec_health:auto_freeze:v1')
        self.event_stream = os.getenv('EXEC_HEALTH_FREEZE_EVENT_STREAM', 'ops:exec_health:freeze_events:v1')
        self.request_stream = os.getenv('EXEC_HEALTH_FREEZE_REQUEST_STREAM', DEFAULT_REQUEST_STREAM)
        self.notify_stream = os.getenv('EXEC_HEALTH_AUTOGUARD_NOTIFY_STREAM', 'notify:telegram')
        self.guard_state_key = os.getenv('EXEC_HEALTH_TAMPER_GUARD_STATE_KEY', 'metrics:exec_health:freeze_tamper_guard:last')
        self.interval_s = float(os.getenv('EXEC_HEALTH_TAMPER_GUARD_INTERVAL_S', '10') or 10)
        self.cooldown_s = max(30, int(os.getenv('EXEC_HEALTH_TAMPER_GUARD_COOLDOWN_S', '300') or 300))
        self.freeze_minutes = max(1, int(os.getenv('EXEC_HEALTH_TAMPER_GUARD_FREEZE_MINUTES', '30') or 30))
        self.event_count = max(10, int(os.getenv('EXEC_HEALTH_FREEZE_INTEGRITY_EVENT_COUNT', '100') or 100))
        self.request_count = max(10, int(os.getenv('EXEC_HEALTH_FREEZE_REQUEST_EVENT_COUNT', '200') or 200))
        self.r = self._connect_with_retry()
        ensure_service_identity_sync(self.r, "exec_health_freeze_tamper_guard_v1")
        heal_service_identity_sync(self.r, "exec_health_freeze_tamper_guard_v1", force=True)

    def _connect_with_retry(self, max_attempts: int = 0) -> Any:
        """Connect to Redis with exponential backoff — tolerates startup DNS race conditions.""",
        import sys
        delay = 2.0
        last_exc: Exception = RuntimeError("No attempt made")
        attempt = 1
        while True:
            try:
                r = redis.Redis.from_url(self.redis_url, decode_responses=True, socket_connect_timeout=5)
                r.ping()
                return r
            except Exception as exc:
                last_exc = exc
                max_str = str(max_attempts) if max_attempts > 0 else "∞"
                print(
                    f"[tamper-guard] Redis connect attempt {attempt}/{max_str} failed: {exc}; "
                    f"retrying in {delay:.0f}s",
                    file=sys.stderr, flush=True,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
                
                if max_attempts > 0 and attempt >= max_attempts:
                    raise RuntimeError(
                        f"[tamper-guard] Redis unavailable after {max_attempts} attempts: {last_exc}"
                    )
                attempt += 1

    def _read_hash(self, key: str) -> Dict[str, Any]:
        try:
            return self.r.hgetall(key) or {}
        except Exception:
            return {}

    def _write_hash(self, key: str, payload: Dict[str, Any], *, entrypoint: str, force: bool = False, force_reason: str = '') -> None:
        """P11: write hash. guard_state_key пишем напрямую (seal не применяется к guard state).
        control/state ключи пишем через sealed_set_sync (FCALL whitelist entrypoint).
        """,
        if key == self.guard_state_key:
            self.r.hset(key, mapping=stringify_mapping(payload))
            self.r.expire(key, 86400 * 7)
            return
        prev = self._read_hash(key)
        res = sealed_set_sync(
            self.r,
            key=key,
            prev_raw=prev,
            mapping=stringify_mapping(payload),
            entrypoint=entrypoint,
            ttl_s=86400 * 7,
            force=force,
            force_reason=force_reason,
        )
        if not res.get('ok'):
            raise RuntimeError(f"tamper guard sealed write failed for {key}: {res.get('error') or res.get('rc')}")

    def _emit_event(self, payload: Dict[str, Any]) -> str:
        try:
            return str(self.r.xadd(self.event_stream, stringify_mapping(payload), maxlen=5000) or '')
        except Exception:
            return ''

    def _notify(self, text: str) -> None:
        try:
            self.r.xadd(self.notify_stream, {'ts_ms': str(_now_ms()), 'source': 'exec_health_freeze_tamper_guard_v1', 'text': text}, maxlen=5000)
        except Exception:
            pass

    def run_once(self) -> Dict[str, Any]:
        try:
            heal_service_identity_sync(self.r, "exec_health_freeze_tamper_guard_v1")
        except Exception:
            pass
        now = _now_ms()
        control = self._read_hash(self.control_key)
        state = self._read_hash(self.state_key)
        guard_state = self._read_hash(self.guard_state_key)
        events = _read_events(self.r, self.event_stream, self.event_count)
        request_events = _read_events(self.r, self.request_stream, self.request_count)
        integ = evaluate_freeze_integrity(control_raw=control, state_raw=state, events=events, request_events=request_events, now_ms=now)
        tamper = should_refreeze(integ.violation_kinds)
        cooldown_until = _i(guard_state.get('cooldown_until_ts_ms'), 0)
        last_refreeze_ts_ms = _i(guard_state.get('last_refreeze_ts_ms'), 0)
        out = {
            'ok': True,
            'tamper_active': int(tamper),
            'violation_kinds_json': json.dumps(list(integ.violation_kinds), ensure_ascii=False),
            'last_refreeze_ts_ms': int(last_refreeze_ts_ms),
            'cooldown_until_ts_ms': int(cooldown_until),
        }
        if tamper and now >= cooldown_until:
            freeze_until_ts_ms = now + self.freeze_minutes * 60 * 1000
            ack_nonce = secrets.token_hex(16)
            reasons = ['freeze_control_tamper'] + [k for k in integ.violation_kinds if k != 'none']
            trigger_event_id = self._emit_event({
                'ts_ms': now,
                'kind': 'tamper_refreeze_latch',
                'ack_nonce': ack_nonce,
                'trigger_ts_ms': now,
                'reasons_json': json.dumps(reasons, ensure_ascii=False),
                'freeze_until_ts_ms': freeze_until_ts_ms,
                'source': 'exec_health_freeze_tamper_guard_v1',
                'request_id': integ.request_log_request_id,
            })
            raw = {
                'schema_name': 'exec_health_auto_freeze',
                'schema_version': 1,
                'ts_ms': now,
                'freeze_active': 1,
                'freeze_reason': ','.join(reasons),
                'freeze_until_ts_ms': freeze_until_ts_ms,
            }
            self.r.set(self.freeze_key, json.dumps(raw, separators=(',', ':')))
            self.r.pexpire(self.freeze_key, self.freeze_minutes * 60 * 1000)
            latch = build_autoguard_latch_update(prev=control, now_ms=now, reasons=reasons, freeze_until_ts_ms=freeze_until_ts_ms, ack_nonce=ack_nonce, trigger_event_id=trigger_event_id)
            self._write_hash(self.control_key, latch, entrypoint='tamper_refreeze_control', force=True, force_reason='tamper_refreeze')
            state_upd = dict(state)
            state_upd.update(latch)
            state_upd['state_tamper_refreeze_written_ts_ms'] = int(now)
            self._write_hash(self.state_key, state_upd, entrypoint='tamper_refreeze_state', force=True, force_reason='tamper_refreeze')
            guard_state.update({
                'schema_name': 'exec_health_freeze_tamper_guard_state',
                'schema_version': 1,
                'updated_ts_ms': int(now),
                'last_refreeze_ts_ms': int(now),
                'cooldown_until_ts_ms': int(now + self.cooldown_s * 1000),
                'last_reason': ','.join(reasons),
                'request_id': str(integ.request_log_request_id or ''),
                'refreeze_total': _i(guard_state.get('refreeze_total'), 0) + 1,
            })
            self._write_hash(self.guard_state_key, guard_state, entrypoint='guard_state')
            self._notify(f'ExecHealth tamper detected → auto re-freeze: {", ".join(reasons)}')
            REFREEZE_TOTAL.labels(reason='freeze_control_tamper').inc()
            out.update({'refreeze_performed': 1, 'last_refreeze_ts_ms': int(now), 'cooldown_until_ts_ms': int(now + self.cooldown_s * 1000)})
        else:
            guard_state.update({
                'schema_name': 'exec_health_freeze_tamper_guard_state',
                'schema_version': 1,
                'updated_ts_ms': int(now),
                'last_refreeze_ts_ms': int(last_refreeze_ts_ms),
                'cooldown_until_ts_ms': int(cooldown_until),
                'last_reason': guard_state.get('last_reason', ''),
                'request_id': str(integ.request_log_request_id or guard_state.get('request_id', '')),
                'refreeze_total': _i(guard_state.get('refreeze_total'), 0),
            })
            self._write_hash(self.guard_state_key, guard_state, entrypoint='guard_state')
            out['refreeze_performed'] = 0

        UP.set(1.0)
        TAMPER_ACTIVE.set(1.0 if tamper else 0.0)
        LAST_REFREEZE_TS_MS.set(float(_i(guard_state.get('last_refreeze_ts_ms'), 0) or out.get('last_refreeze_ts_ms', 0) or 0))
        age = 0.0 if not guard_state else max(0.0, float(now - _i(guard_state.get('updated_ts_ms'), 0)) / 1000.0)
        STATE_AGE_S.set(age)
        active = set(integ.violation_kinds)
        for kind in sorted(TAMPER_REFREEZE_KINDS):
            VIOLATION.labels(kind=kind).set(1.0 if kind in active else 0.0)
        return out


def main() -> None:
    g = Guard()
    port = int(os.getenv('EXEC_HEALTH_TAMPER_GUARD_EXPORTER_PORT', '9830'))
    start_http_server(port)
    while True:
        try:
            g.run_once()
        except Exception:
            UP.set(0.0)
        time.sleep(g.interval_s)


if __name__ == '__main__':
    main()
