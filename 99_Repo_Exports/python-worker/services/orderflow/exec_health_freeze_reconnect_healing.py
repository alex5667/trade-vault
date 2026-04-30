from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Reconnect self-healing for ExecHealth Redis service identity.

P15 closes the operational gap left after P13/P14: CLIENT LIST/exporters can
see wrong name/lib-name after reconnect, but trusted services should also try to
repair themselves on the live connection and emit an explicit recovery signal.

This module keeps a tiny in-process cache keyed by (service, redis object id)
so long-running loops can cheaply detect a new CLIENT ID, re-assert
CLIENT SETNAME / CLIENT SETINFO LIB-NAME, and write recovery state/events for
Prometheus export.
"""

import json
import os
import time
from typing import Any, Dict, Tuple

from services.orderflow.exec_health_freeze_service_identity import (
    IDENTITY_ENFORCE_ENV
    IDENTITY_REQUIRE_LIBNAME_ENV
    _b
    _read_current_client_line_async
    _read_current_client_line_sync
    get_expected_service
    normalize_client_entry
    verify_entry_against_expected
)

DEFAULT_EVENT_STREAM = 'ops:exec_health:freeze_events:v1'
HEAL_STATE_PREFIX_ENV = 'EXEC_HEALTH_FREEZE_CLIENT_HEAL_STATE_PREFIX'
HEAL_CHECK_MS_ENV = 'EXEC_HEALTH_FREEZE_CLIENT_HEAL_CHECK_MS'
HEAL_EVENT_STREAM_ENV = 'EXEC_HEALTH_FREEZE_EVENT_STREAM'

_CACHE: Dict[Tuple[str, int], Dict[str, Any]] = {}


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _s(x: Any, d: str = '') -> str:
    try:
        return str(x) if x is not None else str(d)
    except Exception:
        return str(d)


def get_heal_state_key(service: str) -> str:
    prefix = os.getenv(HEAL_STATE_PREFIX_ENV, 'metrics:exec_health:freeze_client_heal:last')
    return f'{prefix}:{service}'


def _event_stream() -> str:
    return os.getenv(HEAL_EVENT_STREAM_ENV, DEFAULT_EVENT_STREAM)


def _check_ms() -> int:
    return max(500, _i(os.getenv(HEAL_CHECK_MS_ENV, '3000'), 3000))


def _cache_key(r: Any, service: str) -> Tuple[str, int]:
    return (service, id(r))


def _read_state_sync(r: Any, service: str) -> Dict[str, Any]:
    try:
        return r.hgetall(get_heal_state_key(service)) or {}
    except Exception:
        return {}


async def _read_state_async(r: Any, service: str) -> Dict[str, Any]:
    try:
        return await r.hgetall(get_heal_state_key(service)) or {}
    except Exception:
        return {}


def _write_state_sync(r: Any, service: str, mapping: Dict[str, Any]) -> None:
    try:
        r.hset(get_heal_state_key(service), mapping={str(k): str(v) for k, v in mapping.items()})
        r.expire(get_heal_state_key(service), 86400 * 14)
    except Exception:
        pass


async def _write_state_async(r: Any, service: str, mapping: Dict[str, Any]) -> None:
    try:
        await r.hset(get_heal_state_key(service), mapping={str(k): str(v) for k, v in mapping.items()})
        await r.expire(get_heal_state_key(service), 86400 * 14)
    except Exception:
        pass


def _emit_event_sync(r: Any, payload: Dict[str, Any]) -> str:
    try:
        return _s(r.xadd(_event_stream(), {str(k): str(v) for k, v in payload.items()}, maxlen=5000) or '')
    except Exception:
        return ''


async def _emit_event_async(r: Any, payload: Dict[str, Any]) -> str:
    try:
        return _s(await r.xadd(_event_stream(), {str(k): str(v) for k, v in payload.items()}, maxlen=5000) or '')
    except Exception:
        return ''


def _should_attempt_repair(violations: Any) -> bool:
    bad = set(str(v) for v in list(violations or []))
    return bool(bad) and bad.issubset({'wrong_name', 'wrong_lib_name'})


def _build_state(prev: Dict[str, Any], *, service: str, client_id: int, before: Dict[str, Any], after: Dict[str, Any], recovered: bool, reconnect_detected: bool, event_id: str, now_ms: int, repair_attempted: bool) -> Dict[str, Any]:
    entry = dict(after.get('entry') or before.get('entry') or {})
    out = dict(prev or {})
    out.update({
        'schema_name': 'exec_health_freeze_client_heal_state'
        'schema_version': 1
        'service': service
        'updated_ts_ms': int(now_ms)
        'last_check_ts_ms': int(now_ms)
        'last_client_id': int(client_id)
        'last_user': _s(entry.get('user'))
        'last_name': _s(entry.get('name'))
        'last_lib_name': _s(entry.get('lib-name'))
        'last_result_ok': 1 if after.get('ok') else 0
        'last_reconnect_detected': 1 if reconnect_detected else 0
        'last_repair_attempted': 1 if repair_attempted else 0
        'last_recovered': 1 if recovered else 0
        'last_before_violations_json': json.dumps(list(before.get('violations') or []), ensure_ascii=False)
        'last_after_violations_json': json.dumps(list(after.get('violations') or []), ensure_ascii=False)
    })
    if reconnect_detected:
        out['reconnect_seen_total'] = int(_i(prev.get('reconnect_seen_total'), 0) + 1)
    if repair_attempted and not recovered:
        out['repair_failed_total'] = int(_i(prev.get('repair_failed_total'), 0) + 1)
    if recovered:
        out['recovery_total'] = int(_i(prev.get('recovery_total'), 0) + 1)
        out['last_recovery_ts_ms'] = int(now_ms)
        out['last_recovery_client_id'] = int(client_id)
        out['last_recovery_event_id'] = _s(event_id)
        out['last_recovery_reason'] = ','.join(list(before.get('violations') or []))
    return out


def _repair_sync(r: Any, service: str, before: Dict[str, Any]) -> Dict[str, Any]:
    expected = get_expected_service(service)
    require_lib_name = _b(os.getenv(IDENTITY_REQUIRE_LIBNAME_ENV, '1'), True)
    if 'wrong_name' in list(before.get('violations') or []):
        r.execute_command('CLIENT', 'SETNAME', expected.client_name)
    if require_lib_name and 'wrong_lib_name' in list(before.get('violations') or []):
        r.execute_command('CLIENT', 'SETINFO', 'LIB-NAME', expected.lib_name)
    entry = normalize_client_entry(_read_current_client_line_sync(r))
    return verify_entry_against_expected(entry, expected, require_lib_name=require_lib_name)


async def _repair_async(r: Any, service: str, before: Dict[str, Any]) -> Dict[str, Any]:
    expected = get_expected_service(service)
    require_lib_name = _b(os.getenv(IDENTITY_REQUIRE_LIBNAME_ENV, '1'), True)
    if 'wrong_name' in list(before.get('violations') or []):
        await r.execute_command('CLIENT', 'SETNAME', expected.client_name)
    if require_lib_name and 'wrong_lib_name' in list(before.get('violations') or []):
        await r.execute_command('CLIENT', 'SETINFO', 'LIB-NAME', expected.lib_name)
    entry = normalize_client_entry(await _read_current_client_line_async(r))
    return verify_entry_against_expected(entry, expected, require_lib_name=require_lib_name)


def heal_service_identity_sync(r: Any, service: str, *, enforce: bool | None = None, force: bool = False) -> Dict[str, Any]:
    expected = get_expected_service(service)
    enforce = _b(os.getenv(IDENTITY_ENFORCE_ENV, '1'), True) if enforce is None else bool(enforce)
    now = _now_ms()
    cache = _CACHE.get(_cache_key(r, service), {})
    client_id = int(r.execute_command('CLIENT', 'ID'))
    reconnect_detected = int(cache.get('last_client_id') or 0) not in {0, int(client_id)}
    if (not force) and (not reconnect_detected) and (now - _i(cache.get('last_check_ts_ms'), 0) < _check_ms()):
        return {'ok': True, 'cached': True, 'recovered': False, 'client_id': client_id}
    entry = normalize_client_entry(_read_current_client_line_sync(r))
    before = verify_entry_against_expected(entry, expected, require_lib_name=_b(os.getenv(IDENTITY_REQUIRE_LIBNAME_ENV, '1'), True))
    after = before
    recovered = False
    repair_attempted = False
    event_id = ''
    if not before.get('ok') and _should_attempt_repair(before.get('violations')):
        repair_attempted = True
        after = _repair_sync(r, service, before)
        recovered = bool(after.get('ok'))
        if recovered:
            event_id = _emit_event_sync(r, {
                'ts_ms': now
                'kind': 'redis_client_identity_recovered'
                'service': service
                'role': expected.role
                'redis_user': expected.redis_user
                'client_id': int(client_id)
                'reconnect_detected': 1 if reconnect_detected else 0
                'before_violations_json': json.dumps(list(before.get('violations') or []), ensure_ascii=False)
                'client_name': expected.client_name
                'lib_name': expected.lib_name
                'source': service
            })
    state = _build_state(_read_state_sync(r, service), service=service, client_id=client_id, before=before, after=after, recovered=recovered, reconnect_detected=bool(reconnect_detected), event_id=event_id, now_ms=now, repair_attempted=repair_attempted)
    _write_state_sync(r, service, state)
    _CACHE[_cache_key(r, service)] = {'last_client_id': int(client_id), 'last_check_ts_ms': int(now)}
    result = {'ok': bool(after.get('ok')), 'cached': False, 'recovered': recovered, 'repair_attempted': repair_attempted, 'client_id': int(client_id), 'before': before, 'after': after, 'event_id': event_id, 'reconnect_detected': bool(reconnect_detected)}
    if enforce and not result['ok']:
        raise RuntimeError(f'ExecHealth Redis service identity healing failed for {service}: {after.get("violations")}; entry={after.get("entry")}')
    return result


async def heal_service_identity_async(r: Any, service: str, *, enforce: bool | None = None, force: bool = False) -> Dict[str, Any]:
    expected = get_expected_service(service)
    enforce = _b(os.getenv(IDENTITY_ENFORCE_ENV, '1'), True) if enforce is None else bool(enforce)
    now = _now_ms()
    cache = _CACHE.get(_cache_key(r, service), {})
    client_id = int(await r.execute_command('CLIENT', 'ID'))
    reconnect_detected = int(cache.get('last_client_id') or 0) not in {0, int(client_id)}
    if (not force) and (not reconnect_detected) and (now - _i(cache.get('last_check_ts_ms'), 0) < _check_ms()):
        return {'ok': True, 'cached': True, 'recovered': False, 'client_id': client_id}
    entry = normalize_client_entry(await _read_current_client_line_async(r))
    before = verify_entry_against_expected(entry, expected, require_lib_name=_b(os.getenv(IDENTITY_REQUIRE_LIBNAME_ENV, '1'), True))
    after = before
    recovered = False
    repair_attempted = False
    event_id = ''
    if not before.get('ok') and _should_attempt_repair(before.get('violations')):
        repair_attempted = True
        after = await _repair_async(r, service, before)
        recovered = bool(after.get('ok'))
        if recovered:
            event_id = await _emit_event_async(r, {
                'ts_ms': now
                'kind': 'redis_client_identity_recovered'
                'service': service
                'role': expected.role
                'redis_user': expected.redis_user
                'client_id': int(client_id)
                'reconnect_detected': 1 if reconnect_detected else 0
                'before_violations_json': json.dumps(list(before.get('violations') or []), ensure_ascii=False)
                'client_name': expected.client_name
                'lib_name': expected.lib_name
                'source': service
            })
    state = _build_state(await _read_state_async(r, service), service=service, client_id=client_id, before=before, after=after, recovered=recovered, reconnect_detected=bool(reconnect_detected), event_id=event_id, now_ms=now, repair_attempted=repair_attempted)
    await _write_state_async(r, service, state)
    _CACHE[_cache_key(r, service)] = {'last_client_id': int(client_id), 'last_check_ts_ms': int(now)}
    result = {'ok': bool(after.get('ok')), 'cached': False, 'recovered': recovered, 'repair_attempted': repair_attempted, 'client_id': int(client_id), 'before': before, 'after': after, 'event_id': event_id, 'reconnect_detected': bool(reconnect_detected)}
    if enforce and not result['ok']:
        raise RuntimeError(f'ExecHealth Redis service identity healing failed for {service}: {after.get("violations")}; entry={after.get("entry")}')
    return result
