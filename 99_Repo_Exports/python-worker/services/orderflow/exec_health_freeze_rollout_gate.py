from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""Reconnect nightly-smoke rollout gate.

P18 turns the P17 nightly reconnect smoke into an operational gate:
- every run emits a compact ops-event summary;
- failures latch a rollout/apply block key in Redis;
- the block stays active until an explicit manual ack;
- sensitive apply/release paths can call the shared guard helper.

Design notes
- single Redis source of truth: block key + state hash
- fail-open for telemetry only; guard helper fails-closed when Redis is reachable
- success never auto-clears a previously latched failure; operator ack is required
"""

import json
import os
import sys
from collections.abc import Mapping
from typing import Any
import contextlib

DEFAULT_GATE_KEY = 'cfg:orderflow:exec_health:reconnect_smoke:rollout_gate:v1'
DEFAULT_STATE_KEY = 'metrics:exec_health:freeze_reconnect_smoke:gate:last'
DEFAULT_EVENT_STREAM = 'ops:exec_health:freeze_events:v1'
DEFAULT_NOTIFY_STREAM = RS.NOTIFY_TELEGRAM
DEFAULT_NOTIFY_COOLDOWN_KEY = 'state:exec_health:freeze_reconnect_smoke:last_notify_ts_ms'
DEFAULT_ACK_SERVICE = 'exec_health_freeze_reconnect_rollout_gate_v1'


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(x: Any, d: str = '') -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _load_json(s: Any) -> dict[str, Any]:
    if isinstance(s, dict):
        return {str(k): v for k, v in s.items()}
    txt = _s(s).strip()
    if not txt:
        return {}
    try:
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def stringify_mapping(mapping: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in mapping.items():
        if isinstance(v, (dict, list, tuple)):
            out[str(k)] = json.dumps(v, ensure_ascii=False, separators=(',', ':'))
        elif v is None:
            out[str(k)] = ''
        else:
            out[str(k)] = str(v)
    return out


def _report_failed_cases(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in list(report.get('cases', []) or []):
        if row.get('skipped'):
            continue
        if row.get('ok'):
            continue
        out.append({
            'role': _s(row.get('role')),
            'service': _s(row.get('service')),
            'scenario': _s(row.get('scenario')),
            'check_reason': _s(row.get('check_reason')),
        })
    return out


def build_post_run_summary(report: Mapping[str, Any]) -> str:
    host = _s(report.get('host')) or 'unknown-host'
    ok = bool(report.get('ok'))
    enabled = int(report.get('enabled_case_count', 0) or 0)
    duration = float(report.get('duration_seconds', 0.0) or 0.0)
    failed = _report_failed_cases(report)
    head = f"[ExecHealthReconnectNightly] {'OK' if ok else 'FAILED'} host={host} enabled_cases={enabled} duration_s={duration:.2f}"
    if ok:
        return head
    parts = [head, 'failed_cases:']
    for row in failed[:6]:
        parts.append(f"- {row['role']}/{row['service']}/{row['scenario']}: {row['check_reason']}")
    return '\n'.join(parts)


def emit_ops_summary(
    r: Any,
    *,
    report: Mapping[str, Any],
    summary_text: str,
    report_path: str = '',
    event_stream: str | None = None,
) -> str:
    stream = event_stream or os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_EVENT_STREAM', DEFAULT_EVENT_STREAM)
    payload = {
        'ts_ms': int(report.get('ts_ms') or _now_ms()),
        'kind': 'exec_health_reconnect_nightly_summary',
        'source': 'exec_health_freeze_reconnect_nightly_smoke_v1',
        'host': _s(report.get('host')),
        'ok': 1 if report.get('ok') else 0,
        'enabled_case_count': int(report.get('enabled_case_count', 0) or 0),
        'case_count': int(report.get('case_count', 0) or 0),
        'duration_seconds': float(report.get('duration_seconds', 0.0) or 0.0),
        'report_path': _s(report_path),
        'failed_cases': _report_failed_cases(report),
        'text': summary_text[:3500],
    }
    try:
        return _s(r.xadd(stream, stringify_mapping(payload), maxlen=5000))
    except Exception:
        return ''


def maybe_emit_telegram_summary(
    r: Any,
    *,
    report: Mapping[str, Any],
    summary_text: str,
    report_path: str = '',
) -> str:
    notify_always = _s(os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_NOTIFY_ALWAYS', '0')).strip().lower() in {'1', 'true', 'yes', 'on'}
    if not notify_always and bool(report.get('ok')):
        return ''
    stream = os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_NOTIFY_STREAM') or os.getenv('NOTIFY_TELEGRAM_STREAM') or DEFAULT_NOTIFY_STREAM
    cooldown_sec = int(_s(os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_NOTIFY_COOLDOWN_SEC', '1800')) or '1800')
    cooldown_key = os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_NOTIFY_COOLDOWN_KEY', DEFAULT_NOTIFY_COOLDOWN_KEY)
    now = _now_ms()
    try:
        last = int(r.get(cooldown_key) or '0')
    except Exception:
        last = 0
    if last and (now - last) < (cooldown_sec * 1000):
        return ''
    try:
        r.set(cooldown_key, str(now))
        with contextlib.suppress(Exception):
            r.expire(cooldown_key, max(60, cooldown_sec))
        return _s(r.xadd(stream, stringify_mapping({
            'type': 'report',
            'ts_ms': now,
            'text': summary_text[:3500],
            'report_path': _s(report_path),
            'kind': 'exec_health_reconnect_nightly_summary',
            'source': 'exec_health_freeze_reconnect_nightly_smoke_v1',
            'ok': 1 if report.get('ok') else 0,
        }), maxlen=50000))
    except Exception:
        return ''


def get_rollout_gate_state(r: Any, *, gate_key: str | None = None, state_key: str | None = None) -> dict[str, Any]:
    gate_key = gate_key or os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_GATE_KEY', DEFAULT_GATE_KEY)
    state_key = state_key or os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_GATE_STATE_KEY', DEFAULT_STATE_KEY)
    try:
        raw_gate = r.get(gate_key)
    except Exception:
        raw_gate = None
    try:
        state = r.hgetall(state_key) or {}
    except Exception:
        state = {}
    gate_meta = _load_json(raw_gate)
    manual_ack_ts_ms = int(state.get('manual_ack_ts_ms') or 0)
    active = bool(raw_gate) and manual_ack_ts_ms <= 0
    return {
        'active': active,
        'gate_key': gate_key,
        'state_key': state_key,
        'gate_meta': gate_meta,
        'state': dict(state),
        'manual_ack_ts_ms': manual_ack_ts_ms,
        'last_fail_ts_ms': int(state.get('last_fail_ts_ms') or gate_meta.get('ts_ms') or 0),
    }


def update_rollout_gate_from_report(
    r: Any,
    *,
    report: Mapping[str, Any],
    report_path: str = '',
    ops_event_id: str = '',
    telegram_event_id: str = '',
    gate_key: str | None = None,
    state_key: str | None = None,
) -> dict[str, Any]:
    gate_key = gate_key or os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_GATE_KEY', DEFAULT_GATE_KEY)
    state_key = state_key or os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_GATE_STATE_KEY', DEFAULT_STATE_KEY)
    ts_ms = int(report.get('ts_ms') or _now_ms())
    ok = bool(report.get('ok'))
    prev = get_rollout_gate_state(r, gate_key=gate_key, state_key=state_key)
    failed_cases = _report_failed_cases(report)
    state_update: dict[str, Any] = {
        'schema_ver': 'exec_health_reconnect_rollout_gate_v1',
        'updated_ts_ms': ts_ms,
        'last_report_ts_ms': ts_ms,
        'last_report_ok': 1 if ok else 0,
        'last_report_path': _s(report_path),
        'last_failed_cases_json': failed_cases,
        'last_ops_event_id': _s(ops_event_id),
        'last_telegram_event_id': _s(telegram_event_id),
        'host': _s(report.get('host')),
        'enabled_case_count': int(report.get('enabled_case_count', 0) or 0),
    }
    gate_active = bool(prev.get('active'))
    if ok:
        if not gate_active:
            state_update['gate_active'] = 0
        else:
            state_update['gate_active'] = 1
            state_update['gate_reason'] = _s(prev.get('state', {}).get('gate_reason') or 'manual_ack_required_after_failed_smoke')
    else:
        gate_active = True
        state_update.update({
            'gate_active': 1,
            'gate_reason': 'manual_ack_required_after_failed_smoke',
            'last_fail_ts_ms': ts_ms,
            'failed_case_count': len(failed_cases),
            'manual_ack_ts_ms': '',
            'manual_ack_operator': '',
            'manual_ack_reason': '',
            'manual_ack_ticket': '',
        })
        gate_meta = {
            'kind': 'exec_health_reconnect_nightly_rollout_block',
            'ts_ms': ts_ms,
            'reason': 'nightly_reconnect_smoke_failed',
            'report_path': _s(report_path),
            'failed_cases': failed_cases,
            'ops_event_id': _s(ops_event_id),
            'telegram_event_id': _s(telegram_event_id),
        }
        with contextlib.suppress(Exception):
            r.set(gate_key, json.dumps(gate_meta, ensure_ascii=False, separators=(',', ':')))
    try:
        r.hset(state_key, mapping=stringify_mapping(state_update))
        with contextlib.suppress(Exception):
            r.expire(state_key, 86400 * 30)
    except Exception:
        pass
    return get_rollout_gate_state(r, gate_key=gate_key, state_key=state_key)


def manual_ack_rollout_gate(
    r: Any,
    *,
    operator: str,
    reason: str,
    ticket: str = '',
    gate_key: str | None = None,
    state_key: str | None = None,
    event_stream: str | None = None,
) -> dict[str, Any]:
    if not _s(operator).strip() or not _s(reason).strip():
        raise ValueError('operator and reason are required')
    gate_key = gate_key or os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_GATE_KEY', DEFAULT_GATE_KEY)
    state_key = state_key or os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_GATE_STATE_KEY', DEFAULT_STATE_KEY)
    event_stream = event_stream or os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_EVENT_STREAM', DEFAULT_EVENT_STREAM)
    now = _now_ms()
    prev = get_rollout_gate_state(r, gate_key=gate_key, state_key=state_key)
    with contextlib.suppress(Exception):
        r.delete(gate_key)
    payload = {
        'schema_ver': 'exec_health_reconnect_rollout_gate_v1',
        'updated_ts_ms': now,
        'gate_active': 0,
        'manual_ack_ts_ms': now,
        'manual_ack_operator': operator,
        'manual_ack_reason': reason,
        'manual_ack_ticket': ticket,
        'manual_ack_required': 0,
    }
    try:
        r.hset(state_key, mapping=stringify_mapping(payload))
        with contextlib.suppress(Exception):
            r.expire(state_key, 86400 * 30)
    except Exception:
        pass
    evt = {
        'ts_ms': now,
        'kind': 'exec_health_reconnect_nightly_rollout_gate_ack',
        'source': DEFAULT_ACK_SERVICE,
        'operator': operator,
        'reason': reason,
        'ticket': ticket,
        'previous_gate_active': 1 if prev.get('active') else 0,
        'previous_last_fail_ts_ms': int(prev.get('last_fail_ts_ms') or 0),
    }
    event_id = ''
    with contextlib.suppress(Exception):
        event_id = _s(r.xadd(event_stream, stringify_mapping(evt), maxlen=5000))
    out = get_rollout_gate_state(r, gate_key=gate_key, state_key=state_key)
    out.update({'ok': True, 'event_id': event_id, 'operator': operator, 'reason': reason, 'ticket': ticket})
    return out


def assert_rollout_gate_open(
    r: Any,
    *,
    purpose: str,
    exit_code: int | None = None,
    gate_key: str | None = None,
    state_key: str | None = None,
) -> None:
    st = get_rollout_gate_state(r, gate_key=gate_key, state_key=state_key)
    if not st.get('active'):
        return
    meta = dict(st.get('gate_meta') or {})
    payload = {
        'blocked': True,
        'purpose': purpose,
        'gate_key': st.get('gate_key'),
        'state_key': st.get('state_key'),
        'last_fail_ts_ms': int(st.get('last_fail_ts_ms') or 0),
        'reason': _s(meta.get('reason') or st.get('state', {}).get('gate_reason') or 'nightly_reconnect_smoke_failed'),
        'failed_cases': meta.get('failed_cases') or _load_json(st.get('state', {}).get('last_failed_cases_json')) or [],
    }
    msg = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if exit_code is None:
        raise RuntimeError(f'ExecHealth reconnect nightly rollout gate is active: {msg}')
    sys.stderr.write(msg + '\n')
    raise SystemExit(int(exit_code))
