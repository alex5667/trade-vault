#!/usr/bin/env python3
from __future__ import annotations

"""Nightly reconnect chaos smoke for ExecHealth trusted Redis clients.

P17 wires the P16 reconnect chaos harness into a regular nightly job suitable for
systemd + docker compose on a staging/infra host.

P18 extends it with post-run ops summaries and a latched rollout/apply gate:
- every run emits a Redis ops-event summary;
- Telegram summary is emitted on failure (or always when enabled);
- failed runs latch a rollout gate in Redis;
- the gate remains active until an explicit manual ack.
"""

import argparse
import json
import os
import socket
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from orderflow_services.exec_health_freeze_reconnect_chaos_harness_v1 import ChaosHarness
from services.orderflow.exec_health_freeze_rollout_gate import (
    build_post_run_summary,
    emit_ops_summary,
    maybe_emit_telegram_summary,
    stringify_mapping,
    update_rollout_gate_from_report,
)
from services.orderflow.exec_health_freeze_service_identity import get_expected_service


DEFAULT_REPORT_PATH = '/tmp/exec_health_freeze_reconnect_smoke_report.json'
DEFAULT_TEXTFILE_PATH = '/tmp/exec_health_freeze_reconnect_smoke.prom'
STATE_KEY = 'metrics:exec_health:freeze_reconnect_smoke:last'


@dataclass(frozen=True)
class SmokeCase:
    role: str
    service: str
    redis_url: str
    scenario: str
    wrong_user_url: str = ''
    enabled: bool = True
    skip_reason: str = ''


REPAIRABLE_CASES = [
    ('writer', 'exec_health_freeze_override_v1', 'REDIS_URL'),
    ('audit', 'exec_health_freeze_client_name_audit_exporter_v1', 'EXEC_HEALTH_REDIS_AUDIT_URL'),
    ('bootstrap', 'exec_health_freeze_acl_policy_v1', 'EXEC_HEALTH_REDIS_BOOTSTRAP_URL'),
]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _s(x: Any, d: str = '') -> str:
    try:
        return str(x) if x is not None else str(d)
    except Exception:
        return str(d)


def _b(x: Any, default: bool = False) -> bool:
    try:
        if isinstance(x, str):
            t = x.strip().lower()
            if not t:
                return bool(default)
            return t in {'1', 'true', 'yes', 'on'}
        return bool(int(x))
    except Exception:
        return bool(default)


def _atomic_write(path: str, text: str) -> None:
    p = Path(path).expanduser().resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', encoding='utf-8', delete=False, dir=str(p.parent)) as tmp:
        tmp.write(text)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    Path(tmp_name).replace(p)


def _write_json(path: str, obj: Any) -> None:
    _atomic_write(path, json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


def _render_textfile(report: Dict[str, Any]) -> str:
    now_ms = int(report.get('ts_ms', 0) or 0)
    duration_s = float(report.get('duration_seconds', 0.0) or 0.0)
    overall_ok = 1 if report.get('ok') else 0
    success_ts = int(report.get('last_success_ts_ms', 0) or 0)
    gate_active = 1 if report.get('rollout_gate_active') else 0
    ops_event_id = 1 if _s(report.get('ops_event_id')) else 0
    telegram_event_id = 1 if _s(report.get('telegram_event_id')) else 0
    lines = [
        '# HELP exec_health_freeze_reconnect_smoke_last_run_ok 1 if last nightly reconnect smoke run succeeded.',
        '# TYPE exec_health_freeze_reconnect_smoke_last_run_ok gauge',
        f'exec_health_freeze_reconnect_smoke_last_run_ok {overall_ok}',
        '# HELP exec_health_freeze_reconnect_smoke_last_run_ts_ms Last nightly reconnect smoke run timestamp in epoch ms.',
        '# TYPE exec_health_freeze_reconnect_smoke_last_run_ts_ms gauge',
        f'exec_health_freeze_reconnect_smoke_last_run_ts_ms {now_ms}',
        '# HELP exec_health_freeze_reconnect_smoke_last_success_ts_ms Last successful nightly reconnect smoke timestamp in epoch ms.',
        '# TYPE exec_health_freeze_reconnect_smoke_last_success_ts_ms gauge',
        f'exec_health_freeze_reconnect_smoke_last_success_ts_ms {success_ts}',
        '# HELP exec_health_freeze_reconnect_smoke_last_duration_seconds Duration of last nightly reconnect smoke run.',
        '# TYPE exec_health_freeze_reconnect_smoke_last_duration_seconds gauge',
        f'exec_health_freeze_reconnect_smoke_last_duration_seconds {duration_s:.6f}',
        '# HELP exec_health_freeze_reconnect_rollout_gate_active 1 if nightly reconnect smoke rollout/apply gate is latched active.',
        '# TYPE exec_health_freeze_reconnect_rollout_gate_active gauge',
        f'exec_health_freeze_reconnect_rollout_gate_active {gate_active}',
        '# HELP exec_health_freeze_reconnect_smoke_ops_event_emitted 1 if a Redis ops summary event was emitted for the last run.',
        '# TYPE exec_health_freeze_reconnect_smoke_ops_event_emitted gauge',
        f'exec_health_freeze_reconnect_smoke_ops_event_emitted {ops_event_id}',
        '# HELP exec_health_freeze_reconnect_smoke_telegram_event_emitted 1 if a Telegram summary event was emitted for the last run.',
        '# TYPE exec_health_freeze_reconnect_smoke_telegram_event_emitted gauge',
        f'exec_health_freeze_reconnect_smoke_telegram_event_emitted {telegram_event_id}',
        '# HELP exec_health_freeze_reconnect_smoke_case_ok 1 if reconnect smoke case passed.',
        '# TYPE exec_health_freeze_reconnect_smoke_case_ok gauge',
        '# HELP exec_health_freeze_reconnect_smoke_case_skipped 1 if reconnect smoke case was skipped.',
        '# TYPE exec_health_freeze_reconnect_smoke_case_skipped gauge',
        '# HELP exec_health_freeze_reconnect_smoke_case_duration_seconds Duration of reconnect smoke case.',
        '# TYPE exec_health_freeze_reconnect_smoke_case_duration_seconds gauge',
        '# HELP exec_health_freeze_reconnect_smoke_case_recovery_total Recovery total observed in heal-state after reconnect smoke case.',
        '# TYPE exec_health_freeze_reconnect_smoke_case_recovery_total gauge',
    ]
    for row in list(report.get('cases', []) or []):
        role = _s(row.get('role'))
        service = _s(row.get('service'))
        scenario = _s(row.get('scenario')).replace('-', '_')
        labels = f'role="{role}",service="{service}",scenario="{scenario}"'
        ok = 1 if row.get('ok') else 0
        skipped = 1 if row.get('skipped') else 0
        duration = float(row.get('duration_seconds', 0.0) or 0.0)
        recovery_total = float(row.get('recovery_total', 0) or 0)
        lines.append(f'exec_health_freeze_reconnect_smoke_case_ok{{{labels}}} {ok}')
        lines.append(f'exec_health_freeze_reconnect_smoke_case_skipped{{{labels}}} {skipped}')
        lines.append(f'exec_health_freeze_reconnect_smoke_case_duration_seconds{{{labels}}} {duration:.6f}')
        lines.append(f'exec_health_freeze_reconnect_smoke_case_recovery_total{{{labels}}} {recovery_total:.0f}')
    return '\n'.join(lines) + '\n'


def _build_cases_from_env() -> List[SmokeCase]:
    out: List[SmokeCase] = []
    include_bootstrap = _b(os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_INCLUDE_BOOTSTRAP', '1'), True)
    include_wrong_user = _b(os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_INCLUDE_WRONG_USER', '1'), True)
    for role, service, url_env in REPAIRABLE_CASES:
        if role == 'bootstrap' and not include_bootstrap:
            out.append(SmokeCase(role, service, '', 'reconnect-both', enabled=False, skip_reason='bootstrap disabled'))
            continue
        url = _s(os.getenv(url_env)).strip()
        if not url:
            out.append(SmokeCase(role, service, '', 'reconnect-both', enabled=False, skip_reason=f'missing {url_env}'))
            continue
        out.append(SmokeCase(role, service, url, 'reconnect-both'))
    wrong_user_url = _s(os.getenv('EXEC_HEALTH_REDIS_WRONG_USER_URL')).strip()
    writer_url = _s(os.getenv('REDIS_URL')).strip()
    if include_wrong_user and writer_url and wrong_user_url:
        out.append(SmokeCase('writer', 'exec_health_freeze_override_v1', writer_url, 'wrong-user', wrong_user_url=wrong_user_url))
    else:
        reason = 'wrong-user disabled'
        if include_wrong_user and not wrong_user_url:
            reason = 'missing EXEC_HEALTH_REDIS_WRONG_USER_URL'
        elif include_wrong_user and not writer_url:
            reason = 'missing REDIS_URL'
        out.append(SmokeCase('writer', 'exec_health_freeze_override_v1', writer_url, 'wrong-user', wrong_user_url=wrong_user_url, enabled=False, skip_reason=reason))
    return out


def _evaluate_case(case: SmokeCase, raw: Dict[str, Any]) -> Dict[str, Any]:
    if case.scenario == 'wrong-user':
        ok = (raw.get('ok') is False) and (raw.get('unexpected_success') is False) and ('wrong_user' in _s(raw.get('error')).lower())
        return {
            'ok': bool(ok),
            'reason': 'wrong_user_violation_expected' if ok else 'wrong_user_path_failed',
            'recovery_total': int((raw.get('state') or {}).get('recovery_total', 0) or 0),
        }
    expected = get_expected_service(case.service)
    state = dict(raw.get('state') or {})
    after = dict(raw.get('after_entry') or {})
    recovery_total = int(state.get('recovery_total', 0) or 0)
    ok = bool(raw.get('ok')) and bool(raw.get('recovered')) and bool(raw.get('event_id')) and after.get('name') == expected.client_name and after.get('lib-name') == expected.lib_name and recovery_total >= 1
    return {
        'ok': bool(ok),
        'reason': 'repairable_reconnect_recovered' if ok else 'repairable_reconnect_failed',
        'recovery_total': recovery_total,
    }


def run_case(case: SmokeCase) -> Dict[str, Any]:
    started = time.time()
    if not case.enabled:
        return {
            'role': case.role,
            'service': case.service,
            'scenario': case.scenario,
            'skipped': True,
            'skip_reason': case.skip_reason,
            'ok': True,
            'duration_seconds': 0.0,
            'recovery_total': 0,
        }
    harness = ChaosHarness(case.redis_url, service=case.service, wrong_user_url=case.wrong_user_url)
    if case.scenario == 'wrong-user':
        raw = harness.run_wrong_user()
    else:
        raw = harness.run_repairable(case.scenario)
    chk = _evaluate_case(case, raw)
    duration_s = max(0.0, time.time() - started)
    return {
        'role': case.role,
        'service': case.service,
        'scenario': case.scenario,
        'skipped': False,
        'ok': bool(chk['ok']),
        'check_reason': chk['reason'],
        'duration_seconds': duration_s,
        'recovery_total': int(chk['recovery_total']),
        'raw': raw,
    }


class NightlySmokeRunner:
    def __init__(self, *, report_path: str, textfile_path: str) -> None:
        self.report_path = report_path
        self.textfile_path = textfile_path
        self.hostname = socket.gethostname()
        self.state_key = os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_STATE_KEY', STATE_KEY)
        self.redis_url = os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_NOTIFY_REDIS_URL') or os.getenv('REDIS_URL') or ''

    def _connect_redis(self):
        if redis is None or not self.redis_url:
            return None
        try:
            return redis.Redis.from_url(self.redis_url, decode_responses=True)
        except Exception:
            return None

    def _persist_state(self, report: Dict[str, Any]) -> None:
        r = self._connect_redis()
        if r is None:
            return
        summary_text = build_post_run_summary(report)
        ops_event_id = emit_ops_summary(r, report=report, summary_text=summary_text, report_path=self.report_path)
        telegram_event_id = maybe_emit_telegram_summary(r, report=report, summary_text=summary_text, report_path=self.report_path)
        gate_state = update_rollout_gate_from_report(
            r,
            report=report,
            report_path=self.report_path,
            ops_event_id=ops_event_id,
            telegram_event_id=telegram_event_id,
        )
        report['ops_event_id'] = ops_event_id
        report['telegram_event_id'] = telegram_event_id
        report['rollout_gate_active'] = bool(gate_state.get('active'))
        report['rollout_gate_key'] = _s(gate_state.get('gate_key'))
        report['rollout_gate_state_key'] = _s(gate_state.get('state_key'))
        report['rollout_gate_state'] = gate_state.get('state') or {}
        try:
            r.hset(self.state_key, mapping=stringify_mapping({
                'ts_ms': int(report.get('ts_ms') or _now_ms()),
                'ok': 1 if report.get('ok') else 0,
                'ops_event_id': ops_event_id,
                'telegram_event_id': telegram_event_id,
                'rollout_gate_active': 1 if report.get('rollout_gate_active') else 0,
                'report_path': self.report_path,
                'summary_text': summary_text,
            }))
            try:
                r.expire(self.state_key, 86400 * 30)
            except Exception:
                pass
        except Exception:
            pass

    def run(self, cases: Optional[Iterable[SmokeCase]] = None) -> Dict[str, Any]:
        started = time.time()
        ts_ms = _now_ms()
        rows: List[Dict[str, Any]] = []
        for case in list(cases or _build_cases_from_env()):
            rows.append(run_case(case))
        enabled_rows = [r for r in rows if not r.get('skipped')]
        ok = all(bool(r.get('ok')) for r in enabled_rows) if enabled_rows else True
        report = {
            'schema_ver': 'exec_health_reconnect_smoke_v2',
            'host': self.hostname,
            'ts_ms': ts_ms,
            'ok': bool(ok),
            'enabled_case_count': len(enabled_rows),
            'case_count': len(rows),
            'duration_seconds': max(0.0, time.time() - started),
            'last_success_ts_ms': ts_ms if ok else 0,
            'cases': rows,
            'state_key': self.state_key,
            'ops_event_id': '',
            'telegram_event_id': '',
            'rollout_gate_active': False,
        }
        self._persist_state(report)
        _write_json(self.report_path, report)
        _atomic_write(self.textfile_path, _render_textfile(report))
        return report


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='Nightly ExecHealth reconnect chaos smoke runner')
    p.add_argument('--report-path', default=os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_REPORT_PATH', DEFAULT_REPORT_PATH))
    p.add_argument('--textfile-path', default=os.getenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_TEXTFILE_PATH', DEFAULT_TEXTFILE_PATH))
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = NightlySmokeRunner(report_path=args.report_path, textfile_path=args.textfile_path).run()
    return 0 if report.get('ok') else 2


if __name__ == '__main__':
    raise SystemExit(main())
