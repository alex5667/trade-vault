#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Operator ack/silence workflow for latency deploy-lint notifier.

P4.9 adds operator policy controls for rolling silence budget and re-ack limits.
When the same purpose exceeds configured limits, the silence action requires a
separate escalation ticket; otherwise the action is denied but gate enforcement
remains unchanged.
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

from services.observability.latency_deploy_contract import CONTRACTS
from services.observability.latency_deploy_lint_silence_state import (
    clear_ack_silence,
    parse_silence_state,
    upsert_ack_silence,
)
from services.observability.latency_deploy_lint_silence_state import (
    state_key as silence_state_key,
)
from services.observability.latency_deploy_lint_state import state_key as lint_state_key


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


@dataclass(frozen=True)
class Cfg:
    redis_url: str
    state_prefix: str
    silence_prefix: str
    ops_stream: str
    silence_ttl_s: int
    default_minutes: int
    # P4.9 policy configuration
    policy_window_s: int
    policy_max_budget_minutes: int
    policy_max_acks: int
    policy_denied_exit_code: int


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
        state_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_STATE_PREFIX', 'metrics:latency_contract:deploy_lint:last'),
        silence_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_PREFIX', 'cfg:orderflow:latency_contract:deploy_lint:silence'),
        ops_stream=_env('LATENCY_CONTRACT_DEPLOY_LINT_OPS_EVENT_STREAM', 'ops:latency_contract:events:v1'),
        silence_ttl_s=max(3600, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_TTL_S', '2592000'), 2592000)),
        default_minutes=max(1, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_DEFAULT_MINUTES', '360'), 360)),
        policy_window_s=max(3600, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_WINDOW_HOURS', '168'), 168) * 3600),
        policy_max_budget_minutes=max(1, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_MAX_BUDGET_MINUTES', '1440'), 1440)),
        policy_max_acks=max(1, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_MAX_ACKS', '3'), 3)),
        policy_denied_exit_code=max(1, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_DENIED_EXIT_CODE', '27'), 27)),
    )


def _read_purpose_status(r: Any, cfg: Cfg, purpose: str, now_ms: int) -> dict[str, Any]:
    lint_raw = r.hgetall(lint_state_key(cfg.state_prefix, purpose)) or {}
    silence_raw = r.hgetall(silence_state_key(cfg.silence_prefix, purpose)) or {}
    silence = parse_silence_state(silence_raw, now_ms=now_ms)
    return {
        'purpose': purpose,
        'gate_active': (lint_raw.get('gate_active', '0')) == '1',
        'gate_reason_code': (lint_raw.get('gate_reason_code', 'unknown') or 'unknown'),
        'error_codes': (lint_raw.get('error_codes', 'ok') or 'ok'),
        'fail_age_s': _i(lint_raw.get('fail_age_s'), 0),
        'silence_active': silence.silence_active,
        'silence_until_ts_ms': silence.silence_until_ts_ms,
        'silence_remaining_s': silence.remaining_s,
        'ack_operator': silence.ack_operator,
        'ack_ticket': silence.ack_ticket,
        'ack_reason': silence.ack_reason,
        'unsilence_operator': silence.unsilence_operator,
        'unsilence_ticket': silence.unsilence_ticket,
        'unsilence_reason': silence.unsilence_reason,
        'last_action': silence.last_action,
        'last_action_ts_ms': silence.last_action_ts_ms,
        'policy_window_start_ts_ms': silence.policy_window_start_ts_ms,
        'policy_window_end_ts_ms': silence.policy_window_end_ts_ms,
        'policy_window_ack_count': silence.policy_window_ack_count,
        'policy_window_budget_minutes_used': silence.policy_window_budget_minutes_used,
        'policy_limit_hit_total': silence.policy_limit_hit_total,
        'policy_denied_total': silence.policy_denied_total,
        'policy_last_limit_kind': silence.policy_last_limit_kind,
        'policy_last_limit_ts_ms': silence.policy_last_limit_ts_ms,
        'policy_last_deny_ts_ms': silence.policy_last_deny_ts_ms,
        'policy_last_deny_reason': silence.policy_last_deny_reason,
        'policy_current_override_active': silence.policy_current_override_active,
        'policy_current_override_ticket': silence.policy_current_override_ticket,
        'policy_last_override_ticket': silence.policy_last_override_ticket,
    }


def cmd_status(r: Any, cfg: Cfg, *, purpose: str | None = None, now_ms: int | None = None) -> dict[str, Any]:
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    purposes = [purpose] if purpose else sorted(CONTRACTS.keys())
    return {'schema_version': 2, 'ts_ms': now_ms, 'rows': [_read_purpose_status(r, cfg, p, now_ms) for p in purposes]}


def cmd_ack(
    r: Any,
    cfg: Cfg,
    *,
    purpose: str,
    operator: str,
    ticket: str,
    reason: str,
    minutes: int,
    escalation_ticket: str = '',
    now_ms: int | None = None,
) -> dict[str, Any]:
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    lint_raw = r.hgetall(lint_state_key(cfg.state_prefix, purpose)) or {}
    gate_active = (lint_raw.get('gate_active', '0')) == '1'
    state = upsert_ack_silence(
        r,
        prefix=cfg.silence_prefix,
        purpose=purpose,
        operator=operator,
        ticket=ticket,
        reason=reason,
        silence_minutes=minutes,
        ttl_s=cfg.silence_ttl_s,
        ops_stream=cfg.ops_stream,
        gate_active=gate_active,
        now_ms=now_ms,
        policy_window_s=cfg.policy_window_s,
        policy_max_budget_minutes=cfg.policy_max_budget_minutes,
        policy_max_acks=cfg.policy_max_acks,
        escalation_ticket=escalation_ticket,
    ),
    status = _read_purpose_status(r, cfg, purpose, now_ms),
    denied = state.get('last_action') == 'ack_denied_policy',
    return {
        'ok': not denied,
        'action': 'ack',
        'purpose': purpose,
        'operator': operator,
        'ticket': ticket,
        'minutes': int(minutes),
        'escalation_ticket': (escalation_ticket or ''),
        'policy': {
            'window_hours': int(cfg.policy_window_s / 3600),
            'max_budget_minutes': cfg.policy_max_budget_minutes,
            'max_acks': cfg.policy_max_acks,
            'denied': denied,
            'requires_escalation': bool(status['policy_last_limit_kind'] and status['policy_last_limit_kind'] != 'none'),
            'limit_kind': status['policy_last_limit_kind'],
            'denied_reason': status['policy_last_deny_reason'] if denied else '',
            'override_active': status['policy_current_override_active'],
        },
        'status': status,
    }


def cmd_unsilence(r: Any, cfg: Cfg, *, purpose: str, operator: str, ticket: str, reason: str, now_ms: int | None = None) -> dict[str, Any]:
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    clear_ack_silence(r, prefix=cfg.silence_prefix, purpose=purpose, operator=operator, ticket=ticket, reason=reason, ttl_s=cfg.silence_ttl_s, ops_stream=cfg.ops_stream, now_ms=now_ms)
    return {'ok': True, 'action': 'unsilence', 'purpose': purpose, 'operator': operator, 'ticket': ticket, 'status': _read_purpose_status(r, cfg, purpose, now_ms)}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='Latency deploy-lint notifier ack/silence workflow')
    sub = p.add_subparsers(dest='cmd', required=True)
    st = sub.add_parser('status')
    st.add_argument('--purpose', choices=sorted(CONTRACTS.keys()), default=None)
    for name in ('ack', 'silence'):
        sp = sub.add_parser(name)
        sp.add_argument('--purpose', required=True, choices=sorted(CONTRACTS.keys()))
        sp.add_argument('--operator', required=True)
        sp.add_argument('--ticket', required=True)
        sp.add_argument('--reason', required=True)
        sp.add_argument('--minutes', type=int, default=None)
        sp.add_argument('--escalation-ticket', default='')
    un = sub.add_parser('unsilence')
    un.add_argument('--purpose', required=True, choices=sorted(CONTRACTS.keys()))
    un.add_argument('--operator', required=True)
    un.add_argument('--ticket', required=True)
    un.add_argument('--reason', required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    cfg = load_cfg()
    args = build_parser().parse_args(argv)
    import redis  # type: ignore
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    if args.cmd == 'status':
        out = cmd_status(r, cfg, purpose=args.purpose)
        rc = 0
    elif args.cmd in {'ack', 'silence'}:
        out = cmd_ack(
            r,
            cfg,
            purpose=args.purpose,
            operator=args.operator,
            ticket=args.ticket,
            reason=args.reason,
            minutes=cfg.default_minutes if args.minutes is None else max(1, int(args.minutes)),
            escalation_ticket=str(args.escalation_ticket or ''),
        )
        rc = 0 if out.get('ok') else cfg.policy_denied_exit_code
    else:
        out = cmd_unsilence(r, cfg, purpose=args.purpose, operator=args.operator, ticket=args.ticket, reason=args.reason)
        rc = 0
    json.dump(out, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write('\n')
    return rc


if __name__ == '__main__':
    raise SystemExit(main())
