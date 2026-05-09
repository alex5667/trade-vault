from __future__ import annotations

#!/usr/bin/env python3
from utils.time_utils import get_ny_time_millis

"""Operator ack/silence workflow for latency deploy-lint notifier.

P4.9 adds operator policy controls for rolling silence budget and re-ack limits.
When the same purpose exceeds configured limits, the silence action requires a
separate escalation ticket; otherwise the action is denied but gate enforcement
remains unchanged.

P4.10 adds dual-control approval for long override windows so an escalation
ticket alone is insufficient when the requested suppression duration exceeds a
configured threshold.

P4.11 adds freshness windows for prepared/approved approval requests so stale
requests auto-expire or auto-cancel instead of being reusable indefinitely.

P4.13 extends approval binding to richer semantic drift state so a fresh
approval is still rejected if gate_reason_code, errors_count, or the canonical
details_json fingerprint changed since prepare/approve.

P4.14 also binds approval to warning severity policy and notifier route class.
""",
import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

from services.observability.latency_deploy_contract import CONTRACTS
from services.observability.latency_deploy_lint_silence_approval_state import (
    approve_override_approval,
    binding_mismatch_fields,
    build_drift_binding,
    consume_approval,
    invalidate_approval,
    parse_approval_state,
    prepare_override_approval,
    read_latest_approval,
    refresh_approval_state,
    validate_approval_for_ack,
)
from services.observability.latency_deploy_lint_silence_state import (
    clear_ack_silence,
    evaluate_ack_policy,
    parse_silence_state,
    record_dual_control_denial,
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
    # P4.10 dual-control approval configuration
    approval_prefix: str = 'cfg:orderflow:latency_contract:deploy_lint:silence_approval'
    approval_ttl_s: int = 604800
    # P4.11 freshness windows
    approval_prepared_freshness_s: int = 7200
    approval_approved_freshness_s: int = 1800
    # P4.10: silence minutes >= this threshold require dual-control approval when a policy override is active
    dual_control_minutes: int = 480
    # P4.14: warning-code CSV lists for severity-policy / route-class determination
    notify_warn_codes_warn_csv: str = ''
    notify_warn_codes_crit_csv: str = ''
    notify_warn_codes_page_csv: str = ''


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
        state_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_STATE_PREFIX', 'metrics:latency_contract:deploy_lint:last'),
        silence_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_PREFIX', 'cfg:orderflow:latency_contract:deploy_lint:silence'),
        # P4.10: approval state prefix separate from silence prefix
        approval_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_PREFIX', 'cfg:orderflow:latency_contract:deploy_lint:silence_approval'),
        ops_stream=_env('LATENCY_CONTRACT_DEPLOY_LINT_OPS_EVENT_STREAM', 'ops:latency_contract:events:v1'),
        silence_ttl_s=max(3600, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_TTL_S', '2592000'), 2592000)),
        # P4.10: approval TTL for the dual-control request objects
        approval_ttl_s=max(3600, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_TTL_S', '604800'), 604800)),
        # P4.11: freshness windows
        approval_prepared_freshness_s=max(60, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_PREPARED_FRESHNESS_S', '7200'), 7200)),
        approval_approved_freshness_s=max(60, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_APPROVED_FRESHNESS_S', '1800'), 1800)),
        default_minutes=max(1, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_DEFAULT_MINUTES', '360'), 360)),
        # P4.9: rolling policy window controls
        policy_window_s=max(3600, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_WINDOW_HOURS', '168'), 168) * 3600),
        policy_max_budget_minutes=max(1, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_MAX_BUDGET_MINUTES', '1440'), 1440)),
        policy_max_acks=max(1, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_MAX_ACKS', '3'), 3)),
        policy_denied_exit_code=max(1, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_POLICY_DENIED_EXIT_CODE', '27'), 27)),
        dual_control_minutes=max(0, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_DUAL_CONTROL_MINUTES', '480'), 480)),
        # P4.14: warning code policy CSVs for route-aware binding
        notify_warn_codes_warn_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_WARN_CSV', ''),
        notify_warn_codes_crit_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_CRIT_CSV', ''),
        notify_warn_codes_page_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_PAGE_CSV', ''),
    )


def _read_purpose_status(r: Any, cfg: Cfg, purpose: str, now_ms: int) -> dict[str, Any]:
    lint_raw = r.hgetall(lint_state_key(cfg.state_prefix, purpose)) or {}
    silence_raw = r.hgetall(silence_state_key(cfg.silence_prefix, purpose)) or {}
    silence = parse_silence_state(silence_raw, now_ms=now_ms)
    # P4.11: read latest approval with freshness auto-transition
    approval_raw = read_latest_approval(
        r,
        prefix=cfg.approval_prefix,
        purpose=purpose,
        prepared_freshness_s=cfg.approval_prepared_freshness_s,
        approved_freshness_s=cfg.approval_approved_freshness_s,
        ttl_s=cfg.approval_ttl_s,
        ops_stream=cfg.ops_stream,
        now_ms=now_ms,
    )
    approval = parse_approval_state(approval_raw)
    approval_remaining_s = max(0, int((approval.freshness_deadline_ts_ms - now_ms) / 1000)) if approval.freshness_deadline_ts_ms > 0 else 0
    # P4.12 + P4.13 + P4.14: compute current binding and check for semantic/route mismatch
    current_binding = build_drift_binding(
        r, state_prefix=cfg.state_prefix, purpose=purpose, now_ms=now_ms,
        warn_codes_warn_csv=cfg.notify_warn_codes_warn_csv,
        warn_codes_crit_csv=cfg.notify_warn_codes_crit_csv,
        warn_codes_page_csv=cfg.notify_warn_codes_page_csv,
    )
    approval_binding_mismatch = binding_mismatch_fields(approval, current_binding) if approval.present else []
    approval_binding_match = bool(approval.present and not approval_binding_mismatch)
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
        # P4.9 policy status fields
        'policy_window_start_ts_ms': silence.policy_window_start_ts_ms,
        'policy_window_end_ts_ms': silence.policy_window_end_ts_ms,
        'policy_window_ack_count': silence.policy_window_ack_count,
        'policy_window_budget_minutes_used': silence.policy_window_budget_minutes_used,
        'policy_limit_hit_total': silence.policy_limit_hit_total,
        'policy_denied_total': silence.policy_denied_total,
        'policy_last_limit_kind': silence.policy_last_limit_kind,
        'policy_last_deny_ts_ms': silence.policy_last_deny_ts_ms,
        'policy_last_deny_reason': silence.policy_last_deny_reason,
        'policy_current_override_active': silence.policy_current_override_active,
        'policy_current_override_ticket': silence.policy_current_override_ticket,
        'policy_last_override_ticket': silence.policy_last_override_ticket,
        # P4.10 dual-control status fields
        'dual_control_required': silence.dual_control_required,
        'dual_control_request_id': silence.dual_control_request_id,
        'dual_control_prepared_by': silence.dual_control_prepared_by,
        'dual_control_approved_by': silence.dual_control_approved_by,
        'dual_control_approved_ts_ms': silence.dual_control_approved_ts_ms,
        'dual_control_denied_total': silence.dual_control_denied_total,
        'dual_control_last_deny_reason': silence.dual_control_last_deny_reason,
        # P4.11 latest approval status fields
        'latest_approval_request_id': approval.request_id,
        'latest_approval_status': approval.status,
        'latest_approval_prepared_by': approval.prepared_by,
        'latest_approval_approved_by': approval.approved_by,
        'latest_approval_freshness_deadline_ts_ms': approval.freshness_deadline_ts_ms,
        'latest_approval_freshness_remaining_s': approval_remaining_s,
        'latest_approval_expired_reason': approval.expired_reason,
        'latest_approval_cancelled_reason': approval.cancelled_reason,
        # P4.12 + P4.13 binding status fields
        'latest_approval_binding_schema_version': approval.binding_schema_version,
        'latest_approval_bound_snapshot_ts_ms': approval.bound_snapshot_ts_ms,
        'latest_approval_bound_error_codes': approval.bound_error_codes,
        'latest_approval_bound_error_codes_hash': approval.bound_error_codes_hash,
        'latest_approval_bound_active_purposes_csv': approval.bound_active_purposes_csv,
        'latest_approval_bound_active_purposes_hash': approval.bound_active_purposes_hash,
        'latest_approval_bound_gate_reason_code': approval.bound_gate_reason_code,
        'latest_approval_bound_errors_count': approval.bound_errors_count,
        'latest_approval_bound_details_json': approval.bound_details_json,
        'latest_approval_bound_details_fingerprint': approval.bound_details_fingerprint,
        # P4.14 warning-policy binding status
        'latest_approval_bound_warning_codes_hash': approval.bound_warning_codes_hash,
        'latest_approval_bound_warning_severity_policy': approval.bound_warning_severity_policy,
        'latest_approval_bound_notifier_route_class': approval.bound_notifier_route_class,
        'latest_approval_current_gate_reason_code': (current_binding.get('bound_gate_reason_code', 'ok') or 'ok'),
        'latest_approval_current_errors_count': _i(current_binding.get('bound_errors_count'), 0),
        'latest_approval_current_details_fingerprint': (current_binding.get('bound_details_fingerprint', '') or ''),
        'latest_approval_current_warning_codes_hash': current_binding.get('bound_warning_codes_hash', ''),
        'latest_approval_current_warning_severity_policy': current_binding.get('bound_warning_severity_policy', ''),
        'latest_approval_current_notifier_route_class': current_binding.get('bound_notifier_route_class', ''),
        'latest_approval_binding_match': approval_binding_match,
        'latest_approval_binding_mismatch_fields': approval_binding_mismatch,
        'latest_approval_invalidated_reason': approval.invalidated_reason,
        'latest_approval_invalidated_stage': approval.invalidated_stage,
    }


def cmd_status(r: Any, cfg: Cfg, *, purpose: str | None = None, now_ms: int | None = None) -> dict[str, Any]:
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    purposes = [purpose] if purpose else sorted(CONTRACTS.keys())
    return {'schema_version': 5, 'ts_ms': now_ms, 'rows': [_read_purpose_status(r, cfg, p, now_ms) for p in purposes]}


def cmd_prepare_override(
    r: Any,
    cfg: Cfg,
    *,
    purpose: str,
    operator: str,
    ticket: str,
    escalation_ticket: str,
    reason: str,
    minutes: int,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Prepare a dual-control approval request for a long-window override.

    P4.10: Step 1 of the prepare/approve/ack workflow. A second operator must
    approve this request before the final ack can commit the silence.
    P4.12: binds the request to the current drift snapshot (error_codes + active_purposes_hash).
    P4.13: also binds gate_reason_code, errors_count, details_json fingerprint.
    P4.14: also binds warning_codes, warning_severity_policy, notifier_route_class.
    """,
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    if not escalation_ticket:
        raise ValueError('escalation ticket is required')
    raw = prepare_override_approval(
        r,
        prefix=cfg.approval_prefix,
        purpose=purpose,
        operator=operator,
        ticket=ticket,
        escalation_ticket=escalation_ticket,
        reason=reason,
        minutes=minutes,
        ttl_s=cfg.approval_ttl_s,
        prepared_freshness_s=cfg.approval_prepared_freshness_s,
        ops_stream=cfg.ops_stream,
        # P4.12+P4.13+P4.14: bind to current drift snapshot at prepare time (including route class)
        drift_binding=build_drift_binding(
            r, state_prefix=cfg.state_prefix, purpose=purpose, now_ms=now_ms,
            warn_codes_warn_csv=cfg.notify_warn_codes_warn_csv,
            warn_codes_crit_csv=cfg.notify_warn_codes_crit_csv,
            warn_codes_page_csv=cfg.notify_warn_codes_page_csv,
        ),
        now_ms=now_ms,
    )
    return {'ok': True, 'action': 'prepare-override', 'purpose': purpose, 'request_id': raw.get('request_id', ''), 'status': raw}


def cmd_approve_override(
    r: Any,
    cfg: Cfg,
    *,
    request_id: str,
    operator: str,
    reason: str,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Approve a pending dual-control override request (must be a different operator).

    P4.10: Step 2. The approver must differ from the requester.
    """,
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    raw = approve_override_approval(
        r,
        prefix=cfg.approval_prefix,
        request_id=request_id,
        operator=operator,
        reason=reason,
        ttl_s=cfg.approval_ttl_s,
        prepared_freshness_s=cfg.approval_prepared_freshness_s,
        approved_freshness_s=cfg.approval_approved_freshness_s,
        ops_stream=cfg.ops_stream,
        now_ms=now_ms,
    )
    return {'ok': True, 'action': 'approve-override', 'purpose': raw.get('purpose', ''), 'request_id': request_id, 'status': raw}


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
    approval_request_id: str = '',
    now_ms: int | None = None,
) -> dict[str, Any]:
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    lint_raw = r.hgetall(lint_state_key(cfg.state_prefix, purpose)) or {}
    gate_active = (lint_raw.get('gate_active', '0')) == '1'
    # P4.10: preview policy to detect if dual-control gate applies before writing state
    prev = r.hgetall(silence_state_key(cfg.silence_prefix, purpose)) or {}
    policy_preview = evaluate_ack_policy(
        prev,
        now_ms=now_ms,
        silence_minutes=minutes,
        policy_window_s=cfg.policy_window_s,
        max_budget_minutes=cfg.policy_max_budget_minutes,
        max_acks=cfg.policy_max_acks,
        ticket=ticket,
        escalation_ticket=escalation_ticket,
    )
    # Dual-control is required only when: threshold set, override is active, and window is long
    dual_control_required = bool(cfg.dual_control_minutes > 0 and policy_preview.override_active and minutes >= cfg.dual_control_minutes)
    approval_state = None
    # P4.12+P4.13+P4.14: compute current binding snapshot before validation (includes route class)
    current_binding = build_drift_binding(
        r, state_prefix=cfg.state_prefix, purpose=purpose, now_ms=now_ms,
        warn_codes_warn_csv=cfg.notify_warn_codes_warn_csv,
        warn_codes_crit_csv=cfg.notify_warn_codes_crit_csv,
        warn_codes_page_csv=cfg.notify_warn_codes_page_csv,
    )
    if dual_control_required:
        # P4.11: auto-transition stale requests before validating
        approval_raw = refresh_approval_state(
            r,
            prefix=cfg.approval_prefix,
            request_id=approval_request_id,
            prepared_freshness_s=cfg.approval_prepared_freshness_s,
            approved_freshness_s=cfg.approval_approved_freshness_s,
            ttl_s=cfg.approval_ttl_s,
            ops_stream=cfg.ops_stream,
            now_ms=now_ms,
        ) if approval_request_id else {}
        valid = validate_approval_for_ack(
            approval_raw,
            purpose=purpose,
            operator=operator,
            ticket=ticket,
            escalation_ticket=escalation_ticket,
            minutes=minutes,
            # P4.12+P4.13: pass current binding for semantic mismatch check
            current_binding=current_binding,
            now_ms=now_ms,
        )
        if not valid.ok:
            # P4.12+P4.13: auto-invalidate approval if drift snapshot changed
            if valid.invalidate and approval_request_id:
                invalidate_approval(
                    r,
                    prefix=cfg.approval_prefix,
                    request_id=approval_request_id,
                    reason=valid.reason,
                    stage='ack',
                    current_binding=current_binding,
                    ttl_s=cfg.approval_ttl_s,
                    ops_stream=cfg.ops_stream,
                    now_ms=now_ms,
                )
            # Record the denial in silence state and return early without activating silence
            record_dual_control_denial(
                r,
                prefix=cfg.silence_prefix,
                purpose=purpose,
                operator=operator,
                ticket=ticket,
                escalation_ticket=escalation_ticket,
                reason=reason,
                silence_minutes=minutes,
                deny_reason=valid.reason,
                ttl_s=cfg.silence_ttl_s,
                ops_stream=cfg.ops_stream,
                now_ms=now_ms,
            )
            status = _read_purpose_status(r, cfg, purpose, now_ms)
            return {
                'ok': False,
                'action': 'ack',
                'purpose': purpose,
                'operator': operator,
                'ticket': ticket,
                'minutes': int(minutes),
                'escalation_ticket': (escalation_ticket or ''),
                'approval_request_id': (approval_request_id or ''),
                'policy': {
                    'window_hours': int(cfg.policy_window_s / 3600),
                    'max_budget_minutes': cfg.policy_max_budget_minutes,
                    'max_acks': cfg.policy_max_acks,
                    'denied': True,
                    'requires_escalation': bool(policy_preview.requires_escalation),
                    'limit_kind': policy_preview.limit_kind,
                    'denied_reason': valid.reason,
                    'override_active': False,
                    'dual_control_required': True,
                },
                'status': status,
            }
        approval_state = valid.state
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
        # P4.10 dual-control fields passed through
        dual_control_required=dual_control_required,
        dual_control_request_id=approval_state.request_id if approval_state else '',
        dual_control_prepared_by=approval_state.prepared_by if approval_state else '',
        dual_control_approved_by=approval_state.approved_by if approval_state else '',
        dual_control_approved_ts_ms=approval_state.approved_ts_ms if approval_state else 0,
    )
    # P4.10: consume the one-time approval after a successful ack
    if approval_state and state.get('last_action') == 'ack_silence':
        consume_approval(
            r,
            prefix=cfg.approval_prefix,
            request_id=approval_state.request_id,
            operator=operator,
            ttl_s=cfg.approval_ttl_s,
            ops_stream=cfg.ops_stream,
            now_ms=now_ms,
        )
    status = _read_purpose_status(r, cfg, purpose, now_ms)
    denied = state.get('last_action') in {'ack_denied_policy', 'ack_denied_dual_control'}
    return {
        'ok': not denied,
        'action': 'ack',
        'purpose': purpose,
        'operator': operator,
        'ticket': ticket,
        'minutes': int(minutes),
        'escalation_ticket': (escalation_ticket or ''),
        'approval_request_id': str(approval_state.request_id if approval_state else approval_request_id or ''),
        'policy': {
            'window_hours': int(cfg.policy_window_s / 3600),
            'max_budget_minutes': cfg.policy_max_budget_minutes,
            'max_acks': cfg.policy_max_acks,
            'denied': denied,
            'requires_escalation': bool(status['policy_last_limit_kind'] and status['policy_last_limit_kind'] != 'none'),
            'limit_kind': status['policy_last_limit_kind'],
            'denied_reason': status['policy_last_deny_reason'] if state.get('last_action') == 'ack_denied_policy' else (status['dual_control_last_deny_reason'] if state.get('last_action') == 'ack_denied_dual_control' else ''),
            'override_active': status['policy_current_override_active'],
            'dual_control_required': dual_control_required,
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
    # P4.10: dual-control prepare step
    prep = sub.add_parser('prepare-override')
    prep.add_argument('--purpose', required=True, choices=sorted(CONTRACTS.keys()))
    prep.add_argument('--operator', required=True)
    prep.add_argument('--ticket', required=True)
    prep.add_argument('--escalation-ticket', required=True)
    prep.add_argument('--reason', required=True)
    prep.add_argument('--minutes', type=int, required=True)
    # P4.10: dual-control approve step
    appr = sub.add_parser('approve-override')
    appr.add_argument('--request-id', required=True)
    appr.add_argument('--operator', required=True)
    appr.add_argument('--reason', required=True)
    for name in ('ack', 'silence'):
        sp = sub.add_parser(name)
        sp.add_argument('--purpose', required=True, choices=sorted(CONTRACTS.keys()))
        sp.add_argument('--operator', required=True)
        sp.add_argument('--ticket', required=True)
        sp.add_argument('--reason', required=True)
        sp.add_argument('--minutes', type=int, default=None)
        sp.add_argument('--escalation-ticket', default='')
        # P4.10: optional approval request ID for long-window overrides
        sp.add_argument('--approval-request-id', default='')
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
    elif args.cmd == 'prepare-override':
        out = cmd_prepare_override(
            r,
            cfg,
            purpose=args.purpose,
            operator=args.operator,
            ticket=args.ticket,
            escalation_ticket=args.escalation_ticket,
            reason=args.reason,
            minutes=max(1, int(args.minutes)),
        )
        rc = 0
    elif args.cmd == 'approve-override':
        out = cmd_approve_override(r, cfg, request_id=args.request_id, operator=args.operator, reason=args.reason)
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
            approval_request_id=str(args.approval_request_id or ''),
        )
        # P4.9/P4.10: non-zero exit code when policy or dual-control denies the ack
        rc = 0 if out.get('ok') else cfg.policy_denied_exit_code
    else:
        out = cmd_unsilence(r, cfg, purpose=args.purpose, operator=args.operator, ticket=args.ticket, reason=args.reason)
        rc = 0
    json.dump(out, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write('\n')
    return rc


if __name__ == '__main__':
    raise SystemExit(main())
