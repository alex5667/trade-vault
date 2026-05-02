#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
"""Emit ops-event/Telegram summaries for persistent latency deploy-lint drift.

P4.7 adds an operator ack/silence workflow. The rollout gate remains active, but
individual purposes can be temporarily silenced at the notifier layer using
operator/ticket/reason metadata, preserving audit trail without pager spam.

P4.14 adds warning-code policy aware notifier route selection so operational
class changes are deterministic and visible to approval binding.
""",
import json
import os
import time
from dataclasses import dataclass
from typing import Any

from services.observability.latency_deploy_contract import CONTRACTS
from services.observability.latency_deploy_lint_state import state_key as lint_state_key
from services.observability.latency_deploy_lint_notify_state import purposes_hash, state_key as notify_state_key, update_notifier_state
from services.observability.latency_deploy_lint_silence_state import list_silenced_purposes, parse_silence_state


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


@dataclass
class Cfg:
    redis_url: str
    state_prefix: str
    notifier_state_key: str
    silence_prefix: str
    ops_stream: str
    notify_stream: str
    # P4.14: separate page-level stream for critical/page route class events
    notify_page_stream: str
    notify_enable: bool
    reminder_s: int
    state_ttl_s: int
    # P4.14: warning-code CSV lists for severity-policy / route-class determination
    warn_codes_warn_csv: str = ''
    warn_codes_crit_csv: str = ''
    warn_codes_page_csv: str = ''


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
        state_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_STATE_PREFIX', 'metrics:latency_contract:deploy_lint:last'),
        notifier_state_key=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFIER_STATE_KEY', 'metrics:latency_contract:deploy_lint:notifier:last'),
        silence_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_PREFIX', 'cfg:orderflow:latency_contract:deploy_lint:silence'),
        ops_stream=_env('LATENCY_CONTRACT_DEPLOY_LINT_OPS_EVENT_STREAM', 'ops:latency_contract:events:v1'),
        notify_stream=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_STREAM', os.getenv('NOTIFY_TELEGRAM_STREAM') or 'notify:telegram'),
        # P4.14: page stream routes operational class changes requiring pager attention
        notify_page_stream=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_PAGE_STREAM', os.getenv('NOTIFY_TELEGRAM_PAGE_STREAM') or 'notify:telegram:page'),
        notify_enable=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_ENABLE', '1').lower() in ('1', 'true', 'yes', 'on'),
        reminder_s=max(60, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_REMINDER_S', '21600'), 21600)),
        state_ttl_s=max(600, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFIER_STATE_TTL_S', '172800'), 172800)),
        # P4.14: warning code policy CSVs for route-aware severity mapping
        warn_codes_warn_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_WARN_CSV', ''),
        warn_codes_crit_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_CRIT_CSV', ''),
        warn_codes_page_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_PAGE_CSV', ''),
    )


def _active_state_payload(r: Any, prefix: str) -> tuple[list[str], dict[str, dict[str, str]]]:
    active: list[str] = []
    details: dict[str, dict[str, str]] = {}
    for purpose in sorted(CONTRACTS.keys()):
        raw = r.hgetall(lint_state_key(prefix, purpose)) or {}
        details[purpose] = raw
        if str(raw.get('gate_active', '0')) == '1':
            active.append(purpose)
    return active, details


def _partition_active_by_silence(r: Any, *, prefix: str, active: list[str], now_ms: int) -> tuple[list[str], list[str], dict[str, dict[str, str]]]:
    silenced, silence_details = list_silenced_purposes(r, prefix=prefix, purposes=active, now_ms=now_ms)
    effective_active = [p for p in active if p not in set(silenced)]
    return effective_active, silenced, silence_details


def _split_codes(raw: Any) -> set[str]:
    """Split a comma-separated warning_codes field into individual code tokens.""",
    return {x.strip() for x in str(raw or '').split(',') if x.strip() and x.strip() != 'none'}


def _warning_policy_for_active(cfg: Cfg, details: dict[str, dict[str, str]], active: list[str]) -> str:
    """Compute the highest-priority warning severity policy across all active purposes.

    Priority: page > crit > warn.  Returns 'none' when no codes are present.
    """,
    codes: set[str] = set()
    for purpose in active:
        codes |= _split_codes((details.get(purpose) or {}).get('warning_codes'))
    if not codes:
        return 'none'
    if codes & _split_codes(cfg.warn_codes_page_csv):
        return 'page'
    if codes & _split_codes(cfg.warn_codes_crit_csv):
        return 'crit'
    if codes & _split_codes(cfg.warn_codes_warn_csv):
        return 'warn'
    return 'warn'


def _route_class_for_event(event_kind: str, warning_policy: str) -> str:
    """Map event kind and warning policy to a notifier route class.

    TTL-expired reactivation events always page regardless of policy.
    """,
    if event_kind == 'latency_deploy_lint_silence_ttl_expired_reactivated':
        return 'page'
    return 'page' if warning_policy == 'page' else 'notify'


def _summary_text(active: list[str], details: dict[str, dict[str, str]], *, silenced: list[str] | None = None, silence_details: dict[str, dict[str, str]] | None = None) -> str:
    silenced = list(silenced or [])
    silence_details = dict(silence_details or {})
    if not active:
        return 'Latency deploy-lint persistent drift recovered: no active config-drift gates.'
    parts: list[str] = []
    for purpose in active:
        raw = details.get(purpose) or {}
        parts.append(f"{purpose}: {str(raw.get('error_codes') or 'unknown')[:240]}")
    if silenced:
        parts.append('silenced on notifier:')
        for purpose in silenced:
            st = parse_silence_state(silence_details.get(purpose) or {})
            meta = [x for x in [st.ack_operator, st.ack_ticket] if x]
            # P4.9: surface escalation ticket when silence uses override
            if st.policy_current_override_active and st.policy_current_override_ticket:
                meta.append(f"esc={st.policy_current_override_ticket}")
            # P4.10: surface dual-control approval status in notifier message
            if st.dual_control_required and st.dual_control_request_id:
                meta.append(f"dc={st.dual_control_request_id[:8]}:{st.dual_control_approved_by or 'pending'}")
            parts.append(f"- {purpose}{(' (' + '/'.join(meta) + ')') if meta else ''}")
    return 'Latency deploy-lint persistent config drift active\n' + '\n'.join(parts)


def _emit_ops_event(
    r: Any,
    cfg: Cfg,
    *,
    event_kind: str,
    warning_policy: str,
    route_class: str,
    raw_active: list[str],
    active: list[str],
    silenced: list[str],
    details: dict[str, dict[str, str]],
    now_ms: int,
) -> None:
    fields = {
        'ts_ms': str(now_ms),
        'kind': event_kind,
        # P4.14: operational class fields for binding visibility
        'warning_severity_policy': warning_policy,
        'notifier_route_class': route_class,
        'source': 'latency_contract_deploy_lint_notifier_v1',
        'raw_active_purposes_csv': ','.join(raw_active) if raw_active else 'none',
        'raw_active_purposes_count': str(len(raw_active)),
        'active_purposes_csv': ','.join(active) if active else 'none',
        'active_purposes_count': str(len(active)),
        'active_hash': purposes_hash(active),
        'silenced_purposes_csv': ','.join(silenced) if silenced else 'none',
        'silenced_purposes_count': str(len(silenced)),
        'details_json': json.dumps({p: details.get(p) or {} for p in active}, sort_keys=True),
    }
    r.xadd(cfg.ops_stream, fields, maxlen=200000, approximate=True)


def _severity_for_event(event_kind: str, warning_policy: str) -> str:
    """Determine notification severity from event kind and warning policy.""",
    if event_kind == 'latency_deploy_lint_recovered':
        return 'info'
    if event_kind == 'latency_deploy_lint_silence_ttl_expired_reactivated':
        return 'page'
    if warning_policy == 'page':
        return 'page'
    return 'crit'


def _notify_stream_for_event(cfg: Cfg, event_kind: str, warning_policy: str) -> str:
    """Select the correct notify stream based on route class.""",
    return cfg.notify_page_stream if _route_class_for_event(event_kind, warning_policy) == 'page' else cfg.notify_stream


def _emit_notify(r: Any, cfg: Cfg, *, event_kind: str, warning_policy: str, text: str, now_ms: int) -> None:
    if not cfg.notify_enable:
        return
    severity = _severity_for_event(event_kind, warning_policy)
    route_class = _route_class_for_event(event_kind, warning_policy)
    r.xadd(_notify_stream_for_event(cfg, event_kind, warning_policy), {
        'type': 'report',
        'severity': severity,
        # P4.14: operational class fields propagated to notify payload
        'warning_severity_policy': warning_policy,
        'notifier_route_class': route_class,
        'text': text[:3500],
        'ts': str(now_ms),
        'source': 'latency_contract_deploy_lint_notifier_v1',
    }, maxlen=50000, approximate=True)


def _should_emit(*, prev_status: str, prev_hash: str, current_status: str, current_hash: str, raw_active: list[str], last_emit_ts_ms: int, reminder_s: int, now_ms: int) -> tuple[bool, str]:
    if current_status == 'active':
        if current_status != prev_status or current_hash != prev_hash:
            return True, 'latency_deploy_lint_persistent_drift'
        if raw_active and (now_ms - last_emit_ts_ms) >= reminder_s * 1000:
            return True, 'latency_deploy_lint_persistent_drift'
        return False, 'noop'
    if current_status == 'ok' and prev_status == 'active':
        return True, 'latency_deploy_lint_recovered'
    return False, 'noop'


def main() -> int:
    cfg = load_cfg()
    import redis  # type: ignore
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    now_ms = get_ny_time_millis()
    raw_active, details = _active_state_payload(r, cfg.state_prefix)
    active, silenced, silence_details = _partition_active_by_silence(r, prefix=cfg.silence_prefix, active=raw_active, now_ms=now_ms)
    prev = r.hgetall(notify_state_key(cfg.notifier_state_key)) or {}
    prev_hash = str(prev.get('active_hash') or '')
    prev_status = str(prev.get('last_status') or 'unknown')
    last_emit_ts_ms = _i(prev.get('last_emit_ts_ms'), 0)
    current_hash = purposes_hash(active)
    current_status = 'active' if active else ('silenced' if raw_active else 'ok')
    emit, event_kind = _should_emit(prev_status=prev_status, prev_hash=prev_hash, current_status=current_status, current_hash=current_hash, raw_active=raw_active, last_emit_ts_ms=last_emit_ts_ms, reminder_s=cfg.reminder_s, now_ms=now_ms)
    if emit:
        text = _summary_text(active, details, silenced=silenced, silence_details=silence_details)
        # P4.14: compute warning policy and route class for this emit
        warning_policy = _warning_policy_for_active(cfg, details, active)
        route_class = _route_class_for_event(event_kind, warning_policy)
        _emit_ops_event(r, cfg, event_kind=event_kind, warning_policy=warning_policy, route_class=route_class, raw_active=raw_active, active=active, silenced=silenced, details=details, now_ms=now_ms)
        _emit_notify(r, cfg, event_kind=event_kind, warning_policy=warning_policy, text=text, now_ms=now_ms)
    update_notifier_state(r, prefix=cfg.notifier_state_key, active_purposes=active, raw_active_purposes=raw_active, silenced_purposes=silenced, effective_active_purposes=active, emitted=emit, event_kind=event_kind, ttl_s=cfg.state_ttl_s, now_ms=now_ms)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
