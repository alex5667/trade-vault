from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Redis-backed ack/silence helpers for latency deploy-lint notifier control.

P4.7 adds a notifier-level silence workflow so persistent deploy-lint drift can
be acknowledged temporarily without disabling the underlying rollout gate.

P4.8 extended that workflow with explicit TTL-expiry bookkeeping. When a silence
window expires and the gate is still active, notifier code can mark the silence
as expired, emit an audit event, and re-notify with escalated severity.

P4.9 adds operator policy controls so the same purpose cannot be re-acked
indefinitely without explicit escalation accountability. The policy is enforced
at the notifier silence layer only; rollout gate enforcement remains unchanged.

P4.10 adds dual-control exception approval for long override windows: once a
policy override is required and the requested silence duration is large enough
a separate second-operator approval request is required before the final ack can
commit the notifier silence.
"""

from dataclasses import dataclass
from typing import Any
import time


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _s(v: Any, d: str = '') -> str:
    s = str(v if v is not None else '').strip()
    return s or d


@dataclass(frozen=True)
class DeployLintSilenceState:
    purpose: str
    present: bool
    silence_active: bool
    silence_until_ts_ms: int
    remaining_s: int
    ack_ts_ms: int
    ack_operator: str
    ack_ticket: str
    ack_reason: str
    unsilence_ts_ms: int
    unsilence_operator: str
    unsilence_ticket: str
    unsilence_reason: str
    ttl_expired: bool
    ttl_expired_ts_ms: int
    ttl_expiry_notify_count: int
    ttl_expiry_last_notify_ts_ms: int
    last_action: str
    last_action_ts_ms: int
    # P4.9 policy fields
    policy_window_start_ts_ms: int
    policy_window_end_ts_ms: int
    policy_window_ack_count: int
    policy_window_budget_minutes_used: int
    policy_limit_hit_total: int
    policy_denied_total: int
    policy_last_limit_kind: str
    policy_last_limit_ts_ms: int
    policy_last_deny_ts_ms: int
    policy_last_deny_reason: str
    policy_current_override_active: bool
    policy_current_override_ticket: str
    policy_current_override_operator: str
    policy_current_override_ts_ms: int
    policy_last_override_ticket: str
    policy_last_override_operator: str
    policy_last_override_ts_ms: int
    # P4.10 dual-control fields
    dual_control_required: bool
    dual_control_request_id: str
    dual_control_prepared_by: str
    dual_control_approved_by: str
    dual_control_approved_ts_ms: int
    dual_control_denied_total: int
    dual_control_last_deny_reason: str


@dataclass(frozen=True)
class AckPolicyDecision:
    """Result of evaluating the rolling silence policy for a given ack request."""
    allowed: bool
    requires_escalation: bool
    override_active: bool
    limit_kind: str
    denied_reason: str
    window_start_ts_ms: int
    window_end_ts_ms: int
    next_window_ack_count: int
    next_window_budget_minutes_used: int
    next_limit_hit_total: int
    next_denied_total: int
    escalation_ticket: str


def state_key(prefix: str, purpose: str) -> str:
    return f"{prefix.rstrip(':')}:{purpose}"


def parse_silence_state(raw: dict[str, str] | None, *, now_ms: int | None = None) -> DeployLintSilenceState:
    raw = dict(raw or {})
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    until_ms = _i(raw.get('silence_until_ts_ms'), 0)
    active = str(raw.get('silence_active', '0')) == '1' and until_ms > now_ms
    remaining_s = int(max(0, (until_ms - now_ms) / 1000.0)) if active else 0
    ttl_expired_ts_ms = _i(raw.get('ttl_expired_ts_ms'), 0)
    ttl_expired = ttl_expired_ts_ms > 0
    last_action = _s(raw.get('last_action'))
    return DeployLintSilenceState(
        purpose=_s(raw.get('purpose'))
        present=bool(raw)
        silence_active=active
        silence_until_ts_ms=until_ms
        remaining_s=remaining_s
        ack_ts_ms=_i(raw.get('ack_ts_ms'), 0)
        ack_operator=_s(raw.get('ack_operator'))
        ack_ticket=_s(raw.get('ack_ticket'))
        ack_reason=_s(raw.get('ack_reason'))
        unsilence_ts_ms=_i(raw.get('unsilence_ts_ms'), 0)
        unsilence_operator=_s(raw.get('unsilence_operator'))
        unsilence_ticket=_s(raw.get('unsilence_ticket'))
        unsilence_reason=_s(raw.get('unsilence_reason'))
        ttl_expired=ttl_expired
        ttl_expired_ts_ms=ttl_expired_ts_ms
        ttl_expiry_notify_count=_i(raw.get('ttl_expiry_notify_count'), 0)
        ttl_expiry_last_notify_ts_ms=_i(raw.get('ttl_expiry_last_notify_ts_ms'), 0)
        last_action=last_action
        last_action_ts_ms=_i(raw.get('last_action_ts_ms'), 0)
        # P4.9 policy fields
        policy_window_start_ts_ms=_i(raw.get('policy_window_start_ts_ms'), 0)
        policy_window_end_ts_ms=_i(raw.get('policy_window_end_ts_ms'), 0)
        policy_window_ack_count=_i(raw.get('policy_window_ack_count'), 0)
        policy_window_budget_minutes_used=_i(raw.get('policy_window_budget_minutes_used'), 0)
        policy_limit_hit_total=_i(raw.get('policy_limit_hit_total'), 0)
        policy_denied_total=_i(raw.get('policy_denied_total'), 0)
        policy_last_limit_kind=_s(raw.get('policy_last_limit_kind'))
        policy_last_limit_ts_ms=_i(raw.get('policy_last_limit_ts_ms'), 0)
        policy_last_deny_ts_ms=_i(raw.get('policy_last_deny_ts_ms'), 0)
        policy_last_deny_reason=_s(raw.get('policy_last_deny_reason'))
        policy_current_override_active=str(raw.get('policy_current_override_active', '0')) == '1'
        policy_current_override_ticket=_s(raw.get('policy_current_override_ticket'))
        policy_current_override_operator=_s(raw.get('policy_current_override_operator'))
        policy_current_override_ts_ms=_i(raw.get('policy_current_override_ts_ms'), 0)
        policy_last_override_ticket=_s(raw.get('policy_last_override_ticket'))
        policy_last_override_operator=_s(raw.get('policy_last_override_operator'))
        policy_last_override_ts_ms=_i(raw.get('policy_last_override_ts_ms'), 0)
        # P4.10 dual-control fields
        dual_control_required=str(raw.get('dual_control_required', '0')) == '1'
        dual_control_request_id=_s(raw.get('dual_control_request_id'))
        dual_control_prepared_by=_s(raw.get('dual_control_prepared_by'))
        dual_control_approved_by=_s(raw.get('dual_control_approved_by'))
        dual_control_approved_ts_ms=_i(raw.get('dual_control_approved_ts_ms'), 0)
        dual_control_denied_total=_i(raw.get('dual_control_denied_total'), 0)
        dual_control_last_deny_reason=_s(raw.get('dual_control_last_deny_reason'))
    )


def list_silenced_purposes(r: Any, *, prefix: str, purposes: list[str] | tuple[str, ...], now_ms: int | None = None) -> tuple[list[str], dict[str, dict[str, str]]]:
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    active: list[str] = []
    details: dict[str, dict[str, str]] = {}
    for purpose in sorted({str(x).strip() for x in purposes if str(x).strip()}):
        raw = r.hgetall(state_key(prefix, purpose)) or {}
        details[purpose] = raw
        if parse_silence_state(raw, now_ms=now_ms).silence_active:
            active.append(purpose)
    return active, details


def _xadd_best_effort(r: Any, stream: str | None, fields: dict[str, str]) -> str:
    if not stream:
        return ''
    try:
        return str(r.xadd(stream, fields, maxlen=200000, approximate=True) or '')
    except Exception:
        return ''


def _policy_window(prev: dict[str, str], *, now_ms: int, policy_window_s: int) -> tuple[int, int, int, int, bool]:
    """Return (start_ts_ms, end_ts_ms, ack_count, budget_used, is_new_window)."""
    prev_start_ts_ms = _i(prev.get('policy_window_start_ts_ms'), 0)
    prev_end_ts_ms = _i(prev.get('policy_window_end_ts_ms'), 0)
    if policy_window_s <= 0:
        policy_window_s = 1
    # Start a new window if no prior window or current time has passed the end
    if prev_start_ts_ms <= 0 or prev_end_ts_ms <= 0 or now_ms >= prev_end_ts_ms:
        start_ts_ms = now_ms
        end_ts_ms = now_ms + int(policy_window_s) * 1000
        return start_ts_ms, end_ts_ms, 0, 0, True
    return (
        prev_start_ts_ms
        prev_end_ts_ms
        _i(prev.get('policy_window_ack_count'), 0)
        _i(prev.get('policy_window_budget_minutes_used'), 0)
        False
    )


def evaluate_ack_policy(
    raw: dict[str, str] | None
    *
    now_ms: int
    silence_minutes: int
    policy_window_s: int
    max_budget_minutes: int
    max_acks: int
    ticket: str
    escalation_ticket: str = ''
) -> AckPolicyDecision:
    """Evaluate whether a new ack/silence is allowed under the current rolling policy.

    Returns an AckPolicyDecision. If allowed is False the caller must not apply
    the silence and should record a deny event. Gate enforcement is not affected.
    """
    prev = dict(raw or {})
    silence_minutes = max(1, int(silence_minutes))
    max_budget_minutes = max(1, int(max_budget_minutes))
    max_acks = max(1, int(max_acks))
    escalation_ticket = _s(escalation_ticket)
    start_ts_ms, end_ts_ms, ack_count, budget_used, _ = _policy_window(prev, now_ms=now_ms, policy_window_s=policy_window_s)
    next_ack_count = ack_count + 1
    next_budget_used = budget_used + silence_minutes
    limit_parts: list[str] = []
    if next_budget_used > max_budget_minutes:
        limit_parts.append('budget')
    if next_ack_count > max_acks:
        limit_parts.append('ack_limit')
    requires_escalation = bool(limit_parts)
    limit_kind = '+'.join(limit_parts) if limit_parts else 'none'
    denied_reason = ''
    override_active = False
    if requires_escalation:
        if not escalation_ticket:
            denied_reason = 'escalation_ticket_required'
        elif escalation_ticket == _s(ticket):
            denied_reason = 'escalation_ticket_must_differ'
        elif escalation_ticket == _s(prev.get('policy_last_override_ticket')) and _i(prev.get('policy_window_end_ts_ms'), 0) > now_ms:
            # Reuse of the same escalation ticket within the same policy window is not allowed
            denied_reason = 'escalation_ticket_reused'
        else:
            override_active = True
    allowed = not denied_reason
    return AckPolicyDecision(
        allowed=allowed
        requires_escalation=requires_escalation
        override_active=override_active
        limit_kind=limit_kind
        denied_reason=denied_reason
        window_start_ts_ms=start_ts_ms
        window_end_ts_ms=end_ts_ms
        next_window_ack_count=next_ack_count if allowed else ack_count
        next_window_budget_minutes_used=next_budget_used if allowed else budget_used
        next_limit_hit_total=_i(prev.get('policy_limit_hit_total'), 0) + (1 if requires_escalation else 0)
        next_denied_total=_i(prev.get('policy_denied_total'), 0) + (1 if denied_reason else 0)
        escalation_ticket=escalation_ticket
    )


def has_silence_ttl_expired(raw: dict[str, str] | None, *, now_ms: int | None = None) -> bool:
    raw = dict(raw or {})
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    if str(raw.get('silence_active', '0')) != '1':
        return False
    until_ms = _i(raw.get('silence_until_ts_ms'), 0)
    return until_ms > 0 and now_ms >= until_ms



def _base_mapping(prev: dict[str, str], *, purpose: str) -> dict[str, str]:
    """Base state mapping carrying forward all persistent fields from prev.

    Introduced in P4.10 to avoid the copy-paste anti-pattern across
    upsert_ack_silence, clear_ack_silence, and mark_silence_ttl_expired.
    Each caller then overlays only the fields it changes.
    """
    return {
        'schema_version': '3'
        'purpose': str(purpose)
        'silence_active': _s(prev.get('silence_active'), '0')
        'silence_until_ts_ms': str(_i(prev.get('silence_until_ts_ms'), 0))
        'silence_duration_s': str(_i(prev.get('silence_duration_s'), 0))
        'ack_ts_ms': str(_i(prev.get('ack_ts_ms'), 0))
        'ack_operator': _s(prev.get('ack_operator'), '')
        'ack_ticket': _s(prev.get('ack_ticket'), '')
        'ack_reason': _s(prev.get('ack_reason'), '')
        'ack_count': str(_i(prev.get('ack_count'), 0))
        'gate_active_at_ack': _s(prev.get('gate_active_at_ack'), '0')
        'unsilence_ts_ms': str(_i(prev.get('unsilence_ts_ms'), 0))
        'unsilence_operator': _s(prev.get('unsilence_operator'), '')
        'unsilence_ticket': _s(prev.get('unsilence_ticket'), '')
        'unsilence_reason': _s(prev.get('unsilence_reason'), '')
        'ttl_expired_ts_ms': str(_i(prev.get('ttl_expired_ts_ms'), 0))
        'ttl_expiry_notify_count': str(_i(prev.get('ttl_expiry_notify_count'), 0))
        'ttl_expiry_last_notify_ts_ms': str(_i(prev.get('ttl_expiry_last_notify_ts_ms'), 0))
        'policy_window_start_ts_ms': str(_i(prev.get('policy_window_start_ts_ms'), 0))
        'policy_window_end_ts_ms': str(_i(prev.get('policy_window_end_ts_ms'), 0))
        'policy_window_ack_count': str(_i(prev.get('policy_window_ack_count'), 0))
        'policy_window_budget_minutes_used': str(_i(prev.get('policy_window_budget_minutes_used'), 0))
        'policy_limit_hit_total': str(_i(prev.get('policy_limit_hit_total'), 0))
        'policy_denied_total': str(_i(prev.get('policy_denied_total'), 0))
        'policy_last_limit_kind': _s(prev.get('policy_last_limit_kind'), '')
        'policy_last_limit_ts_ms': str(_i(prev.get('policy_last_limit_ts_ms'), 0))
        'policy_last_deny_ts_ms': str(_i(prev.get('policy_last_deny_ts_ms'), 0))
        'policy_last_deny_reason': _s(prev.get('policy_last_deny_reason'), '')
        'policy_current_override_active': _s(prev.get('policy_current_override_active'), '0')
        'policy_current_override_ticket': _s(prev.get('policy_current_override_ticket'), '')
        'policy_current_override_operator': _s(prev.get('policy_current_override_operator'), '')
        'policy_current_override_ts_ms': str(_i(prev.get('policy_current_override_ts_ms'), 0))
        'policy_last_override_ticket': _s(prev.get('policy_last_override_ticket'), '')
        'policy_last_override_operator': _s(prev.get('policy_last_override_operator'), '')
        'policy_last_override_ts_ms': str(_i(prev.get('policy_last_override_ts_ms'), 0))
        # P4.10 dual-control fields (carry forward unless caller overrides)
        'dual_control_required': _s(prev.get('dual_control_required'), '0')
        'dual_control_request_id': _s(prev.get('dual_control_request_id'), '')
        'dual_control_prepared_by': _s(prev.get('dual_control_prepared_by'), '')
        'dual_control_approved_by': _s(prev.get('dual_control_approved_by'), '')
        'dual_control_approved_ts_ms': str(_i(prev.get('dual_control_approved_ts_ms'), 0))
        'dual_control_denied_total': str(_i(prev.get('dual_control_denied_total'), 0))
        'dual_control_last_deny_reason': _s(prev.get('dual_control_last_deny_reason'), '')
    }


def record_dual_control_denial(
    r: Any
    *
    prefix: str
    purpose: str
    operator: str
    ticket: str
    escalation_ticket: str
    reason: str
    silence_minutes: int
    deny_reason: str
    ttl_s: int
    ops_stream: str | None = None
    now_ms: int | None = None
) -> dict[str, str]:
    """Record a dual-control gate denial without activating silence.

    Increments dual_control_denied_total and emits an audit event.
    Gate enforcement is NOT changed; only the silence layer is tracked.
    """
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    skey = state_key(prefix, purpose)
    prev = r.hgetall(skey) or {}
    mapping = _base_mapping(prev, purpose=purpose)
    mapping.update({
        'dual_control_required': '1'
        'dual_control_denied_total': str(_i(prev.get('dual_control_denied_total'), 0) + 1)
        'dual_control_last_deny_reason': str(deny_reason)
        'last_action': 'ack_denied_dual_control'
        'last_action_ts_ms': str(now_ms)
        'last_action_operator': str(operator)
        'last_action_ticket': str(ticket)
        'last_action_reason': str(deny_reason)
    })
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms)
        'kind': 'latency_deploy_lint_ack_silence_dual_control_denied'
        'purpose': str(purpose)
        'operator': str(operator)
        'ticket': str(ticket)
        'escalation_ticket': str(escalation_ticket)
        'reason': str(reason)
        'requested_silence_duration_s': str(max(1, int(silence_minutes)) * 60)
        'dual_control_denied_reason': str(deny_reason)
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping


def upsert_ack_silence(
    r: Any
    *
    prefix: str
    purpose: str
    operator: str
    ticket: str
    reason: str
    silence_minutes: int
    ttl_s: int
    ops_stream: str | None = None
    gate_active: bool | None = None
    now_ms: int | None = None
    policy_window_s: int = 168 * 3600
    policy_max_budget_minutes: int = 1440
    policy_max_acks: int = 3
    escalation_ticket: str = ''
    dual_control_required: bool = False
    dual_control_request_id: str = ''
    dual_control_prepared_by: str = ''
    dual_control_approved_by: str = ''
    dual_control_approved_ts_ms: int = 0
) -> dict[str, str]:
    """Apply or renew a notifier-level silence for the given purpose.

    P4.9: evaluates rolling policy before accepting the ack. If the policy
    limits are exceeded and no valid escalation_ticket is provided, the ack is
    denied — but gate enforcement is NOT affected; only notifier suppression is.
    """
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    silence_minutes = max(1, int(silence_minutes))
    until_ms = now_ms + silence_minutes * 60 * 1000
    skey = state_key(prefix, purpose)
    prev = r.hgetall(skey) or {}
    # P4.9: evaluate rolling policy before accepting
    policy = evaluate_ack_policy(
        prev
        now_ms=now_ms
        silence_minutes=silence_minutes
        policy_window_s=policy_window_s
        max_budget_minutes=policy_max_budget_minutes
        max_acks=policy_max_acks
        ticket=ticket
        escalation_ticket=escalation_ticket
    )
    if not policy.allowed:
        # Deny path: record the denial without activating silence (uses _base_mapping for DRY)
        mapping = _base_mapping(prev, purpose=purpose)
        mapping.update({
            'policy_window_start_ts_ms': str(policy.window_start_ts_ms)
            'policy_window_end_ts_ms': str(policy.window_end_ts_ms)
            'policy_window_ack_count': str(_i(prev.get('policy_window_ack_count'), 0) if _i(prev.get('policy_window_end_ts_ms'), 0) > now_ms else 0)
            'policy_window_budget_minutes_used': str(_i(prev.get('policy_window_budget_minutes_used'), 0) if _i(prev.get('policy_window_end_ts_ms'), 0) > now_ms else 0)
            'policy_limit_hit_total': str(policy.next_limit_hit_total)
            'policy_denied_total': str(policy.next_denied_total)
            'policy_last_limit_kind': str(policy.limit_kind)
            'policy_last_limit_ts_ms': str(now_ms if policy.requires_escalation else _i(prev.get('policy_last_limit_ts_ms'), 0))
            'policy_last_deny_ts_ms': str(now_ms)
            'policy_last_deny_reason': str(policy.denied_reason)
            'policy_current_override_active': '0'
            'policy_current_override_ticket': ''
            'policy_current_override_operator': ''
            'policy_current_override_ts_ms': '0'
            'last_action': 'ack_denied_policy'
            'last_action_ts_ms': str(now_ms)
            'last_action_operator': str(operator)
            'last_action_ticket': str(ticket)
            'last_action_reason': str(policy.denied_reason)
        })
        r.hset(skey, mapping=mapping)
        try:
            r.expire(skey, max(int(ttl_s), 1))
        except Exception:
            pass
        event_id = _xadd_best_effort(r, ops_stream, {
            'ts_ms': str(now_ms)
            'kind': 'latency_deploy_lint_ack_silence_policy_denied'
            'purpose': str(purpose)
            'operator': str(operator)
            'ticket': str(ticket)
            'reason': str(reason)
            'policy_limit_kind': str(policy.limit_kind)
            'policy_denied_reason': str(policy.denied_reason)
            'requested_silence_duration_s': str(silence_minutes * 60)
            'policy_window_ack_count': str(_i(mapping.get('policy_window_ack_count'), 0))
            'policy_window_budget_minutes_used': str(_i(mapping.get('policy_window_budget_minutes_used'), 0))
        })
        if event_id:
            r.hset(skey, mapping={'last_event_id': event_id})
        return r.hgetall(skey) or mapping

    # P4.10: use _base_mapping for DRY; overlay with silence-specific fields
    mapping = _base_mapping(prev, purpose=purpose)
    mapping.update({
        'silence_active': '1'
        'silence_until_ts_ms': str(until_ms)
        'silence_duration_s': str(silence_minutes * 60)
        'ack_ts_ms': str(now_ms)
        'ack_operator': str(operator)
        'ack_ticket': str(ticket)
        'ack_reason': str(reason)
        'ack_count': str(_i(prev.get('ack_count'), 0) + 1)
        'gate_active_at_ack': '1' if gate_active else '0'
        'ttl_expired_ts_ms': '0'
        'policy_window_start_ts_ms': str(policy.window_start_ts_ms)
        'policy_window_end_ts_ms': str(policy.window_end_ts_ms)
        'policy_window_ack_count': str(policy.next_window_ack_count)
        'policy_window_budget_minutes_used': str(policy.next_window_budget_minutes_used)
        'policy_limit_hit_total': str(policy.next_limit_hit_total)
        'policy_denied_total': str(policy.next_denied_total)
        'policy_last_limit_kind': str(policy.limit_kind if policy.requires_escalation else _s(prev.get('policy_last_limit_kind'), 'none'))
        'policy_last_limit_ts_ms': str(now_ms if policy.requires_escalation else _i(prev.get('policy_last_limit_ts_ms'), 0))
        'policy_current_override_active': '1' if policy.override_active else '0'
        'policy_current_override_ticket': str(policy.escalation_ticket if policy.override_active else '')
        'policy_current_override_operator': str(operator if policy.override_active else '')
        'policy_current_override_ts_ms': str(now_ms if policy.override_active else 0)
        'policy_last_override_ticket': str(policy.escalation_ticket if policy.override_active else _s(prev.get('policy_last_override_ticket'), ''))
        'policy_last_override_operator': str(operator if policy.override_active else _s(prev.get('policy_last_override_operator'), ''))
        'policy_last_override_ts_ms': str(now_ms if policy.override_active else _i(prev.get('policy_last_override_ts_ms'), 0))
        # P4.10 dual-control: record approval metadata if a dual-control request was used
        'dual_control_required': '1' if dual_control_required else '0'
        'dual_control_request_id': str(dual_control_request_id if dual_control_required else '')
        'dual_control_prepared_by': str(dual_control_prepared_by if dual_control_required else '')
        'dual_control_approved_by': str(dual_control_approved_by if dual_control_required else '')
        'dual_control_approved_ts_ms': str(int(dual_control_approved_ts_ms) if dual_control_required else 0)
        'dual_control_last_deny_reason': ''
        'last_action': 'ack_silence'
        'last_action_ts_ms': str(now_ms)
        'last_action_operator': str(operator)
        'last_action_ticket': str(ticket)
        'last_action_reason': str(reason)
    })
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(int(ttl_s), silence_minutes * 60, 1))
    except Exception:
        pass
    # Emit appropriate audit event: override or normal ack
    event_kind = 'latency_deploy_lint_ack_silence_policy_override' if policy.override_active else 'latency_deploy_lint_ack_silence_set'
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms)
        'kind': event_kind
        'purpose': str(purpose)
        'operator': str(operator)
        'ticket': str(ticket)
        'reason': str(reason)
        'silence_until_ts_ms': str(until_ms)
        'silence_duration_s': str(silence_minutes * 60)
        'gate_active': '1' if gate_active else '0'
        'policy_limit_kind': str(policy.limit_kind)
        'policy_override_active': '1' if policy.override_active else '0'
        'policy_window_ack_count': str(policy.next_window_ack_count)
        'policy_window_budget_minutes_used': str(policy.next_window_budget_minutes_used)
        'escalation_ticket': str(policy.escalation_ticket if policy.override_active else '')
        # P4.10 dual-control audit fields
        'dual_control_required': '1' if dual_control_required else '0'
        'dual_control_request_id': str(dual_control_request_id if dual_control_required else '')
        'dual_control_prepared_by': str(dual_control_prepared_by if dual_control_required else '')
        'dual_control_approved_by': str(dual_control_approved_by if dual_control_required else '')
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping


def clear_ack_silence(r: Any, *, prefix: str, purpose: str, operator: str, ticket: str, reason: str, ttl_s: int, ops_stream: str | None = None, now_ms: int | None = None) -> dict[str, str]:
    """Manually unsilence a purpose, preserving ack context and policy state for audit."""
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    skey = state_key(prefix, purpose)
    prev = r.hgetall(skey) or {}
    # P4.10: use _base_mapping; policy window persists across unsilence
    mapping = _base_mapping(prev, purpose=purpose)
    mapping.update({
        'silence_active': '0'
        'silence_until_ts_ms': '0'
        'unsilence_ts_ms': str(now_ms)
        'unsilence_operator': str(operator)
        'unsilence_ticket': str(ticket)
        'unsilence_reason': str(reason)
        'policy_current_override_active': '0'
        'policy_current_override_ticket': ''
        'policy_current_override_operator': ''
        'policy_current_override_ts_ms': '0'
        'last_action': 'unsilence'
        'last_action_ts_ms': str(now_ms)
        'last_action_operator': str(operator)
        'last_action_ticket': str(ticket)
        'last_action_reason': str(reason)
    })
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms)
        'kind': 'latency_deploy_lint_ack_silence_cleared'
        'purpose': str(purpose)
        'operator': str(operator)
        'ticket': str(ticket)
        'reason': str(reason)
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping


def mark_silence_ttl_expired(r: Any, *, prefix: str, purpose: str, ttl_s: int, ops_stream: str | None = None, now_ms: int | None = None) -> dict[str, str]:
    """Mark a silence window as naturally expired (TTL elapsed without manual clear).

    Called by the notifier when it detects that a silence window has elapsed.
    Carries forward all policy state to preserve audit trail.
    P4.10: refactored to use _base_mapping for DRY.
    """
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    skey = state_key(prefix, purpose)
    prev = r.hgetall(skey) or {}
    until_ms = _i(prev.get('silence_until_ts_ms'), 0)
    expired_ts_ms = until_ms if until_ms > 0 else now_ms
    # P4.10: use _base_mapping; TTL expiry does not reset rolling window or dual-control state
    mapping = _base_mapping(prev, purpose=purpose)
    mapping.update({
        'silence_active': '0'
        'silence_until_ts_ms': str(until_ms)
        'ttl_expired_ts_ms': str(expired_ts_ms)
        'ttl_expiry_notify_count': str(_i(prev.get('ttl_expiry_notify_count'), 0) + 1)
        'ttl_expiry_last_notify_ts_ms': str(now_ms)
        'policy_current_override_active': '0'
        'policy_current_override_ticket': ''
        'policy_current_override_operator': ''
        'policy_current_override_ts_ms': '0'
        'last_action': 'ttl_expired'
        'last_action_ts_ms': str(now_ms)
        'last_action_operator': 'system'
        'last_action_ticket': _s(prev.get('ack_ticket'), '')
        'last_action_reason': 'silence_ttl_expired'
    })
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms)
        'kind': 'latency_deploy_lint_silence_ttl_expired'
        'purpose': str(purpose)
        'expired_ts_ms': str(expired_ts_ms)
        'ttl_expiry_notify_count': str(_i(prev.get('ttl_expiry_notify_count'), 0) + 1)
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping
