from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
from typing import Any, Dict

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from orderflow_services.strategy_research_stats_alert_policy_exporter_v1 import FAMILIES


SUPPORTED_OVERRIDE_FAMILIES = ('pbo_high', 'report_stale', 'psr_dsr_low')


class OverrideWorkflowError(RuntimeError):
    """Raised when renewal workflow invariants are violated."""


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _now_ms() -> int:
    return get_ny_time_millis()


def _redis_client() -> Any:
    if redis is None:  # pragma: no cover
        raise RuntimeError('redis package is not available')
    return redis.Redis.from_url(_env('REDIS_URL', 'redis://redis-worker-1:6379/0'), decode_responses=True)


def override_key(purpose: str, family: str) -> str:
    prefix = _env(
        'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX',
        'cfg:strategy_research_stats:alert_policy:suppress_override:v1',
    ).rstrip(':')
    return f'{prefix}:{purpose}:{family}'


def ops_stream() -> str:
    return _env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OPS_STREAM', 'ops:strategy_research_stats:alert_policy:v1')


def override_state_key(purpose: str, family: str) -> str:
    prefix = _env(
        'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_STATE_PREFIX',
        'cfg:strategy_research_stats:alert_policy:suppress_state:v1',
    ).rstrip(':')
    return f'{prefix}:{purpose}:{family}'


def default_ttl_s() -> int:
    return max(300, _to_int(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_DEFAULT_TTL_S', '86400'), 86400))


def max_ttl_s() -> int:
    return max(default_ttl_s(), _to_int(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_MAX_TTL_S', '604800'), 604800))




def require_dual_control_on_limit() -> int:
    return 1 if _to_int(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_DUAL_CONTROL_ON_LIMIT', '1'), 1) > 0 else 0


def dual_control_approved_freshness_s() -> int:
    return max(60, _to_int(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_DUAL_CONTROL_APPROVED_FRESHNESS_S', '1800'), 1800))

def override_limits_defaults_key() -> str:
    return _env(
        'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_LIMITS_DEFAULTS_KEY',
        'cfg:strategy_research_stats:alert_policy:override_limits:v1:defaults',
    )


def override_limits_key(purpose: str) -> str:
    prefix = _env(
        'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_LIMITS_PREFIX',
        'cfg:strategy_research_stats:alert_policy:override_limits:v1',
    ).rstrip(':')
    return f'{prefix}:{purpose}'


def _read_limits(client: Any, purpose: str) -> tuple[Dict[str, str], Dict[str, str]]:
    return (
        client.hgetall(override_limits_defaults_key()) or {},
        client.hgetall(override_limits_key(purpose)) or {},
    )


def resolve_override_limits(client: Any, purpose: str, family: str) -> Dict[str, int]:
    defaults_hash, purpose_hash = _read_limits(client, purpose)
    max_budget_default = max(300, _to_int(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_DEFAULT_MAX_BUDGET_S', '259200'), 259200))
    max_renew_default = max(0, _to_int(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_DEFAULT_MAX_RENEW_COUNT', '2'), 2))
    require_escalation_default = 1 if _to_int(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_REQUIRE_ESCALATION_ON_LIMIT', '1'), 1) > 0 else 0
    values = {
        'max_budget_s': max_budget_default,
        'max_renew_count': max_renew_default,
        'require_escalation': require_escalation_default,
    }
    for src in (defaults_hash, purpose_hash):
        if not src:
            continue
        for field in tuple(values.keys()):
            key = f'{field}_{family}'
            if key in src and str(src[key]).strip() not in ('', 'None'):
                values[field] = _to_int(src[key], values[field])
    values['max_budget_s'] = max(300, values['max_budget_s'])
    values['max_renew_count'] = max(0, values['max_renew_count'])
    values['require_escalation'] = 1 if values['require_escalation'] > 0 else 0
    return values


def _policy_chain_start_ms(previous: Dict[str, str] | None, fallback_now_ms: int) -> int:
    prev = previous or {}
    return max(0, _to_int(prev.get('policy_chain_started_ts_ms'), 0) or _to_int(prev.get('created_ts_ms'), 0) or fallback_now_ms)


def _policy_budget_used_s(previous: Dict[str, str] | None) -> int:
    prev = previous or {}
    return max(0, _to_int(prev.get('policy_budget_used_s'), 0) or _to_int(prev.get('ttl_s'), 0))


def _policy_limit_kind(limits: Dict[str, int], *, proposed_budget_s: int, proposed_renew_count: int) -> str:
    if proposed_budget_s > int(limits.get('max_budget_s', 0)):
        return 'budget'
    if proposed_renew_count > int(limits.get('max_renew_count', 0)):
        return 'max_renew'
    return ''


def _require_escalation_fields(state: Dict[str, str]) -> Dict[str, str]:
    esc = {
        'ticket': str(state.get('renew_escalation_ticket') or '').strip(),
        'operator': str(state.get('renew_escalation_operator') or '').strip(),
        'reason': str(state.get('renew_escalation_reason') or '').strip(),
    }
    if not esc['ticket'] or not esc['operator'] or not esc['reason']:
        raise OverrideWorkflowError('renew exceeds policy limits; acknowledge-renew must include escalation-ticket/operator/reason')
    return esc


def _ensure_new_escalation_identity(escalation_ticket: str, current: Dict[str, str], ack_ticket: str) -> None:
    previous_ticket = str(current.get('ticket') or '').strip()
    if escalation_ticket == ack_ticket:
        raise OverrideWorkflowError('escalation ticket must be distinct from renewal ticket')
    if previous_ticket and escalation_ticket == previous_ticket:
        raise OverrideWorkflowError('escalation ticket must be distinct from current/previous override ticket')




def _ensure_dual_control_approver_distinct(state: Dict[str, str], approval_operator: str, approval_ticket: str) -> None:
    renew_operator = str(state.get('renew_ack_operator') or '').strip()
    escalation_operator = str(state.get('renew_escalation_operator') or '').strip()
    renew_ticket = str(state.get('renew_ack_ticket') or '').strip()
    escalation_ticket = str(state.get('renew_escalation_ticket') or '').strip()
    if renew_operator and approval_operator == renew_operator:
        raise OverrideWorkflowError('dual-control approver must differ from renewal operator')
    if escalation_operator and approval_operator == escalation_operator:
        raise OverrideWorkflowError('dual-control approver must differ from escalation operator')
    if renew_ticket and approval_ticket == renew_ticket:
        raise OverrideWorkflowError('dual-control approval ticket must differ from renewal ticket')
    if escalation_ticket and approval_ticket == escalation_ticket:
        raise OverrideWorkflowError('dual-control approval ticket must differ from escalation ticket')

def _dual_control_approval_deadline_ts_ms(state: Dict[str, str]) -> int:
    approved_ts_ms = _to_int(state.get('dual_control_approved_ts_ms'), 0)
    freshness_s = max(60, _to_int(state.get('dual_control_approved_freshness_s'), dual_control_approved_freshness_s()))
    stored_deadline_ts_ms = _to_int(state.get('dual_control_approved_deadline_ts_ms'), 0)
    if stored_deadline_ts_ms > 0:
        return stored_deadline_ts_ms
    if approved_ts_ms <= 0:
        return 0
    return approved_ts_ms + freshness_s * 1000


def _invalidate_stale_dual_control_approval(
    client: Any,
    *,
    purpose: str,
    family: str,
    state_key: str,
    state: Dict[str, str],
    stage: str,
) -> Dict[str, str]:
    approval_state = str(state.get('dual_control_approval_state') or '')
    if approval_state != 'approved':
        return state
    approved_ts_ms = _to_int(state.get('dual_control_approved_ts_ms'), 0)
    if approved_ts_ms <= 0:
        return state
    now_ms = _now_ms()
    deadline_ts_ms = _dual_control_approval_deadline_ts_ms(state)
    if deadline_ts_ms <= 0 or deadline_ts_ms > now_ms:
        return state
    updated = dict(state)
    updated.update({
        'dual_control_required': '1',
        'dual_control_approval_state': 'invalidated',
        'dual_control_approved_deadline_ts_ms': str(deadline_ts_ms),
        'dual_control_approved_freshness_s': str(max(60, _to_int(state.get('dual_control_approved_freshness_s'), dual_control_approved_freshness_s()))),
        'dual_control_invalidated_ts_ms': str(now_ms),
        'dual_control_invalidated_reason': 'approval_freshness_expired',
        'dual_control_invalidated_stage': stage,
    })
    client.hset(state_key, mapping=updated)
    _emit_event(
        client,
        'strategy_research_stats_alert_policy_suppress_override_dual_control_invalidated',
        {
            'purpose': purpose,
            'family': family,
            'reason': 'approval_freshness_expired',
            'stage': stage,
            'approval_ticket': str(state.get('dual_control_approved_ticket') or ''),
            'approver': str(state.get('dual_control_approved_operator') or ''),
            'approved_ts_ms': approved_ts_ms,
            'deadline_ts_ms': deadline_ts_ms,
            'freshness_s': max(60, _to_int(state.get('dual_control_approved_freshness_s'), dual_control_approved_freshness_s())),
        }
    )
    return updated


def _mark_dual_control_pending(client: Any, *, purpose: str, family: str, state_key: str, state: Dict[str, str], limit_kind: str) -> Dict[str, str]:
    now_ms = _now_ms()
    updated = dict(state)
    updated.update({
        'dual_control_required': '1',
        'dual_control_approval_state': 'pending',
        'dual_control_request_ts_ms': str(now_ms),
        'dual_control_request_ticket': str(state.get('renew_escalation_ticket') or ''),
        'dual_control_request_operator': str(state.get('renew_escalation_operator') or ''),
        'dual_control_request_reason': str(state.get('renew_escalation_reason') or ''),
        'dual_control_approved_ts_ms': '0',
        'dual_control_approved_ticket': '',
        'dual_control_approved_operator': '',
        'dual_control_approved_reason': '',
        'dual_control_approved_deadline_ts_ms': '0',
        'dual_control_approved_freshness_s': str(dual_control_approved_freshness_s()),
        'dual_control_invalidated_ts_ms': '0',
        'dual_control_invalidated_reason': '',
        'dual_control_invalidated_stage': '',
        'dual_control_consumed_ts_ms': '0',
    })
    client.hset(state_key, mapping=updated)
    _emit_event(
        client,
        'strategy_research_stats_alert_policy_suppress_override_dual_control_required',
        {
            'purpose': purpose,
            'family': family,
            'limit_kind': limit_kind,
            'renew_ticket': str(state.get('renew_ack_ticket') or ''),
            'renew_operator': str(state.get('renew_ack_operator') or ''),
            'escalation_ticket': str(state.get('renew_escalation_ticket') or ''),
            'escalation_operator': str(state.get('renew_escalation_operator') or ''),
        }
    )
    return updated


def _require_dual_control_approval(client: Any, *, purpose: str, family: str, state_key: str, state: Dict[str, str], limit_kind: str) -> Dict[str, str]:
    approval_state = str(state.get('dual_control_approval_state') or '')
    if approval_state == 'approved' and str(state.get('dual_control_approved_operator') or '').strip() and str(state.get('dual_control_approved_ticket') or '').strip():
        return state
    if approval_state == 'invalidated':
        raise OverrideWorkflowError('renew exceeds policy limits; dual-control approval freshness expired and must be re-approved by a second approver')
    _mark_dual_control_pending(client, purpose=purpose, family=family, state_key=state_key, state=state, limit_kind=limit_kind)
    raise OverrideWorkflowError('renew exceeds policy limits; escalation path requires dual-control approval from a second approver')


def _record_limit_block(
    client: Any,
    *,
    purpose: str,
    family: str,
    state_key: str,
    state: Dict[str, str],
    limits: Dict[str, int],
    limit_kind: str,
    proposed_budget_s: int,
    proposed_renew_count: int,
) -> None:
    now_ms = _now_ms()
    client.hset(
        state_key,
        mapping={
            'purpose': purpose,
            'family': family,
            'policy_limit_hit_kind': limit_kind,
            'policy_limit_hit_ts_ms': str(now_ms),
            'policy_limit_requires_escalation': '1' if limits.get('require_escalation', 0) > 0 else '0',
            'policy_max_budget_s': str(limits.get('max_budget_s', 0)),
            'policy_max_renew_count': str(limits.get('max_renew_count', 0)),
            'policy_budget_used_s': str(_policy_budget_used_s(state)),
            'policy_proposed_budget_s': str(proposed_budget_s),
            'policy_proposed_renew_count': str(proposed_renew_count),
            'renew_ack_required': '1',
        }
    )
    _emit_event(
        client,
        'strategy_research_stats_alert_policy_suppress_override_limit_blocked',
        {
            'purpose': purpose,
            'family': family,
            'limit_kind': limit_kind,
            'current_ticket': str(state.get('ticket') or ''),
            'current_operator': str(state.get('operator') or ''),
            'policy_max_budget_s': limits.get('max_budget_s', 0),
            'policy_max_renew_count': limits.get('max_renew_count', 0),
            'policy_budget_used_s': _policy_budget_used_s(state),
            'policy_proposed_budget_s': proposed_budget_s,
            'policy_proposed_renew_count': proposed_renew_count,
        }
    )


def _validate_family(family: str) -> str:
    family = (family or '').strip()
    if family not in SUPPORTED_OVERRIDE_FAMILIES:
        raise SystemExit(f'unsupported family: {family!r}; expected one of {SUPPORTED_OVERRIDE_FAMILIES}')
    return family


def _validate_nonempty(name: str, value: str) -> str:
    value = (value or '').strip()
    if not value:
        raise SystemExit(f'{name} is required')
    return value


def _emit_event(client: Any, kind: str, payload: Dict[str, Any]) -> None:
    fields = {
        'ts_ms': str(_now_ms()),
        'kind': kind,
        'source': 'strategy_research_stats_alert_policy_override_v1',
    }
    for key, value in payload.items():
        if isinstance(value, (dict, list, tuple)):
            fields[key] = json.dumps(value, sort_keys=True)
        else:
            fields[key] = '' if value is None else str(value)
    client.xadd(ops_stream(), fields, maxlen=200000, approximate=True)


def _read_state(client: Any, purpose: str, family: str) -> Dict[str, str]:
    """Read the persistent lifecycle state hash for a purpose/family override."""
    return client.hgetall(override_state_key(purpose, family)) or {}


def _renewal_required(state: Dict[str, str], now_ms: int) -> bool:
    """Return True if the override lifecycle state mandates the acknowledge-renew workflow."""
    lifecycle = str(state.get('lifecycle_state') or '')
    last_reminder = _to_int(state.get('last_reminder_ts_ms'), 0)
    expired_ts = _to_int(state.get('expired_ts_ms'), 0)
    expire_ts = _to_int(state.get('expire_ts_ms'), 0)
    # Fully expired override → must use renewal workflow
    if lifecycle == 'expired' or expired_ts > 0:
        return True
    # Active override that has received at least one expiry reminder → must use renewal workflow
    if lifecycle == 'active' and last_reminder > 0:
        return True
    # Active override whose TTL has elapsed (not yet detected by exporter) → treat as expired
    if lifecycle == 'active' and expire_ts and expire_ts <= now_ms:
        return True
    return False


def _ensure_new_identity(ticket: str, operator: str, reason: str, current: Dict[str, str]) -> None:
    """Enforce that the renewal ticket/operator/reason triple is distinct from the current override.

    Prevents infinite renewal on the same identity — every renewal cycle must
    represent a genuinely new decision with a different ticket, operator, and reason.
    """
    previous_ticket = str(current.get('ticket') or '').strip()
    previous_operator = str(current.get('operator') or '').strip()
    previous_reason = str(current.get('reason') or '').strip()
    if previous_ticket and ticket == previous_ticket:
        raise OverrideWorkflowError('renew requires a new ticket distinct from the current/previous override ticket')
    if previous_operator and operator == previous_operator:
        raise OverrideWorkflowError('renew requires a new operator distinct from the current/previous override operator')
    if previous_reason and reason == previous_reason:
        raise OverrideWorkflowError('renew requires a new reason distinct from the current/previous override reason')


def _build_active_state_payload(
    *,
    purpose: str,
    family: str,
    ticket: str,
    operator: str,
    reason: str,
    now_ms: int,
    expire_ts_ms: int,
    ttl_s: int,
    previous: Dict[str, str] | None = None,
    renewal_ack: Dict[str, str] | None = None,
    limits: Dict[str, int] | None = None,
    escalation_ack: Dict[str, str] | None = None,
    dual_control_approval: Dict[str, str] | None = None,
) -> Dict[str, str]:
    """Build the full lifecycle-state hash payload for an active (or renewed) override.

    When *renewal_ack* is provided the payload increments ``renew_count`` and seeds
    ``renewed_from_*`` / ``renew_ack_consumed_*`` audit fields.  On a fresh suppress (no
    ack) these fields are reset to empty so the state is self-contained.
    """
    prev = previous or {}
    ack = renewal_ack or {}
    esc = escalation_ack or {}
    approval = dual_control_approval or {}
    policy = limits or {}
    renew_count = _to_int(prev.get('renew_count'), 0)
    is_renewal = 1 if ack else 0
    renew_count += is_renewal
    chain_started_ts_ms = _policy_chain_start_ms(prev, now_ms)
    previous_budget_s = _policy_budget_used_s(prev)
    budget_used_s = previous_budget_s + int(ttl_s)
    return {
        'purpose': purpose,
        'family': family,
        'ticket': ticket,
        'operator': operator,
        'reason': reason,
        'created_ts_ms': str(now_ms),
        'expire_ts_ms': str(expire_ts_ms),
        'ttl_s': str(ttl_s),
        'active': '1',
        'lifecycle_state': 'active',
        'cleared_ts_ms': '0',
        'expired_ts_ms': '0',
        'last_reminder_ts_ms': '0',
        'last_reminder_expire_ts_ms': '0',
        'last_reminder_kind': '',
        # Renewal acknowledgement fields — cleared after renew to prevent double-use
        'renew_ack_required': '0',
        'renew_ack_ts_ms': '0',
        'renew_ack_ticket': '',
        'renew_ack_operator': '',
        'renew_ack_reason': '',
        # Audit trail: who was renewed from
        'renewed_from_ticket': str(prev.get('ticket') or ''),
        'renewed_from_operator': str(prev.get('operator') or ''),
        'renewed_from_reason': str(prev.get('reason') or ''),
        # Renewal counters and consumed-ack audit
        'last_renew_ts_ms': str(now_ms if is_renewal else _to_int(prev.get('last_renew_ts_ms'), 0)),
        'renew_count': str(renew_count),
        'renew_ack_consumed_ts_ms': str(now_ms if is_renewal else 0),
        'renew_ack_consumed_ticket': str(ack.get('ticket') or ''),
        'renew_ack_consumed_operator': str(ack.get('operator') or ''),
        'renew_ack_consumed_reason': str(ack.get('reason') or ''),
        'renew_escalation_ticket': '',
        'renew_escalation_operator': '',
        'renew_escalation_reason': '',
        'policy_chain_started_ts_ms': str(chain_started_ts_ms),
        'policy_budget_used_s': str(budget_used_s),
        'policy_max_budget_s': str(policy.get('max_budget_s', 0)),
        'policy_max_renew_count': str(policy.get('max_renew_count', 0)),
        'policy_limit_requires_escalation': str(1 if policy.get('require_escalation', 0) > 0 else 0),
        'policy_limit_hit_kind': '',
        'policy_limit_hit_ts_ms': '0',
        'policy_last_escalation_ticket': str(esc.get('ticket') or ''),
        'policy_last_escalation_operator': str(esc.get('operator') or ''),
        'policy_last_escalation_reason': str(esc.get('reason') or ''),
        'policy_last_escalation_ts_ms': str(now_ms if esc else _to_int(prev.get('policy_last_escalation_ts_ms'), 0)),
        'policy_escalation_count': str(_to_int(prev.get('policy_escalation_count'), 0) + (1 if esc else 0)),
        'dual_control_required': str(1 if approval else 0),
        'dual_control_approval_state': 'consumed' if approval else 'none',
        'dual_control_request_ts_ms': str(_to_int(prev.get('dual_control_request_ts_ms'), 0) if approval else 0),
        'dual_control_request_ticket': str(prev.get('renew_escalation_ticket') or prev.get('dual_control_request_ticket') or '') if approval else '',
        'dual_control_request_operator': str(prev.get('renew_escalation_operator') or prev.get('dual_control_request_operator') or '') if approval else '',
        'dual_control_request_reason': str(prev.get('renew_escalation_reason') or prev.get('dual_control_request_reason') or '') if approval else '',
        'dual_control_approved_ts_ms': str(approval.get('approved_ts_ms') or 0) if approval else '0',
        'dual_control_approved_ticket': str(approval.get('ticket') or '') if approval else '',
        'dual_control_approved_operator': str(approval.get('operator') or '') if approval else '',
        'dual_control_approved_reason': str(approval.get('reason') or '') if approval else '',
        'dual_control_approved_deadline_ts_ms': str(approval.get('deadline_ts_ms') or 0) if approval else '0',
        'dual_control_approved_freshness_s': str(approval.get('freshness_s') or 0) if approval else '0',
        'dual_control_invalidated_ts_ms': '0',
        'dual_control_invalidated_reason': '',
        'dual_control_invalidated_stage': '',
        'dual_control_consumed_ts_ms': str(now_ms if approval else 0),
    }


def set_override(
    client: Any,
    *,
    purpose: str,
    family: str,
    ticket: str,
    operator: str,
    reason: str,
    ttl_s: int,
) -> Dict[str, Any]:
    """Create or replace a TTL-backed suppress override.

    Blocked when the existing lifecycle state is in a reminder/expired renewal flow —
    the operator must use ``acknowledge-renew`` + ``renew`` in that case.
    Also enforces a new identity (ticket/operator/reason) vs any existing override.
    """
    purpose = _validate_nonempty('purpose', purpose)
    family = _validate_family(family)
    ticket = _validate_nonempty('ticket', ticket)
    operator = _validate_nonempty('operator', operator)
    reason = _validate_nonempty('reason', reason)
    ttl_s = max(300, min(int(ttl_s), max_ttl_s()))
    now_ms = _now_ms()
    key = override_key(purpose, family)
    state_key = override_state_key(purpose, family)
    existing = client.hgetall(key) or {}
    state = _read_state(client, purpose, family)
    # P6.9: block direct suppression when reminder/expiry renewal workflow is in progress
    if state and _renewal_required(state, now_ms):
        raise OverrideWorkflowError('override is in reminder/expired renewal flow; use acknowledge-renew and renew instead of suppress')
    # P6.9: enforce distinct identity vs current override to prevent silent perpetuation
    if existing or state:
        _ensure_new_identity(ticket, operator, reason, existing or state)
    expire_ts_ms = now_ms + ttl_s * 1000
    payload = {
        'purpose': purpose,
        'family': family,
        'ticket': ticket,
        'operator': operator,
        'reason': reason,
        'created_ts_ms': str(now_ms),
        'expire_ts_ms': str(expire_ts_ms),
        'ttl_s': str(ttl_s),
        'suppress_active': '1',
    }
    client.hset(key, mapping=payload)
    client.expire(key, ttl_s)
    client.hset(
        state_key,
        mapping=_build_active_state_payload(
            purpose=purpose,
            family=family,
            ticket=ticket,
            operator=operator,
            reason=reason,
            now_ms=now_ms,
            expire_ts_ms=expire_ts_ms,
            ttl_s=ttl_s,
            previous=state or existing,
            renewal_ack=None,
            limits=resolve_override_limits(client, purpose, family),
            escalation_ack=None,
            dual_control_approval=None,
        )
    )
    _emit_event(
        client,
        'strategy_research_stats_alert_policy_suppress_override_set',
        {
            'purpose': purpose,
            'family': family,
            'ticket': ticket,
            'operator': operator,
            'reason': reason,
            'ttl_s': ttl_s,
            'expire_ts_ms': expire_ts_ms,
            'override_key': key,
        }
    )
    return payload


def acknowledge_renewal(
    client: Any,
    *,
    purpose: str,
    family: str,
    ticket: str,
    operator: str,
    reason: str,
    escalation_ticket: str = '',
    escalation_operator: str = '',
    escalation_reason: str = '',
) -> Dict[str, Any]:
    """Acknowledge an expiry reminder or expired override with a new ticket/operator/reason.

    This is the first step of the two-step renewal workflow.  The provided identity must
    be distinct from the current/previous override to satisfy the no-infinite-renewal
    invariant.  After acknowledgement ``renew_override()`` may be called to activate a new
    TTL with the acknowledged identity.
    """
    purpose = _validate_nonempty('purpose', purpose)
    family = _validate_family(family)
    ticket = _validate_nonempty('ticket', ticket)
    operator = _validate_nonempty('operator', operator)
    reason = _validate_nonempty('reason', reason)
    now_ms = _now_ms()
    state_key = override_state_key(purpose, family)
    state = _read_state(client, purpose, family)
    if not state:
        raise OverrideWorkflowError('no override lifecycle state found for purpose/family')
    if not _renewal_required(state, now_ms):
        raise OverrideWorkflowError('renew acknowledgement is only allowed after expiry reminder or expiry')
    # New identity required to prevent rubber-stamping the same override
    _ensure_new_identity(ticket, operator, reason, state)
    escalation_ticket = str(escalation_ticket or '').strip()
    escalation_operator = str(escalation_operator or '').strip()
    escalation_reason = str(escalation_reason or '').strip()
    if escalation_ticket:
        _ensure_new_escalation_identity(escalation_ticket, state, ticket)
        escalation_operator = _validate_nonempty('escalation_operator', escalation_operator)
        escalation_reason = _validate_nonempty('escalation_reason', escalation_reason)
    updated = dict(state)
    updated.update({
        'renew_ack_required': '1',
        'renew_ack_ts_ms': str(now_ms),
        'renew_ack_ticket': ticket,
        'renew_ack_operator': operator,
        'renew_ack_reason': reason,
        'renew_escalation_ticket': escalation_ticket,
        'renew_escalation_operator': escalation_operator,
        'renew_escalation_reason': escalation_reason,
        'dual_control_required': '0',
        'dual_control_approval_state': 'none',
        'dual_control_request_ts_ms': '0',
        'dual_control_request_ticket': '',
        'dual_control_request_operator': '',
        'dual_control_request_reason': '',
        'dual_control_approved_ts_ms': '0',
        'dual_control_approved_ticket': '',
        'dual_control_approved_operator': '',
        'dual_control_approved_reason': '',
        'dual_control_approved_deadline_ts_ms': '0',
        'dual_control_approved_freshness_s': str(dual_control_approved_freshness_s()),
        'dual_control_invalidated_ts_ms': '0',
        'dual_control_invalidated_reason': '',
        'dual_control_invalidated_stage': '',
        'dual_control_consumed_ts_ms': '0',
    })
    client.hset(state_key, mapping=updated)
    _emit_event(
        client,
        'strategy_research_stats_alert_policy_suppress_override_renew_acknowledged',
        {
            'purpose': purpose,
            'family': family,
            'ticket': ticket,
            'operator': operator,
            'reason': reason,
            'current_ticket': str(state.get('ticket') or ''),
            'current_operator': str(state.get('operator') or ''),
            'current_reason': str(state.get('reason') or ''),
            'lifecycle_state': str(state.get('lifecycle_state') or ''),
            'escalation_ticket': escalation_ticket,
            'escalation_operator': escalation_operator,
            'escalation_reason': escalation_reason,
        }
    )
    return updated




def approve_dual_control_renewal(
    client: Any,
    *,
    purpose: str,
    family: str,
    approval_ticket: str,
    approver: str,
    approval_reason: str,
) -> Dict[str, Any]:
    purpose = _validate_nonempty('purpose', purpose)
    family = _validate_family(family)
    approval_ticket = _validate_nonempty('approval_ticket', approval_ticket)
    approver = _validate_nonempty('approver', approver)
    approval_reason = _validate_nonempty('approval_reason', approval_reason)
    state_key = override_state_key(purpose, family)
    state = _read_state(client, purpose, family)
    if not state:
        raise OverrideWorkflowError('no override lifecycle state found for purpose/family')
    state = _invalidate_stale_dual_control_approval(
        client,
        purpose=purpose,
        family=family,
        state_key=state_key,
        state=state,
        stage='approve',
    )
    if str(state.get('dual_control_approval_state') or '') not in ('pending', 'invalidated') or _to_int(state.get('dual_control_required'), 0) != 1:
        raise OverrideWorkflowError('dual-control approval is only allowed when a pending or invalidated limit-hit escalation approval exists')
    _ensure_dual_control_approver_distinct(state, approver, approval_ticket)
    now_ms = _now_ms()
    updated = dict(state)
    freshness_s = dual_control_approved_freshness_s()
    updated.update({
        'dual_control_required': '1',
        'dual_control_approval_state': 'approved',
        'dual_control_approved_ts_ms': str(now_ms),
        'dual_control_approved_ticket': approval_ticket,
        'dual_control_approved_operator': approver,
        'dual_control_approved_reason': approval_reason,
        'dual_control_approved_deadline_ts_ms': str(now_ms + freshness_s * 1000),
        'dual_control_approved_freshness_s': str(freshness_s),
        'dual_control_invalidated_ts_ms': '0',
        'dual_control_invalidated_reason': '',
        'dual_control_invalidated_stage': '',
    })
    client.hset(state_key, mapping=updated)
    _emit_event(
        client,
        'strategy_research_stats_alert_policy_suppress_override_dual_control_approved',
        {
            'purpose': purpose,
            'family': family,
            'approval_ticket': approval_ticket,
            'approver': approver,
            'approval_reason': approval_reason,
            'renew_ticket': str(state.get('renew_ack_ticket') or ''),
            'escalation_ticket': str(state.get('renew_escalation_ticket') or ''),
            'freshness_s': freshness_s,
            'deadline_ts_ms': now_ms + freshness_s * 1000,
        }
    )
    return updated


def renew_override(
    client: Any,
    *,
    purpose: str,
    family: str,
    ttl_s: int,
) -> Dict[str, Any]:
    """Renew a suppression override after a valid acknowledgement.

    Second step of the renewal workflow.  Validates that:
    - The override is in a reminder/expired state (renewal is contextually needed).
    - ``acknowledge_renewal()`` was called with a distinct identity first.
    - The acknowledged identity still satisfies the new-identity invariant vs the current state.

    On success writes a fresh override hash + renewal-enriched lifecycle state, emits the
    ``renewed`` audit event, resets ``renew_ack_required`` to 0, and increments ``renew_count``.
    """
    purpose = _validate_nonempty('purpose', purpose)
    family = _validate_family(family)
    ttl_s = max(300, min(int(ttl_s), max_ttl_s()))
    now_ms = _now_ms()
    state_key = override_state_key(purpose, family)
    state = _read_state(client, purpose, family)
    if not state:
        raise OverrideWorkflowError('no override lifecycle state found for purpose/family')
    if not _renewal_required(state, now_ms):
        raise OverrideWorkflowError('renew is only allowed after expiry reminder or expiry')
    # Require prior acknowledgement — renew_ack_required must be set to '1'
    if _to_int(state.get('renew_ack_required'), 0) != 1:
        raise OverrideWorkflowError('renew requires an acknowledged reminder with a new ticket/operator/reason')
    ack_ticket_raw = str(state.get('renew_ack_ticket') or '').strip()
    ack_operator_raw = str(state.get('renew_ack_operator') or '').strip()
    ack_reason_raw = str(state.get('renew_ack_reason') or '').strip()
    if not (ack_ticket_raw and ack_operator_raw and ack_reason_raw):
        raise OverrideWorkflowError('renew requires an acknowledged reminder with a new ticket/operator/reason')
    ack_ticket = _validate_nonempty('renew_ack_ticket', ack_ticket_raw)
    ack_operator = _validate_nonempty('renew_ack_operator', ack_operator_raw)
    ack_reason = _validate_nonempty('renew_ack_reason', ack_reason_raw)
    # Double-check identity is still distinct (state may have been modified between ack and renew)
    _ensure_new_identity(ack_ticket, ack_operator, ack_reason, state)
    limits = resolve_override_limits(client, purpose, family)
    proposed_budget_s = _policy_budget_used_s(state) + int(ttl_s)
    proposed_renew_count = _to_int(state.get('renew_count'), 0) + 1
    limit_kind = _policy_limit_kind(limits, proposed_budget_s=proposed_budget_s, proposed_renew_count=proposed_renew_count)
    escalation_ack: Dict[str, str] = {}
    dual_control_approval: Dict[str, str] = {}
    if limit_kind:
        _record_limit_block(
            client,
            purpose=purpose,
            family=family,
            state_key=state_key,
            state=state,
            limits=limits,
            limit_kind=limit_kind,
            proposed_budget_s=proposed_budget_s,
            proposed_renew_count=proposed_renew_count,
        )
        state = client.hgetall(state_key) or state
        if limits.get('require_escalation', 0) <= 0:
            raise OverrideWorkflowError(f'renew exceeds policy {limit_kind} limit for purpose/family')
        escalation_ack = _require_escalation_fields(state)
        _ensure_new_escalation_identity(str(escalation_ack.get('ticket') or ''), state, ack_ticket)
        if require_dual_control_on_limit() > 0:
            state = _invalidate_stale_dual_control_approval(
                client,
                purpose=purpose,
                family=family,
                state_key=state_key,
                state=state,
                stage='renew',
            )
            state = _require_dual_control_approval(
                client,
                purpose=purpose,
                family=family,
                state_key=state_key,
                state=state,
                limit_kind=limit_kind,
            )
            dual_control_approval = {
                'approved_ts_ms': str(state.get('dual_control_approved_ts_ms') or '0'),
                'ticket': str(state.get('dual_control_approved_ticket') or ''),
                'operator': str(state.get('dual_control_approved_operator') or ''),
                'reason': str(state.get('dual_control_approved_reason') or ''),
                'deadline_ts_ms': str(_dual_control_approval_deadline_ts_ms(state) or 0),
                'freshness_s': str(max(60, _to_int(state.get('dual_control_approved_freshness_s'), dual_control_approved_freshness_s()))),
            }
    expire_ts_ms = now_ms + ttl_s * 1000
    key = override_key(purpose, family)
    payload = {
        'purpose': purpose,
        'family': family,
        'ticket': ack_ticket,
        'operator': ack_operator,
        'reason': ack_reason,
        'created_ts_ms': str(now_ms),
        'expire_ts_ms': str(expire_ts_ms),
        'ttl_s': str(ttl_s),
        'suppress_active': '1',
    },
    client.hset(key, mapping=payload),
    client.expire(key, ttl_s),
    client.hset(
        state_key,
        mapping=_build_active_state_payload(
            purpose=purpose,
            family=family,
            ticket=ack_ticket,
            operator=ack_operator,
            reason=ack_reason,
            now_ms=now_ms,
            expire_ts_ms=expire_ts_ms,
            ttl_s=ttl_s,
            previous=state,
            renewal_ack={
                'ticket': ack_ticket,
                'operator': ack_operator,
                'reason': ack_reason,
            },
            limits=limits,
            escalation_ack=escalation_ack,
            dual_control_approval=dual_control_approval,
        )
    )
    _emit_event(
        client,
        'strategy_research_stats_alert_policy_suppress_override_renewed',
        {
            'purpose': purpose,
            'family': family,
            'ticket': ack_ticket,
            'operator': ack_operator,
            'reason': ack_reason,
            'previous_ticket': str(state.get('ticket') or ''),
            'previous_operator': str(state.get('operator') or ''),
            'previous_reason': str(state.get('reason') or ''),
            'ttl_s': ttl_s,
            'expire_ts_ms': expire_ts_ms,
            'renew_count': str(_to_int(state.get('renew_count'), 0) + 1),
            'limit_kind': limit_kind,
            'policy_max_budget_s': limits.get('max_budget_s', 0),
            'policy_max_renew_count': limits.get('max_renew_count', 0),
            'policy_proposed_budget_s': proposed_budget_s,
            'policy_proposed_renew_count': proposed_renew_count,
            'escalation_ticket': escalation_ack.get('ticket', ''),
            'escalation_operator': escalation_ack.get('operator', ''),
            'escalation_reason': escalation_ack.get('reason', ''),
        }
    )
    return payload


def clear_override(
    client: Any,
    *,
    purpose: str,
    family: str,
    ticket: str,
    operator: str,
    reason: str,
    escalation_ticket: str = '',
    escalation_operator: str = '',
    escalation_reason: str = '',
) -> Dict[str, Any]:
    purpose = _validate_nonempty('purpose', purpose)
    family = _validate_family(family)
    ticket = _validate_nonempty('ticket', ticket)
    operator = _validate_nonempty('operator', operator)
    reason = _validate_nonempty('reason', reason)
    key = override_key(purpose, family)
    existing = client.hgetall(key) or {}
    client.delete(key)
    now_ms = _now_ms()
    client.hset(
        override_state_key(purpose, family),
        mapping={
            'purpose': purpose,
            'family': family,
            'ticket': existing.get('ticket', ''),
            'operator': existing.get('operator', ''),
            'reason': existing.get('reason', ''),
            'created_ts_ms': existing.get('created_ts_ms', '0'),
            'expire_ts_ms': existing.get('expire_ts_ms', '0'),
            'active': '0',
            'lifecycle_state': 'cleared',
            'cleared_ts_ms': str(now_ms),
            'cleared_by_ticket': ticket,
            'cleared_by_operator': operator,
            'cleared_reason': reason,
            # P6.9: clear any pending renewal ack so it doesn't persist after explicit clear
            'renew_ack_required': '0',
            'renew_ack_ts_ms': '0',
            'renew_ack_ticket': '',
            'renew_ack_operator': '',
            'renew_ack_reason': '',
            'renew_escalation_ticket': '',
            'renew_escalation_operator': '',
            'renew_escalation_reason': '',
            'dual_control_required': '0',
            'dual_control_approval_state': 'none',
            'dual_control_request_ts_ms': '0',
            'dual_control_request_ticket': '',
            'dual_control_request_operator': '',
            'dual_control_request_reason': '',
            'dual_control_approved_ts_ms': '0',
            'dual_control_approved_ticket': '',
            'dual_control_approved_operator': '',
            'dual_control_approved_reason': '',
            'dual_control_approved_deadline_ts_ms': '0',
            'dual_control_approved_freshness_s': str(dual_control_approved_freshness_s()),
            'dual_control_invalidated_ts_ms': '0',
            'dual_control_invalidated_reason': '',
            'dual_control_invalidated_stage': '',
            'dual_control_consumed_ts_ms': '0',
        }
    )
    _emit_event(
        client,
        'strategy_research_stats_alert_policy_suppress_override_cleared',
        {
            'purpose': purpose,
            'family': family,
            'ticket': ticket,
            'operator': operator,
            'reason': reason,
            'previous_ticket': existing.get('ticket', ''),
            'previous_operator': existing.get('operator', ''),
            'previous_reason': existing.get('reason', ''),
            'previous_expire_ts_ms': existing.get('expire_ts_ms', ''),
            'override_key': key,
        }
    )
    return existing


def list_active_overrides(client: Any, *, purpose: str = '') -> list[Dict[str, Any]]:
    prefix = _env(
        'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX',
        'cfg:strategy_research_stats:alert_policy:suppress_override:v1',
    ).rstrip(':')
    pattern = f'{prefix}:{purpose}:*' if purpose else f'{prefix}:*'
    now_ms = _now_ms()
    items: list[Dict[str, Any]] = []
    for key in sorted(client.keys(pattern) or []):
        raw = client.hgetall(key) or {}
        if not raw:
            continue
        expire_ts_ms = _to_int(raw.get('expire_ts_ms'), 0)
        if expire_ts_ms and expire_ts_ms <= now_ms:
            continue
        items.append(
            {
                'key': key,
                'purpose': raw.get('purpose', ''),
                'family': raw.get('family', ''),
                'ticket': raw.get('ticket', ''),
                'operator': raw.get('operator', ''),
                'reason': raw.get('reason', ''),
                'created_ts_ms': _to_int(raw.get('created_ts_ms'), 0),
                'expire_ts_ms': expire_ts_ms,
                'remaining_s': max(0, (expire_ts_ms - now_ms) // 1000),
            }
        )
    return items


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='Manage TTL-backed strategy_research_stats alert-policy suppression overrides')
    sub = p.add_subparsers(dest='cmd', required=True)

    ps = sub.add_parser('suppress', help='Create or replace a TTL-backed suppress override')
    ps.add_argument('--purpose', required=True)
    ps.add_argument('--family', required=True, choices=SUPPORTED_OVERRIDE_FAMILIES)
    ps.add_argument('--ticket', required=True)
    ps.add_argument('--operator', required=True)
    ps.add_argument('--reason', required=True)
    ps.add_argument('--ttl-s', type=int, default=default_ttl_s())

    # P6.9: two-step renewal workflow subcommands
    pa = sub.add_parser('acknowledge-renew', help='Acknowledge reminder/expiry with a new renewal ticket/operator/reason')
    pa.add_argument('--purpose', required=True)
    pa.add_argument('--family', required=True, choices=SUPPORTED_OVERRIDE_FAMILIES)
    pa.add_argument('--ticket', required=True)
    pa.add_argument('--operator', required=True)
    pa.add_argument('--reason', required=True)
    pa.add_argument('--escalation-ticket', default='')
    pa.add_argument('--escalation-operator', default='')
    pa.add_argument('--escalation-reason', default='')

    pd = sub.add_parser('approve-renew-escalation', help='Approve a pending limit-hit escalation renewal as a second approver')
    pd.add_argument('--purpose', required=True)
    pd.add_argument('--family', required=True, choices=SUPPORTED_OVERRIDE_FAMILIES)
    pd.add_argument('--approval-ticket', required=True)
    pd.add_argument('--approver', required=True)
    pd.add_argument('--approval-reason', required=True)

    pr = sub.add_parser('renew', help='Renew an override after acknowledgement with a new TTL')
    pr.add_argument('--purpose', required=True)
    pr.add_argument('--family', required=True, choices=SUPPORTED_OVERRIDE_FAMILIES)
    pr.add_argument('--ttl-s', type=int, default=default_ttl_s())

    pc = sub.add_parser('clear', help='Clear an active suppress override')
    pc.add_argument('--purpose', required=True)
    pc.add_argument('--family', required=True, choices=SUPPORTED_OVERRIDE_FAMILIES)
    pc.add_argument('--ticket', required=True)
    pc.add_argument('--operator', required=True)
    pc.add_argument('--reason', required=True)

    pst = sub.add_parser('status', help='Print active suppress overrides as JSON')
    pst.add_argument('--purpose', default='')
    return p


def main(argv: list[str] | None = None) -> int:
    ns = build_arg_parser().parse_args(argv)
    client = _redis_client()
    try:
        if ns.cmd == 'suppress':
            payload = set_override(
                client,
                purpose=ns.purpose,
                family=ns.family,
                ticket=ns.ticket,
                operator=ns.operator,
                reason=ns.reason,
                ttl_s=ns.ttl_s,
            )
            sys.stdout.write(json.dumps(payload, sort_keys=True) + '\n')
            return 0
        if ns.cmd == 'acknowledge-renew':
            payload = acknowledge_renewal(
                client,
                purpose=ns.purpose,
                family=ns.family,
                ticket=ns.ticket,
                operator=ns.operator,
                reason=ns.reason,
                escalation_ticket=getattr(ns, 'escalation_ticket', ''),
                escalation_operator=getattr(ns, 'escalation_operator', ''),
                escalation_reason=getattr(ns, 'escalation_reason', ''),
            )
            sys.stdout.write(json.dumps(payload, sort_keys=True) + '\n')
            return 0
        if ns.cmd == 'approve-renew-escalation':
            payload = approve_dual_control_renewal(
                client,
                purpose=ns.purpose,
                family=ns.family,
                approval_ticket=ns.approval_ticket,
                approver=ns.approver,
                approval_reason=ns.approval_reason,
            )
            sys.stdout.write(json.dumps(payload, sort_keys=True) + '\n')
            return 0
        if ns.cmd == 'renew':
            payload = renew_override(
                client,
                purpose=ns.purpose,
                family=ns.family,
                ttl_s=ns.ttl_s,
            )
            sys.stdout.write(json.dumps(payload, sort_keys=True) + '\n')
            return 0
        if ns.cmd == 'clear':
            payload = clear_override(
                client,
                purpose=ns.purpose,
                family=ns.family,
                ticket=ns.ticket,
                operator=ns.operator,
                reason=ns.reason,
            )
            sys.stdout.write(json.dumps(payload, sort_keys=True) + '\n')
            return 0
        rows = list_active_overrides(client, purpose=ns.purpose)
        sys.stdout.write(json.dumps(rows, sort_keys=True) + '\n')
        return 0
    except OverrideWorkflowError as exc:
        sys.stderr.write(f'{exc}\n')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
