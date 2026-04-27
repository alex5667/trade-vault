from __future__ import annotations

"""Redis-backed dual-control approval state for deploy-lint silence overrides.

P4.10 added second-operator approval for long silence override windows. An
escalation ticket alone is no longer enough once an override request exceeds the
configured long-window threshold; the requesting operator must prepare a request
and a different operator must approve it before the final notifier silence ack
can be committed.

P4.11 adds timeboxed freshness for prepared/approved requests. Old requests are
not allowed to linger indefinitely: prepared requests auto-expire after a
shorter freshness window, while already-approved requests auto-cancel if the
requester does not consume them quickly enough.

P4.12 binds every approval request to a concrete deploy-lint drift snapshot so a
fresh approval cannot be consumed after the underlying drift has changed.

P4.13 extends that binding from coarse hashes to a richer semantic fingerprint:
gate reason, error count, and a canonical details_json hash are now part of the
approval contract as well.

P4.14 adds warning-policy / notifier-route binding so approvals invalidate when
operational escalation class changes even if the raw drift body stays similar.
"""

from dataclasses import dataclass
from typing import Any
import hashlib
import json
import time
import uuid

from services.observability.latency_deploy_contract import CONTRACTS
from services.observability.latency_deploy_lint_notify_state import purposes_hash
from services.observability.latency_deploy_lint_state import state_key as lint_state_key


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _s(v: Any, d: str = '') -> str:
    s = str(v if v is not None else '').strip()
    return s or d


def _csv(items: list[str] | tuple[str, ...]) -> str:
    return ','.join(sorted({str(x).strip() for x in items if str(x).strip()}))


def _codes_csv(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        items = [str(x).strip() for x in value if str(x).strip()]
    else:
        raw = _s(value, 'ok')
        items = [part.strip() for part in raw.split(',') if part.strip() and part.strip() != 'none']
    if not items:
        return 'ok'
    return ','.join(sorted(dict.fromkeys(items)))


def _codes_hash(value: Any) -> str:
    return hashlib.sha1(_codes_csv(value).encode('utf-8')).hexdigest()


def _csv_tokens(value: Any, *, default: str = 'none') -> str:
    if isinstance(value, (list, tuple)):
        items = [str(x).strip() for x in value if str(x).strip() and str(x).strip() != 'none']
    else:
        raw = _s(value, default)
        items = [part.strip() for part in raw.split(',') if part.strip() and part.strip() != 'none']
    if not items:
        return default
    return ','.join(sorted(dict.fromkeys(items)))


def _details_payload(raw: dict[str, str] | None) -> dict[str, Any]:
    raw = dict(raw or {})
    return {
        'compose_file': _s(raw.get('compose_file')),
        'wrapper_file': _s(raw.get('wrapper_file')),
        'unit_file': _s(raw.get('unit_file')),
        'env_file': _s(raw.get('env_file')),
        'missing_runtime_env': _csv_tokens(raw.get('missing_runtime_env'), default='none'),
        'missing_env_file_vars': _csv_tokens(raw.get('missing_env_file_vars'), default='none'),
        'warning_codes': _csv_tokens(raw.get('warning_codes'), default='none'),
        'warnings_count': _i(raw.get('warnings_count'), 0),
    }


def _details_json(raw: dict[str, str] | None) -> str:
    return json.dumps(_details_payload(raw), sort_keys=True, separators=(',', ':'))


def _details_fingerprint(raw: dict[str, str] | None) -> str:
    return hashlib.sha1(_details_json(raw).encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class DeployLintSilenceApprovalState:
    request_id: str
    purpose: str
    status: str
    present: bool
    prepared_by: str
    prepared_ticket: str
    prepared_reason: str
    escalation_ticket: str
    requested_minutes: int
    prepared_ts_ms: int
    approved_by: str
    approved_reason: str
    approved_ts_ms: int
    consumed_by: str
    consumed_ts_ms: int
    # P4.11: freshness tracking
    freshness_deadline_ts_ms: int
    expired_ts_ms: int
    expired_reason: str
    cancelled_by: str
    cancelled_reason: str
    cancelled_ts_ms: int
    # P4.12: drift binding (coarse)
    binding_schema_version: int
    bound_snapshot_ts_ms: int
    bound_error_codes: str
    bound_error_codes_hash: str
    bound_active_purposes_csv: str
    bound_active_purposes_hash: str
    # P4.13: semantic binding (extended)
    bound_gate_reason_code: str
    bound_errors_count: int
    bound_details_json: str
    bound_details_fingerprint: str
    # P4.14: warning-policy / notifier-route binding
    bound_warning_codes: str
    bound_warning_codes_hash: str
    bound_warning_severity_policy: str
    bound_notifier_route_class: str
    # P4.12: invalidation fields (coarse)
    invalidated_ts_ms: int
    invalidated_reason: str
    invalidated_stage: str
    invalidated_error_codes: str
    invalidated_error_codes_hash: str
    invalidated_active_purposes_csv: str
    invalidated_active_purposes_hash: str
    # P4.13: invalidation snapshot (semantic)
    invalidated_gate_reason_code: str
    invalidated_errors_count: int
    invalidated_details_json: str
    invalidated_details_fingerprint: str
    # P4.14: invalidation warning-policy snapshot
    invalidated_warning_codes: str
    invalidated_warning_codes_hash: str
    invalidated_warning_severity_policy: str
    invalidated_notifier_route_class: str

    @property
    def freshness_remaining_s(self) -> int:
        if self.freshness_deadline_ts_ms <= 0:
            return 0
        ref_ts_ms = (
            self.prepared_ts_ms
            or self.approved_ts_ms
            or self.consumed_ts_ms
            or self.cancelled_ts_ms
            or self.expired_ts_ms
            or self.invalidated_ts_ms
        )
        if ref_ts_ms <= 0:
            return 0
        return max(0, int((self.freshness_deadline_ts_ms - ref_ts_ms) / 1000))


@dataclass(frozen=True)
class ApprovalValidation:
    ok: bool
    reason: str
    state: DeployLintSilenceApprovalState
    invalidate: bool = False


def state_key(prefix: str, request_id: str) -> str:
    return f"{prefix.rstrip(':')}:req:{request_id}"


def latest_key(prefix: str, purpose: str) -> str:
    return f"{prefix.rstrip(':')}:latest:{purpose}"


def parse_approval_state(raw: dict[str, str] | None) -> DeployLintSilenceApprovalState:
    raw = dict(raw or {})
    return DeployLintSilenceApprovalState(
        request_id=_s(raw.get('request_id')),
        purpose=_s(raw.get('purpose')),
        status=_s(raw.get('status'), 'none'),
        present=bool(raw),
        prepared_by=_s(raw.get('prepared_by')),
        prepared_ticket=_s(raw.get('prepared_ticket')),
        prepared_reason=_s(raw.get('prepared_reason')),
        escalation_ticket=_s(raw.get('escalation_ticket')),
        requested_minutes=_i(raw.get('requested_minutes'), 0),
        prepared_ts_ms=_i(raw.get('prepared_ts_ms'), 0),
        approved_by=_s(raw.get('approved_by')),
        approved_reason=_s(raw.get('approved_reason')),
        approved_ts_ms=_i(raw.get('approved_ts_ms'), 0),
        consumed_by=_s(raw.get('consumed_by')),
        consumed_ts_ms=_i(raw.get('consumed_ts_ms'), 0),
        # P4.11 freshness
        freshness_deadline_ts_ms=_i(raw.get('freshness_deadline_ts_ms'), 0),
        expired_ts_ms=_i(raw.get('expired_ts_ms'), 0),
        expired_reason=_s(raw.get('expired_reason')),
        cancelled_by=_s(raw.get('cancelled_by')),
        cancelled_reason=_s(raw.get('cancelled_reason')),
        cancelled_ts_ms=_i(raw.get('cancelled_ts_ms'), 0),
        # P4.12 binding (coarse)
        binding_schema_version=max(1, _i(raw.get('binding_schema_version'), 1)),
        bound_snapshot_ts_ms=_i(raw.get('bound_snapshot_ts_ms'), 0),
        bound_error_codes=_s(raw.get('bound_error_codes'), 'ok'),
        bound_error_codes_hash=_s(raw.get('bound_error_codes_hash')),
        bound_active_purposes_csv=_s(raw.get('bound_active_purposes_csv'), 'none'),
        bound_active_purposes_hash=_s(raw.get('bound_active_purposes_hash')),
        # P4.13 semantic binding
        bound_gate_reason_code=_s(raw.get('bound_gate_reason_code'), 'ok'),
        bound_errors_count=_i(raw.get('bound_errors_count'), 0),
        bound_details_json=_s(raw.get('bound_details_json'), '{}'),
        bound_details_fingerprint=_s(raw.get('bound_details_fingerprint')),
        # P4.14 warning-policy binding
        bound_warning_codes=_s(raw.get('bound_warning_codes'), 'none'),
        bound_warning_codes_hash=_s(raw.get('bound_warning_codes_hash')),
        bound_warning_severity_policy=_s(raw.get('bound_warning_severity_policy'), 'none'),
        bound_notifier_route_class=_s(raw.get('bound_notifier_route_class'), 'notify'),
        # P4.12 invalidation (coarse)
        invalidated_ts_ms=_i(raw.get('invalidated_ts_ms'), 0),
        invalidated_reason=_s(raw.get('invalidated_reason')),
        invalidated_stage=_s(raw.get('invalidated_stage')),
        invalidated_error_codes=_s(raw.get('invalidated_error_codes'), 'ok'),
        invalidated_error_codes_hash=_s(raw.get('invalidated_error_codes_hash')),
        invalidated_active_purposes_csv=_s(raw.get('invalidated_active_purposes_csv'), 'none'),
        invalidated_active_purposes_hash=_s(raw.get('invalidated_active_purposes_hash')),
        # P4.13 invalidation (semantic)
        invalidated_gate_reason_code=_s(raw.get('invalidated_gate_reason_code'), 'ok'),
        invalidated_errors_count=_i(raw.get('invalidated_errors_count'), 0),
        invalidated_details_json=_s(raw.get('invalidated_details_json'), '{}'),
        invalidated_details_fingerprint=_s(raw.get('invalidated_details_fingerprint')),
        # P4.14 invalidation warning-policy snapshot
        invalidated_warning_codes=_s(raw.get('invalidated_warning_codes'), 'none'),
        invalidated_warning_codes_hash=_s(raw.get('invalidated_warning_codes_hash')),
        invalidated_warning_severity_policy=_s(raw.get('invalidated_warning_severity_policy'), 'none'),
        invalidated_notifier_route_class=_s(raw.get('invalidated_notifier_route_class'), 'notify'),
    )


def _xadd_best_effort(r: Any, stream: str | None, fields: dict[str, str]) -> str:
    if not stream:
        return ''
    try:
        return str(r.xadd(stream, fields, maxlen=200000, approximate=True) or '')
    except Exception:
        return ''


def _deadline_for_status(*, now_ms: int, status: str, prepared_freshness_s: int, approved_freshness_s: int) -> int:
    if status == 'prepared':
        return now_ms + max(1, int(prepared_freshness_s)) * 1000
    if status == 'approved':
        return now_ms + max(1, int(approved_freshness_s)) * 1000
    return 0


def refresh_approval_state(
    r: Any,
    *,
    prefix: str,
    request_id: str,
    prepared_freshness_s: int,
    approved_freshness_s: int,
    ttl_s: int,
    ops_stream: str | None = None,
    now_ms: int | None = None,
) -> dict[str, str]:
    """Auto-transition stale prepared/approved requests to expired/cancelled."""
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    skey = state_key(prefix, request_id)
    prev = r.hgetall(skey) or {}
    st = parse_approval_state(prev)
    if not st.present:
        return {}
    if st.status not in {'prepared', 'approved'}:
        return prev
    deadline_ts_ms = st.freshness_deadline_ts_ms
    if deadline_ts_ms <= 0:
        deadline_ts_ms = _deadline_for_status(
            now_ms=st.prepared_ts_ms if st.status == 'prepared' else st.approved_ts_ms or now_ms,
            status=st.status,
            prepared_freshness_s=prepared_freshness_s,
            approved_freshness_s=approved_freshness_s,
        )
        prev = {**prev, 'freshness_deadline_ts_ms': str(deadline_ts_ms)}
        r.hset(skey, mapping={'freshness_deadline_ts_ms': str(deadline_ts_ms)})
    if deadline_ts_ms > now_ms:
        try:
            r.expire(skey, max(1, int(ttl_s)))
        except Exception:
            pass
        return prev
    if st.status == 'prepared':
        mapping = {
            **prev,
            'status': 'expired',
            'expired_ts_ms': str(now_ms),
            'expired_reason': 'prepared_freshness_elapsed',
            'bound_error_codes_hash': st.bound_error_codes_hash,
            'bound_active_purposes_hash': st.bound_active_purposes_hash,
            'bound_gate_reason_code': st.bound_gate_reason_code,
            'bound_errors_count': str(st.bound_errors_count),
            'bound_details_fingerprint': st.bound_details_fingerprint,
        }
    else:
        mapping = {
            **prev,
            'status': 'cancelled',
            'cancelled_ts_ms': str(now_ms),
            'cancelled_by': '',
            'cancelled_reason': 'approved_freshness_elapsed',
            'bound_error_codes_hash': st.bound_error_codes_hash,
            'bound_active_purposes_hash': st.bound_active_purposes_hash,
            'bound_gate_reason_code': st.bound_gate_reason_code,
            'bound_errors_count': str(st.bound_errors_count),
            'bound_details_fingerprint': st.bound_details_fingerprint,
        }
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass
    event_kind = (
        'latency_deploy_lint_override_approval_expired'
        if st.status == 'prepared' else
        'latency_deploy_lint_override_approval_cancelled'
    )
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms),
        'kind': event_kind,
        'purpose': st.purpose,
        'request_id': st.request_id,
        'prepared_by': st.prepared_by,
        'ticket': st.prepared_ticket,
        'escalation_ticket': st.escalation_ticket,
        'requested_minutes': str(st.requested_minutes),
        'expired_reason': 'prepared_freshness_elapsed',
        'bound_error_codes_hash': st.bound_error_codes_hash,
        'bound_active_purposes_hash': st.bound_active_purposes_hash,
        'bound_gate_reason_code': st.bound_gate_reason_code,
        'bound_errors_count': str(st.bound_errors_count),
        'bound_details_fingerprint': st.bound_details_fingerprint,
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping


def read_latest_approval(
    r: Any,
    *,
    prefix: str,
    purpose: str,
    prepared_freshness_s: int = 7200,
    approved_freshness_s: int = 1800,
    ttl_s: int = 604800,
    ops_stream: str | None = None,
    now_ms: int | None = None,
) -> dict[str, str]:
    rid = _s(r.get(latest_key(prefix, purpose)) if hasattr(r, 'get') else '')
    if not rid:
        return {}
    return refresh_approval_state(
        r,
        prefix=prefix,
        request_id=rid,
        prepared_freshness_s=prepared_freshness_s,
        approved_freshness_s=approved_freshness_s,
        ttl_s=ttl_s,
        ops_stream=ops_stream,
        now_ms=now_ms,
    )


def _policy_set(raw: str) -> set[str]:
    """Split a comma-separated policy CSV into a set of trimmed code strings."""
    return {x.strip() for x in str(raw or '').split(',') if x.strip()}


def resolve_warning_severity_policy(
    warning_codes_csv: str,
    *,
    warn_codes_warn_csv: str = '',
    warn_codes_crit_csv: str = '',
    warn_codes_page_csv: str = '',
) -> str:
    """Map a set of warning codes to a severity policy label.

    Priority: page > crit > warn.  Returns 'none' when no codes given.
    """
    codes = {x.strip() for x in str(warning_codes_csv or '').split(',') if x.strip() and x.strip() != 'none'}
    if not codes:
        return 'none'
    if codes & _policy_set(warn_codes_page_csv):
        return 'page'
    if codes & _policy_set(warn_codes_crit_csv):
        return 'crit'
    if codes & _policy_set(warn_codes_warn_csv):
        return 'warn'
    return 'warn'


def resolve_notifier_route_class(*, warning_severity_policy: str) -> str:
    """Determine notifier route class from warning severity policy."""
    return 'page' if str(warning_severity_policy or 'none') == 'page' else 'notify'


def build_drift_binding(
    r: Any,
    *,
    state_prefix: str,
    purpose: str,
    now_ms: int | None = None,
    warn_codes_warn_csv: str = '',
    warn_codes_crit_csv: str = '',
    warn_codes_page_csv: str = '',
) -> dict[str, str]:
    """Build the current deploy-lint drift snapshot for binding.

    P4.12: coarse hash binding (error_codes + active_purposes_hash).
    P4.13: semantic binding extended with gate_reason_code, errors_count, details_json fingerprint.
    P4.14: also binds warning_codes, warning_severity_policy, notifier_route_class.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    current = r.hgetall(lint_state_key(state_prefix, purpose)) or {}
    active_purposes: list[str] = []
    for candidate in sorted(CONTRACTS.keys()):
        raw = r.hgetall(lint_state_key(state_prefix, candidate)) or {}
        if str(raw.get('gate_active', '0')) == '1':
            active_purposes.append(candidate)
    error_codes = _codes_csv(current.get('error_codes', 'ok'))
    active_csv = _csv(active_purposes) or 'none'
    details_json = _details_json(current)
    # P4.14: warning codes from current lint state
    warning_codes_csv = _csv_tokens(current.get('warning_codes'), default='none')
    warning_policy = resolve_warning_severity_policy(
        warning_codes_csv,
        warn_codes_warn_csv=warn_codes_warn_csv,
        warn_codes_crit_csv=warn_codes_crit_csv,
        warn_codes_page_csv=warn_codes_page_csv,
    )
    notifier_route_class = resolve_notifier_route_class(warning_severity_policy=warning_policy)
    return {
        'binding_schema_version': '2',
        'purpose': str(purpose),
        'bound_snapshot_ts_ms': str(now_ms),
        'bound_error_codes': error_codes,
        'bound_error_codes_hash': _codes_hash(error_codes),
        'bound_active_purposes_csv': active_csv,
        'bound_active_purposes_hash': purposes_hash(active_purposes),
        # P4.13 semantic fields
        'bound_gate_reason_code': _s(current.get('gate_reason_code'), 'ok'),
        'bound_errors_count': str(_i(current.get('errors_count'), 0)),
        'bound_details_json': details_json,
        'bound_details_fingerprint': hashlib.sha1(details_json.encode('utf-8')).hexdigest(),
        # P4.14 warning-policy fields
        'bound_warning_codes': warning_codes_csv,
        'bound_warning_codes_hash': hashlib.sha1(warning_codes_csv.encode('utf-8')).hexdigest(),
        'bound_warning_severity_policy': warning_policy,
        'bound_notifier_route_class': notifier_route_class,
    }


def binding_mismatch_fields(st: DeployLintSilenceApprovalState, current_binding: dict[str, str] | None) -> list[str]:
    """Return list of field names that differ between approval snapshot and current drift state.

    P4.12: checks error_codes and active_purposes_hash.
    P4.13: also checks gate_reason_code, errors_count, details_fingerprint if binding_schema_version >= 2.
    P4.14: also checks warning_codes_hash, warning_severity_policy, notifier_route_class.
    """
    if not current_binding:
        return []
    mismatch: list[str] = []
    if st.bound_error_codes_hash and st.bound_error_codes_hash != _s(current_binding.get('bound_error_codes_hash')):
        mismatch.append('error_codes')
    if st.bound_active_purposes_hash and st.bound_active_purposes_hash != _s(current_binding.get('bound_active_purposes_hash')):
        mismatch.append('active_purposes_hash')
    if st.binding_schema_version >= 2:
        if st.bound_gate_reason_code and st.bound_gate_reason_code != _s(current_binding.get('bound_gate_reason_code'), 'ok'):
            mismatch.append('gate_reason_code')
        if st.bound_errors_count != _i(current_binding.get('bound_errors_count'), 0):
            mismatch.append('errors_count')
        if st.bound_details_fingerprint and st.bound_details_fingerprint != _s(current_binding.get('bound_details_fingerprint')):
            mismatch.append('details_fingerprint')
    # P4.14: warning-policy / notifier-route binding checks
    if st.bound_warning_codes_hash and st.bound_warning_codes_hash != _s(current_binding.get('bound_warning_codes_hash')):
        mismatch.append('warning_codes')
    if st.bound_warning_severity_policy and st.bound_warning_severity_policy != _s(current_binding.get('bound_warning_severity_policy')):
        mismatch.append('warning_severity_policy')
    if st.bound_notifier_route_class and st.bound_notifier_route_class != _s(current_binding.get('bound_notifier_route_class')):
        mismatch.append('notifier_route_class')
    return mismatch


def _binding_mismatch_reason(st: DeployLintSilenceApprovalState, current_binding: dict[str, str] | None) -> str:
    mismatch = binding_mismatch_fields(st, current_binding)
    if not mismatch:
        return ''
    return 'dual_control_drift_binding_mismatch:' + '+'.join(mismatch)


def prepare_override_approval(
    r: Any,
    *,
    prefix: str,
    purpose: str,
    operator: str,
    ticket: str,
    escalation_ticket: str,
    reason: str,
    minutes: int,
    ttl_s: int,
    prepared_freshness_s: int = 7200,
    ops_stream: str | None = None,
    request_id: str = '',
    drift_binding: dict[str, str] | None = None,
    now_ms: int | None = None,
) -> dict[str, str]:
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    rid = _s(request_id) or uuid.uuid4().hex
    freshness_deadline_ts_ms = _deadline_for_status(
        now_ms=now_ms,
        status='prepared',
        prepared_freshness_s=prepared_freshness_s,
        approved_freshness_s=1,
    )
    binding = dict(drift_binding or {})
    mapping = {
        'schema_version': '4',
        'binding_schema_version': _s(binding.get('binding_schema_version'), '2'),
        'request_id': rid,
        'purpose': str(purpose),
        'status': 'prepared',
        'prepared_by': str(operator),
        'prepared_ticket': str(ticket),
        'prepared_reason': str(reason),
        'escalation_ticket': str(escalation_ticket),
        'requested_minutes': str(max(1, int(minutes))),
        'prepared_ts_ms': str(now_ms),
        'freshness_deadline_ts_ms': str(freshness_deadline_ts_ms),
        'approved_by': '',
        'approved_reason': '',
        'approved_ts_ms': '0',
        'consumed_by': '',
        'consumed_ts_ms': '0',
        'expired_ts_ms': '0',
        'expired_reason': '',
        'cancelled_by': '',
        'cancelled_reason': '',
        'cancelled_ts_ms': '0',
        # P4.12 drift binding
        'bound_snapshot_ts_ms': _s(binding.get('bound_snapshot_ts_ms'), str(now_ms)),
        'bound_error_codes': _s(binding.get('bound_error_codes'), 'ok'),
        'bound_error_codes_hash': _s(binding.get('bound_error_codes_hash'), _codes_hash('ok')),
        'bound_active_purposes_csv': _s(binding.get('bound_active_purposes_csv'), 'none'),
        'bound_active_purposes_hash': _s(binding.get('bound_active_purposes_hash'), purposes_hash([])),
        # P4.13 semantic binding
        'bound_gate_reason_code': _s(binding.get('bound_gate_reason_code'), 'ok'),
        'bound_errors_count': _s(binding.get('bound_errors_count'), '0'),
        'bound_details_json': _s(binding.get('bound_details_json'), '{}'),
        'bound_details_fingerprint': _s(binding.get('bound_details_fingerprint'), _details_fingerprint(None)),
        # P4.14 warning-policy binding
        'bound_warning_codes': _s(binding.get('bound_warning_codes'), 'none'),
        'bound_warning_codes_hash': _s(binding.get('bound_warning_codes_hash')),
        'bound_warning_severity_policy': _s(binding.get('bound_warning_severity_policy'), 'none'),
        'bound_notifier_route_class': _s(binding.get('bound_notifier_route_class'), 'notify'),
        # P4.12 invalidation fields (reset on prepare)
        'invalidated_ts_ms': '0',
        'invalidated_reason': '',
        'invalidated_stage': '',
        'invalidated_error_codes': 'ok',
        'invalidated_error_codes_hash': '',
        'invalidated_active_purposes_csv': 'none',
        'invalidated_active_purposes_hash': '',
        # P4.13 invalidation semantic (reset on prepare)
        'invalidated_gate_reason_code': 'ok',
        'invalidated_errors_count': '0',
        'invalidated_details_json': '{}',
        'invalidated_details_fingerprint': '',
        # P4.14 invalidation warning-policy (reset on prepare)
        'invalidated_warning_codes': 'none',
        'invalidated_warning_codes_hash': '',
        'invalidated_warning_severity_policy': 'none',
        'invalidated_notifier_route_class': 'notify',
    }
    skey = state_key(prefix, rid)
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass
    try:
        r.set(latest_key(prefix, purpose), rid, ex=max(1, int(ttl_s)))
    except Exception:
        pass
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms),
        'kind': 'latency_deploy_lint_override_approval_prepared',
        'purpose': str(purpose),
        'request_id': rid,
        'operator': str(operator),
        'ticket': str(ticket),
        'escalation_ticket': str(escalation_ticket),
        'reason': str(reason),
        'requested_minutes': str(max(1, int(minutes))),
        'freshness_deadline_ts_ms': str(freshness_deadline_ts_ms),
        'bound_error_codes': mapping['bound_error_codes'],
        'bound_error_codes_hash': mapping['bound_error_codes_hash'],
        'bound_active_purposes_csv': mapping['bound_active_purposes_csv'],
        'bound_active_purposes_hash': mapping['bound_active_purposes_hash'],
        'bound_gate_reason_code': mapping['bound_gate_reason_code'],
        'bound_errors_count': mapping['bound_errors_count'],
        'bound_details_fingerprint': mapping['bound_details_fingerprint'],
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping


def approve_override_approval(
    r: Any,
    *,
    prefix: str,
    request_id: str,
    operator: str,
    reason: str,
    ttl_s: int,
    prepared_freshness_s: int = 7200,
    approved_freshness_s: int = 1800,
    ops_stream: str | None = None,
    now_ms: int | None = None,
) -> dict[str, str]:
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    skey = state_key(prefix, request_id)
    prev = r.hgetall(skey) or {}
    st = parse_approval_state(prev)
    if not st.present:
        raise ValueError('approval request not found')
    if st.status != 'prepared':
        raise ValueError('approval request is not in prepared state')
    if operator == st.prepared_by:
        raise ValueError('second approver must be different from requester')
    freshness_deadline_ts_ms = _deadline_for_status(
        now_ms=now_ms,
        status='approved',
        prepared_freshness_s=prepared_freshness_s,
        approved_freshness_s=approved_freshness_s,
    )
    mapping = {
        **prev,
        'status': 'approved',
        'approved_by': str(operator),
        'approved_reason': str(reason),
        'approved_ts_ms': str(now_ms),
        'freshness_deadline_ts_ms': str(freshness_deadline_ts_ms),
    }
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms),
        'kind': 'latency_deploy_lint_override_approval_approved',
        'purpose': st.purpose,
        'request_id': st.request_id,
        'prepared_by': st.prepared_by,
        'operator': str(operator),
        'ticket': st.prepared_ticket,
        'escalation_ticket': st.escalation_ticket,
        'reason': str(reason),
        'requested_minutes': str(st.requested_minutes),
        'freshness_deadline_ts_ms': str(freshness_deadline_ts_ms),
        'bound_error_codes_hash': st.bound_error_codes_hash,
        'bound_active_purposes_hash': st.bound_active_purposes_hash,
        'bound_gate_reason_code': st.bound_gate_reason_code,
        'bound_errors_count': str(st.bound_errors_count),
        'bound_details_fingerprint': st.bound_details_fingerprint,
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping


def invalidate_approval(
    r: Any,
    *,
    prefix: str,
    request_id: str,
    reason: str,
    stage: str,
    current_binding: dict[str, str] | None,
    ttl_s: int,
    ops_stream: str | None = None,
    now_ms: int | None = None,
) -> dict[str, str]:
    """Transition a prepared/approved request to invalidated when drift snapshot changed.

    P4.12: records coarse error_codes/purposes mismatch.
    P4.13: also records semantic gate_reason_code, errors_count, details_json snapshot.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    skey = state_key(prefix, request_id)
    prev = r.hgetall(skey) or {}
    st = parse_approval_state(prev)
    if not st.present:
        return {}
    if st.status == 'invalidated':
        return prev
    binding = dict(current_binding or {})
    mapping = {
        **prev,
        'status': 'invalidated',
        'invalidated_ts_ms': str(now_ms),
        'invalidated_reason': str(reason),
        'invalidated_stage': str(stage),
        # P4.12: coarse snapshot of current drift (for audit)
        'invalidated_error_codes': _s(binding.get('bound_error_codes'), 'ok'),
        'invalidated_error_codes_hash': _s(binding.get('bound_error_codes_hash')),
        'invalidated_active_purposes_csv': _s(binding.get('bound_active_purposes_csv'), 'none'),
        'invalidated_active_purposes_hash': _s(binding.get('bound_active_purposes_hash')),
        # P4.13: semantic snapshot of current drift
        'invalidated_gate_reason_code': _s(binding.get('bound_gate_reason_code'), 'ok'),
        'invalidated_errors_count': _s(binding.get('bound_errors_count'), '0'),
        'invalidated_details_json': _s(binding.get('bound_details_json'), '{}'),
        'invalidated_details_fingerprint': _s(binding.get('bound_details_fingerprint')),
        # P4.14: warning-policy snapshot of current drift
        'invalidated_warning_codes': _s(binding.get('bound_warning_codes'), 'none'),
        'invalidated_warning_codes_hash': _s(binding.get('bound_warning_codes_hash')),
        'invalidated_warning_severity_policy': _s(binding.get('bound_warning_severity_policy'), 'none'),
        'invalidated_notifier_route_class': _s(binding.get('bound_notifier_route_class'), 'notify'),
    }
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms),
        'kind': 'latency_deploy_lint_override_approval_invalidated',
        'purpose': st.purpose,
        'request_id': st.request_id,
        'prepared_by': st.prepared_by,
        'approved_by': st.approved_by,
        'ticket': st.prepared_ticket,
        'escalation_ticket': st.escalation_ticket,
        'requested_minutes': str(st.requested_minutes),
        'invalidate_stage': str(stage),
        'invalidate_reason': str(reason),
        'bound_error_codes_hash': st.bound_error_codes_hash,
        'bound_active_purposes_hash': st.bound_active_purposes_hash,
        'bound_gate_reason_code': st.bound_gate_reason_code,
        'bound_errors_count': str(st.bound_errors_count),
        'bound_details_fingerprint': st.bound_details_fingerprint,
        'current_error_codes_hash': _s(binding.get('bound_error_codes_hash')),
        'current_active_purposes_hash': _s(binding.get('bound_active_purposes_hash')),
        'current_gate_reason_code': _s(binding.get('bound_gate_reason_code'), 'ok'),
        'current_errors_count': _s(binding.get('bound_errors_count'), '0'),
        'current_details_fingerprint': _s(binding.get('bound_details_fingerprint')),
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping


def validate_approval_for_ack(
    raw: dict[str, str] | None,
    *,
    purpose: str,
    operator: str,
    ticket: str,
    escalation_ticket: str,
    minutes: int,
    current_binding: dict[str, str] | None = None,
    now_ms: int | None = None,
) -> ApprovalValidation:
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    st = parse_approval_state(raw)
    if not st.present:
        return ApprovalValidation(ok=False, reason='dual_control_approval_missing', state=st)
    if st.status == 'expired':
        return ApprovalValidation(ok=False, reason='dual_control_approval_expired', state=st)
    if st.status == 'cancelled':
        return ApprovalValidation(ok=False, reason='dual_control_approval_cancelled', state=st)
    if st.status == 'invalidated':
        return ApprovalValidation(ok=False, reason='dual_control_approval_invalidated', state=st)
    if st.status != 'approved':
        return ApprovalValidation(ok=False, reason='dual_control_not_approved', state=st)
    if st.freshness_deadline_ts_ms > 0 and now_ms > st.freshness_deadline_ts_ms:
        return ApprovalValidation(ok=False, reason='dual_control_approval_stale', state=st)
    if st.purpose != str(purpose):
        return ApprovalValidation(ok=False, reason='dual_control_purpose_mismatch', state=st)
    if st.prepared_by != str(operator):
        return ApprovalValidation(ok=False, reason='dual_control_requester_mismatch', state=st)
    if st.prepared_ticket != str(ticket):
        return ApprovalValidation(ok=False, reason='dual_control_ticket_mismatch', state=st)
    if st.escalation_ticket != str(escalation_ticket):
        return ApprovalValidation(ok=False, reason='dual_control_escalation_ticket_mismatch', state=st)
    if st.requested_minutes != max(1, int(minutes)):
        return ApprovalValidation(ok=False, reason='dual_control_minutes_mismatch', state=st)
    if not st.approved_by or st.approved_by == st.prepared_by:
        return ApprovalValidation(ok=False, reason='dual_control_invalid_approver', state=st)
    # P4.12 + P4.13: semantic drift binding check
    mismatch_reason = _binding_mismatch_reason(st, current_binding)
    if mismatch_reason:
        return ApprovalValidation(ok=False, reason=mismatch_reason, state=st, invalidate=True)
    return ApprovalValidation(ok=True, reason='ok', state=st)


def consume_approval(
    r: Any,
    *,
    prefix: str,
    request_id: str,
    operator: str,
    ttl_s: int,
    ops_stream: str | None = None,
    now_ms: int | None = None,
) -> dict[str, str]:
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    skey = state_key(prefix, request_id)
    prev = r.hgetall(skey) or {}
    st = parse_approval_state(prev)
    if not st.present:
        return {}
    mapping = {
        **prev,
        'status': 'consumed',
        'consumed_by': str(operator),
        'consumed_ts_ms': str(now_ms),
    }
    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass
    event_id = _xadd_best_effort(r, ops_stream, {
        'ts_ms': str(now_ms),
        'kind': 'latency_deploy_lint_override_approval_consumed',
        'purpose': st.purpose,
        'request_id': st.request_id,
        'prepared_by': st.prepared_by,
        'approved_by': st.approved_by,
        'operator': str(operator),
        'ticket': st.prepared_ticket,
        'escalation_ticket': st.escalation_ticket,
        'requested_minutes': str(st.requested_minutes),
        'bound_error_codes_hash': st.bound_error_codes_hash,
        'bound_active_purposes_hash': st.bound_active_purposes_hash,
        'bound_gate_reason_code': st.bound_gate_reason_code,
        'bound_errors_count': str(st.bound_errors_count),
        'bound_details_fingerprint': st.bound_details_fingerprint,
    })
    if event_id:
        r.hset(skey, mapping={'last_event_id': event_id})
    return r.hgetall(skey) or mapping
