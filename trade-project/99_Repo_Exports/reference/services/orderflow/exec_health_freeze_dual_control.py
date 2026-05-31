from __future__ import annotations

"""P9 dual-control thaw evaluator — pure logic, no Redis I/O.

Evaluates the dual-control thaw chain (prepare → approve → commit) and detects
violations such as same-operator approval, missing events, or invalid commit signatures.
Used by exec_health_freeze_dual_control_exporter_v1.py.

Violation kinds:
  none                              — no violations detected
  prepare_missing                   — thaw status implies prepare but no prepare event found
  approval_missing                  — thaw status implies approval but no approve event found
  commit_missing                    — thaw status is 'approved' but no commit yet
  same_operator_dual_control_violation — prepared_by == approved_by (four-eyes violation)
  commit_without_prepare            — commit event exists but no prepare event
  commit_without_approval           — commit event exists but no approve event
  invalid_commit_signature          — commit event signature failed verification
  invalid_control_commit_signature  — control hash thaw signature failed verification
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from services.orderflow.exec_health_freeze_control import parse_exec_health_freeze_control, verify_dual_control_commit_signature


def _now_ms() -> int:
    return int(time.time() * 1000)


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else str(d)
    except Exception:
        return str(d)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


DUAL_CONTROL_VIOLATION_KINDS = [
    'none',
    'prepare_missing',
    'approval_missing',
    'commit_missing',
    'same_operator_dual_control_violation',
    'commit_without_prepare',
    'commit_without_approval',
    'invalid_commit_signature',
    'invalid_control_commit_signature',
]


@dataclass(frozen=True)
class FreezeDualControlResult:
    """Result of a single dual-control thaw evaluation cycle."""
    request_id: str
    request_status: str
    request_nonce: str
    pending_request: bool
    dual_control_ready: bool
    prepare_event_present: bool
    approve_event_present: bool
    commit_event_present: bool
    valid_commit_event_present: bool
    prepared_by: str
    approved_by: str
    commit_by: str
    same_operator_violation: bool
    violation_kinds: List[str]


def _event_payload(rec: Any) -> Tuple[str, Dict[str, Any]]:
    if isinstance(rec, (list, tuple)) and len(rec) >= 2:
        return str(rec[0]), dict(rec[1] or {})
    return '', {}


def _find_latest(events: Sequence[Any], *, kind: str, request_id: str) -> Tuple[str, Dict[str, Any]]:
    """Find the latest event of a given kind for a specific request_id."""
    for rec in events:
        eid, payload = _event_payload(rec)
        if _s(payload.get('kind')) == kind and _s(payload.get('request_id')) == str(request_id):
            return eid, payload
    return '', {}


def _commit_event_to_control_like(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Map a manual_ack_thaw_commit event payload to the shape expected by verify_dual_control_commit_signature."""
    return {
        'manual_override_action': 'thaw',
        'active_thaw_request_id': _s(payload.get('request_id')),
        'manual_commit_request_id': _s(payload.get('request_id')),
        'thaw_request_nonce': _s(payload.get('ack_nonce')),
        'manual_ack_nonce': _s(payload.get('ack_nonce')),
        'thaw_prepared_by': _s(payload.get('prepared_by')),
        'thaw_approved_by': _s(payload.get('approved_by')),
        'manual_commit_by': _s(payload.get('operator')),
        'thaw_request_reason': _s(payload.get('reason')),
        'thaw_request_ticket': _s(payload.get('ticket')),
        'last_trigger_ts_ms': _i(payload.get('trigger_ts_ms'), 0),
        'thaw_prepare_ts_ms': _i(payload.get('prepared_ts_ms'), 0),
        'thaw_approve_ts_ms': _i(payload.get('approved_ts_ms'), 0),
        'manual_commit_ts_ms': _i(payload.get('ts_ms'), 0),
        'manual_ack_ts_ms': _i(payload.get('ts_ms'), 0),
        'manual_commit_sig': _s(payload.get('commit_sig')),
        'manual_ack_sig': _s(payload.get('commit_sig')),
    }


def evaluate_freeze_dual_control(
    *,
    control_raw: Mapping[str, Any] | None,
    state_raw: Mapping[str, Any] | None,
    events: Sequence[Any] | None,
    now_ms: int | None = None,
) -> FreezeDualControlResult:
    """Evaluate the dual-control thaw chain integrity.

    Checks:
    - Both prepare and approve events are present for the active request
    - The approver differs from the preparer (four-eyes)
    - A valid signed commit event exists for the request
    - The control hash thaw signature is valid if override_action='thaw'

    Returns FreezeDualControlResult with all detected violations.
    """
    _ = int(now_ms or _now_ms())
    control = parse_exec_health_freeze_control(control_raw or {})
    state = parse_exec_health_freeze_control(state_raw or {})
    evs = list(events or [])

    # Derive dual-control state from control hash (state hash is fallback)
    request_id = control.active_thaw_request_id or state.active_thaw_request_id
    request_status = control.thaw_request_status or state.thaw_request_status
    request_nonce = control.thaw_request_nonce or state.thaw_request_nonce or control.expected_ack_nonce or state.expected_ack_nonce
    prepared_by = control.thaw_prepared_by or state.thaw_prepared_by
    approved_by = control.thaw_approved_by or state.thaw_approved_by
    commit_by = control.manual_commit_by or state.manual_commit_by or control.manual_ack_operator or state.manual_ack_operator

    # Find phase events in stream
    prepare_eid, prepare_ev = _find_latest(evs, kind='manual_ack_thaw_prepare', request_id=request_id)
    approve_eid, approve_ev = _find_latest(evs, kind='manual_ack_thaw_approve', request_id=request_id)
    commit_eid, commit_ev = _find_latest(evs, kind='manual_ack_thaw_commit', request_id=request_id)

    # Determine presence from both events and control hash status
    prepare_present = bool(prepare_eid or request_status in {'prepared', 'approved', 'committed'})
    approve_present = bool(approve_eid or (request_status in {'approved', 'committed'} and approved_by))
    commit_present = bool(commit_eid or request_status == 'committed' or control.manual_override_action == 'thaw')
    same_operator = bool(prepared_by and approved_by and prepared_by == approved_by)

    # Verify commit event signature
    valid_commit_event_present = False
    if commit_present:
        if commit_ev:
            valid_commit_event_present = verify_dual_control_commit_signature(_commit_event_to_control_like(commit_ev))
        elif control.manual_override_action == 'thaw':
            # No stream event — try to verify directly from control hash
            valid_commit_event_present = verify_dual_control_commit_signature(control.raw_payload)

    pending_request = bool(request_id and request_status in {'prepared', 'approved'})
    dual_control_ready = bool(request_id and prepare_present and approve_present and not same_operator)

    violations: List[str] = []
    if request_id:
        if request_status in {'prepared', 'approved', 'committed'} and not prepare_present:
            violations.append('prepare_missing')
        if request_status in {'approved', 'committed'} and not approve_present:
            violations.append('approval_missing')
        if request_status == 'approved' and not commit_present:
            violations.append('commit_missing')
    if commit_present and not prepare_present:
        violations.append('commit_without_prepare')
    if commit_present and not approve_present:
        violations.append('commit_without_approval')
    if same_operator:
        violations.append('same_operator_dual_control_violation')
    if commit_present and not valid_commit_event_present:
        violations.append('invalid_commit_signature')
    if control.manual_override_action == 'thaw' and control.manual_override_active and not verify_dual_control_commit_signature(control.raw_payload):
        violations.append('invalid_control_commit_signature')

    if not violations:
        violations = ['none']

    return FreezeDualControlResult(
        request_id=str(request_id or ''),
        request_status=str(request_status or ''),
        request_nonce=str(request_nonce or ''),
        pending_request=bool(pending_request),
        dual_control_ready=bool(dual_control_ready),
        prepare_event_present=bool(prepare_present),
        approve_event_present=bool(approve_present),
        commit_event_present=bool(commit_present),
        valid_commit_event_present=bool(valid_commit_event_present),
        prepared_by=str(prepared_by or ''),
        approved_by=str(approved_by or ''),
        commit_by=str(commit_by or ''),
        same_operator_violation=bool(same_operator),
        violation_kinds=violations,
    )
