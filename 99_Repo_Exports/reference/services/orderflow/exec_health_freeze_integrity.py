from __future__ import annotations

"""P8/P9/P10 freeze integrity evaluator — pure logic, no Redis I/O.

Evaluates whether the freeze control/state hashes and the freeze event stream
are consistent with each other. Used by exec_health_freeze_integrity_exporter_v1.py.

Violation kinds detected:
  control_missing_pending_ack       — control hash gone but pending ack nonce exists
  state_missing_pending_ack         — state hash gone but pending ack nonce exists
  control_state_missing_without_valid_ack — both gone, trigger event still in stream
  thaw_without_valid_ack_event      — thaw recorded in control but no valid signed ack event found
  invalid_ack_event_signature       — ack event in stream has invalid HMAC signature
  invalid_control_ack_signature     — thaw written in control/state has invalid HMAC signature
  none                              — no violations detected

P9 update: dual-control commit events (kind=manual_ack_thaw_commit) are verified
using verify_thaw_release_signature which enforces the dual-control constraint.

P10 update: request log violations from evaluate_request_log_sequence are merged.
Direct hash edit without a backing request-log sequence is treated as tamper.
tamper_refreeze_latch events are also accepted as valid trigger events.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from services.orderflow.exec_health_freeze_control import parse_exec_health_freeze_control, verify_manual_ack_signature, verify_thaw_release_signature
from services.orderflow.exec_health_freeze_sealed_state import verify_sealed_hash
from services.orderflow.exec_health_freeze_request_log import REQUEST_LOG_VIOLATION_KINDS, evaluate_request_log_sequence


VIOLATION_KINDS = [
    "none",
    "control_missing_pending_ack",
    "state_missing_pending_ack",
    "control_state_missing_without_valid_ack",
    "thaw_without_valid_ack_event",
    "invalid_ack_event_signature",
    "invalid_control_ack_signature",
    # P11: нарушения сильного — прямое редактирование hash мимо FCALL entrypoints
    "control_seal_missing",
    "control_seal_invalid",
    "state_seal_missing",
    "state_seal_invalid",
    *[k for k in REQUEST_LOG_VIOLATION_KINDS if k != "none"],
]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else str(d)
    except Exception:
        return str(d)


def _event_payload(rec: Any) -> Tuple[str, Dict[str, Any]]:
    """Extract (event_id, payload_dict) from a Redis stream record tuple."""
    if isinstance(rec, (list, tuple)) and len(rec) >= 2:
        return str(rec[0]), dict(rec[1] or {})
    return "", {}


@dataclass(frozen=True)
class FreezeIntegrityResult:
    """Result of a single freeze integrity evaluation cycle."""
    control_present: bool
    state_present: bool
    pending_ack_nonce: str
    pending_trigger_ts_ms: int
    valid_ack_event_present: bool
    valid_ack_event_id: str
    latest_trigger_event_id: str
    latest_trigger_nonce: str
    invalid_ack_event_present: bool
    # P10: request log evaluation results
    request_log_valid_sequence: bool
    request_log_request_id: str
    violation_kinds: List[str]


class _ReqSeqProxy:
    """Fallback proxy when no request_events are provided."""
    request_id: str = ""
    commit_event_id: str = ""
    valid_sequence: bool = False
    violation_kinds: List[str] = ["none"]


def find_latest_trigger_event(events: Sequence[Any]) -> Tuple[str, Dict[str, Any]]:
    """Return (event_id, payload) for the most recent autoguard_freeze_latch or tamper_refreeze_latch event.

    P10: tamper_refreeze_latch events are also valid trigger events so the guard
    can detect re-freeze triggers from tamper detection, not just autoguard.
    """
    for rec in events:
        eid, payload = _event_payload(rec)
        if _s(payload.get("kind")) in {"autoguard_freeze_latch", "tamper_refreeze_latch"}:
            return eid, payload
    return "", {}


def _event_to_control_like(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Map a stream event payload to the shape expected by verify_*_signature.

    P9: handles both legacy manual_ack_thaw and new manual_ack_thaw_commit kinds.
    """
    kind = _s(payload.get('kind'), '')
    if kind == 'manual_ack_thaw_commit':
        # Dual-control commit event — map to dual-control control-like shape
        return {
            'manual_override_action': 'thaw',
            'manual_override_active': 1,
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
    # Legacy P8 manual_ack_thaw event
    return {
        'manual_override_action': 'thaw',
        'manual_ack_sig': _s(payload.get('ack_sig'), ''),
        'manual_ack_nonce': _s(payload.get('ack_nonce'), ''),
        'manual_ack_operator': _s(payload.get('operator'), ''),
        'manual_ack_reason': _s(payload.get('reason'), ''),
        'manual_ack_ticket': _s(payload.get('ticket'), ''),
        'manual_ack_ts_ms': _i(payload.get('ts_ms'), 0),
        'last_trigger_ts_ms': _i(payload.get('trigger_ts_ms'), 0),
    }


def find_ack_events_for_nonce(events: Sequence[Any], nonce: str) -> List[Tuple[str, Dict[str, Any]]]:
    """Return all ack events (both P8 and P9 kinds) matching the given pending nonce."""
    out: List[Tuple[str, Dict[str, Any]]] = []
    for rec in events:
        eid, payload = _event_payload(rec)
        # P9: accept both legacy manual_ack_thaw and new manual_ack_thaw_commit
        if _s(payload.get("ack_nonce")) == str(nonce) and _s(payload.get('kind')) in {'manual_ack_thaw', 'manual_ack_thaw_commit'}:
            out.append((eid, payload))
    return out


def evaluate_freeze_integrity(
    *,
    control_raw: Mapping[str, Any] | None,
    state_raw: Mapping[str, Any] | None,
    events: Sequence[Any] | None,
    request_events: Sequence[Any] | None = None,
    now_ms: int | None = None,
) -> FreezeIntegrityResult:
    """Evaluate the consistency of control/state hashes and event stream.

    Logic:
    1. Parse control and state via standard parser
    2. Extract pending nonce from control/state or latest trigger event in stream
    3. Search for a signed ack event matching the pending nonce
    4. Verify ack event signature (P9: dual-control check for commit events) and control/state signature
    5. P10: evaluate append-only request log sequence and merge violations
    6. Raise violations for any detected tampering or unsigned thaw

    Returns FreezeIntegrityResult with all detected violation kinds.
    """
    _ = int(now_ms or _now_ms())
    control = parse_exec_health_freeze_control(control_raw or {})
    state = parse_exec_health_freeze_control(state_raw or {})
    evs = list(events or [])
    req_evs = list(request_events or [])

    control_present = bool(getattr(control, 'raw_payload', None))
    state_present = bool(getattr(state, 'raw_payload', None))

    # P11: проверяем целостность seal для control и state —
    # невалидный seal означает прямую запись мимо whitelist FCALL entrypoints
    control_seal_valid = False
    state_seal_valid = False
    if control_present:
        control_seal_valid = verify_sealed_hash(control.raw_payload)
    if state_present:
        state_seal_valid = verify_sealed_hash(state.raw_payload)

    # Derive pending nonce: prefer control/state, then fall back to last stream event
    pending_ack_nonce = control.expected_ack_nonce or state.expected_ack_nonce
    pending_trigger_ts_ms = _i(control.raw_payload.get('last_trigger_ts_ms'), 0) or _i(state.raw_payload.get('last_trigger_ts_ms'), 0)

    latest_trigger_event_id, latest_trigger_event = find_latest_trigger_event(evs)
    latest_trigger_nonce = _s(latest_trigger_event.get('ack_nonce'), '')
    if not pending_ack_nonce:
        pending_ack_nonce = latest_trigger_nonce
    if pending_trigger_ts_ms <= 0:
        pending_trigger_ts_ms = _i(latest_trigger_event.get('trigger_ts_ms'), 0)

    # Search for a valid signed ack event for the pending nonce (legacy + P9 events)
    valid_ack_event_present = False
    valid_ack_event_id = ''
    invalid_ack_event_present = False
    if pending_ack_nonce:
        for eid, payload in find_ack_events_for_nonce(evs, pending_ack_nonce):
            event_like = _event_to_control_like(payload)
            # P9: route to correct validator based on event kind
            if _s(payload.get('kind')) == 'manual_ack_thaw_commit':
                ok = verify_thaw_release_signature(event_like)
            else:
                ok = verify_manual_ack_signature(event_like)
            if ok:
                valid_ack_event_present = True
                valid_ack_event_id = eid
                break
            invalid_ack_event_present = True

    # P10: evaluate request log sequence — this is the true SoT for thaw authorisation
    request_log_res = evaluate_request_log_sequence(
        control_raw=control.raw_payload,
        request_events=req_evs,
        request_id=control.active_thaw_request_id or control.manual_commit_request_id or None,
    )

    violations: List[str] = []

    # P11: проверяем seal нарушения до остальных проверок интегритности
    if control_present:
        if not _s(control.raw_payload.get('seal_digest')) or _i(control.raw_payload.get('seal_version'), 0) <= 0:
            violations.append('control_seal_missing')
        elif not control_seal_valid:
            violations.append('control_seal_invalid')
    if state_present:
        if not _s(state.raw_payload.get('seal_digest')) or _i(state.raw_payload.get('seal_version'), 0) <= 0:
            violations.append('state_seal_missing')
        elif not state_seal_valid:
            violations.append('state_seal_invalid')

    # Check control/state for an unsigned thaw record
    # P9: verify_thaw_release_signature enforces dual-control by default
    # P10: also require valid request log sequence
    if control_present and control.manual_override_action == 'thaw' and control.manual_override_active:
        if not verify_thaw_release_signature(control.raw_payload):
            violations.append('invalid_control_ack_signature')
        elif not valid_ack_event_present and not request_log_res.valid_sequence:
            # P10: thaw is only considered untrusted if NEITHER the event stream
            # NOR the request log can back it up.
            violations.append('thaw_without_valid_ack_event')

    # Check for disappearance of control/state while a pending ack nonce exists
    # P10: a valid request-log sequence or valid ack event also satisfies the pending ack requirement
    ack_satisfied = bool(valid_ack_event_present or request_log_res.valid_sequence)
    pending_ack = bool((control.manual_ack_required or state.manual_ack_required or pending_ack_nonce) and not ack_satisfied)
    if pending_ack and not control_present and state_present:
        violations.append('control_missing_pending_ack')
    if pending_ack and control_present and not state_present:
        violations.append('state_missing_pending_ack')
    if pending_ack and not control_present and not state_present and latest_trigger_nonce:
        violations.append('control_state_missing_without_valid_ack')
    if invalid_ack_event_present and not valid_ack_event_present and not request_log_res.valid_sequence:
        violations.append('invalid_ack_event_signature')

    # P10: merge request log violations
    for kind in request_log_res.violation_kinds:
        if kind != 'none':
            violations.append(kind)

    # P10: direct hash edit without corresponding request-log commit sequence is tamper
    if control_present and control.manual_override_action == 'thaw' and control.manual_override_active and not request_log_res.valid_sequence:
        if 'thaw_without_valid_ack_event' not in violations:
            violations.append('thaw_without_valid_ack_event')

    if not violations:
        violations = ['none']

    return FreezeIntegrityResult(
        control_present=control_present,
        state_present=state_present,
        pending_ack_nonce=str(pending_ack_nonce or ''),
        pending_trigger_ts_ms=int(pending_trigger_ts_ms or 0),
        valid_ack_event_present=bool(valid_ack_event_present or request_log_res.valid_sequence),
        valid_ack_event_id=str(valid_ack_event_id or request_log_res.commit_event_id or ''),
        latest_trigger_event_id=str(latest_trigger_event_id or ''),
        latest_trigger_nonce=str(latest_trigger_nonce or ''),
        invalid_ack_event_present=bool(invalid_ack_event_present),
        request_log_valid_sequence=bool(request_log_res.valid_sequence),
        request_log_request_id=str(request_log_res.request_id or ''),
        violation_kinds=violations,
    )
