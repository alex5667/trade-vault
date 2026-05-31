from __future__ import annotations

from dataclasses import dataclass

STATE_REQUESTED = "REQUESTED"
STATE_EXECUTED = "EXECUTED"
STATE_VERIFY_PENDING = "VERIFY_PENDING"
STATE_ROLLBACK_SUCCESS = "ROLLBACK_SUCCESS"
STATE_ROLLBACK_FAILED = "ROLLBACK_FAILED"
STATE_MANUAL_REVIEW = "MANUAL_REVIEW"

EVENT_REQUEST_CREATED = "REQUEST_CREATED"
EVENT_ROLLBACK_EXECUTED = "ROLLBACK_EXECUTED"
EVENT_VERIFY_START = "VERIFY_START"
EVENT_VERIFY_PASS = "VERIFY_PASS"
EVENT_VERIFY_FAIL = "VERIFY_FAIL"
EVENT_VERIFY_INCONCLUSIVE = "VERIFY_INCONCLUSIVE"
EVENT_ROLLBACK_ERROR = "ROLLBACK_ERROR"
EVENT_MANUAL_ESCALATE = "MANUAL_ESCALATE"

TERMINAL_STATES = {
    STATE_ROLLBACK_SUCCESS,
    STATE_ROLLBACK_FAILED,
    STATE_MANUAL_REVIEW,
}

_ALLOWED_TRANSITIONS: dict[str | None, dict[str, str]] = {
    None: {
        EVENT_REQUEST_CREATED: STATE_REQUESTED,
    },
    STATE_REQUESTED: {
        EVENT_ROLLBACK_EXECUTED: STATE_EXECUTED,
        EVENT_ROLLBACK_ERROR: STATE_ROLLBACK_FAILED,
        EVENT_MANUAL_ESCALATE: STATE_MANUAL_REVIEW,
    },
    STATE_EXECUTED: {
        EVENT_VERIFY_START: STATE_VERIFY_PENDING,
        EVENT_VERIFY_PASS: STATE_ROLLBACK_SUCCESS,
        EVENT_VERIFY_FAIL: STATE_ROLLBACK_FAILED,
        EVENT_VERIFY_INCONCLUSIVE: STATE_MANUAL_REVIEW,
        EVENT_MANUAL_ESCALATE: STATE_MANUAL_REVIEW,
    },
    STATE_VERIFY_PENDING: {
        EVENT_VERIFY_PASS: STATE_ROLLBACK_SUCCESS,
        EVENT_VERIFY_FAIL: STATE_ROLLBACK_FAILED,
        EVENT_VERIFY_INCONCLUSIVE: STATE_MANUAL_REVIEW,
        EVENT_MANUAL_ESCALATE: STATE_MANUAL_REVIEW,
    }
}


@dataclass(frozen=True)
class Transition:
    prev_state: str | None
    event: str
    next_state: str
    changed: bool


def is_terminal(state: str | None) -> bool:
    return bool(state in TERMINAL_STATES)


def next_state_for_event(prev_state: str | None, event: str) -> str:
    if is_terminal(prev_state):
        raise ValueError(f"terminal state has no outgoing transitions: {prev_state}")
    try:
        return _ALLOWED_TRANSITIONS[prev_state][event]
    except KeyError as exc:
        raise ValueError(f"invalid rollback transition: {prev_state!r} + {event!r}") from exc


def apply_event(prev_state: str | None, event: str) -> Transition:
    ns = next_state_for_event(prev_state, event)
    return Transition(prev_state=prev_state, event=event, next_state=ns, changed=(ns != prev_state))


def infer_event_from_result_payload(payload: dict[str, str]) -> str:
    status = (payload.get("status", "") or "").upper()
    if status in {"OK", "EXECUTED", "DONE", "SUCCESS"}:
        return EVENT_ROLLBACK_EXECUTED
    return EVENT_ROLLBACK_ERROR


def infer_event_from_verification_payload(payload: dict[str, str]) -> str:
    status = str(payload.get("verification_status", "") or payload.get("status", "")).upper()
    if status in {"PASS", "ROLLBACK_SUCCESS"}:
        return EVENT_VERIFY_PASS
    if status in {"FAIL", "ROLLBACK_FAILED"}:
        return EVENT_VERIFY_FAIL
    return EVENT_VERIFY_INCONCLUSIVE


def bounded_reason_codes(state: str, reason_codes: list[str]) -> list[str]:
    reason_codes = [str(x) for x in (reason_codes or []) if str(x)]
    if state == STATE_ROLLBACK_SUCCESS:
        return reason_codes[:4]
    if state == STATE_MANUAL_REVIEW:
        return reason_codes[:8]
    if state == STATE_ROLLBACK_FAILED:
        return reason_codes[:8]
    return reason_codes[:6]
