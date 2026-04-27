from orderflow_services.rollback_state_machine_v1 import (
    EVENT_REQUEST_CREATED,
    EVENT_ROLLBACK_EXECUTED,
    EVENT_VERIFY_PASS,
    EVENT_VERIFY_FAIL,
    EVENT_VERIFY_INCONCLUSIVE,
    EVENT_ROLLBACK_ERROR,
    STATE_REQUESTED,
    STATE_EXECUTED,
    STATE_ROLLBACK_SUCCESS,
    STATE_ROLLBACK_FAILED,
    STATE_MANUAL_REVIEW,
    apply_event,
)


def test_happy_path_transitions():
    t1 = apply_event(None, EVENT_REQUEST_CREATED)
    assert t1.next_state == STATE_REQUESTED

    t2 = apply_event(t1.next_state, EVENT_ROLLBACK_EXECUTED)
    assert t2.next_state == STATE_EXECUTED

    t3 = apply_event(t2.next_state, EVENT_VERIFY_PASS)
    assert t3.next_state == STATE_ROLLBACK_SUCCESS


def test_fail_path_transitions():
    t1 = apply_event(None, EVENT_REQUEST_CREATED)
    t2 = apply_event(t1.next_state, EVENT_ROLLBACK_ERROR)
    assert t2.next_state == STATE_ROLLBACK_FAILED


def test_inconclusive_goes_manual_review():
    t1 = apply_event(None, EVENT_REQUEST_CREATED)
    t2 = apply_event(t1.next_state, EVENT_ROLLBACK_EXECUTED)
    t3 = apply_event(t2.next_state, EVENT_VERIFY_INCONCLUSIVE)
    assert t3.next_state == STATE_MANUAL_REVIEW


def test_invalid_terminal_transition_raises():
    t1 = apply_event(None, EVENT_REQUEST_CREATED)
    t2 = apply_event(t1.next_state, EVENT_ROLLBACK_EXECUTED)
    t3 = apply_event(t2.next_state, EVENT_VERIFY_FAIL)
    assert t3.next_state == STATE_ROLLBACK_FAILED
    try:
        apply_event(t3.next_state, EVENT_VERIFY_PASS)
    except ValueError:
        assert True
    else:
        assert False
