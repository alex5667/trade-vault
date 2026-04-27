from __future__ import annotations

from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy


def test_recovery_gate_requires_n_ok_after_freeze():
    pol = BadTimeQuarantinePolicy(
        # make freeze easy
        state_trigger_streak=1,
        state_freeze_ms=1000,
        trigger_streak=999,
        trigger_score=999.0,
        state_trigger_score=999.0,
        recovery_ok_streak=3,
        recovery_max_ms=10_000,
    )
    q = BadTimeQuarantine(pol)
    t0 = 1_700_000_000_000

    # hard drop triggers state_freeze immediately
    q.on_hard_drop("future_hard", t0)
    assert q.is_state_frozen(t0)
    assert q.should_suppress_processing(t0)

    # after freeze ends -> recovery starts automatically and still suppresses
    t1 = t0 + 1000 + 1
    assert not q.is_state_frozen(t1)
    assert q.should_suppress_processing(t1)
    assert q.is_in_recovery(t1)

    # 1 ok -> still suppress
    q.on_ok_tick()
    assert q.should_suppress_processing(t1 + 1)

    # 2 ok -> still suppress
    q.on_ok_tick()
    assert q.should_suppress_processing(t1 + 2)

    # 3 ok -> pass recovery; suppress should stop
    q.on_ok_tick()
    assert not q.should_suppress_processing(t1 + 3)
