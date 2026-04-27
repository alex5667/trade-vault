from __future__ import annotations

from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy


def test_state_freeze_triggers_stronger_than_emission_quarantine():
    pol = BadTimeQuarantinePolicy(
        trigger_streak=2,
        quarantine_ms=5000,
        state_trigger_streak=4,
        state_freeze_ms=3000,
        trigger_score=999.0,
        state_trigger_score=999.0,
    )
    q = BadTimeQuarantine(pol)
    now = 1_700_000_000_000

    q.on_hard_drop("future_hard", now)
    q.on_hard_drop("future_hard", now)
    assert q.is_quarantined(now)
    assert not q.is_state_frozen(now)

    q.on_hard_drop("future_hard", now)
    q.on_hard_drop("future_hard", now)
    assert q.is_state_frozen(now)
    assert q.is_quarantined(now)


def test_state_freeze_expires():
    pol = BadTimeQuarantinePolicy(
        trigger_streak=1,
        quarantine_ms=10000,
        state_trigger_streak=1,
        state_freeze_ms=1000,
        trigger_score=999.0,
        state_trigger_score=999.0,
    )
    q = BadTimeQuarantine(pol)
    now = 1_700_000_000_000

    q.on_hard_drop("past_hard", now)
    assert q.is_state_frozen(now)
    assert not q.is_state_frozen(now + 1000)
