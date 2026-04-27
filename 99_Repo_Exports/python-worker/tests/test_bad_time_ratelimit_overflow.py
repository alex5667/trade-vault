from __future__ import annotations

from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy


def test_hard_drop_ratelimit_escalates_freeze_and_quarantine():
    # tiny bucket -> overflow quickly
    pol = BadTimeQuarantinePolicy(
        trigger_streak=999,
        state_trigger_streak=999,
        trigger_score=999.0,
        state_trigger_score=999.0,
        hard_drops_rate_per_sec=1.0,
        hard_drops_burst=2.0,
        ratelimit_penalty_freeze_ms=8000,
        ratelimit_penalty_quarantine_ms=15000,
    )
    q = BadTimeQuarantine(pol)
    now = 1_700_000_000_000

    # 1st, 2nd allowed; 3rd should overflow (same timestamp => no refill)
    q.on_hard_drop("future_hard", now)
    q.on_hard_drop("future_hard", now)
    assert not q.is_state_frozen(now)

    q.on_hard_drop("future_hard", now)
    assert q.is_state_frozen(now)
    assert q.is_quarantined(now)


def test_reorder_soft_streak_triggers_state_freeze():
    pol = BadTimeQuarantinePolicy(
        trigger_streak=999,
        state_trigger_streak=999,
        trigger_score=999.0,
        state_trigger_score=999.0,
        reorder_soft_streak_trigger=5,
        ratelimit_penalty_freeze_ms=3000,
    )
    q = BadTimeQuarantine(pol)

    # emulate consecutive reorder_soft
    for _ in range(5):
        q.on_soft_event("reorder_soft")

    now = q._now_ms_fallback()
    assert q.is_state_frozen(now)
