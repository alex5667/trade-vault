from __future__ import annotations

from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy


def test_quarantine_triggers_by_streak():
    pol = BadTimeQuarantinePolicy(trigger_streak=3, quarantine_ms=5000, trigger_score=999.0)
    q = BadTimeQuarantine(pol)
    now = 1_700_000_000_000
    assert not q.is_quarantined(now)

    q.on_hard_drop("future_hard", now)
    assert not q.is_quarantined(now)
    q.on_hard_drop("future_hard", now)
    assert not q.is_quarantined(now)

    q.on_hard_drop("future_hard", now)
    assert q.is_quarantined(now)
    assert q.until_ms == now + 5000


def test_quarantine_expires():
    pol = BadTimeQuarantinePolicy(trigger_streak=2, quarantine_ms=1000, trigger_score=999.0)
    q = BadTimeQuarantine(pol)
    now = 1_700_000_000_000
    q.on_hard_drop("past_hard", now)
    q.on_hard_drop("past_hard", now)
    assert q.is_quarantined(now)
    assert not q.is_quarantined(now + 1000)


def test_soft_events_do_not_increase_streak_but_increase_score():
    pol = BadTimeQuarantinePolicy(trigger_streak=10, quarantine_ms=5000, trigger_score=1.0, soft_penalty=0.5)
    q = BadTimeQuarantine(pol)
    now = 1_700_000_000_000
    assert q.hard_streak == 0
    assert q.score == 0.0

    q.on_soft_event("clamped_soft_future")
    assert q.hard_streak == 0
    assert q.score == 0.5

    # second soft event crosses trigger_score => quarantine by score
    q.on_soft_event("reorder_soft")
    # NOTE: score-trigger quarantine is enabled only by hard_drop in this design.
    # Soft events should NOT alone quarantine; they are used to accelerate quarantine once hard drops happen.
    assert not q.is_quarantined(now)


def test_ok_tick_decays_score_and_resets_streak():
    pol = BadTimeQuarantinePolicy(trigger_streak=3, quarantine_ms=5000, decay_per_ok=0.5)
    q = BadTimeQuarantine(pol)
    now = 1_700_000_000_000
    q.on_hard_drop("reorder_hard", now)
    q.on_hard_drop("reorder_hard", now)
    assert q.hard_streak == 2
    assert q.score >= 2.0
    q.on_ok_tick()
    assert q.hard_streak == 0
    assert q.score == max(0.0, 2.0 - 0.5)
