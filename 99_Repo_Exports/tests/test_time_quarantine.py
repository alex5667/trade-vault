from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy


def test_quarantine_on_hard_streak():
    policy = BadTimeQuarantinePolicy(hard_drop_streak_threshold=3, quarantine_ttl_ms=1000)
    q = BadTimeQuarantine(policy=policy, inc=None)

    now = 1_000_000
    q.on_hard_drop("future", now)
    assert not q.is_quarantined(now)
    q.on_hard_drop("future", now + 1)
    assert not q.is_quarantined(now + 1)
    q.on_hard_drop("future", now + 2)
    assert q.is_quarantined(now + 2)


def test_recover_after_ttl():
    policy = BadTimeQuarantinePolicy(hard_drop_streak_threshold=1, quarantine_ttl_ms=100)
    q = BadTimeQuarantine(policy=policy, inc=None)

    now = 1_000_000
    q.on_hard_drop("past", now)
    assert q.is_quarantined(now)
    assert not q.is_quarantined(now + 200)

