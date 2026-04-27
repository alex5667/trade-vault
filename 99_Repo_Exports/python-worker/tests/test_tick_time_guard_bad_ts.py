from __future__ import annotations

from common.tick_time import TickTimeGuard, TickTimePolicy


def test_bad_ts_zero_or_negative_is_dropped():
    now = 1_700_000_000_000
    g = TickTimeGuard(TickTimePolicy(), now_provider=lambda: now)

    r0 = g.sanitize_ts_ms(0, now_ms=now)
    assert r0 is not None
    assert r0.drop_reason == "bad_ts"

    r1 = g.sanitize_ts_ms(-123, now_ms=now)
    assert r1 is not None
    assert r1.drop_reason == "bad_ts"
