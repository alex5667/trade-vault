from __future__ import annotations

from common.tick_time import TickTimeGuard, TickTimePolicy


def test_seconds_are_normalized_to_ms_and_accepted():
    # epoch seconds (~1.7e9) should normalize to ms (~1.7e12)
    now = 1_700_000_000_000
    pol = TickTimePolicy(max_future_ms=5000, max_past_ms=120000)
    g = TickTimeGuard(pol, now_provider=lambda: now)

    ts_seconds = 1_700_000_000  # seconds
    res = g.sanitize_ts_ms(ts_seconds, now_ms=now)
    assert res is not None
    assert res.drop_reason is None
    assert res.ts_ms == ts_seconds * 1000
    assert res.flags and "normalized_seconds" in res.flags
    assert g.watermark_ms == ts_seconds * 1000


def test_soft_future_is_clamped_to_now_and_never_moves_watermark_past_now():
    now = 1_700_000_000_000
    pol = TickTimePolicy(max_future_ms=5000, clamp_soft_future=True)
    g = TickTimeGuard(pol, now_provider=lambda: now)

    res = g.sanitize_ts_ms(now + 2000, now_ms=now)
    assert res is not None
    assert res.drop_reason is None
    assert res.ts_ms == now
    assert res.flags and "clamped_soft_future" in res.flags
    assert g.watermark_ms == now


def test_hard_future_is_dropped():
    now = 1_700_000_000_000
    pol = TickTimePolicy(max_future_ms=1000, clamp_soft_future=True)
    g = TickTimeGuard(pol, now_provider=lambda: now)

    res = g.sanitize_ts_ms(now + 5000, now_ms=now)
    assert res is not None
    assert res.drop_reason == "future_hard"


def test_reorder_soft_clamps_to_watermark_when_enabled():
    now = 1_700_000_000_000
    pol = TickTimePolicy(max_reorder_ms=1000, allow_soft_reorder=True)
    g = TickTimeGuard(pol, now_provider=lambda: now)

    r1 = g.sanitize_ts_ms(now - 10, now_ms=now)
    assert r1 and r1.drop_reason is None
    wm = g.watermark_ms
    assert wm == (now - 10)

    r2 = g.sanitize_ts_ms(wm - 200, now_ms=now)
    assert r2 and r2.drop_reason is None
    assert r2.ts_ms == wm
    assert r2.flags and "reorder_soft" in r2.flags
    assert g.watermark_ms == wm


def test_reorder_hard_drops_when_too_far():
    now = 1_700_000_000_000
    pol = TickTimePolicy(max_reorder_ms=1000, allow_soft_reorder=True)
    g = TickTimeGuard(pol, now_provider=lambda: now)

    r1 = g.sanitize_ts_ms(now, now_ms=now)
    assert r1 and r1.drop_reason is None
    wm = g.watermark_ms

    r2 = g.sanitize_ts_ms(wm - 50000, now_ms=now)
    assert r2 and r2.drop_reason == "reorder_hard"
