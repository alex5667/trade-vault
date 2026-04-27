from __future__ import annotations

from common.tick_time import TickTimeGuard, TickTimePolicy


def test_normalize_seconds_to_ms():
    g = TickTimeGuard(TickTimePolicy(seconds_threshold=1_000_000_000_000))
    res = g.sanitize_ts_ms(1_700_000_000)
    assert res and res.ts_ms == 1_700_000_000_000


def test_normalize_ms_kept():
    g = TickTimeGuard(TickTimePolicy(seconds_threshold=1_000_000_000_000))
    res = g.sanitize_ts_ms(1_700_000_000_123)
    assert res and res.ts_ms == 1_700_000_000_123


def test_drop_future_hard_tick():
    pol = TickTimePolicy(max_future_ms=5_000, max_past_ms=120_000, max_reorder_ms=1_500, clamp_soft_future=True)
    g = TickTimeGuard(pol, now_provider=lambda: 1_700_000_000_000)
    now = 1_700_000_000_000
    res = g.sanitize_ts_ms(now + 5_001, now_ms=now)
    assert res is not None
    assert res.drop_reason == "future_hard"


def test_drop_past_hard_tick():
    pol = TickTimePolicy(max_future_ms=5_000, max_past_ms=120_000, max_reorder_ms=1_500)
    g = TickTimeGuard(pol, now_provider=lambda: 1_700_000_000_000)
    now = 1_700_000_000_000
    res = g.sanitize_ts_ms(now - 120_001, now_ms=now)
    assert res is not None
    assert res.drop_reason == "past_hard"


def test_watermark_reorder_hard_drop():
    pol = TickTimePolicy(max_future_ms=5_000, max_past_ms=120_000, max_reorder_ms=1_000, allow_soft_reorder=True)
    g = TickTimeGuard(pol, now_provider=lambda: 1_700_000_000_000)
    now = 1_700_000_000_000
    # accept first tick
    assert g.sanitize_ts_ms(now - 10, now_ms=now).drop_reason is None
    wm = g.watermark_ms
    assert wm == now - 10
    # too old vs watermark -> reorder_hard
    res = g.sanitize_ts_ms((wm - 1_001), now_ms=now)
    assert res is not None
    assert res.drop_reason == "reorder_hard"


def test_watermark_never_advances_into_future():
    pol = TickTimePolicy(max_future_ms=5_000, max_past_ms=120_000, max_reorder_ms=1_500, clamp_soft_future=True)
    g = TickTimeGuard(pol, now_provider=lambda: 1_700_000_000_000)
    now = 1_700_000_000_000
    # accept soft-future and clamp ts to now
    res = g.sanitize_ts_ms(now + 100, now_ms=now)
    assert res is not None
    assert res.drop_reason is None
    assert res.ts_ms == now
    assert "clamped_soft_future" in res.flags
    assert g.watermark_ms == now
    # next normal tick should not be reorder
    assert g.sanitize_ts_ms(now - 200, now_ms=now).drop_reason is None


def test_soft_reorder_accept_does_not_move_watermark():
    pol = TickTimePolicy(max_future_ms=5_000, max_past_ms=120_000, max_reorder_ms=1_000, allow_soft_reorder=True)
    g = TickTimeGuard(pol, now_provider=lambda: 1_700_000_000_000)
    now = 1_700_000_000_000
    assert g.sanitize_ts_ms(now - 10, now_ms=now).drop_reason is None
    wm = g.watermark_ms
    # out-of-order but within reorder window => accepted with flag reorder_soft and watermark unchanged
    res = g.sanitize_ts_ms(wm - 500, now_ms=now)
    assert res is not None
    assert res.drop_reason is None
    assert "reorder_soft" in res.flags
    assert g.watermark_ms == wm
