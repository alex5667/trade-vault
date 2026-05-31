from __future__ import annotations

import random

from common.tick_time import TickTimeGuard, TickTimePolicy


def test_randomized_watermark_never_decreases_and_never_goes_future():
    """
    "Property-style" без hypothesis:
      - watermark монотонен
      - watermark <= now
      - hard future/past/reorder_hard корректно дропаются
    """
    rng = random.Random(1337)
    now = 1_700_000_000_000
    pol = TickTimePolicy(
        max_future_ms=5000,
        max_past_ms=120000,
        max_reorder_ms=1500,
        clamp_soft_future=True,
        allow_soft_reorder=True,
    )
    g = TickTimeGuard(pol, now_provider=lambda: now)

    last_wm = 0
    for _ in range(2000):
        # генерим смесь:
        #  - нормальные тики рядом с now
        #  - немного future в пределах окна
        #  - иногда hard future/past
        roll = rng.random()
        if roll < 0.70:
            ts = now + rng.randint(-2000, 2000)
        elif roll < 0.85:
            ts = now + rng.randint(1, 5000)  # soft-future window
        elif roll < 0.93:
            ts = now + rng.randint(5001, 20000)  # hard future
        else:
            ts = now - rng.randint(120001, 300000)  # hard past

        res = g.sanitize_ts_ms(ts, now_ms=now)
        assert res is not None
        wm = g.watermark_ms

        # watermark constraints always hold
        assert wm >= last_wm
        assert wm <= now
        last_wm = wm

        # if accepted and soft-future: ts_ms gets clamped to now
        if res.drop_reason is None and ts > now and ts <= now + pol.max_future_ms:
            assert res.ts_ms == now
