import os
import math
import pytest
from core.atr_sanity import ATRSanity

def _feed(ats: ATRSanity, *, atr: float, px: float, now_ms: int, tf: str = "1m"):
    return ats.update(atr=atr, px=px, age_ms=0, now_ms=now_ms, symbol="BTCUSDT", tf=tf)

def test_step_change_accept_after_k_buckets(monkeypatch):
    # Strict jump threshold reproduces the original failure mode (lockout),
    # but step-change acceptance should recover after K TF buckets.
    monkeypatch.setenv("ATR_JUMP_MAX_REL", "0.8")
    monkeypatch.setenv("ATR_JUMP_ACCEPT_K", "3")
    monkeypatch.setenv("ATR_JUMP_ACCEPT_REBASE", "1")

    ats = ATRSanity(window=60)

    px = 100.0
    # Seed history around 10 bps (atr=0.10)
    now = 0
    for i in range(20):
        now = i * 60_000
        r = _feed(ats, atr=0.10, px=px, now_ms=now, tf="1m")
        assert r.bad == 0

    # Step-change to 20 bps (atr=0.20) should be rejected initially...
    r1 = _feed(ats, atr=0.20, px=px, now_ms=now + 60_000, tf="1m")
    assert r1.bad == 1
    assert r1.used_last_good == 1

    # ...but accepted after 3 distinct 1m buckets
    r2 = _feed(ats, atr=0.20, px=px, now_ms=now + 2 * 60_000, tf="1m")
    r3 = _feed(ats, atr=0.20, px=px, now_ms=now + 3 * 60_000, tf="1m")
    assert r3.bad == 0
    assert r3.jump_accept == 1
    assert math.isclose(r3.atr_used, 0.20, rel_tol=1e-9)

def test_no_accept_on_single_spike(monkeypatch):
    monkeypatch.setenv("ATR_JUMP_MAX_REL", "0.8")
    monkeypatch.setenv("ATR_JUMP_ACCEPT_K", "3")
    monkeypatch.setenv("ATR_JUMP_ACCEPT_REBASE", "1")

    ats = ATRSanity(window=60)
    px = 100.0
    now = 0
    for i in range(20):
        now = i * 60_000
        _feed(ats, atr=0.10, px=px, now_ms=now, tf="1m")

    # One spike (atr=0.40) -> reject, but should not accept because streak doesn't reach K
    r = _feed(ats, atr=0.40, px=px, now_ms=now + 60_000, tf="1m")
    assert r.bad == 1
    assert r.jump_accept == 0

    # Back to normal -> should clear streak
    r_ok = _feed(ats, atr=0.10, px=px, now_ms=now + 2 * 60_000, tf="1m")
    assert r_ok.bad == 0
    assert r_ok.jump_streak == 0

def test_dedup_per_bucket_avoids_fast_accept_on_many_ticks(monkeypatch):
    monkeypatch.setenv("ATR_JUMP_MAX_REL", "0.8")
    monkeypatch.setenv("ATR_JUMP_ACCEPT_K", "3")
    monkeypatch.setenv("ATR_JUMP_ACCEPT_REBASE", "1")

    ats = ATRSanity(window=60)
    px = 100.0
    # Seed
    for i in range(20):
        _feed(ats, atr=0.10, px=px, now_ms=i * 60_000, tf="1m")

    base = 20 * 60_000
    # Simulate many ticks within the same 1m bucket (same now_ms bucket)
    r1 = _feed(ats, atr=0.20, px=px, now_ms=base + 10_000, tf="1m")
    r2 = _feed(ats, atr=0.20, px=px, now_ms=base + 20_000, tf="1m")
    r3 = _feed(ats, atr=0.20, px=px, now_ms=base + 30_000, tf="1m")

    # Still should not accept within the same bucket
    assert r3.jump_accept == 0
    assert r3.bad == 1

    # Only after distinct buckets
    r4 = _feed(ats, atr=0.20, px=px, now_ms=base + 60_000, tf="1m")
    r5 = _feed(ats, atr=0.20, px=px, now_ms=base + 2 * 60_000, tf="1m")
    assert r5.jump_accept == 1
