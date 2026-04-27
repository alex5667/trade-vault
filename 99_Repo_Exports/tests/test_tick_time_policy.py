import pytest

from core.tick_time import TickTimePolicy, apply_tick_time_policy


def test_ok_tick():
    policy = TickTimePolicy(max_future_ms=5000, max_past_ms=120000, max_reorder_ms=1500)
    ts, decision, meta = apply_tick_time_policy(1_700_000_000_000, 1_700_000_000_100, None, policy)
    assert decision == "ok"
    assert ts == 1_700_000_000_000
    assert meta["age_ms"] == 100


def test_future_clamp():
    policy = TickTimePolicy(max_future_ms=5000, clamp_soft_future=True)
    ts, decision, meta = apply_tick_time_policy(1_700_000_000_000 + 10_000, 1_700_000_000_000, None, policy)
    assert decision == "clamp_future"
    assert ts == 1_700_000_000_000


def test_future_drop_when_no_clamp():
    policy = TickTimePolicy(max_future_ms=5000, clamp_soft_future=False)
    ts, decision, meta = apply_tick_time_policy(1_700_000_000_000 + 10_000, 1_700_000_000_000, None, policy)
    assert decision == "drop_future"


def test_past_drop():
    policy = TickTimePolicy(max_past_ms=120000)
    ts, decision, meta = apply_tick_time_policy(1_700_000_000_000, 1_700_000_000_000 + 200_000, None, policy)
    assert decision == "drop_past"


def test_reorder_soft():
    policy = TickTimePolicy(max_reorder_ms=1500, allow_soft_reorder=True)
    ts, decision, meta = apply_tick_time_policy(1_700_000_000_000, 1_700_000_000_100, 1_700_000_000_900, policy)
    assert decision == "reorder_soft"
    assert ts == 1_700_000_000_900


def test_reorder_hard():
    policy = TickTimePolicy(max_reorder_ms=1500, allow_soft_reorder=True)
    ts, decision, meta = apply_tick_time_policy(1_700_000_000_000, 1_700_000_000_100, 1_700_000_000_000 + 10_000, policy)
    assert decision == "reorder_hard"

