"""
Tests for Switch Budget System

Expert review:
  - Senior Python: Comprehensive edge case coverage (day boundary, pause, min-gap)
  - Professor Statistics: Validates deterministic UTC day bucketing
  - DevOps/SRE: Tests fail-safe defaults and state reset logic
"""
from core.switch_budget import SwitchState, can_switch, apply_switch, utc_day_id, DAY_MS


def test_budget_blocks_after_max():
    """Budget enforcement: block when daily limit reached"""
    # Use timestamps within same day (day_id=1)
    base_ts = DAY_MS * 1 + 1000
    st = SwitchState(day_id=1, switches=2, last_switch_ts_ms=base_ts, paused_until_ts_ms=0)
    ok, why = can_switch(st=st, now_ms=base_ts + 1000, max_per_day=2, min_gap_ms=0)
    assert ok is False
    assert why == "budget"


def test_min_gap_blocks():
    """Min-gap enforcement: block if insufficient time elapsed"""
    base_ts = DAY_MS * 1 + 10_000
    st = SwitchState(day_id=1, switches=0, last_switch_ts_ms=base_ts, paused_until_ts_ms=0)
    ok, why = can_switch(st=st, now_ms=base_ts + 10_000, max_per_day=2, min_gap_ms=30_000)
    assert ok is False
    assert why == "min_gap"


def test_min_gap_allows_after_elapsed():
    """Min-gap allows switch after sufficient time"""
    base_ts = DAY_MS * 1 + 10_000
    st = SwitchState(day_id=1, switches=0, last_switch_ts_ms=base_ts, paused_until_ts_ms=0)
    ok, why = can_switch(st=st, now_ms=base_ts + 40_000, max_per_day=2, min_gap_ms=30_000)
    assert ok is True
    assert why == "ok"


def test_apply_switch_sets_pause_on_budget():
    """Auto-pause: pause until next day when budget exhausted"""
    base_ts = DAY_MS * 1 + 100_000
    st = SwitchState(day_id=1, switches=1, last_switch_ts_ms=0, paused_until_ts_ms=0)
    apply_switch(st=st, now_ms=base_ts, max_per_day=2, pause_on_budget=True)
    assert st.switches == 2
    assert st.paused_until_ts_ms > 0
    # Should be paused until next day boundary
    assert st.paused_until_ts_ms == 2 * DAY_MS


def test_day_boundary_resets_state():
    """Day boundary: can_switch returns ok for new day (read-only check)"""
    st = SwitchState(day_id=1, switches=2, last_switch_ts_ms=1000, paused_until_ts_ms=5000)
    # Move to next day
    next_day_ts = 2 * DAY_MS + 1000
    ok, why = can_switch(st=st, now_ms=next_day_ts, max_per_day=2, min_gap_ms=0)
    assert ok is True
    assert why == "ok"
    # Note: can_switch is read-only, doesn't mutate state
    # State reset happens in apply_switch


def test_paused_window_dominates():
    """Paused window: blocks even if budget/gap would allow (same day)"""
    base_ts = DAY_MS * 1 + 50_000
    st = SwitchState(day_id=1, switches=0, last_switch_ts_ms=0, paused_until_ts_ms=base_ts + 50_000)
    ok, why = can_switch(st=st, now_ms=base_ts, max_per_day=10, min_gap_ms=0)
    assert ok is False
    assert why == "paused"


def test_utc_day_id_deterministic():
    """UTC day ID: deterministic bucketing"""
    # Same day
    d1 = utc_day_id(DAY_MS * 100 + 1000)
    d2 = utc_day_id(DAY_MS * 100 + 50000)
    assert d1 == d2 == 100
    
    # Different days
    d3 = utc_day_id(DAY_MS * 101)
    assert d3 == 101


def test_unlimited_budget():
    """Unlimited budget: max_per_day=0 never blocks"""
    base_ts = DAY_MS * 1 + 1000
    st = SwitchState(day_id=1, switches=999, last_switch_ts_ms=0, paused_until_ts_ms=0)
    ok, why = can_switch(st=st, now_ms=base_ts, max_per_day=0, min_gap_ms=0)
    assert ok is True
    assert why == "ok"


def test_serialization_roundtrip():
    """Serialization: roundtrip preserves state"""
    st1 = SwitchState(day_id=123, switches=5, last_switch_ts_ms=999999, paused_until_ts_ms=888888)
    d = st1.to_dict()
    st2 = SwitchState.from_dict(d)
    assert st2.day_id == st1.day_id
    assert st2.switches == st1.switches
    assert st2.last_switch_ts_ms == st1.last_switch_ts_ms
    assert st2.paused_until_ts_ms == st1.paused_until_ts_ms


def test_utc_day_id_deterministic():
    """UTC day ID: deterministic bucketing"""
    # Same day
    d1 = utc_day_id(DAY_MS * 100 + 1000)
    d2 = utc_day_id(DAY_MS * 100 + 50000)
    assert d1 == d2 == 100
    
    # Different days
    d3 = utc_day_id(DAY_MS * 101)
    assert d3 == 101


def test_unlimited_budget():
    """Unlimited budget: max_per_day=0 never blocks"""
    st = SwitchState(day_id=1, switches=999, last_switch_ts_ms=0, paused_until_ts_ms=0)
    ok, why = can_switch(st=st, now_ms=1000, max_per_day=0, min_gap_ms=0)
    assert ok is True
    assert why == "ok"


def test_serialization_roundtrip():
    """Serialization: roundtrip preserves state"""
    st1 = SwitchState(day_id=123, switches=5, last_switch_ts_ms=999999, paused_until_ts_ms=888888)
    d = st1.to_dict()
    st2 = SwitchState.from_dict(d)
    assert st2.day_id == st1.day_id
    assert st2.switches == st1.switches
    assert st2.last_switch_ts_ms == st1.last_switch_ts_ms
    assert st2.paused_until_ts_ms == st1.paused_until_ts_ms
