"""Tests for macro_calendar_scheduler compute_proximity logic."""
from __future__ import annotations

import time
import pytest
from services.macro_calendar_scheduler import compute_proximity, _parse_events, _BUILTIN_EVENTS


def _evt(delta_min: float, severity: int = 2, name: str = "CPI") -> tuple[float, str, int]:
    now_ms = time.time() * 1000.0
    return (now_ms + delta_min * 60_000.0, name, severity)


def test_no_events_returns_zeros():
    result = compute_proximity([], now_ms=time.time() * 1000, active_win_min=120)
    assert result["macro_event_severity"] == 0.0
    assert result["minutes_to_macro_event"] == 10080.0
    assert result["minutes_after_macro_event"] == 0.0


def test_future_event_not_in_window():
    evts = [_evt(+300, severity=2, name="FOMC")]
    r = compute_proximity(evts, now_ms=time.time() * 1000, active_win_min=120)
    assert r["macro_event_severity"] == 0.0
    assert abs(r["minutes_to_macro_event"] - 300) < 2
    assert r["minutes_after_macro_event"] == 0.0


def test_event_inside_future_window():
    evts = [_evt(+30, severity=2, name="CPI")]
    r = compute_proximity(evts, now_ms=time.time() * 1000, active_win_min=120)
    assert r["macro_event_severity"] == 2.0
    assert r["minutes_to_macro_event"] == 0.0  # inside window → clamp to 0


def test_event_inside_past_window():
    evts = [_evt(-60, severity=2, name="NFP")]
    r = compute_proximity(evts, now_ms=time.time() * 1000, active_win_min=120)
    assert r["macro_event_severity"] == 2.0
    assert r["minutes_to_macro_event"] == 0.0


def test_past_event_outside_window():
    evts = [_evt(-300, severity=2, name="PPI")]
    r = compute_proximity(evts, now_ms=time.time() * 1000, active_win_min=120)
    assert r["macro_event_severity"] == 0.0
    assert abs(r["minutes_after_macro_event"] - 300) < 2


def test_severity_picks_highest_in_window():
    now_ms = time.time() * 1000
    evts = [
        (now_ms + 30 * 60_000, "PPI", 1),   # medium, inside window
        (now_ms + 45 * 60_000, "FOMC", 2),  # high, inside window
    ]
    r = compute_proximity(evts, now_ms=now_ms, active_win_min=120)
    assert r["macro_event_severity"] == 2.0
    assert r["event_name"] == "FOMC"


def test_medium_severity_event():
    evts = [_evt(+10, severity=1, name="PPI")]
    r = compute_proximity(evts, now_ms=time.time() * 1000, active_win_min=120)
    assert r["macro_event_severity"] == 1.0


def test_minutes_capped_at_max_horizon():
    evts = [_evt(+20000, severity=2, name="FOMC")]
    r = compute_proximity(evts, now_ms=time.time() * 1000, active_win_min=120)
    assert r["minutes_to_macro_event"] == 10080.0


def test_builtin_events_parse_cleanly():
    evts = _parse_events(_BUILTIN_EVENTS)
    assert len(evts) >= 40
    for ts_ms, name, severity in evts:
        assert ts_ms > 0
        assert isinstance(name, str) and name
        assert severity in (1, 2)


def test_builtin_events_sorted():
    evts = _parse_events(_BUILTIN_EVENTS)
    tss = [e[0] for e in evts]
    assert tss == sorted(tss)
