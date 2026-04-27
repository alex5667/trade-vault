from __future__ import annotations

from common.steady_clock import SteadyClock


def test_steady_clock_is_monotonic_even_if_wall_time_jumps_back(monkeypatch):
    """
    Поведенческий тест: steady now_ms не должен идти назад.
    Monkeypatch'им time.time и time.monotonic в грубой форме.
    """
    import time as _t

    wall = {"v": 1000.0}
    mono = {"v": 500.0}

    def fake_time():
        return wall["v"]

    def fake_mono():
        return mono["v"]

    monkeypatch.setattr(_t, "time", fake_time)
    monkeypatch.setattr(_t, "monotonic", fake_mono)

    c = SteadyClock()
    t1 = c.now_ms()
    # normal forward
    wall["v"] += 1.0
    mono["v"] += 1.0
    t2 = c.now_ms()
    assert t2 >= t1
    # wall jumps back hard, monotonic moves forward
    wall["v"] -= 100.0
    mono["v"] += 1.0
    t3 = c.now_ms()
    assert t3 >= t2
