from __future__ import annotations

from types import SimpleNamespace
import domain.handlers as h


def test_should_start_trailing_reads_signal_payload(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "0")

    pos = SimpleNamespace(signal_payload={"trail_after_tp1": False})
    assert h._should_start_trailing_after_tp1(pos) is False

    pos2 = SimpleNamespace(signal_payload={"trail_after_tp1": True})
    assert h._should_start_trailing_after_tp1(pos2) is True
