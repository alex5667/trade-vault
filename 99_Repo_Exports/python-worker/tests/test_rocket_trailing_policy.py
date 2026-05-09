from __future__ import annotations

from domain.handlers import _rocket_trailing_only_mode, _should_start_trailing_after_tp1


class P:
    def __init__(self):
        self.trailing_started = True
        self.trail_after_tp1 = True


def test_should_trail_after_tp1_conditional_true(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.delenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", raising=False)
    monkeypatch.delenv("FORCE_TRAIL_AFTER_TP1", raising=False)

    pos = P()
    pos.trail_after_tp1 = True
    assert _should_start_trailing_after_tp1(pos) is True


def test_should_trail_after_tp1_conditional_false(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.delenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", raising=False)
    monkeypatch.delenv("FORCE_TRAIL_AFTER_TP1", raising=False)

    pos = P()
    pos.trail_after_tp1 = False
    assert _should_start_trailing_after_tp1(pos) is False


def test_should_trail_after_tp1_force_override(monkeypatch):
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "1")
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")

    pos = P()
    pos.trail_after_tp1 = False
    assert _should_start_trailing_after_tp1(pos) is True


def test_rocket_trailing_only_mode_respects_policy(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.delenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", raising=False)

    pos = P()
    pos.trailing_started = True

    # idx >= 1 means TP2/TP3 zone.
    pos.trail_after_tp1 = False
    assert _rocket_trailing_only_mode(pos, is_rocket_trail=True, idx=1) is False

    pos.trail_after_tp1 = True
    assert _rocket_trailing_only_mode(pos, is_rocket_trail=True, idx=1) is True


def test_rocket_trailing_only_mode_requires_trailing_started(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    pos = P()
    pos.trailing_started = False
    pos.trail_after_tp1 = True
    assert _rocket_trailing_only_mode(pos, is_rocket_trail=True, idx=1) is False
