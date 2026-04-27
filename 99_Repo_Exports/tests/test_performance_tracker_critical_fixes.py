# tests/test_performance_tracker_critical_fixes.py
"""
Тесты для критичных исправлений SignalPerformanceTracker:
1. ttl_bars=0 должен означать "выключено", а не мгновенный expire
2. При финализации state должен удаляться из _ids_by_symbol
3. EXPIRED_NO_TARGET должен использовать mark-to-market (bar.close)
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Optional
from collections import deque

import pytest

# Импорты из проекта
try:
    from signal_exec.performance_tracker import SignalPerformanceTracker, Outcome
    from signal_exec.models import Side
except ImportError:
    # Fallback для случая, когда тесты запускаются из другой директории
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-worker'))
    from signal_exec.performance_tracker import SignalPerformanceTracker, Outcome
    from signal_exec.models import Side


class DummyRepo:
    """Минимальный репозиторий для тестов."""
    def __init__(self):
        self.rows = []

    def insert_signal_performance(self, perf):
        self.rows.append(perf)


def _make_tracker():
    """Создает минимальный SignalPerformanceTracker для тестов."""
    repo = DummyRepo()
    tracker = SignalPerformanceTracker(repo=repo, max_ttd_bars=30)
    tracker._default_max_lifetime_bars_after_entry = 0  # по умолчанию выключено
    return tracker


def test_expired_no_target_respects_ttl_zero_disabled(monkeypatch):
    """Тест: ttl_bars=0 должен означать "выключено", а не мгновенный EXPIRED_NO_TARGET."""
    tracker = _make_tracker()
    tracker._default_max_lifetime_bars_after_entry = 0  # явное отключение

    # создаем state с entry, но без exit
    st = SimpleNamespace(
        signal_id="sid1",
        symbol="BTCUSDT",
        setup_type="x",
        side=Side.LONG,
        ts_signal=datetime.now(timezone.utc).replace(tzinfo=None),
        price_at_signal=100.0,
        atr_1m=1.0,
        stop_price=99.0,
        expiry_bars=999,
        max_ttd_bars=30,
        ts_entry=datetime.now(timezone.utc).replace(tzinfo=None),
        entry_price=100.0,
        ts_exit=None,
        exit_price=None,
        bar_signal=None,
        bar_entry=None,
        bar_exit=None,
        ttd_bars=None,
        ttd_seconds=None,
        mfe_R=0.0,
        mae_R=0.0,
        bars_seen=0,
        bars_to_entry=None,
        bars_to_exit=None,
        expired_without_entry=False,
        finalized=False,
        outcome=Outcome.UNKNOWN,
        notes="",
        finalize_reason=None,
        extra={"max_lifetime_bars_after_entry": 0},  # явное отключение
    )
    
    tracker._states = {"sid1": st}
    tracker._ids_by_symbol = {"BTCUSDT": {"sid1"}}

    finalized = []
    monkeypatch.setattr(tracker, "_finalize_and_store", lambda s, reason="": finalized.append((s.outcome, reason)))
    monkeypatch.setattr(tracker, "_update_ttd", lambda st, bar: None)
    monkeypatch.setattr(tracker, "_update_mfe_mae", lambda st, bar: None)
    monkeypatch.setattr(tracker, "_dt_to_naive_utc", lambda dt: dt)
    monkeypatch.setattr(tracker, "_naive_utc_from_ms", lambda ms: datetime.now(timezone.utc).replace(tzinfo=None))

    # 1-й бар после entry
    bar = SimpleNamespace(ts=int(datetime.now(timezone.utc).timestamp() * 1000), close=101.0)
    tracker.on_bar_1m("BTCUSDT", bar)

    # НЕ должно финализироваться мгновенно (т.к. ttl=0 = выключено)
    assert finalized == []


def test_expired_no_target_triggers_after_ttl(monkeypatch):
    """Тест: EXPIRED_NO_TARGET срабатывает после превышения TTL."""
    tracker = _make_tracker()
    tracker._default_max_lifetime_bars_after_entry = 3

    st = SimpleNamespace(
        signal_id="sid1",
        symbol="BTCUSDT",
        setup_type="x",
        side=Side.LONG,
        ts_signal=datetime.now(timezone.utc).replace(tzinfo=None),
        price_at_signal=100.0,
        atr_1m=1.0,
        stop_price=99.0,
        expiry_bars=999,
        max_ttd_bars=30,
        ts_entry=datetime.now(timezone.utc).replace(tzinfo=None),
        entry_price=100.0,
        ts_exit=None,
        exit_price=None,
        bar_signal=None,
        bar_entry=None,
        bar_exit=None,
        ttd_bars=None,
        ttd_seconds=None,
        mfe_R=0.0,
        mae_R=0.0,
        bars_seen=0,
        bars_to_entry=None,
        bars_to_exit=None,
        expired_without_entry=False,
        finalized=False,
        outcome=Outcome.UNKNOWN,
        notes="",
        finalize_reason=None,
        extra={},  # берём default=3
    )
    
    tracker._states = {"sid1": st}
    tracker._ids_by_symbol = {"BTCUSDT": {"sid1"}}

    finalized = []
    
    def _fin(s, reason=""):
        finalized.append((s.outcome, s.exit_price, s.bars_to_exit, reason))
        s.finalized = True

    monkeypatch.setattr(tracker, "_finalize_and_store", _fin)
    monkeypatch.setattr(tracker, "_update_ttd", lambda st, bar: None)
    monkeypatch.setattr(tracker, "_update_mfe_mae", lambda st, bar: None)
    monkeypatch.setattr(tracker, "_dt_to_naive_utc", lambda dt: dt)
    monkeypatch.setattr(tracker, "_naive_utc_from_ms", lambda ms: datetime.now(timezone.utc).replace(tzinfo=None))

    # Прогоняем бары до превышения TTL
    # bars_to_entry выставляется на 1-м баре после entry
    # ttl=3, значит на 4-м баре после entry должно финализироваться
    for i in range(1, 6):
        bar = SimpleNamespace(ts=int(datetime.now(timezone.utc).timestamp() * 1000), close=100.0 + i)
        tracker.on_bar_1m("BTCUSDT", bar)
        if finalized:
            break

    assert finalized, "Должно финализироваться по EXPIRED_NO_TARGET"
    outcome, exit_price, bars_to_exit, reason = finalized[0]
    assert outcome == Outcome.EXPIRED_NO_TARGET
    assert exit_price is not None, "exit_price должен быть mark-to-market (bar.close)"
    assert bars_to_exit is not None


def test_finalize_removes_from_ids_by_symbol(monkeypatch):
    """Тест: финализация удаляет signal_id из _ids_by_symbol."""
    tracker = _make_tracker()
    
    st = SimpleNamespace(
        signal_id="sid1",
        symbol="BTCUSDT",
        setup_type="x",
        side=Side.LONG,
        ts_signal=datetime.now(timezone.utc).replace(tzinfo=None),
        price_at_signal=100.0,
        atr_1m=1.0,
        stop_price=99.0,
        expiry_bars=10,
        max_ttd_bars=30,
        ts_entry=None,
        entry_price=None,
        ts_exit=None,
        exit_price=None,
        bar_signal=None,
        bar_entry=None,
        bar_exit=None,
        ttd_bars=None,
        ttd_seconds=None,
        mfe_R=0.0,
        mae_R=0.0,
        bars_seen=0,
        bars_to_entry=None,
        bars_to_exit=None,
        expired_without_entry=False,
        finalized=False,
        outcome=Outcome.UNKNOWN,
        notes="",
        finalize_reason=None,
        extra={},
    )
    
    tracker._states = {"sid1": st}
    tracker._ids_by_symbol = {"BTCUSDT": {"sid1", "sid2"}}  # есть другие id
    tracker._finalized_set = set()
    tracker._finalized_lru = deque()

    # вызываем финализацию напрямую
    tracker._finalize_and_store(st, reason="test")

    # проверяем что sid1 удален из _ids_by_symbol
    assert "sid1" not in tracker._ids_by_symbol.get("BTCUSDT", set())
    assert "sid2" in tracker._ids_by_symbol.get("BTCUSDT", set())  # sid2 остался
    assert "sid1" not in tracker._states


def test_finalize_removes_empty_symbol_from_ids_by_symbol(monkeypatch):
    """Тест: финализация удаляет symbol из _ids_by_symbol если set пуст."""
    tracker = _make_tracker()
    
    st = SimpleNamespace(
        signal_id="sid1",
        symbol="ETHUSDT",
        setup_type="x",
        side=Side.LONG,
        ts_signal=datetime.now(timezone.utc).replace(tzinfo=None),
        price_at_signal=100.0,
        atr_1m=1.0,
        stop_price=99.0,
        expiry_bars=10,
        max_ttd_bars=30,
        ts_entry=None,
        entry_price=None,
        ts_exit=None,
        exit_price=None,
        bar_signal=None,
        bar_entry=None,
        bar_exit=None,
        ttd_bars=None,
        ttd_seconds=None,
        mfe_R=0.0,
        mae_R=0.0,
        bars_seen=0,
        bars_to_entry=None,
        bars_to_exit=None,
        expired_without_entry=False,
        finalized=False,
        outcome=Outcome.UNKNOWN,
        notes="",
        finalize_reason=None,
        extra={},
    )
    
    tracker._states = {"sid1": st}
    tracker._ids_by_symbol = {"ETHUSDT": {"sid1"}}  # только один id
    tracker._finalized_set = set()
    tracker._finalized_lru = deque()

    # вызываем финализацию напрямую
    tracker._finalize_and_store(st, reason="test")

    # проверяем что ETHUSDT полностью удален из _ids_by_symbol (т.к. set стал пустым)
    assert "ETHUSDT" not in tracker._ids_by_symbol


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

