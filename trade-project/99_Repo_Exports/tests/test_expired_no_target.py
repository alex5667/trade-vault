# tests/test_expired_no_target.py
"""
Тесты для проверки финализации зависших позиций (EXPIRED_NO_TARGET).

Проверяют:
1. Финализацию позиций по TTL в барах (held_bars >= max_lifetime_bars_after_entry)
2. Финализацию позиций по TTL в миллисекундах (fallback)
3. Защиту от "поздних exit событий" после финализации
4. Финализацию сигналов, которые протухли до входа (pre-entry expiry)
5. Идемпотентность финализации (повторные вызовы безопасны)
"""

from __future__ import annotations

import pytest
from datetime import datetime, timedelta
from typing import Optional

# Импорты из проекта
try:
    from signal_exec.performance_tracker import (
        SignalPerformanceTracker, 
        SignalPerfState, 
        Outcome
    )
    from signal_exec.models import Bar1m, Side, ExecutionPlan
    from signal_exec.context import SignalContext
    from signal_exec.repository import SignalRepository
except ImportError:
    # Fallback для случая, когда тесты запускаются из другой директории
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-worker'))
    
    from signal_exec.performance_tracker import (
        SignalPerformanceTracker, 
        SignalPerfState, 
        Outcome
    )
    from signal_exec.models import Bar1m, Side, ExecutionPlan
    from signal_exec.context import SignalContext
    from signal_exec.repository import SignalRepository


# ============================================================================
# Mock Repository для тестов (не требует реальной БД)
# ============================================================================

class MockSignalRepository:
    """Mock repository для тестов - сохраняет результаты в памяти."""
    
    def __init__(self):
        self.stored_performances = []
    
    def insert_signal_performance(self, perf):
        """Сохраняет performance в памяти вместо БД."""
        self.stored_performances.append(perf)


# ============================================================================
# Фикстуры для тестов
# ============================================================================

@pytest.fixture
def mock_repo():
    """Создает mock repository для каждого теста."""
    return MockSignalRepository()


@pytest.fixture
def tracker(mock_repo):
    """
    Создает SignalPerformanceTracker с настройками для тестирования.
    
    Параметры:
    - max_lifetime_bars_after_entry=5 (быстрая финализация для тестов)
    - max_lifetime_ms_after_entry=0 (отключен по умолчанию)
    - housekeeping_every_ms=0 (чтобы sampler.hit() всегда срабатывал)
    """
    return SignalPerformanceTracker(
        repo=mock_repo,
        ttd_target_R=1.0,
        max_ttd_bars=30,
        bus=None,
        max_lifetime_bars_after_entry=5,
        max_lifetime_ms_after_entry=0,
        housekeeping_every_ms=0,  # для тестов - всегда срабатывает
    )


@pytest.fixture
def base_timestamp():
    """Базовая временная метка для тестов."""
    return datetime(2025, 1, 1, 12, 0, 0)


# ============================================================================
# Вспомогательные функции
# ============================================================================

def create_test_state(
    tracker: SignalPerformanceTracker,
    signal_id: str,
    bar_signal: int = 100,
    bar_entry: Optional[int] = None,
    ts_signal: Optional[datetime] = None,
    ts_entry: Optional[datetime] = None,
) -> SignalPerfState:
    """
    Создает тестовый state и добавляет его в tracker._states.
    
    Args:
        tracker: экземпляр SignalPerformanceTracker
        signal_id: идентификатор сигнала
        bar_signal: индекс бара сигнала
        bar_entry: индекс бара входа (если был вход)
        ts_signal: временная метка сигнала
        ts_entry: временная метка входа (если был вход)
    
    Returns:
        Созданный SignalPerfState
    """
    if ts_signal is None:
        ts_signal = datetime(2025, 1, 1, 12, 0, 0)
    
    state = SignalPerfState(
        signal_id=signal_id,
        symbol="BTCUSDT",
        setup_type="orderflow",
        side=Side.LONG,
        ts_signal=ts_signal,
        price_at_signal=50000.0,
        atr_1m=100.0,
        stop_price=49900.0,
        expiry_bars=10,
        max_ttd_bars=30,
        bar_signal=bar_signal,
    )
    
    if bar_entry is not None:
        state.ts_entry = ts_entry or (ts_signal + timedelta(minutes=1))
        state.entry_price = 50010.0
        state.bar_entry = bar_entry
    
    tracker._states[signal_id] = state
    return state


# ============================================================================
# Тесты: финализация по TTL в барах
# ============================================================================

def test_expired_no_target_by_bars_finalizes_and_removes_state(tracker, mock_repo):
    """
    Тест: позиция финализируется по TTL в барах и удаляется из памяти.
    
    Сценарий:
    1. Создаем позицию с входом на баре 101
    2. Вызываем housekeeping на баре 106 (held_bars = 5)
    3. Проверяем, что позиция финализирована с EXPIRED_NO_TARGET
    4. Проверяем, что state удален из памяти
    """
    # Arrange: создаем позицию с входом
    trade_id = "T1"
    state = create_test_state(
        tracker, 
        trade_id, 
        bar_signal=100, 
        bar_entry=101
    )
    
    assert trade_id in tracker._states
    assert not state.finalized
    
    # Act: вызываем on_bar_1m на баре 106 (held_bars = 5 >= ttl)
    # Прогоняем несколько баров, чтобы достичь TTL
    base_ts = datetime(2025, 1, 1, 12, 2, 0)
    for i in range(6):
        bar = Bar1m(
            ts=base_ts + timedelta(minutes=i),
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0
        )
        tracker.on_bar_1m(symbol="BTCUSDT", bar=bar)
    
    # Assert: позиция финализирована и удалена
    assert trade_id not in tracker._states, "State должен быть удален из памяти"
    assert state.finalized is True, "State должен быть помечен как finalized"
    assert state.outcome == Outcome.EXPIRED_NO_TARGET, "Outcome должен быть EXPIRED_NO_TARGET"
    assert state.finalize_reason is not None, "Должна быть причина финализации"
    assert "held_bars" in state.finalize_reason, "Причина должна содержать held_bars"
    
    # Проверяем, что результат сохранен в репозиторий
    assert len(mock_repo.stored_performances) == 1
    perf = mock_repo.stored_performances[0]
    assert perf.signal_id == trade_id
    assert perf.outcome == Outcome.EXPIRED_NO_TARGET


def test_expired_no_target_not_triggered_before_ttl(tracker):
    """
    Тест: позиция НЕ финализируется до истечения TTL.
    
    Сценарий:
    1. Создаем позицию с входом на баре 101
    2. Вызываем housekeeping на баре 105 (held_bars = 4 < 5)
    3. Проверяем, что позиция НЕ финализирована
    """
    # Arrange
    trade_id = "T2"
    state = create_test_state(tracker, trade_id, bar_signal=100, bar_entry=101)
    
    # Act: on_bar_1m на нескольких барах, но до TTL
    base_ts = datetime(2025, 1, 1, 12, 2, 0)
    for i in range(4):  # 4 бара, меньше ttl=5
        bar = Bar1m(
            ts=base_ts + timedelta(minutes=i),
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0
        )
        tracker.on_bar_1m(symbol="BTCUSDT", bar=bar)
    
    # Assert: позиция НЕ финализирована
    assert trade_id in tracker._states, "State должен остаться в памяти"
    assert not state.finalized, "State НЕ должен быть финализирован"


# ============================================================================
# Тесты: защита от поздних exit событий
# ============================================================================

def test_late_exit_is_ignored_after_expire(tracker, mock_repo):
    """
    Тест: "поздний exit" игнорируется после финализации.
    
    Сценарий:
    1. Создаем позицию с входом
    2. Финализируем её по TTL (EXPIRED_NO_TARGET)
    3. Пытаемся отправить "поздний exit" событие
    4. Проверяем, что exit проигнорирован и state не пересоздан
    
    Это критично для корректности статистики после рестартов/лагов.
    """
    # Arrange: создаем и финализируем позицию
    trade_id = "T3"
    state = create_test_state(tracker, trade_id, bar_signal=100, bar_entry=101)
    
    # Финализируем по TTL через on_bar_1m
    base_ts = datetime(2025, 1, 1, 12, 2, 0)
    for i in range(200):  # Много баров, чтобы превысить TTL
        bar = Bar1m(
            ts=base_ts + timedelta(minutes=i),
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0
        )
        tracker.on_bar_1m(symbol="BTCUSDT", bar=bar)
    
    assert trade_id not in tracker._states
    assert state.outcome == Outcome.EXPIRED_NO_TARGET
    
    # Act: пытаемся отправить "поздний exit"
    exit_ts = datetime(2025, 1, 1, 12, 10, 0)
    tracker.on_execution_event(
        signal_id=trade_id,
        event_type="TP_HIT",
        ts=exit_ts,
        price=50100.0,
        bar_idx=201
    )
    
    # Assert: exit проигнорирован, state не пересоздан
    assert trade_id not in tracker._states, "State НЕ должен быть пересоздан"
    
    # Проверяем, что в репозитории только одна запись (от финализации)
    assert len(mock_repo.stored_performances) == 1
    perf = mock_repo.stored_performances[0]
    assert perf.outcome == Outcome.EXPIRED_NO_TARGET
    assert perf.ts_exit is None, "Exit не должен быть записан"


def test_late_entry_is_ignored_after_expire(tracker):
    """
    Тест: "поздний entry" игнорируется после финализации сигнала.
    
    Сценарий:
    1. Создаем сигнал без входа
    2. Финализируем его по expiry_bars (EXPIRED_NO_ENTRY)
    3. Пытаемся отправить "поздний entry"
    4. Проверяем, что entry проигнорирован
    """
    # Arrange: создаем сигнал без входа
    trade_id = "T4"
    state = create_test_state(tracker, trade_id, bar_signal=100, bar_entry=None)
    
    # Финализируем по expiry_bars через on_bar_1m (age_bars = 10 >= expiry_bars=10)
    base_ts = datetime(2025, 1, 1, 12, 1, 0)
    for i in range(11):  # 11 баров, чтобы превысить expiry_bars=10
        bar = Bar1m(
            ts=base_ts + timedelta(minutes=i),
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0
        )
        tracker.on_bar_1m(symbol="BTCUSDT", bar=bar)
    
    assert trade_id not in tracker._states
    
    # Act: пытаемся отправить "поздний entry"
    entry_ts = datetime(2025, 1, 1, 12, 11, 0)
    tracker.on_execution_event(
        signal_id=trade_id,
        event_type="ENTRY_FILLED",
        ts=entry_ts,
        price=50020.0,
        bar_idx=111
    )
    
    # Assert: entry проигнорирован
    assert trade_id not in tracker._states, "State НЕ должен быть пересоздан"


# ============================================================================
# Тесты: финализация сигналов до входа (pre-entry expiry)
# ============================================================================

def test_expire_pre_entry_if_you_keep_pre_entry_states(tracker, mock_repo):
    """
    Тест: сигнал финализируется, если не было входа и истек expiry_bars.
    
    Сценарий:
    1. Создаем сигнал без входа на баре 100
    2. Вызываем housekeeping на баре 110 (age_bars = 10 >= expiry_bars=10)
    3. Проверяем финализацию с EXPIRED_NO_TARGET (или EXPIRED_NO_ENTRY)
    """
    # Arrange: создаем сигнал без входа
    trade_id = "T5"
    state = create_test_state(tracker, trade_id, bar_signal=100, bar_entry=None)
    
    # Act: housekeeping на баре 110 (age_bars = 10 >= expiry_bars=10)
    tracker._housekeep_expired(now_ts_ms=999999, now_bar=110)
    
    # Assert: сигнал финализирован
    assert trade_id not in tracker._states
    assert state.finalized is True
    assert state.outcome == Outcome.EXPIRED_NO_TARGET
    assert "expired_pre_entry" in state.finalize_reason or "age_bars" in state.finalize_reason
    
    # Проверяем сохранение в репозиторий
    assert len(mock_repo.stored_performances) == 1


# ============================================================================
# Тесты: fallback TTL по времени (миллисекунды)
# ============================================================================

def test_expired_no_target_by_time_fallback(mock_repo):
    """
    Тест: финализация по TTL в миллисекундах (fallback).
    
    Сценарий:
    1. Создаем tracker с max_lifetime_ms_after_entry=60000 (1 минута)
    2. Создаем позицию с входом
    3. Вызываем housekeeping через 61 секунду
    4. Проверяем финализацию
    """
    # Arrange: tracker с TTL по времени
    tracker = SignalPerformanceTracker(
        repo=mock_repo,
        max_lifetime_bars_after_entry=999,  # большое значение, не сработает
        max_lifetime_ms_after_entry=60000,  # 1 минута
        housekeeping_every_ms=0,
    )
    
    trade_id = "T6"
    ts_signal = datetime(2025, 1, 1, 12, 0, 0)
    ts_entry = datetime(2025, 1, 1, 12, 1, 0)
    
    state = create_test_state(
        tracker, 
        trade_id, 
        bar_signal=100, 
        bar_entry=101,
        ts_signal=ts_signal,
        ts_entry=ts_entry
    )
    
    # Act: вызываем on_execution_event через 61 секунду после входа
    # Это должно вызвать _maybe_expire_by_time внутри
    now_ts = ts_entry + timedelta(seconds=61)
    
    # Создаем dummy событие, которое вызовет _maybe_expire_by_time
    tracker.on_execution_event(
        signal_id=trade_id,
        event_type="ENTRY_FILLED",  # любое событие вызовет проверку
        ts=now_ts,
        price=50020.0
    )
    
    # Assert: финализация по времени
    assert trade_id not in tracker._states
    assert state.finalized is True
    assert state.outcome == Outcome.EXPIRED_NO_TARGET
    assert "held_ms" in state.finalize_reason


# ============================================================================
# Тесты: идемпотентность финализации
# ============================================================================

def test_finalize_is_idempotent(tracker, mock_repo):
    """
    Тест: повторная финализация безопасна (идемпотентна).
    
    Сценарий:
    1. Создаем позицию
    2. Финализируем её
    3. Пытаемся финализировать повторно
    4. Проверяем, что в репозитории только одна запись
    """
    # Arrange
    trade_id = "T7"
    state = create_test_state(tracker, trade_id, bar_signal=100, bar_entry=101)
    
    # Act: первая финализация
    tracker._finalize_and_store(state, reason="test_finalization")
    
    assert state.finalized is True
    assert len(mock_repo.stored_performances) == 1
    
    # Act: повторная финализация
    tracker._finalize_and_store(state, reason="duplicate_finalization")
    
    # Assert: только одна запись в репозитории
    assert len(mock_repo.stored_performances) == 1, "Не должно быть дубликатов"


# ============================================================================
# Тесты: интеграция с on_bar
# ============================================================================

def test_housekeeping_triggered_by_on_bar(tracker, mock_repo):
    """
    Тест: housekeeping автоматически вызывается через on_bar.
    
    Сценарий:
    1. Создаем позицию с входом
    2. Вызываем on_bar с bar_idx, который превышает TTL
    3. Проверяем автоматическую финализацию
    """
    # Arrange
    trade_id = "T8"
    state = create_test_state(tracker, trade_id, bar_signal=100, bar_entry=101)
    
    # Act: on_bar_1m прогоняет бары до превышения TTL
    base_ts = datetime(2025, 1, 1, 12, 2, 0)
    for i in range(6):  # 6 баров, чтобы held_bars >= 5
        bar = Bar1m(
            ts=base_ts + timedelta(minutes=i),
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0
        )
        tracker.on_bar_1m(symbol="BTCUSDT", bar=bar)
    
    # Assert: автоматическая финализация
    assert trade_id not in tracker._states
    assert state.finalized is True
    assert state.outcome == Outcome.EXPIRED_NO_TARGET


# ============================================================================
# Тесты: нормальный exit работает корректно
# ============================================================================

def test_normal_exit_works_correctly(tracker, mock_repo):
    """
    Тест: нормальный exit (до TTL) работает корректно.
    
    Сценарий:
    1. Создаем позицию с входом
    2. Отправляем exit событие до истечения TTL
    3. Проверяем корректную финализацию с правильным outcome
    """
    # Arrange
    trade_id = "T9"
    ts_signal = datetime(2025, 1, 1, 12, 0, 0)
    ts_entry = datetime(2025, 1, 1, 12, 1, 0)
    
    state = create_test_state(
        tracker, 
        trade_id, 
        bar_signal=100, 
        bar_entry=101,
        ts_signal=ts_signal,
        ts_entry=ts_entry
    )
    
    # Act: отправляем exit на баре 103 (до TTL)
    exit_ts = datetime(2025, 1, 1, 12, 3, 0)
    tracker.on_execution_event(
        signal_id=trade_id,
        event_type="TP_HIT",
        ts=exit_ts,
        price=50100.0,
        bar_idx=103
    )
    
    # Обрабатываем бары до и после exit
    base_ts = datetime(2025, 1, 1, 12, 1, 0)
    for i in range(5):  # Прогоняем несколько баров
        bar = Bar1m(
            ts=base_ts + timedelta(minutes=i),
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50100.0
        )
        tracker.on_bar_1m(symbol="BTCUSDT", bar=bar)
    
    # Assert: корректная финализация
    assert trade_id not in tracker._states
    assert state.finalized is True
    assert state.outcome == Outcome.TARGET_HIT
    assert state.bar_exit == 103
    
    # Проверяем сохранение
    assert len(mock_repo.stored_performances) == 1
    perf = mock_repo.stored_performances[0]
    assert perf.outcome == Outcome.TARGET_HIT


# ============================================================================
# Запуск тестов
# ============================================================================

if __name__ == "__main__":
    # Для запуска тестов напрямую (без pytest)
    pytest.main([__file__, "-v", "--tb=short"])

