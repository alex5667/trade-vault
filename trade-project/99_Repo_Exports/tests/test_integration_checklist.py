# tests/test_integration_checklist.py
"""
Интеграционный тест для проверки чеклиста после применения исправлений:
1. close_reason_raw: ORPHAN_TIMEOUT* и bucket EXPIRED
2. _states реально уменьшается после finalize
3. _ids_by_symbol[symbol] не копит "мертвые" id
4. поздние STOP_HIT/TP_HIT по финализированному signal_id игнорируются
"""

from datetime import datetime, timezone
from types import SimpleNamespace
import threading

import pytest

# Импорты
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-worker'))

from signal_exec.performance_tracker import SignalPerformanceTracker, Outcome
from signal_exec.models import Side
from services.trade_monitor import TradeMonitorService
from domain.normalizers import bucket_close_reason


class DummyRepo:
    def __init__(self):
        self.rows = []

    def insert_signal_performance(self, perf):
        self.rows.append(perf)


class DummyPos:
    def __init__(self, id="1", sid="s1", symbol="BTCUSDT", tf="1m", entry_ts_ms=0, entry_price=100.0, signal_payload=None):
        self.id = id
        self.sid = sid
        self.symbol = symbol
        self.tf = tf
        self.entry_ts_ms = entry_ts_ms
        self.entry_price = entry_price
        self.closed = False
        self.signal_payload = signal_payload or {}


def test_checklist_1_bucket_close_reason_orphan_timeout():
    """
    ✅ Чеклист 1: close_reason_raw: ORPHAN_TIMEOUT* корректно мапится в bucket EXPIRED
    """
    # Проверяем все варианты ORPHAN_TIMEOUT*
    assert bucket_close_reason("ORPHAN_TIMEOUT") == "EXPIRED"
    assert bucket_close_reason("ORPHAN_TIMEOUT_NO_PRICE") == "EXPIRED"
    assert bucket_close_reason("ORPHAN_TIMEOUT_STALE_PRICE") == "EXPIRED"
    
    # Проверяем что другие причины не ломаются
    assert bucket_close_reason("SL") == "SL"
    assert bucket_close_reason("TP1") == "TP1"
    assert bucket_close_reason("TRAILING_STOP") == "TRAILING_STOP"
    
    print("✅ Чеклист 1 PASSED: ORPHAN_TIMEOUT* → bucket EXPIRED")


def test_checklist_2_states_really_decrease():
    """
    ✅ Чеклист 2: _states реально уменьшается после finalize
    """
    repo = DummyRepo()
    tracker = SignalPerformanceTracker(
        repo=repo,
        max_ttd_bars=30,
        housekeeping_every_ms=0,
        max_lifetime_bars_after_entry=3
    )
    
    # Создаем несколько states
    for i in range(5):
        st = SimpleNamespace(
            signal_id=f"sid{i}",
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
            extra={},
        )
        tracker._states[st.signal_id] = st
        tracker._ids_by_symbol[st.symbol].add(st.signal_id)
    
    # Проверяем начальное состояние
    initial_count = len(tracker._states)
    assert initial_count == 5, f"Должно быть 5 states, получено {initial_count}"
    
    # Финализируем 3 state
    for i in range(3):
        st = tracker._states[f"sid{i}"]
        tracker._finalize_and_store(st, reason="test")
    
    # Проверяем что states уменьшился
    final_count = len(tracker._states)
    assert final_count == 2, f"Должно остаться 2 states, получено {final_count}"
    assert "sid0" not in tracker._states
    assert "sid1" not in tracker._states
    assert "sid2" not in tracker._states
    assert "sid3" in tracker._states
    assert "sid4" in tracker._states
    
    print(f"✅ Чеклист 2 PASSED: _states уменьшился с {initial_count} до {final_count}")


def test_checklist_3_ids_by_symbol_cleanup():
    """
    ✅ Чеклист 3: _ids_by_symbol[symbol] не копит "мертвые" id
    """
    repo = DummyRepo()
    tracker = SignalPerformanceTracker(
        repo=repo,
        max_ttd_bars=30,
        housekeeping_every_ms=0,
        max_lifetime_bars_after_entry=3
    )
    
    # Создаем states для разных символов
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for sym in symbols:
        for i in range(3):
            st = SimpleNamespace(
                signal_id=f"{sym}_sid{i}",
                symbol=sym,
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
                extra={},
            )
            tracker._states[st.signal_id] = st
            tracker._ids_by_symbol[sym].add(st.signal_id)
    
    # Проверяем начальное состояние
    for sym in symbols:
        assert len(tracker._ids_by_symbol[sym]) == 3, f"{sym} должен иметь 3 id"
    
    # Финализируем все BTCUSDT
    for i in range(3):
        st = tracker._states[f"BTCUSDT_sid{i}"]
        tracker._finalize_and_store(st, reason="test")
    
    # Проверяем что BTCUSDT удален полностью из индекса
    assert "BTCUSDT" not in tracker._ids_by_symbol, "BTCUSDT должен быть удален из индекса"
    
    # Финализируем 2 из ETHUSDT
    for i in range(2):
        st = tracker._states[f"ETHUSDT_sid{i}"]
        tracker._finalize_and_store(st, reason="test")
    
    # Проверяем что ETHUSDT еще есть но с 1 id
    assert "ETHUSDT" in tracker._ids_by_symbol, "ETHUSDT должен остаться в индексе"
    assert len(tracker._ids_by_symbol["ETHUSDT"]) == 1, "ETHUSDT должен иметь 1 id"
    assert "ETHUSDT_sid2" in tracker._ids_by_symbol["ETHUSDT"]
    
    # SOLUSDT не трогали
    assert len(tracker._ids_by_symbol["SOLUSDT"]) == 3, "SOLUSDT должен иметь все 3 id"
    
    print("✅ Чеклист 3 PASSED: _ids_by_symbol корректно очищается при finalize")


def test_checklist_4_late_events_ignored():
    """
    ✅ Чеклист 4: поздние STOP_HIT/TP_HIT по финализированному signal_id игнорируются
    """
    repo = DummyRepo()
    tracker = SignalPerformanceTracker(
        repo=repo,
        max_ttd_bars=30,
        housekeeping_every_ms=0,
        max_lifetime_bars_after_entry=3
    )
    
    # Создаем state
    st = SimpleNamespace(
        signal_id="sid_late",
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
        extra={},
    )
    tracker._states[st.signal_id] = st
    tracker._ids_by_symbol[st.symbol].add(st.signal_id)
    
    # Финализируем
    tracker._finalize_and_store(st, reason="test_finalize")
    
    # Проверяем что state удален
    assert "sid_late" not in tracker._states
    # Проверяем что id в finalized_set
    assert "sid_late" in tracker._finalized_set
    
    # Пытаемся отправить поздний STOP_HIT
    tracker.on_execution_event(
        signal_id="sid_late",
        event_type="STOP_HIT",
        ts=datetime.now(timezone.utc).replace(tzinfo=None),
        price=99.0
    )
    
    # Проверяем что state НЕ воскрес
    assert "sid_late" not in tracker._states, "Late STOP_HIT не должен воскресить state"
    
    # Пытаемся отправить поздний TP_HIT
    tracker.on_execution_event(
        signal_id="sid_late",
        event_type="TP_HIT",
        ts=datetime.now(timezone.utc).replace(tzinfo=None),
        price=105.0
    )
    
    # Проверяем что state все еще НЕ воскрес
    assert "sid_late" not in tracker._states, "Late TP_HIT не должен воскресить state"
    
    # Проверяем что не было лишних вызовов insert_signal_performance
    # Должна быть только 1 запись от первой финализации
    assert len(repo.rows) == 1, f"Должна быть только 1 финализация, получено {len(repo.rows)}"
    
    print("✅ Чеклист 4 PASSED: поздние события игнорируются через _finalized_set")


def test_trade_monitor_orphan_timeout_integration():
    """
    Интеграционный тест для TradeMonitor: ORPHAN_TIMEOUT с stale price
    """
    # Создаем минимальный TradeMonitorService
    svc = TradeMonitorService.__new__(TradeMonitorService)
    svc._lock = threading.RLock()
    svc.open_positions = {}
    svc.pos_by_sid = {}
    svc.open_by_symbol = {}
    svc._last_price_by_symbol = {}
    svc._last_housekeep_ms = 0
    svc._orphan_housekeep_interval_ms = 0
    svc._orphan_max_lifetime_ms_default = 60_000
    svc._orphan_max_lifetime_bars_default = 0
    svc._orphan_max_last_price_age_ms = 5 * 60_000  # 5 минут
    
    def _index_remove(pos):
        s = svc.open_by_symbol.get(pos.symbol)
        if s:
            s.discard(pos.id)
            if not s:
                svc.open_by_symbol.pop(pos.symbol, None)
    
    svc._index_remove = _index_remove
    
    # Создаем позицию
    now = 978307200000 + 600_000
    entry = now - 600_000
    pos = DummyPos(
        id="p1",
        symbol="BTCUSDT",
        entry_ts_ms=entry,
        entry_price=100.0,
        signal_payload={"orphan_ttl_ms": 60_000}
    )
    
    svc.open_positions[pos.id] = pos
    svc.pos_by_sid[pos.sid] = pos.id
    svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)
    
    # Устанавливаем stale price (10 минут назад)
    svc._last_price_by_symbol[pos.symbol] = (now - 10 * 60_000, 110.0)
    
    # Вызываем housekeeping
    closures = svc._collect_orphan_closures(now)
    
    # Проверяем результат
    assert len(closures) == 1, f"Должна быть 1 closure, получено {len(closures)}"
    _, exit_price, _, reason = closures[0]
    
    # Проверяем что использован entry_price из-за stale price
    assert exit_price == 100.0, f"exit_price должен быть 100.0 (entry_price), получено {exit_price}"
    assert reason == "ORPHAN_TIMEOUT_STALE_PRICE", f"reason должен быть ORPHAN_TIMEOUT_STALE_PRICE, получено {reason}"
    
    # Проверяем что bucket корректный
    bucket = bucket_close_reason(reason)
    assert bucket == "EXPIRED", f"bucket должен быть EXPIRED, получено {bucket}"
    
    # Проверяем что позиция удалена из памяти
    assert pos.id not in svc.open_positions, "Позиция должна быть удалена из open_positions"
    
    print("✅ TradeMonitor ORPHAN_TIMEOUT_STALE_PRICE интеграция PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

