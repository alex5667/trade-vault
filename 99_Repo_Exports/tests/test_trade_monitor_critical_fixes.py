# tests/test_trade_monitor_critical_fixes.py
"""
Тесты для критичных исправлений TradeMonitorService:
1. Проверка свежести last_price (защита от stale price)
2. Потокобезопасность throttle
3. Проверка _is_plausible_epoch_ms (защита от bool)
"""

import threading
from dataclasses import dataclass
from typing import Any, Dict, Tuple

# Импорты из проекта
import sys
import os

# Добавляем python-worker в path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-worker'))

from services.trade_monitor import TradeMonitorService

# Alias для удобства
class tm:
    TradeMonitorService = TradeMonitorService


@dataclass
class DummyPos:
    """Минимальная позиция для тестов."""
    id: str
    sid: str
    symbol: str
    tf: str
    entry_price: float
    entry_ts_ms: int
    closed: bool = False
    signal_payload: Dict[str, Any] = None

    def __post_init__(self):
        if self.signal_payload is None:
            self.signal_payload = {}


def _make_svc():
    """Создает минимальный TradeMonitorService для тестов."""
    svc = tm.TradeMonitorService.__new__(tm.TradeMonitorService)
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

    # индексация (минимальная)
    def _index_remove(pos):
        s = svc.open_by_symbol.get(pos.symbol)
        if s:
            s.discard(pos.id)
            if not s:
                svc.open_by_symbol.pop(pos.symbol, None)

    svc._index_remove = _index_remove
    return svc


def test_is_plausible_epoch_ms_rejects_bool():
    """Тест: _is_plausible_epoch_ms отклоняет bool."""
    svc = _make_svc()
    
    # bool должен быть отклонен (даже если True/False технически int)
    assert not svc._is_plausible_epoch_ms(True)
    assert not svc._is_plausible_epoch_ms(False)
    
    # валидные timestamp должны проходить
    assert svc._is_plausible_epoch_ms(978307200000)  # 2001-01-01
    assert svc._is_plausible_epoch_ms(1700000000000)  # 2023


def test_resolve_orphan_ttl_ms_priority_signal_payload_ms():
    """Тест: TTL из signal_payload["orphan_ttl_ms"] имеет приоритет."""
    svc = _make_svc()
    pos = DummyPos(
        id="p1",
        sid="s1",
        symbol="BTCUSDT",
        tf="5m",
        entry_price=100.0,
        entry_ts_ms=1700000000000,
        signal_payload={"orphan_ttl_ms": 12345}
    )
    
    assert svc._resolve_orphan_ttl_ms(pos) == 12345


def test_collect_orphan_closure_uses_last_price_if_fresh():
    """Тест: orphan closure использует последнюю цену если она свежая."""
    svc = _make_svc()
    now = 978307200000 + 120_000  # plausible epoch
    entry = now - 120_000
    
    pos = DummyPos(
        id="p1",
        sid="s1",
        symbol="BTCUSDT",
        tf="1m",
        entry_price=100.0,
        entry_ts_ms=entry,
        signal_payload={"orphan_ttl_ms": 60_000}
    )

    svc.open_positions[pos.id] = pos
    svc.pos_by_sid[pos.sid] = pos.id
    svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    # fresh last price (10 секунд назад)
    svc._last_price_by_symbol[pos.symbol] = (now - 10_000, 110.0)

    closures = svc._collect_orphan_closures(now)
    
    assert len(closures) == 1
    cpos, exit_price, exit_ts, reason = closures[0]
    assert exit_price == 110.0
    assert reason == "ORPHAN_TIMEOUT"
    
    # removed from memory
    assert pos.id not in svc.open_positions
    assert pos.symbol not in svc.open_by_symbol or pos.id not in svc.open_by_symbol[pos.symbol]


def test_collect_orphan_closure_stale_price_fallbacks_to_entry_price():
    """Тест: orphan closure использует entry_price если last price устарела."""
    svc = _make_svc()
    now = 978307200000 + 600_000
    entry = now - 600_000
    
    pos = DummyPos(
        id="p1",
        sid="s1",
        symbol="BTCUSDT",
        tf="1m",
        entry_price=100.0,
        entry_ts_ms=entry,
        signal_payload={"orphan_ttl_ms": 60_000}
    )

    svc.open_positions[pos.id] = pos
    svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    # stale last price (10 минут назад, больше чем max_age=5m)
    svc._orphan_max_last_price_age_ms = 5 * 60_000
    svc._last_price_by_symbol[pos.symbol] = (now - 10 * 60_000, 110.0)

    closures = svc._collect_orphan_closures(now)
    
    assert len(closures) == 1
    _, exit_price, _, reason = closures[0]
    assert exit_price == 100.0  # fallback to entry_price
    assert reason == "ORPHAN_TIMEOUT_STALE_PRICE"


def test_collect_orphan_closure_no_price_uses_entry_price():
    """Тест: orphan closure использует entry_price если нет last price."""
    svc = _make_svc()
    now = 978307200000 + 600_000
    entry = now - 600_000
    
    pos = DummyPos(
        id="p1",
        sid="s1",
        symbol="ETHUSDT",
        tf="1m",
        entry_price=200.0,
        entry_ts_ms=entry,
        signal_payload={"orphan_ttl_ms": 60_000}
    )

    svc.open_positions[pos.id] = pos
    svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    # NO last price
    closures = svc._collect_orphan_closures(now)
    
    assert len(closures) == 1
    _, exit_price, _, reason = closures[0]
    assert exit_price == 200.0
    assert reason == "ORPHAN_TIMEOUT_NO_PRICE"


def test_tf_to_ms_conversion():
    """Тест: конвертация различных форматов таймфреймов."""
    svc = _make_svc()
    
    # Минутные таймфреймы
    assert svc._tf_to_ms("1m") == 60_000
    assert svc._tf_to_ms("5m") == 300_000
    assert svc._tf_to_ms("15m") == 900_000
    
    # Часовые таймфреймы
    assert svc._tf_to_ms("1h") == 3_600_000
    assert svc._tf_to_ms("4h") == 14_400_000
    
    # Дневные таймфреймы
    assert svc._tf_to_ms("1d") == 86_400_000
    
    # MT5 style (M1, H1, D1)
    assert svc._tf_to_ms("m1") == 60_000
    assert svc._tf_to_ms("h1") == 3_600_000
    assert svc._tf_to_ms("d1") == 86_400_000
    
    # Fallback для некорректных значений
    assert svc._tf_to_ms("") == 60_000
    assert svc._tf_to_ms("invalid") == 60_000


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v", "--tb=short"])

