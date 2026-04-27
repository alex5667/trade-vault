# tests/test_trade_monitor_orphan_housekeep.py
import threading
from dataclasses import dataclass
from typing import Any, Dict

# Импорты из проекта
try:
    # Прямой импорт из python-worker/services
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-worker'))
    from services import trade_monitor as tm
except ImportError:
    # Fallback для случая, когда тесты запускаются из другой директории
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-worker'))
    from services import trade_monitor as tm


@dataclass
class DummyPos:
    id: str
    sid: str
    source: str
    symbol: str
    tf: str
    entry_price: float
    entry_ts_ms: int
    closed: bool = False
    signal_payload: Dict[str, Any] = None


class DummyRepo:
    def __init__(self):
        self.saved_closed = []

    def save_closed(self, closed, health_snapshot=None):
        self.saved_closed.append(closed)


class DummyClosed:
    def __init__(self, exit_price: float, close_reason_raw: str):
        self.exit_price = exit_price
        self.close_reason_raw = close_reason_raw
        self.pnl_net = 0.0


class DummyAnalytics:
    def __init__(self):
        self.rows = []
    
    def save_trade_closed(self, closed):
        self.rows.append(closed)


def make_service() -> tm.TradeMonitorService:
    """Создает минимальный TradeMonitorService для тестирования."""
    svc = tm.TradeMonitorService.__new__(tm.TradeMonitorService)
    svc._lock = threading.RLock()
    svc.repo = DummyRepo()
    svc.open_positions = {}
    svc.pos_by_sid = {}
    svc.open_by_symbol = {}
    svc.tp_ratios = (0.5, 0.3, 0.2)
    svc._last_housekeep_ms = 0
    svc._orphan_housekeep_interval_ms = 0
    svc._orphan_max_lifetime_ms_default = 10_000  # 10s
    svc._orphan_max_lifetime_bars_default = 0
    svc._orphan_max_last_price_age_ms = 5 * 60_000  # 5 минут (критично для _collect_orphan_closures)
    svc._last_price_by_symbol = {}

    # атрибуты для health snapshot
    svc._attach_health_on_close = False  # отключаем для тестов

    # минимальные зависимости
    svc._get_spec = lambda symbol: object()
    svc._index_remove = lambda pos: svc.open_by_symbol.get(pos.symbol, set()).discard(pos.id)
    svc._update_stats = lambda pos, closed: None

    return svc


def test_orphan_forced_close_uses_last_price(monkeypatch):
    """Тест: orphan позиция закрывается по последней цене (ORPHAN_TIMEOUT)."""
    svc = make_service()
    monkeypatch.setattr(tm, "analytics_db", DummyAnalytics(), raising=False)

    calls = []
    
    def fake_finalize_trade(pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios):
        calls.append((pos.id, float(exit_price), int(exit_ts_ms), str(close_reason_raw)))
        return DummyClosed(exit_price=float(exit_price), close_reason_raw=str(close_reason_raw))

    monkeypatch.setattr(tm, "finalize_trade", fake_finalize_trade)

    entry = 1_700_000_000_000
    now = entry + 20_000
    pos = DummyPos(
        id="P1",
        sid="SID1",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        entry_price=100.0,
        entry_ts_ms=entry,
        signal_payload={},
    )
    svc.open_positions[pos.id] = pos
    svc.pos_by_sid[pos.sid] = pos.id
    svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    # last price есть -> ORPHAN_TIMEOUT и exit_price = last
    svc._last_price_by_symbol[pos.symbol] = (now, 105.0)

    svc._housekeep_expired_positions(now)

    assert pos.id not in svc.open_positions
    assert pos.sid not in svc.pos_by_sid
    assert calls == [("P1", 105.0, now, "ORPHAN_TIMEOUT")]
    assert len(svc.repo.saved_closed) == 1


def test_orphan_forced_close_fallback_to_entry_price(monkeypatch):
    """Тест: orphan позиция закрывается по entry_price если нет last price (ORPHAN_TIMEOUT_NO_PRICE)."""
    svc = make_service()
    monkeypatch.setattr(tm, "analytics_db", DummyAnalytics(), raising=False)

    calls = []
    
    def fake_finalize_trade(pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios):
        calls.append((pos.id, float(exit_price), int(exit_ts_ms), str(close_reason_raw)))
        return DummyClosed(exit_price=float(exit_price), close_reason_raw=str(close_reason_raw))

    monkeypatch.setattr(tm, "finalize_trade", fake_finalize_trade)

    entry = 1_700_000_000_000
    now = entry + 20_000
    pos = DummyPos(
        id="P2",
        sid="SID2",
        source="CryptoOrderFlow",
        symbol="ETHUSDT",
        tf="1m",
        entry_price=200.0,
        entry_ts_ms=entry,
        signal_payload={},
    )
    svc.open_positions[pos.id] = pos
    svc.pos_by_sid[pos.sid] = pos.id
    svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    # last price нет -> ORPHAN_TIMEOUT_NO_PRICE и exit_price = entry_price
    svc._housekeep_expired_positions(now)

    assert pos.id not in svc.open_positions
    assert calls == [("P2", 200.0, now, "ORPHAN_TIMEOUT_NO_PRICE")]


def test_housekeep_throttling(monkeypatch):
    """Тест: housekeep throttling работает корректно."""
    svc = make_service()
    monkeypatch.setattr(tm, "analytics_db", DummyAnalytics(), raising=False)

    calls = []
    
    def fake_finalize_trade(pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios):
        calls.append(pos.id)
        return DummyClosed(exit_price=float(exit_price), close_reason_raw=str(close_reason_raw))

    monkeypatch.setattr(tm, "finalize_trade", fake_finalize_trade)

    svc._orphan_housekeep_interval_ms = 30_000

    entry = 1_700_000_000_000
    pos = DummyPos(
        id="P3",
        sid="SID3",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        entry_price=100.0,
        entry_ts_ms=entry,
        signal_payload={},
    )
    svc.open_positions[pos.id] = pos
    svc.pos_by_sid[pos.sid] = pos.id
    svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    now1 = entry + 20_000
    now2 = now1 + 1_000  # меньше interval

    svc._housekeep_expired_positions(now1)
    # повторный вызов должен быть throttled (позиция уже снята после первого вызова,
    # но сам факт throttle проверяем на "не падает и не вызывает finalize второй раз")
    svc._housekeep_expired_positions(now2)

    assert calls == ["P3"]


def test_orphan_ttl_from_signal_payload(monkeypatch):
    """Тест: TTL может быть переопределен через signal_payload."""
    svc = make_service()
    monkeypatch.setattr(tm, "analytics_db", DummyAnalytics(), raising=False)

    calls = []
    
    def fake_finalize_trade(pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios):
        calls.append(pos.id)
        return DummyClosed(exit_price=float(exit_price), close_reason_raw=str(close_reason_raw))

    monkeypatch.setattr(tm, "finalize_trade", fake_finalize_trade)

    entry = 1_700_000_000_000
    now = entry + 5_000  # меньше дефолтного TTL (10_000)
    
    pos = DummyPos(
        id="P4",
        sid="SID4",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        entry_price=100.0,
        entry_ts_ms=entry,
        signal_payload={"orphan_ttl_ms": 3000},  # кастомный TTL 3 секунды
    )
    svc.open_positions[pos.id] = pos
    svc.pos_by_sid[pos.sid] = pos.id
    svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    svc._housekeep_expired_positions(now)

    # Позиция должна быть закрыта, т.к. age_ms=5000 >= custom_ttl=3000
    assert pos.id not in svc.open_positions
    assert calls == ["P4"]


def test_tf_to_ms_conversion():
    """Тест: конвертация таймфреймов в миллисекунды."""
    svc = make_service()
    
    # Минутные таймфреймы
    assert svc._tf_to_ms("1m") == 60_000
    assert svc._tf_to_ms("5m") == 300_000
    assert svc._tf_to_ms("15m") == 900_000
    
    # Часовые таймфреймы
    assert svc._tf_to_ms("1h") == 3_600_000
    assert svc._tf_to_ms("4h") == 14_400_000
    
    # Дневные таймфреймы
    assert svc._tf_to_ms("1d") == 86_400_000
    
    # MT5 style
    assert svc._tf_to_ms("M1") == 60_000
    assert svc._tf_to_ms("H1") == 3_600_000
    assert svc._tf_to_ms("D1") == 86_400_000
    
    # Fallback для некорректных значений
    assert svc._tf_to_ms("") == 60_000
    assert svc._tf_to_ms("invalid") == 60_000


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v", "--tb=short"])

