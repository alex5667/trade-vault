"""
Тесты для критичных исправлений RedisTradeRepository.

✅ FIX #1: bytes vs str декодирование
✅ FIX #2: entry_time vs entry_ts_ms (совместимость ключей)
✅ FIX #3: direction и булевы поля (нормализация)
✅ FIX #4: атомарность save_open (pipeline)
✅ FIX #5: идемпотентность save_closed (dedup)
✅ FIX #6: logger определён до использования
✅ FIX #7: load_open_positions масштабирование (SSCAN)
"""

import sys
from pathlib import Path

# Добавляем путь к модулям
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "python-worker"))

import pytest
from unittest.mock import MagicMock, patch, call
from types import SimpleNamespace

try:
    from infra.redis_repo import (
        RedisTradeRepository,
        _decode_map,
        _side_to_str,
        _b01,
    )
    from domain.models import PositionState, TradeClosed
except ImportError as e:
    pytest.skip(f"Cannot import modules: {e}", allow_module_level=True)


# ========================================
# ✅ FIX #1: Тесты для _decode_map
# ========================================
def test_decode_map_handles_bytes():
    """
    Проверяем, что _decode_map корректно декодирует bytes → str.
    Это критично для Redis клиентов без decode_responses=True.
    """
    input_map = {
        b"status": b"open",
        b"entry_price": b"100.5",
        b"tp1_hit": b"1",
    }
    result = _decode_map(input_map)
    
    assert result["status"] == "open"
    assert result["entry_price"] == "100.5"
    assert result["tp1_hit"] == "1"


def test_decode_map_handles_mixed_types():
    """
    Проверяем, что _decode_map работает со смешанными типами (bytes + str).
    """
    input_map = {
        "status": "open",
        b"entry_price": b"100.5",
        "tp1_hit": "1",
    }
    result = _decode_map(input_map)
    
    assert result["status"] == "open"
    assert result["entry_price"] == "100.5"
    assert result["tp1_hit"] == "1"


def test_decode_map_handles_empty():
    """
    Проверяем, что _decode_map корректно обрабатывает пустые входы.
    """
    assert _decode_map(None) == {}
    assert _decode_map({}) == {}


# ========================================
# ✅ FIX #3: Тесты для _side_to_str
# ========================================
def test_side_to_str_normalizes_enum():
    """
    Проверяем, что _side_to_str нормализует Enum в "long"/"short".
    """
    # Мокаем Enum с custom __str__
    class MockSideLong:
        def __str__(self):
            return "Side.LONG"
    
    class MockSideShort:
        def __str__(self):
            return "Side.SHORT"
    
    assert _side_to_str(MockSideLong()) == "long"
    assert _side_to_str(MockSideShort()) == "short"


def test_side_to_str_handles_strings():
    """
    Проверяем, что _side_to_str работает со строками.
    """
    assert _side_to_str("long") == "long"
    assert _side_to_str("LONG") == "long"
    assert _side_to_str("short") == "short"
    assert _side_to_str("SHORT") == "short"
    assert _side_to_str("BUY_LONG") == "long"
    assert _side_to_str("SELL_SHORT") == "short"


# ========================================
# ✅ FIX #3: Тесты для _b01
# ========================================
def test_b01_normalizes_bool():
    """
    Проверяем, что _b01 нормализует булево в "0"/"1".
    """
    assert _b01(True) == "1"
    assert _b01(False) == "0"
    assert _b01(1) == "1"
    assert _b01(0) == "0"
    assert _b01("yes") == "1"  # непустая строка = True
    assert _b01("") == "0"     # пустая строка = False
    assert _b01(None) == "0"   # None = False


# ========================================
# ✅ FIX #2, #4: Тест save_open
# ========================================
def test_save_open_uses_pipeline_and_dual_keys():
    """
    Проверяем, что save_open:
    - Использует pipeline для атомарности (FIX #4)
    - Сохраняет оба ключа entry_time и entry_ts_ms (FIX #2)
    - Нормализует direction (FIX #3)
    - Нормализует булевы через _b01 (FIX #3)
    """
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe
    
    repo = RedisTradeRepository(mock_redis)
    
    pos = SimpleNamespace(
        id="P1",
        sid="S1",
        strategy="test",
        source="test",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",  # будет нормализовано в "long"
        entry_price=100.0,
        entry_ts_ms=1700000000000,
        lot=1.0,
        remaining_qty=1.0,
        sl=95.0,
        tp_levels=[105.0, 110.0, 115.0],
        tp_hits=0,
        trailing_distance=0.0,
        trailing_point=0.0,
        max_favorable_price=100.0,
        max_favorable_ts=1700000000000,
        mfe_pnl=0.0,
        mae_pnl=0.0,
        one_r_money=50.0,
        entry_tag="test",
        trail_profile="default",
        trailing_min_lock_r=0.0,
        min_lock_price=0.0,
        baseline_mode="none",
        baseline_horizon_ms=0,
        baseline_sl=0.0,
        baseline_tp1=0.0,
        baseline_tp2=0.0,
        baseline_tp3=0.0,
    )
    
    repo.save_open(pos)
    
    # ✅ Проверяем, что pipeline был использован (атомарность)
    mock_redis.pipeline.assert_called_once_with(transaction=True)
    mock_pipe.hset.assert_called_once()
    mock_pipe.sadd.assert_called_once_with("orders:open", "P1")
    mock_pipe.execute.assert_called_once()
    
    # ✅ Проверяем mapping
    mapping = mock_pipe.hset.call_args[1]["mapping"]
    
    # FIX #2: оба ключа присутствуют
    assert "entry_time" in mapping
    assert "entry_ts_ms" in mapping
    assert mapping["entry_time"] == "1700000000000"
    assert mapping["entry_ts_ms"] == "1700000000000"
    
    # FIX #3: direction нормализован
    assert mapping["direction"] == "long"
    
    # FIX #3: булевы нормализованы в "0"/"1"
    assert mapping["tp1_hit"] == "0"
    assert mapping["tp2_hit"] == "0"
    assert mapping["tp3_hit"] == "0"
    assert mapping["trailing_started"] == "0"
    assert mapping["trailing_active"] == "0"


# ========================================
# ✅ FIX #5: Тест идемпотентности save_closed
# ========================================
def test_save_closed_is_idempotent():
    """
    Проверяем, что save_closed:
    - При повторном вызове не дублирует записи (FIX #5)
    - Использует done_key + lock_key механизм для идемпотентности
    - Ранний выход при обнаружении closed_done ключа
    """
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe
    
    # Первый вызов - done_key не существует (get возвращает None)
    mock_redis.get.return_value = None
    # lock_key успешно создан (set возвращает True)
    mock_redis.set.return_value = True
    
    repo = RedisTradeRepository(mock_redis)
    
    # Создаём минимальный мок для TradeClosed
    closed = SimpleNamespace(
        order_id="P1",
        exit_ts_ms=1700000001000,
        exit_price=105.0,
        entry_price=100.0,
        lot=1.0,
        notional_usd=100.0,
        pnl_net=5.0,
        pnl_gross=5.5,
        fees=0.5,
        pnl_pct=5.0,
        pnl_if_fixed_exit=4.0,
        tp_hits=1,
        tp1_hit=True,
        tp2_hit=False,
        tp3_hit=False,
        tp_before_sl=True,
        close_reason_raw="TP1",
        close_reason="TP",
        close_reason_detail="",
        baseline_exit_reason="",
        baseline_exit_ts_ms=0,
        baseline_exit_price=0.0,
        entry_tag="",
        trailing_profile="",
        trailing_min_lock_r=0.0,
        min_lock_price=0.0,
        trailing_active=False,
        trailing_started=False,
        trailing_moves=0,
        duration_ms=1000,
        mfe_pnl=5.0,
        mae_pnl=-1.0,
        giveback=0.0,
        missed_profit=0.0,
        one_r_money=50.0,
        r_multiple=0.1,
        max_favorable_price=105.5,
        max_favorable_ts=1700000000500,
        schema_version=1,
        strategy="test",
        source="test",
        symbol="BTCUSDT",
        tf="1m",
    )
    
    # Патчим asdict, чтобы обойти dataclass requirement
    with patch("infra.redis_repo.asdict") as mock_asdict:
        mock_asdict.return_value = {
            "order_id": "P1",
            "exit_ts_ms": 1700000001000,
            "strategy": "test",
            "source": "test",
            "symbol": "BTCUSDT",
            "tf": "1m",
        }
        
        # Первый вызов - должен пройти
        repo.save_closed(closed)
        
        # Проверяем, что done_key был проверен
        mock_redis.get.assert_called_with("closed_done:P1")
        
        # Проверяем, что lock_key был установлен
        assert any("close_lock:P1" in str(call) for call in mock_redis.set.call_args_list)
        
        # Проверяем, что данные были сохранены
        mock_pipe.execute.assert_called_once()
        
        # Сбрасываем моки
        mock_redis.reset_mock()
        mock_pipe.reset_mock()
        mock_asdict.reset_mock()
        
        # Второй вызов - done_key уже существует (get возвращает "1")
        mock_redis.get.return_value = "1"
        
        # Повторный вызов - ранний выход
        repo.save_closed(closed)
        
        # Проверяем, что был проверен done_key
        mock_redis.get.assert_called_with("closed_done:P1")
        
        # Проверяем, что asdict НЕ был вызван (ранний выход)
        mock_asdict.assert_not_called()
        
        # Проверяем, что pipeline.execute НЕ был вызван
        mock_pipe.execute.assert_not_called()


# ========================================
# ✅ FIX #7: Тест load_open_positions с SSCAN
# ========================================
def test_load_open_positions_uses_sscan():
    """
    Проверяем, что load_open_positions:
    - Использует SSCAN вместо smembers (FIX #7)
    - Декодирует bytes через _decode_map (FIX #1)
    - Корректно применяет limit
    """
    mock_redis = MagicMock()
    
    # Мокаем SSCAN - вернём 3 позиции за 2 итерации
    mock_redis.sscan.side_effect = [
        (1, [b"P1", b"P2"]),  # cursor=1, batch=[P1, P2]
        (0, [b"P3"]),         # cursor=0, batch=[P3] (конец)
    ]
    
    # Мокаем hgetall для каждой позиции
    mock_redis.hgetall.side_effect = [
        {b"status": b"open", b"id": b"P1", b"entry_price": b"100.0"},
        {b"status": b"open", b"id": b"P2", b"entry_price": b"101.0"},
        {b"status": b"closed", b"id": b"P3"},  # закрытая - не должна попасть
    ]
    
    repo = RedisTradeRepository(mock_redis)
    
    # Загружаем с limit=10
    result = repo.load_open_positions(limit=10)
    
    # Проверяем, что sscan был вызван дважды
    assert mock_redis.sscan.call_count == 2
    
    # Проверяем, что hgetall был вызван для всех позиций
    assert mock_redis.hgetall.call_count == 3
    
    # Проверяем результат
    assert len(result) == 2  # только открытые
    assert result[0]["id"] == "P1"  # декодирован из bytes
    assert result[0]["status"] == "open"
    assert result[0]["entry_price"] == "100.0"
    assert result[1]["id"] == "P2"


def test_load_open_positions_respects_limit():
    """
    Проверяем, что limit применяется после валидации status=open.
    """
    mock_redis = MagicMock()
    
    # Возвращаем много позиций
    mock_redis.sscan.side_effect = [
        (1, [f"P{i}".encode() for i in range(500)]),
        (0, [f"P{i}".encode() for i in range(500, 600)]),
    ]
    
    # Все позиции открыты
    def mock_hgetall(key):
        pos_id = key.split(":")[-1]
        return {b"status": b"open", b"id": pos_id.encode()}
    
    mock_redis.hgetall.side_effect = mock_hgetall
    
    repo = RedisTradeRepository(mock_redis)
    
    # Загружаем с limit=10
    result = repo.load_open_positions(limit=10)
    
    # Проверяем, что вернули ровно 10
    assert len(result) == 10
    
    # Проверяем, что sscan был вызван только один раз (limit достигнут раньше)
    assert mock_redis.sscan.call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

