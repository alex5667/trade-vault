#!/usr/bin/env python3
"""
Интеграционный тест для проверки логики закрытия сделок.
Тестирует полный цикл: сигнал → открытие → тики → закрытие.
"""

import sys
import os
import time
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock

# Add paths
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'python-worker'))
sys.path.insert(0, current_dir)

# Import test dependencies
from domain.models import SignalNorm, PositionState, TradeClosed, Tick
from domain.handlers import process_tick
from domain.tick_price import build_tick
from services.trade_monitor import TradeMonitorService
from services.pnl_math import SymbolSpec

class TestTradeClosureIntegration(unittest.TestCase):
    """Интеграционные тесты для логики закрытия сделок."""

    def setUp(self):
        """Подготовка тестового окружения."""
        # Mock Redis to avoid connection issues
        self.redis_mock = Mock()
        self.redis_mock.hgetall.return_value = {}
        self.redis_mock.get.return_value = None
        self.redis_mock.set.return_value = True
        self.redis_mock.delete.return_value = 1

        # Mock repo
        self.repo_mock = Mock()
        self.repo_mock.load_open_positions.return_value = []
        self.repo_mock.save_closed.return_value = None
        self.repo_mock.append_event.return_value = None
        self.repo_mock.persist_signal.return_value = None
        self.repo_mock.save_open.return_value = None

        # Mock spec
        self.spec_mock = Mock()
        self.spec_mock.contract_size = 1.0
        self.spec_mock.max_time_back_ms = 0
        self.spec_mock.commission_rate = 0.0
        self.spec_mock.report_min_risk_usd = 1.0
        self.spec_mock.report_fees_risk_mult = 3.0
        self.spec_mock.pnl_money = Mock(side_effect=lambda e, p, l, d, symbol=None: (p - e) * l if d == "LONG" else (e - p) * l)
        self.spec_mock.calculate_fees = Mock(return_value=0.0)  # No fees for tests

    def test_long_position_tp1_hit(self):
        """Тест закрытия LONG позиции по TP1."""
        # Создаем сигнал
        signal = SignalNorm(
            sid="test-sid-1",
            strategy="TestStrategy",
            source="TestSource",
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            entry_price=50000.0,
            entry_ts_ms=int(time.time() * 1000),
            lot=0.1,
            sl=49500.0,  # SL ниже entry
            tp_levels=[50500.0],  # Только один TP уровень для полного закрытия
            payload={}
        )

        # Создаем позицию
        pos = PositionState(
            id="test-pos-1",
            sid=signal.sid,
            strategy=signal.strategy,
            source=signal.source,
            symbol=signal.symbol,
            tf=signal.tf,
            direction=signal.direction,
            entry_price=signal.entry_price,
            entry_ts_ms=signal.entry_ts_ms,
            lot=signal.lot,
            remaining_qty=signal.lot,
            sl=signal.sl,
            tp_levels=signal.tp_levels,
            tp_hits=0,
            tp1_hit=False,
            tp2_hit=False,
            tp3_hit=False,
            trailing_started=False,
            trailing_active=False,
            trailing_moves_count=0,
            trailing_distance=0.0,
            trailing_point=0.0,
            max_favorable_price=signal.entry_price,
            max_favorable_ts=signal.entry_ts_ms,
            atr=500.0
        )

        # Создаем tick на уровне TP1
        tick_data = {
            "symbol": "BTCUSDT",
            "ts": signal.entry_ts_ms + 60000,  # +1 минута
            "bid": 50500.0,
            "ask": 50505.0,
            "last": 50502.5,
            "price": 50502.5
        }
        tick = build_tick(tick_data)
        self.assertIsNotNone(tick, "Tick должен быть создан")

        # Обрабатываем tick
        print(f"DEBUG LONG TP1: pos.closed={pos.closed}, remaining_qty={pos.remaining_qty}, tp_levels={pos.tp_levels}")
        print(f"DEBUG LONG TP1: tick.mid={tick.mid}, bid={tick.bid}, ask={tick.ask}")
        events, closed = process_tick(
            pos, tick, self.spec_mock,
            tp_ratios=(1.0,),  # 100% на первом TP уровне
            fill_policy="level"
        )
        print(f"DEBUG LONG TP1: events={len(events)}, closed={closed is not None}")

        # Проверяем результаты
        self.assertIsNotNone(closed, "Позиция должна быть закрыта по TP1")
        print(f"Events: {[e.event_type for e in events]}")
        # Может быть TP_HIT, TRAILING_SYNC и CLOSE
        self.assertGreaterEqual(len(events), 2, f"Должно быть минимум 2 события, получено {len(events)}")
        self.assertIn("TP_HIT", [e.event_type for e in events], "Должно быть TP_HIT событие")
        self.assertIn("CLOSE", [e.event_type for e in events], "Должно быть CLOSE событие")

        # Проверяем состояние позиции
        self.assertTrue(pos.closed, "Позиция должна быть помечена как закрытая")
        self.assertEqual(pos.exit_price, 50500.0, "Цена закрытия должна быть TP1")
        self.assertEqual(pos.remaining_qty, 0.0, "Оставшийся объем должен быть 0")
        self.assertEqual(pos.tp_hits, 1, "Должен быть 1 TP hit")

        # Проверяем закрытую сделку
        self.assertEqual(closed.close_reason_raw, "TP1", "Причина закрытия должна быть TP1")
        self.assertGreater(closed.pnl_net, 0, "PnL должен быть положительным для TP1")

        print("✅ Тест LONG TP1 прошел успешно")

    def test_long_position_sl_hit(self):
        """Тест закрытия LONG позиции по SL."""
        # Создаем сигнал
        signal = SignalNorm(
            sid="test-sid-2",
            strategy="TestStrategy",
            source="TestSource",
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            entry_price=50000.0,
            entry_ts_ms=int(time.time() * 1000),
            lot=0.1,
            sl=49500.0,
            tp_levels=[50500.0, 51000.0, 51500.0],
            payload={}
        )

        # Создаем позицию
        pos = PositionState(
            id="test-pos-2",
            sid=signal.sid,
            strategy=signal.strategy,
            source=signal.source,
            symbol=signal.symbol,
            tf=signal.tf,
            direction=signal.direction,
            entry_price=signal.entry_price,
            entry_ts_ms=signal.entry_ts_ms,
            lot=signal.lot,
            remaining_qty=signal.lot,
            sl=signal.sl,
            tp_levels=signal.tp_levels,
            tp_hits=0,
            tp1_hit=False,
            tp2_hit=False,
            tp3_hit=False,
            trailing_started=False,
            trailing_active=False,
            trailing_moves_count=0,
            trailing_distance=0.0,
            trailing_point=0.0,
            max_favorable_price=signal.entry_price,
            max_favorable_ts=signal.entry_ts_ms,
            atr=500.0
        )

        # Создаем tick на уровне SL
        tick_data = {
            "symbol": "BTCUSDT",
            "ts": signal.entry_ts_ms + 60000,
            "bid": 49495.0,  # SL уровень
            "ask": 49500.0,
            "last": 49497.5,
            "price": 49497.5
        }
        tick = build_tick(tick_data)
        self.assertIsNotNone(tick)

        # Обрабатываем tick
        events, closed = process_tick(
            pos, tick, self.spec_mock,
            tp_ratios=(0.5, 0.3, 0.2),
            fill_policy="level"
        )

        # Проверяем результаты
        self.assertIsNotNone(closed, "Позиция должна быть закрыта по SL")
        self.assertEqual(len(events), 2, "Должно быть 2 события: SL_HIT и CLOSE")
        self.assertEqual(events[0].event_type, "SL_HIT")
        self.assertEqual(events[1].event_type, "CLOSE")

        # Проверяем состояние позиции
        self.assertTrue(pos.closed)
        self.assertEqual(pos.exit_price, 49500.0, "Цена закрытия должна быть SL")
        self.assertEqual(pos.remaining_qty, 0.0)

        # Проверяем закрытую сделку
        self.assertIn(closed.close_reason, ["SL", "INITIAL_SL"], f"Причина закрытия должна быть SL-related, получено: {closed.close_reason}")
        self.assertLess(closed.pnl_net, 0, "PnL должен быть отрицательным для SL")

        print("✅ Тест LONG SL прошел успешно")

    def test_short_position_tp1_hit(self):
        """Тест закрытия SHORT позиции по TP1."""
        # Создаем сигнал
        signal = SignalNorm(
            sid="test-sid-3",
            strategy="TestStrategy",
            source="TestSource",
            symbol="BTCUSDT",
            tf="1m",
            direction="SHORT",
            entry_price=50000.0,
            entry_ts_ms=int(time.time() * 1000),
            lot=0.1,
            sl=50500.0,  # SL выше entry для SHORT
            tp_levels=[49500.0, 49000.0, 48500.0],  # TP ниже entry для SHORT
            payload={}
        )

        # Создаем позицию
        pos = PositionState(
            id="test-pos-3",
            sid=signal.sid,
            strategy=signal.strategy,
            source=signal.source,
            symbol=signal.symbol,
            tf=signal.tf,
            direction=signal.direction,
            entry_price=signal.entry_price,
            entry_ts_ms=signal.entry_ts_ms,
            lot=signal.lot,
            remaining_qty=signal.lot,
            sl=signal.sl,
            tp_levels=signal.tp_levels,
            tp_hits=0,
            tp1_hit=False,
            tp2_hit=False,
            tp3_hit=False,
            trailing_started=False,
            trailing_active=False,
            trailing_moves_count=0,
            trailing_distance=0.0,
            trailing_point=0.0,
            max_favorable_price=signal.entry_price,  # Для SHORT max_favorable - это минимальная цена
            max_favorable_ts=signal.entry_ts_ms,
            atr=500.0
        )

        # Создаем tick на уровне TP1 (ниже entry для SHORT)
        tick_data = {
            "symbol": "BTCUSDT",
            "ts": signal.entry_ts_ms + 60000,
            "bid": 49490.0,  # Ниже TP уровня
            "ask": 49495.0,  # TP1 уровень для SHORT (ask <= tp_level)
            "last": 49492.5,
            "price": 49492.5
        }
        tick = build_tick(tick_data)
        self.assertIsNotNone(tick)

        # Обрабатываем tick
        print(f"DEBUG SHORT TP1: pos.closed={pos.closed}, remaining_qty={pos.remaining_qty}, tp_levels={pos.tp_levels}")
        print(f"DEBUG SHORT TP1: tick.mid={tick.mid}, bid={tick.bid}, ask={tick.ask}")
        events, closed = process_tick(
            pos, tick, self.spec_mock,
            tp_ratios=(1.0,),
            fill_policy="level"
        )
        print(f"DEBUG SHORT TP1: events={len(events)}, closed={closed is not None}")

        # Проверяем результаты
        self.assertIsNotNone(closed, "SHORT позиция должна быть закрыта по TP1")
        print(f"SHORT Events: {[e.event_type for e in events]}")
        self.assertGreaterEqual(len(events), 2, f"Должно быть минимум 2 события, получено {len(events)}")
        self.assertIn("TP_HIT", [e.event_type for e in events], "Должно быть TP_HIT событие")
        self.assertIn("CLOSE", [e.event_type for e in events], "Должно быть CLOSE событие")

        # Проверяем состояние позиции
        self.assertTrue(pos.closed)
        self.assertEqual(pos.exit_price, 49500.0)
        self.assertEqual(pos.remaining_qty, 0.0)
        self.assertEqual(pos.tp_hits, 1)

        # Проверяем закрытую сделку
        self.assertEqual(closed.close_reason_raw, "TP1")
        self.assertGreater(closed.pnl_net, 0, "PnL должен быть положительным для TP1 SHORT")

        print("✅ Тест SHORT TP1 прошел успешно")

    def test_partial_tp_closures(self):
        """Тест частичного закрытия позиций по TP уровням."""
        # Создаем сигнал
        signal = SignalNorm(
            sid="test-sid-4",
            strategy="TestStrategy",
            source="TestSource",
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            entry_price=50000.0,
            entry_ts_ms=int(time.time() * 1000),
            lot=1.0,  # Больший объем для тестирования частичных закрытий
            sl=49500.0,
            tp_levels=[50200.0, 50400.0, 50600.0],  # TP уровни
            payload={}
        )

        # Создаем позицию
        pos = PositionState(
            id="test-pos-4",
            sid=signal.sid,
            strategy=signal.strategy,
            source=signal.source,
            symbol=signal.symbol,
            tf=signal.tf,
            direction=signal.direction,
            entry_price=signal.entry_price,
            entry_ts_ms=signal.entry_ts_ms,
            lot=signal.lot,
            remaining_qty=signal.lot,
            sl=signal.sl,
            tp_levels=signal.tp_levels,
            tp_hits=0,
            tp1_hit=False,
            tp2_hit=False,
            tp3_hit=False,
            trailing_started=False,
            trailing_active=False,
            trailing_moves_count=0,
            trailing_distance=0.0,
            trailing_point=0.0,
            max_favorable_price=signal.entry_price,
            max_favorable_ts=signal.entry_ts_ms,
            atr=200.0
        )

        # Первый TP hit - TP1
        tick_data_1 = {
            "symbol": "BTCUSDT",
            "ts": signal.entry_ts_ms + 60000,
            "bid": 50200.0,
            "ask": 50205.0,
            "last": 50202.5,
            "price": 50202.5
        }
        tick_1 = build_tick(tick_data_1)

        events_1, closed_1 = process_tick(
            pos, tick_1, self.spec_mock,
            tp_ratios=(0.5, 0.3, 0.2),  # 50%, 30%, 20%
            fill_policy="level"
        )

        # Проверяем частичное закрытие - TP1 не должен полностью закрывать позицию
        self.assertIsNone(closed_1, "TP1 не должен полностью закрывать позицию")
        self.assertGreater(len(events_1), 0, "Должны быть события")
        self.assertEqual(pos.tp_hits, 1)
        self.assertEqual(pos.tp1_hit, True)
        self.assertEqual(pos.remaining_qty, 0.5, "Должно остаться 50% от исходного объема")

        # Второй TP hit - TP2
        tick_data_2 = {
            "symbol": "BTCUSDT",
            "ts": signal.entry_ts_ms + 120000,  # +2 минуты
            "bid": 50400.0,
            "ask": 50405.0,
            "last": 50402.5,
            "price": 50402.5
        }
        tick_2 = build_tick(tick_data_2)

        events_2, closed_2 = process_tick(
            pos, tick_2, self.spec_mock,
            tp_ratios=(0.5, 0.3, 0.2),
            fill_policy="level"
        )

        # Проверяем второе частичное закрытие - TP2 тоже не должен полностью закрывать
        self.assertIsNone(closed_2, "TP2 не должен полностью закрывать позицию")
        self.assertEqual(pos.tp_hits, 2)
        self.assertEqual(pos.tp2_hit, True)
        self.assertAlmostEqual(pos.remaining_qty, 0.2, places=2, msg="Должно остаться 20% от исходного объема")

        # Третий TP hit - TP3 (финальное закрытие)
        tick_data_3 = {
            "symbol": "BTCUSDT",
            "ts": signal.entry_ts_ms + 180000,  # +3 минуты
            "bid": 50600.0,
            "ask": 50605.0,
            "last": 50602.5,
            "price": 50602.5
        }
        tick_3 = build_tick(tick_data_3)

        events_3, closed_3 = process_tick(
            pos, tick_3, self.spec_mock,
            tp_ratios=(0.5, 0.3, 0.2),
            fill_policy="level"
        )

        # Проверяем финальное закрытие
        self.assertIsNotNone(closed_3, "Должно быть финальное закрытие по TP3")
        self.assertEqual(pos.tp_hits, 3)
        self.assertEqual(pos.tp3_hit, True)
        self.assertEqual(pos.remaining_qty, 0.0, "Вся позиция должна быть закрыта")
        self.assertTrue(pos.closed, "Позиция должна быть помечена как закрытая")

        print("✅ Тест частичных TP закрытий прошел успешно")

    def test_trade_monitor_integration(self):
        """Интеграционный тест TradeMonitorService."""
        with patch('services.trade_monitor.get_redis', return_value=self.redis_mock):
            with patch('infra.redis_repo.RedisTradeRepository') as mock_repo_class:
                mock_repo_class.return_value = self.repo_mock

                # Создаем TradeMonitorService
                monitor = TradeMonitorService(
                    redis_client=self.redis_mock,
                    repo=self.repo_mock,
                    config={}
                )

                # Проверяем инициализацию
                self.assertIsNotNone(monitor.redis)
                self.assertIsNotNone(monitor.repo)
                self.assertEqual(len(monitor.open_positions), 0)

                print("✅ TradeMonitorService инициализация успешна")

    def test_tick_price_logic(self):
        """Тест логики определения trigger prices."""
        from domain.tick_price import trigger_prices

        # LONG позиция
        tick_long = Tick(
            symbol="BTCUSDT",
            ts_ms=int(time.time() * 1000),
            bid=49995.0,
            ask=50005.0,
            last=50000.0,
            price=50000.0,
            mid=50000.0
        )

        tp_px, sl_px, mid = trigger_prices(tick_long, "LONG")
        self.assertEqual(tp_px, 49995.0, "Для LONG TP должен использовать BID")
        self.assertEqual(sl_px, 49995.0, "Для LONG SL должен использовать BID")

        # SHORT позиция
        tp_px, sl_px, mid = trigger_prices(tick_long, "SHORT")
        self.assertEqual(tp_px, 50005.0, "Для SHORT TP должен использовать ASK")
        self.assertEqual(sl_px, 50005.0, "Для SHORT SL должен использовать ASK")

        print("✅ Логика trigger prices работает корректно")


if __name__ == "__main__":
    print("🚀 Запуск интеграционных тестов закрытия сделок...")
    unittest.main(verbosity=2)
