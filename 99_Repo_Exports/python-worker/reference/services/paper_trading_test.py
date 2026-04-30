from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
Paper Trading Test Script для TP1 Trailing System.

Этот скрипт эмулирует полный цикл paper trading с трейлингом:
1. Создаёт тестовый сигнал с trail_after_tp1=True
2. Эмитирует события TP1, TP2 (или TP1->SL для тестов откатов)
3. Проверяет правильность работы trailing системы
4. Собирает метрики и статистику

Использование:
    python -m services.paper_trading_test --scenario full --signals 10
"""

import argparse
import json
import os
import redis
import time
import random
from typing import List
from dataclasses import dataclass, asdict

from services.tp_event_emulator import TPEventEmulator
from common.log import setup_logger

log = setup_logger("paper_trading_test")


@dataclass
class TestSignal:
    """Тестовый сигнал для paper trading."""
    sid: str
    symbol: str
    side: str
    entry: float
    sl: float
    tp_levels: List[float]
    lot: float
    trail_after_tp1: bool
    trail_profile: str
    confidence: float
    atr: float
    ts: int


@dataclass
class TestResult:
    """Результат теста."""
    signal_id: str
    scenario: str
    success: bool
    trailing_activated: bool
    tp1_reached: bool
    tp2_reached: bool
    tp3_reached: bool
    sl_hit: bool
    error: str = ""


class PaperTradingTest:
    """
    Paper trading тест для TP1 Trailing System.
    
    Эмулирует различные сценарии движения цены после сигнала.
    """
    
    def __init__(self, redis_url: str = None):
        """
        Args:
            redis_url: URL Redis (если None, берётся из REDIS_URL env)
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(self.redis_url, decode_responses=True)
        self.emulator = TPEventEmulator(redis_url)
        
        self.results: List[TestResult] = []
        
        log.info("✅ PaperTradingTest initialized: redis=%s", self.redis_url)
    
    def create_test_signal(
        self
        symbol: str = "XAUUSD"
        side: str = "LONG"
        confidence: float = 80.0
        trail_profile: str = "rocket_v1"
    ) -> TestSignal:
        """
        Создать тестовый сигнал.
        
        Args:
            symbol: Символ
            side: Направление (LONG/SHORT)
            confidence: Уверенность в сигнале (0-100)
            trail_profile: Профиль трейлинга
            
        Returns:
            Тестовый сигнал
        """
        ts = get_ny_time_millis()
        sid = f"paper-test-{symbol}-{ts}-{random.randint(1000, 9999)}"
        
        # Симулируем реальные цены
        if symbol == "XAUUSD":
            base_price = 2765.5
        elif symbol == "BTCUSD":
            base_price = 50000.0
        else:
            base_price = 1.1000
        
        # Добавляем случайное отклонение
        entry = base_price + random.uniform(-10, 10)
        atr = 2.5 if symbol == "XAUUSD" else 250.0
        
        if side == "LONG":
            sl = entry - atr * 1.5
            tp_levels = [
                entry + atr * 2.0,  # TP1
                entry + atr * 3.0,  # TP2
                entry + atr * 4.0   # TP3
            ]
        else:
            sl = entry + atr * 1.5
            tp_levels = [
                entry - atr * 2.0
                entry - atr * 3.0
                entry - atr * 4.0
            ]
        
        signal = TestSignal(
            sid=sid
            symbol=symbol
            side=side
            entry=entry
            sl=sl
            tp_levels=tp_levels
            lot=0.03
            trail_after_tp1=confidence >= 60
            trail_profile=trail_profile
            confidence=confidence
            atr=atr
            ts=ts
        )
        
        return signal
    
    def save_signal_to_redis(self, signal: TestSignal):
        """Сохранить сигнал в Redis."""
        signal_data = asdict(signal)
        signal_key = f"signals:{signal.sid}"
        
        self.r.set(signal_key, json.dumps(signal_data), ex=3600)  # TTL 1 час
        log.info("💾 Signal saved: %s", signal.sid)
    
    def run_scenario(
        self
        signal: TestSignal
        scenario: str
    ) -> TestResult:
        """
        Запустить сценарий paper trading.
        
        Args:
            signal: Тестовый сигнал
            scenario: Название сценария
            
        Returns:
            Результат теста
        """
        log.info("🎬 Running scenario '%s' for %s", scenario, signal.sid)
        
        result = TestResult(
            signal_id=signal.sid
            scenario=scenario
            success=False
            trailing_activated=False
            tp1_reached=False
            tp2_reached=False
            tp3_reached=False
            sl_hit=False
        )
        
        try:
            # Сохраняем сигнал в Redis
            self.save_signal_to_redis(signal)
            
            # Ждём немного
            time.sleep(0.5)
            
            # Запускаем сценарий
            if scenario == "tp1_only":
                self.emulator.emit_tp1_hit(signal.sid, signal.symbol, signal.tp_levels[0])
                result.tp1_reached = True
                
            elif scenario == "tp1_then_tp2":
                self.emulator.emit_tp1_hit(signal.sid, signal.symbol, signal.tp_levels[0])
                result.tp1_reached = True
                time.sleep(1)
                self.emulator.emit_tp2_hit(signal.sid, signal.symbol, signal.tp_levels[1])
                result.tp2_reached = True
                
            elif scenario == "tp1_then_tp2_then_tp3":
                self.emulator.emit_tp1_hit(signal.sid, signal.symbol, signal.tp_levels[0])
                result.tp1_reached = True
                time.sleep(1)
                self.emulator.emit_tp2_hit(signal.sid, signal.symbol, signal.tp_levels[1])
                result.tp2_reached = True
                time.sleep(1)
                self.emulator.emit_tp3_hit(signal.sid, signal.symbol, signal.tp_levels[2])
                result.tp3_reached = True
                
            elif scenario == "tp1_then_sl":
                self.emulator.emit_tp1_hit(signal.sid, signal.symbol, signal.tp_levels[0])
                result.tp1_reached = True
                time.sleep(1)
                self.emulator.emit_sl_hit(signal.sid, signal.symbol, signal.sl)
                result.sl_hit = True
                
            elif scenario == "direct_sl":
                self.emulator.emit_sl_hit(signal.sid, signal.symbol, signal.sl)
                result.sl_hit = True
            
            # Ждём обработки
            time.sleep(2)
            
            # Проверяем активацию трейлинга
            if signal.trail_after_tp1 and result.tp1_reached:
                # Проверяем события трейлинга в Redis
                events_stream = "events:trades"
                trailing_events = self.r.xrevrange(events_stream, count=10)
                
                for msg_id, fields in trailing_events:
                    if fields.get("event_type") == "TRAILING_STARTED" and fields.get("sid") == signal.sid:
                        result.trailing_activated = True
                        break
            
            result.success = True
            
        except Exception as e:
            result.error = str(e)
            log.error("❌ Scenario failed: %s - %s", scenario, e)
        
        return result
    
    def run_test_suite(
        self
        num_signals: int = 5
        scenarios: List[str] = None
    ) -> List[TestResult]:
        """
        Запустить набор тестов.
        
        Args:
            num_signals: Количество сигналов для каждого сценария
            scenarios: Список сценариев (если None, используются все)
            
        Returns:
            Список результатов
        """
        if scenarios is None:
            scenarios = [
                "tp1_only"
                "tp1_then_tp2"
                "tp1_then_tp2_then_tp3"
                "tp1_then_sl"
                "direct_sl"
            ]
        
        log.info("🧪 Starting test suite: %d signals per scenario", num_signals)
        
        results = []
        
        for scenario in scenarios:
            log.info("\n" + "=" * 60)
            log.info("Testing scenario: %s", scenario)
            log.info("=" * 60)
            
            for i in range(num_signals):
                # Варьируем параметры
                confidence = random.uniform(50, 95)
                profile = random.choice(["rocket_v1", "lock_and_trail", "wide_swing"])
                side = random.choice(["LONG", "SHORT"])
                
                signal = self.create_test_signal(
                    symbol="XAUUSD"
                    side=side
                    confidence=confidence
                    trail_profile=profile
                )
                
                result = self.run_scenario(signal, scenario)
                results.append(result)
                
                log.info(
                    "Test %d/%d: %s - %s | trailing=%s"
                    i + 1, num_signals, scenario
                    "✅ OK" if result.success else "❌ FAIL"
                    "✅" if result.trailing_activated else "❌"
                )
                
                # Небольшая пауза между тестами
                time.sleep(0.5)
        
        self.results = results
        return results
    
    def print_summary(self):
        """Вывести сводку результатов."""
        if not self.results:
            log.warning("No results to summarize")
            return
        
        total = len(self.results)
        successful = sum(1 for r in self.results if r.success)
        trailing_activated = sum(1 for r in self.results if r.trailing_activated)
        tp1_reached = sum(1 for r in self.results if r.tp1_reached)
        tp2_reached = sum(1 for r in self.results if r.tp2_reached)
        tp3_reached = sum(1 for r in self.results if r.tp3_reached)
        sl_hit = sum(1 for r in self.results if r.sl_hit)
        
        print("\n" + "=" * 60)
        print("PAPER TRADING TEST SUMMARY")
        print("=" * 60)
        print(f"Total tests:            {total}")
        print(f"Successful:             {successful} ({successful/total*100:.1f}%)")
        print(f"Trailing activated:     {trailing_activated}")
        print(f"TP1 reached:            {tp1_reached}")
        print(f"TP2 reached:            {tp2_reached}")
        print(f"TP3 reached:            {tp3_reached}")
        print(f"SL hit:                 {sl_hit}")
        print("=" * 60)
        
        # Группируем по сценариям
        scenarios = {}
        for result in self.results:
            if result.scenario not in scenarios:
                scenarios[result.scenario] = []
            scenarios[result.scenario].append(result)
        
        print("\nRESULTS BY SCENARIO:")
        print("=" * 60)
        
        for scenario, scenario_results in scenarios.items():
            total_sc = len(scenario_results)
            success_sc = sum(1 for r in scenario_results if r.success)
            trailing_sc = sum(1 for r in scenario_results if r.trailing_activated)
            
            print(f"\n{scenario}:")
            print(f"  Total:     {total_sc}")
            print(f"  Success:   {success_sc}/{total_sc}")
            print(f"  Trailing:  {trailing_sc}/{total_sc}")


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description="Paper Trading Test for TP1 Trailing")
    parser.add_argument(
        "--scenario"
        choices=["tp1_only", "tp1_then_tp2", "tp1_then_tp2_then_tp3", "tp1_then_sl", "direct_sl", "all"]
        default="all"
        help="Test scenario"
    )
    parser.add_argument("--signals", type=int, default=5, help="Number of signals per scenario")
    parser.add_argument("--symbol", default="XAUUSD", help="Trading symbol")
    
    args = parser.parse_args()
    
    log.info("=" * 60)
    log.info("PAPER TRADING TEST")
    log.info("=" * 60)
    log.info("Scenario: %s", args.scenario)
    log.info("Signals per scenario: %d", args.signals)
    log.info("Symbol: %s", args.symbol)
    log.info("=" * 60)
    
    test = PaperTradingTest()
    
    if args.scenario == "all":
        test.run_test_suite(num_signals=args.signals)
    else:
        test.run_test_suite(num_signals=args.signals, scenarios=[args.scenario])
    
    test.print_summary()


if __name__ == "__main__":
    main()

