#!/usr/bin/env python3
from __future__ import annotations
"""
Demo: Signal Execution Planning and Performance Analysis

Этот скрипт демонстрирует полный цикл работы с сигналами:
1. Создание ExecutionPlan из SignalContext
2. Симуляция исполнения сделки
3. Анализ производительности (TTD, MFE/MAE, Realized R)
4. Опционально - сохранение в TimescaleDB

Запуск: python examples/demo_execution_and_performance.py

Результаты:
- EXECUTION PLAN: Детальный план исполнения с уровнями и рисками
- SIGNAL PERFORMANCE: Метрики производительности и outcome
"""


import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

# Импорт из signal_exec модуля
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from signal_exec.models import (
    Side,
    SwingPoint,
    HTFLevel,
    OrderBookSnapshot,
    AccountState,
    ExtendedSignalContext,
    ExecutionPlan,
    Bar1m,
)
from signal_exec.execution_planner import ExecutionPlanner, SymbolSetupConfig
from signal_exec.performance_tracker import SignalPerformanceTracker


# ---------- 1. Конфигурация по инструменту/сетапу ----------

def build_setup_configs() -> dict[tuple[str, str], SymbolSetupConfig]:
    """
    Конфигурация для разных (symbol, setup_type).
    Пример для  + 'volatility_spike'.
    """
    cfg_xau_vol_spike = SymbolSetupConfig(
        symbol="",
        setup_type="volatility_spike",

        # Время жизни сигнала (bars 1m).
        # Можно обновлять из таблицы signal_ttd_config.
        expiry_bars=4,  # по статистике edge живет ~4 бара

        # Минимальный стоп и ограничения
        min_stop_ticks=10,       # минимум 10 тиков
        max_stop_R=3.0,          # стоп не шире 3 * ATR(1m)
        atr_buffer_ratio=0.15,   # 15% ATR в качестве буфера за swing

        # Entry-зона в R
        entry_zone_min_R=0.3,
        entry_zone_max_R=0.7,

        # Дефолтные цели в R
        default_tp_R=(1.0, 2.0, 3.0),

        # Бакеты конфи-скора и множители риска
        #    score < 0.4  → 0.5× базового риска
        # 0.4 ≤ score < 0.7 → 1.0×
        # 0.7 ≤ score < 0.85→ 1.5×
        # 0.85 ≤ score      → 2.0×
        score_buckets=(0.4, 0.7, 0.85),
        risk_multipliers=(0.5, 1.0, 1.5, 2.0),

        # Глобальные ограничения
        max_risk_R_per_trade=1.0,
        max_portfolio_risk_pct=5.0,
    )

    return {
        ("volatility_spike"): cfg_xau_vol_spike,
    }


# ---------- 2. Синтетический SignalContext ----------

def build_mock_signal_context() -> ExtendedSignalContext:
    """
    Создаем моковый контекст сигнала для .
    Имитируем выход детектора сигналов с микроструктурными данными.
    """
    now = datetime.now(timezone.utc)

    # Локальные экстремумы по LTF вокруг сигнала
    local_swings = [
        SwingPoint(
            ts=now - timedelta(minutes=5),
            price=2600.0,
            type="low",
            volume=150.0,
            delta=50.0,
        ),
        SwingPoint(
            ts=now - timedelta(minutes=3),
            price=2603.0,
            type="high",
            volume=200.0,
            delta=-80.0,
        ),
        SwingPoint(
            ts=now - timedelta(minutes=1),
            price=2601.5,
            type="low",
            volume=220.0,
            delta=70.0,
        ),
    ]

    # HTF-уровни (дневные high/low, VWAP-зоны)
    htf_levels = [
        HTFLevel(
            ts=now - timedelta(hours=1),
            price=2610.0,
            kind="D_high",
            strength=0.9,
        ),
        HTFLevel(
            ts=now - timedelta(hours=2),
            price=2620.0,
            kind="H1_high",
            strength=0.7,
        ),
    ]

    # L2 snapshot на момент сигнала
    l2_snapshot = OrderBookSnapshot(
        ts=now,
        best_bid=2602.0,
        best_ask=2602.1,
        bids=[2602.0, 2601.9, 2601.8],
        asks=[2602.1, 2602.2, 2602.3],
    )

    # Состояние счета
    account_state = AccountState(
        equity_usd=10_000.0,       # Equity = 10k USD
        open_risk_usd=100.0,       # Уже открытый риск ($100)
        max_risk_per_trade_pct=0.5,   # 0.5% на сделку
        max_portfolio_risk_pct=5.0,   # 5% максимум по всем сделкам
    )

    # TTD expiry из предварительного анализа
    ttd_expiry_bars = 4

    ctx = ExtendedSignalContext(
        signal_id=str(uuid.uuid4()),
        symbol="",
        side=Side.LONG,             # Ищем long
        setup_type="volatility_spike",

        ts_signal=now,
        price_at_signal=2602.0,

        atr_1m=1.0,                 # ATR(1m) ~ $1
        atr_5m=2.5,

        final_score=0.82,           # Высокий скор (между 0.7 и 0.85)

        l2_snapshot=l2_snapshot,
        local_swings=local_swings,
        htf_levels=htf_levels,

        tick_size=0.1,              # Шаг цены $0.1
        contract_size=100.0,        # 1 лот = 100 унций

        account_state=account_state,
        ttd_expiry_bars=ttd_expiry_bars,
    )
    return ctx


# ---------- 3. Генерация ExecutionPlan ----------

def demo_execution_planner() -> ExecutionPlan | None:
    """
    Демонстрация создания плана исполнения.
    """
    setup_configs = build_setup_configs()
    planner = ExecutionPlanner(setup_configs)

    ctx = build_mock_signal_context()
    plan = planner.build_plan(ctx)

    print("=" * 80)
    print("EXECUTION PLAN FOR  VOLATILITY SPIKE")
    print("=" * 80)

    if plan is None:
        print("❌ План не построен (риск=0 или стоп слишком широкий)")
        return None

    print("📊 Signal Details:")
    print(f"   Signal ID      : {plan.signal_id}")
    print(f"   Symbol         : {plan.symbol}")
    print(f"   Side           : {plan.side.value.upper()}")
    print()

    print("🎯 Entry Zone:")
    print(f"   Entry Low      : {plan.entry_zone_low:.2f}")
    print(f"   Entry High     : {plan.entry_zone_high:.2f}")
    print()

    print("🛑 Risk Management:")
    print(f"   Stop Price     : {plan.stop_price:.2f}")
    print(f"   TP Levels      : {[round(x, 2) for x in plan.tp_levels]}")
    print(f"   Partials       : {plan.partials}")
    print()

    print("💰 Position Sizing:")
    print(f"   Risk R         : {plan.pos_risk_R:.2f}")
    print(f"   Risk USD       : ${plan.risk_usd:.2f}")
    print(f"   Position Size  : {plan.position_size:.3f} lots")
    print()

    print("⏰ Timing:")
    print(f"   Expiry Bars    : {plan.expiry_bars} (1m bars)")
    print(f"   Created At     : {plan.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    return plan


# ---------- 4. Моделирование баров и сделки для Performance ----------

def build_mock_bars(
    ts_start: datetime,
    n_bars: int,
    base_price: float,
    side: Side,
) -> List[Bar1m]:
    """
    Генерация синтетических 1m баров для симуляции рынка.

    Простая модель:
    - Первые 5 баров: тренд в сторону edge
    - Остальное: откат и шум
    """
    bars: List[Bar1m] = []
    ts = ts_start

    for i in range(n_bars):
        # Трендовая фаза (первые 5 баров)
        if i < 5:
            drift = 0.5 if side == Side.LONG else -0.5
        else:
            drift = 0.0

        # Простое случайное движение (детерминированное для повторяемости)
        noise = 0.1 * math.sin(i * 0.5)
        price = base_price + i * drift + noise

        # Формируем OHLC
        high = price + 0.3
        low = price - 0.3
        open_ = price - 0.1
        close = price + 0.1

        bars.append(
            Bar1m(
                ts=ts,
                open=open_,
                high=high,
                low=low,
                close=close,
            )
        )
        ts += timedelta(minutes=1)

    return bars


def demo_performance(plan: ExecutionPlan, ctx: ExtendedSignalContext) -> None:
    """
    Демонстрация анализа производительности сигнала.

    Симулируем:
    - Вход через 1 минуту по средней цене entry-зоны
    - Выход через 7 минут по второму TP уровню
    - 30 минут баров для анализа MFE/MAE и TTD
    """
    tracker = SignalPerformanceTracker(r_target=1.0, max_ttd_bars=30)

    # Симуляция входа в позицию
    entry_ts = ctx.ts_signal + timedelta(minutes=1)
    entry_price = 0.5 * (plan.entry_zone_low + plan.entry_zone_high)

    # Симуляция выхода (фиксация части позиции по TP2)
    exit_ts = ctx.ts_signal + timedelta(minutes=7)
    exit_price = plan.tp_levels[1] if len(plan.tp_levels) > 1 else plan.tp_levels[0]

    stop_price = plan.stop_price

    # Генерируем 30 минут баров после сигнала
    bars = build_mock_bars(
        ts_start=ctx.ts_signal,
        n_bars=30,
        base_price=ctx.price_at_signal,
        side=ctx.side,
    )

    # Анализируем производительность
    perf = tracker.build_performance(
        ctx=ctx,
        bars=bars,
        entry_ts=entry_ts,
        exit_ts=exit_ts,
        entry_price=entry_price,
        exit_price=exit_price,
        stop_price=stop_price,
        expired_without_entry=False,
    )

    print("\n" + "=" * 80)
    print("SIGNAL PERFORMANCE ANALYSIS")
    print("=" * 80)

    print("📊 Trade Summary:")
    print(f"   Symbol         : {perf.symbol}")
    print(f"   Setup Type     : {perf.setup_type}")
    print(f"   Side           : {perf.side.value.upper()}")
    print()

    print("⏰ Timing:")
    print(f"   Signal Time    : {perf.ts_signal.strftime('%H:%M:%S')}")
    print(f"   Entry Time     : {perf.ts_entry.strftime('%H:%M:%S') if perf.ts_entry else 'N/A'}")
    print(f"   Exit Time      : {perf.ts_exit.strftime('%H:%M:%S') if perf.ts_exit else 'N/A'}")
    print()

    print("💰 Price Action:")
    print(f"   Signal Price   : {perf.price_at_signal:.2f}")
    print(f"   Entry Price    : {perf.entry_price:.2f}" if perf.entry_price else "   Entry Price    : N/A")
    print(f"   Exit Price     : {perf.exit_price:.2f}" if perf.exit_price else "   Exit Price     : N/A")
    print(f"   Stop Price     : {perf.stop_price:.2f}" if perf.stop_price else "   Stop Price     : N/A")
    print()

    print("📈 Performance Metrics:")
    print(f"   Realized R     : {perf.realized_R:.2f}R" if perf.realized_R is not None else "   Realized R     : N/A")
    print(f"   MFE (Max Up)   : {perf.mfe_R:.2f}R" if perf.mfe_R is not None else "   MFE (Max Up)   : N/A")
    print(f"   MAE (Max Down) : {perf.mae_R:.2f}R" if perf.mae_R is not None else "   MAE (Max Down) : N/A")
    print()

    print("🎯 TTD Analysis:")
    print(f"   TTD Bars       : {perf.ttd_bars} bars" if perf.ttd_bars else "   TTD Bars       : N/A")
    print(f"   TTD Seconds    : {perf.ttd_seconds:.0f} sec" if perf.ttd_seconds else "   TTD Seconds    : N/A")
    print()

    print("📊 Trade Flow:")
    print(f"   Outcome        : {perf.outcome.upper()}")
    print(f"   Bars to Entry  : {perf.bars_to_entry}")
    print(f"   Bars to Exit   : {perf.bars_to_exit}")
    print(f"   Notes          : {perf.notes or 'N/A'}")

    # Интерпретация результатов
    print("\n💡 Analysis:")
    if perf.realized_R and perf.realized_R > 0:
        print("   ✅ Profitable trade - signal captured the edge")
    elif perf.realized_R and perf.realized_R < 0:
        print("   ❌ Losing trade - stopped out")
    else:
        print("   ⚪ Trade in progress or no clear outcome")

    if perf.ttd_bars and perf.ttd_bars <= 5:
        print("   🚀 Fast TTD - quick profit capture")
    elif perf.ttd_bars and perf.ttd_bars > 10:
        print("   🐌 Slow TTD - edge took time to develop")
    else:
        print("   ❓ TTD analysis inconclusive")

    mfe_mae_ratio = abs(perf.mfe_R / perf.mae_R) if perf.mfe_R and perf.mae_R and perf.mae_R != 0 else 0
    if mfe_mae_ratio > 2:
        print(f"   📈 Good MFE/MAE ratio: {mfe_mae_ratio:.1f}")
    elif mfe_mae_ratio < 1:
        print(f"   📉 Poor MFE/MAE ratio: {mfe_mae_ratio:.1f}")
    else:
        print(f"   ➖ Neutral MFE/MAE ratio: {mfe_mae_ratio:.1f}")
# ---------- 5. (Опционально) Запись в TimescaleDB ----------

def demo_save_to_timescale(plan: ExecutionPlan, ctx: ExtendedSignalContext) -> None:
    """
    Пример сохранения данных в TimescaleDB.

    Требуется настроенный DSN в формате:
    postgres://user:password@host:port/dbname
    """
    try:
        from signal_exec.repository import SignalExecutionRepository

        # DSN для подключения (заменить на реальный)
        dsn = "postgresql://postgres:12345@postgres:5434/trade"
        repo = SignalExecutionRepository(dsn=dsn)

        print("\n💾 Saving to TimescaleDB...")

        # 1. Сохраняем сигнал
        repo.insert_signal(ctx, extra_json={"source": "demo_script"})
        print("   ✅ Signal saved")

        # 2. Сохраняем execution plan
        repo.insert_execution_plan(plan)
        print("   ✅ Execution plan saved")

        # 3. Для полного цикла можно добавить performance
        # tracker = SignalPerformanceTracker()
        # perf = tracker.build_performance(...)
        # repo.insert_signal_performance(perf)

        print("   🎉 All data saved to TimescaleDB!")

    except ImportError:
        print("\n⚠️  TimescaleDB integration skipped (psycopg not available)")
    except Exception as e:
        print(f"\n❌ Error saving to TimescaleDB: {e}")


# ---------- 6. Точка входа ----------

def main():
    """
    Основная функция демонстрации.
    """
    print("🚀 Signal Execution & Performance Demo")
    print("=" * 80)
    print("Демонстрация полного цикла обработки сигнала:")
    print("1. Создание плана исполнения")
    print("2. Симуляция исполнения сделки")
    print("3. Анализ производительности")
    print("=" * 80)

    # 1. Генерируем план исполнения
    plan = demo_execution_planner()
    if plan is None:
        print("\n❌ Демо завершено - план не создан")
        return

    # 2. Создаем контекст с тем же signal_id для performance анализа
    ctx = build_mock_signal_context()
    ctx.signal_id = plan.signal_id  # Важно: совпадает с планом

    # 3. Анализируем производительность
    demo_performance(plan, ctx)

    # 4. Опционально: сохраняем в TimescaleDB
    demo_save_to_timescale(plan, ctx)

    print("\n🎯 Demo completed successfully!")
    print("\n💡 Key Takeaways:")
    print("   • Execution planning considers risk, microstructure, and TTD")
    print("   • Performance analysis provides actionable metrics")
    print("   • TimescaleDB integration enables historical analysis")
    print("   • System is ready for production use in scanner_infra")


if __name__ == "__main__":
    main()
