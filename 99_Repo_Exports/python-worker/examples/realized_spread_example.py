from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Пример использования RealizedSpreadTracker.

Демонстрирует:
1. Базовое использование трекера
2. Симуляцию momentum vs absorption сценариев
3. Интерпретацию метрик
"""
import time
from signals.realized_spread import (
    RealizedSpreadTracker,
    create_tracker,
    interpret_metrics,
)


def simulate_momentum_scenario():
    """Симуляция momentum сценария (цена следует за агрессором)."""
    print("\n" + "="*60)
    print("СЦЕНАРИЙ 1: MOMENTUM (агрессивные покупки толкают цену вверх)")
    print("="*60)
    
    tracker = create_tracker(lag_ms=2000, ema_alpha=0.3)
    
    base_ts = get_ny_time_millis()
    base_price = 50000.0
    
    # Серия агрессивных покупок с ростом цены
    for i in range(10):
        ts = base_ts + i * 500
        price = base_price + i * 10  # Цена растет
        
        # Агрессивная покупка (is_buyer_maker=False)
        metrics = tracker.update(
            ts=ts,
            bid=price - 0.5,
            ask=price + 0.5,
            last=price + 0.5,  # Покупка по ask
            is_buyer_maker=False,
        )
        
        if i >= 5:  # После lag_ms начинаем видеть realized
            print(f"\nТик {i}:")
            print(f"  Цена: ${price:.2f}")
            print(f"  Realized spread: {metrics.realized_ema_bps:+.2f} bps")
            print(f"  Adverse ratio: {metrics.adverse_ratio_ema:.2%}")
            print(f"  Market mode: {interpret_metrics(metrics)}")
            print(f"  Trades processed: {metrics.realized_count}")
    
    final_metrics = tracker.get_metrics()
    print(f"\n{'='*60}")
    print(f"ИТОГ: {interpret_metrics(final_metrics).upper()}")
    print(f"  Realized EMA: {final_metrics.realized_ema_bps:+.2f} bps")
    print(f"  Adverse ratio: {final_metrics.adverse_ratio_ema:.2%}")
    print(f"  → Сильный momentum, агрессивные покупки правы")
    print(f"{'='*60}")


def simulate_absorption_scenario():
    """Симуляция absorption сценария (агрессор поглощается)."""
    print("\n" + "="*60)
    print("СЦЕНАРИЙ 2: ABSORPTION (агрессивные покупки поглощаются)")
    print("="*60)
    
    tracker = create_tracker(lag_ms=2000, ema_alpha=0.3)
    
    base_ts = get_ny_time_millis()
    base_price = 50000.0
    
    # Серия агрессивных покупок, но цена падает (absorption)
    for i in range(10):
        ts = base_ts + i * 500
        price = base_price - i * 8  # Цена падает несмотря на покупки
        
        # Агрессивная покупка (is_buyer_maker=False)
        metrics = tracker.update(
            ts=ts,
            bid=price - 0.5,
            ask=price + 0.5,
            last=price + 0.5,  # Покупка по ask
            is_buyer_maker=False,
        )
        
        if i >= 5:  # После lag_ms начинаем видеть realized
            print(f"\nТик {i}:")
            print(f"  Цена: ${price:.2f}")
            print(f"  Realized spread: {metrics.realized_ema_bps:+.2f} bps")
            print(f"  Adverse ratio: {metrics.adverse_ratio_ema:.2%}")
            print(f"  Market mode: {interpret_metrics(metrics)}")
            print(f"  Trades processed: {metrics.realized_count}")
    
    final_metrics = tracker.get_metrics()
    print(f"\n{'='*60}")
    print(f"ИТОГ: {interpret_metrics(final_metrics).upper()}")
    print(f"  Realized EMA: {final_metrics.realized_ema_bps:+.2f} bps")
    print(f"  Adverse ratio: {final_metrics.adverse_ratio_ema:.2%}")
    print(f"  → Absorption, агрессивные покупки поглощаются")
    print(f"{'='*60}")


def simulate_mixed_scenario():
    """Симуляция mixed сценария (неопределенный рынок)."""
    print("\n" + "="*60)
    print("СЦЕНАРИЙ 3: MIXED (смешанный режим, консолидация)")
    print("="*60)
    
    tracker = create_tracker(lag_ms=2000, ema_alpha=0.3)
    
    base_ts = get_ny_time_millis()
    base_price = 50000.0
    
    # Серия сделок с чередующимся успехом
    for i in range(10):
        ts = base_ts + i * 500
        # Цена колеблется вокруг базовой
        price = base_price + (5 if i % 2 == 0 else -5)
        
        # Чередуем покупки и продажи
        is_buy = i % 2 == 0
        metrics = tracker.update(
            ts=ts,
            bid=price - 0.5,
            ask=price + 0.5,
            last=price + 0.5 if is_buy else price - 0.5,
            is_buyer_maker=not is_buy,
        )
        
        if i >= 5:  # После lag_ms начинаем видеть realized
            print(f"\nТик {i}:")
            print(f"  Цена: ${price:.2f}")
            print(f"  Realized spread: {metrics.realized_ema_bps:+.2f} bps")
            print(f"  Adverse ratio: {metrics.adverse_ratio_ema:.2%}")
            print(f"  Market mode: {interpret_metrics(metrics)}")
            print(f"  Trades processed: {metrics.realized_count}")
    
    final_metrics = tracker.get_metrics()
    print(f"\n{'='*60}")
    print(f"ИТОГ: {interpret_metrics(final_metrics).upper()}")
    print(f"  Realized EMA: {final_metrics.realized_ema_bps:+.2f} bps")
    print(f"  Adverse ratio: {final_metrics.adverse_ratio_ema:.2%}")
    print(f"  → Смешанный режим, нет четкого направления")
    print(f"{'='*60}")


def demonstrate_spread_tracking():
    """Демонстрация отслеживания спреда."""
    print("\n" + "="*60)
    print("ДЕМОНСТРАЦИЯ: Отслеживание спреда")
    print("="*60)
    
    tracker = create_tracker(lag_ms=2000)
    
    base_ts = get_ny_time_millis()
    
    # Узкий спред (ликвидный рынок)
    print("\n1. Узкий спред (ликвидный рынок):")
    metrics = tracker.update(
        ts=base_ts,
        bid=50000.0,
        ask=50000.5,  # 0.5 USD спред
        last=50000.25,
    )
    print(f"   Спред: {metrics.spread_bps:.2f} bps")
    print(f"   → Высокая ликвидность")
    
    # Широкий спред (низкая ликвидность)
    print("\n2. Широкий спред (низкая ликвидность):")
    metrics = tracker.update(
        ts=base_ts + 1000,
        bid=50000.0,
        ask=50005.0,  # 5 USD спред
        last=50002.5,
    )
    print(f"   Спред: {metrics.spread_bps:.2f} bps")
    print(f"   Спред EMA: {metrics.spread_ema_bps:.2f} bps")
    print(f"   → Низкая ликвидность")
    
    print(f"\n{'='*60}")


def main():
    """Запуск всех примеров."""
    print("\n" + "="*60)
    print("ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ REALIZED SPREAD TRACKER")
    print("="*60)
    
    # Запускаем все сценарии
    simulate_momentum_scenario()
    simulate_absorption_scenario()
    simulate_mixed_scenario()
    demonstrate_spread_tracking()
    
    print("\n" + "="*60)
    print("РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ:")
    print("="*60)
    print("""
1. MOMENTUM (realized > +2 bps, adverse < 0.3):
   → Агрессивный трейлинг, tight stops
   → Высокая вероятность продолжения тренда
   → Можно увеличивать позицию

2. ABSORPTION (realized < -1 bps, adverse > 0.5):
   → Консервативный трейлинг, широкие stops
   → Возможен разворот или консолидация
   → Избегать новых входов в направлении агрессора

3. MIXED (realized около 0, adverse 0.3-0.5):
   → Стандартный трейлинг
   → Неопределенный рынок
   → Ждать более четких сигналов
    """)
    print("="*60 + "\n")


if __name__ == "__main__":
    main()

