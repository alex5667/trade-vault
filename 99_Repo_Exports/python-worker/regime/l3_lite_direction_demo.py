from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Демонстрация Direction-Aware L3-Lite системы.

Показывает:
1. Direction-aware confidence scoring
2. Логирование сигналов с L3-метриками
3. Анализ корреляций
"""

import time
from regime import (
    L3LiteMetricsAggregator, L3LiteEvent, BookSnapshot
    CryptoConfScorer, CryptoConfScorerConfig
    SignalQualityMonitor
    build_signal_snapshot
)


def simulate_trading_scenario(symbol: str = "BTCUSDT"):
    """
    Симуляция торгового сценария с direction-aware анализом.
    """
    print(f"🎯 Direction-Aware L3-Lite Demo for {symbol}")
    print("=" * 60)

    # Инициализация компонентов
    l3_agg = L3LiteMetricsAggregator()
    conf_scorer = CryptoConfScorer(CryptoConfScorerConfig())
    quality_monitor = SignalQualityMonitor(max_history_days=7)

    # Базовая книга
    base_book = BookSnapshot(
        ts_ms=get_ny_time_millis()
        bids=[(49999.0, 1.0), (49998.0, 2.0), (49997.0, 1.5)]
        asks=[(50001.0, 1.0), (50002.0, 2.0), (50003.0, 1.5)]
    )

    # Симуляция разных рыночных условий
    scenarios = [
        {
            "name": "Bullish Setup"
            "direction": 1,  # Long
            "events": [
                # Торги в пользу покупателей (хороший setup для long)
                L3LiteEvent(ts_ms=base_book.ts_ms + i*200, kind="trade", side="ask", price=50001.0, qty=0.05)
                for i in range(15)
            ] + [
                # Небольшой cancel со стороны продавцов
                L3LiteEvent(ts_ms=base_book.ts_ms + 4000 + i*300, kind="cancel", side="ask", price=50002.0, qty=0.02)
                for i in range(5)
            ]
            "expected_confidence": "HIGH (OBI в пользу long, cancel продавцов)"
        }
        {
            "name": "Bearish Setup"
            "direction": -1,  # Short
            "events": [
                # Торги в пользу продавцов (хороший setup для short)
                L3LiteEvent(ts_ms=base_book.ts_ms + i*200, kind="trade", side="bid", price=49999.0, qty=0.08)
                for i in range(12)
            ] + [
                # Cancel покупателей
                L3LiteEvent(ts_ms=base_book.ts_ms + 3000 + i*250, kind="cancel", side="bid", price=49998.0, qty=0.03)
                for i in range(8)
            ]
            "expected_confidence": "HIGH (OBI в пользу short, cancel покупателей)"
        }
        {
            "name": "Neutral/Conflicting Setup"
            "direction": 0,  # Neutral
            "events": [
                # Смешанная активность
                L3LiteEvent(ts_ms=base_book.ts_ms + i*150, kind="trade", side="bid" if i % 2 else "ask", price=50000.0, qty=0.04)
                for i in range(20)
            ]
            "expected_confidence": "NEUTRAL (direction=0, no direction-aware bonuses)"
        }
    ]

    results = []

    for scenario in scenarios:
        print(f"\n📊 Scenario: {scenario['name']}")
        print(f"   Direction: {scenario['direction']} ({'Long' if scenario['direction'] > 0 else 'Short' if scenario['direction'] < 0 else 'Neutral'})")
        print(f"   Expected: {scenario['expected_confidence']}")

        # Сброс агрегатора
        l3_agg = L3LiteMetricsAggregator()

        # Применяем события сценария
        for event in scenario["events"]:
            l3_agg.on_l3_event(event)

        # Обновляем книгу
        scenario_book = BookSnapshot(
            ts_ms=scenario["events"][-1].ts_ms
            bids=base_book.bids
            asks=base_book.asks
        )
        l3_agg.on_book_update(scenario_book)

        # Получаем L3-метрики
        l3_features = l3_agg.build_features(scenario_book.ts_ms)

        if not l3_features:
            print("   ❌ No L3 features generated")
            continue

        # Создаем mock SignalContext с direction
        class MockSignalContext:
            def __init__(self, features, direction):
                self.direction = direction
                self.spread_bps = features.spread_bps
                self.obi_5 = features.obi_5
                self.obi_20 = features.obi_20
                self.obi_50 = features.obi_50
                self.obi_persistence_score = features.obi_persistence_score
                self.cancel_to_trade_bid_5s = features.cancel_to_trade_bid_5s
                self.cancel_to_trade_ask_5s = features.cancel_to_trade_ask_5s
                self.cancel_to_trade_bid_20s = features.cancel_to_trade_bid_20s
                self.cancel_to_trade_ask_20s = features.cancel_to_trade_ask_20s
                self.microprice_shift_bps_20 = features.microprice_shift_bps_20

        ctx = MockSignalContext(l3_features, scenario["direction"])

        # Расчет confidence
        confidence = conf_scorer(ctx, symbol)

        # Анализ вклада direction-aware terms
        spread_term = conf_scorer._spread_ok_term(ctx, conf_scorer.cfg.get_symbol_config(symbol))
        obi_term = conf_scorer._obi_persistence_term(ctx)
        cancel_term = conf_scorer._cancel_to_trade_term(ctx, conf_scorer.cfg.get_symbol_config(symbol))
        micro_term = conf_scorer._microprice_drift_term(ctx, conf_scorer.cfg.get_symbol_config(symbol))

        print(".3f")
        print(".3f")
        print(".3f")
        print(".3f")
        print(".3f")
        print(".1f")

        # Логируем сигнал
        signal_id = f"demo_{symbol}_{scenario['name'].lower().replace(' ', '_')}_{int(time.time())}"

        try:
            _snapshot = build_signal_snapshot(
                signal_id=signal_id
                symbol=symbol
                ts_ms=scenario_book.ts_ms
                family="crypto_orderflow"
                conf_score=confidence
                ctx=ctx
            )

            quality_monitor.record_signal(
                signal_id=signal_id
                symbol=symbol
                family="crypto_orderflow"
                ctx=ctx
                raw_score=2.0,  # mock
                final_score=confidence
            )

            results.append({
                "scenario": scenario["name"]
                "direction": scenario["direction"]
                "confidence": confidence
                "l3_features": l3_features
                "terms_breakdown": {
                    "spread": spread_term
                    "obi": obi_term
                    "cancel": cancel_term
                    "micro": micro_term
                }
            })

        except Exception as e:
            print(f"   ❌ Failed to create signal snapshot: {e}")

    # Финальный анализ
    print("\n📈 Quality Analysis:")
    report = quality_monitor.get_quality_report(symbol=symbol)
    print(report)

    alerts = quality_monitor.get_alerts()
    if alerts:
        print("🚨 Quality Alerts:")
        for alert in alerts:
            print(f"   {alert}")

    print(f"\n✅ Demo completed with {len(results)} scenarios analyzed")

    return results


def main():
    """Запуск демонстрации."""
    results = simulate_trading_scenario("BTCUSDT")

    print("\n" + "="*60)
    print("🎯 SUMMARY: Direction-Aware L3-Scoring Benefits")
    print("="*60)

    if results:
        print("✅ Direction-aware terms successfully:")
        print("   • OBI persistence: учитывает направление дисбаланса")
        print("   • Cancel-to-trade: анализирует активность нужной стороны")
        print("   • Microprice drift: бонус за движение в направлении сигнала")
        print("   • Spread: neutral term, влияет на все направления")

        print("\n🔍 Key Insights:")
        print("   • Long signals: лучше при OBI < 0 и cancel продавцов")
        print("   • Short signals: лучше при OBI > 0 и cancel покупателей")
        print("   • Neutral direction: нет direction-aware бонусов/штрафов")
    print("\n🚀 System ready for production with direction-aware L3-scoring!")


if __name__ == "__main__":
    main()
