from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Демонстрация полной L3-Lite системы с новыми метриками.

Показывает:
1. Подключение L3-Lite потока
2. Расчет всех метрик
3. Symbol-specific конфигурацию
4. Мониторинг качества
"""

import time
import logging
from regime import (
    L3LiteMetricsAggregator, L3LiteEvent, BookSnapshot,
    CryptoConfScorer, CryptoConfScorerConfig,
    SignalQualityMonitor
)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def simulate_l3_stream(symbol: str = "BTCUSDT"):
    """
    Симуляция L3-Lite потока для демонстрации.
    В реальности данные приходят из Redis/MQTT/Kafka стримов.
    """,
    logger.info(f"🚀 Starting L3-Lite stream simulation for {symbol}"),

    # Инициализация компонентов
    l3_agg = L3LiteMetricsAggregator(
        microprice_horizon_sec=20,
        obi_persistence_sec=30,
    ),

    # Конфигурация для BTCUSDT (ужесточенная)
    l3_cfg = CryptoConfScorerConfig(),
    conf_scorer = CryptoConfScorer(l3_cfg),

    quality_monitor = SignalQualityMonitor(max_history_days=7),

    # Базовая книга для симуляции
    base_book = BookSnapshot(
        ts_ms=get_ny_time_millis(),
        bids=[
            (49999.0, 1.0), (49998.0, 2.0), (49997.0, 1.5),
            (49996.0, 3.0), (49995.0, 2.5), (49994.0, 1.8),
            (49993.0, 2.2), (49992.0, 1.7), (49991.0, 2.8), (49990.0, 1.9)],
        asks=[
            (50001.0, 1.0), (50002.0, 2.0), (50003.0, 1.5),
            (50004.0, 3.0), (50005.0, 2.5), (50006.0, 1.8),
            (50007.0, 2.2), (50008.0, 1.7), (50009.0, 2.8), (50010.0, 1.9)]
    )

    # Симуляция потока событий
    events_sequence = [
        # Торги (активность)
        [L3LiteEvent(ts_ms=base_book.ts_ms + i*100, kind="trade", side="bid", price=49999.0, qty=0.1 + i*0.05)
         for i in range(10)],

        # Отмены (давление на bid)
        [L3LiteEvent(ts_ms=base_book.ts_ms + 2000 + i*150, kind="cancel", side="bid", price=49998.0, qty=0.2 + i*0.1)
         for i in range(8)],

        # Торги на ask (баланс)
        [L3LiteEvent(ts_ms=base_book.ts_ms + 4000 + i*200, kind="trade", side="ask", price=50001.0, qty=0.15 + i*0.03)
         for i in range(6)],
    ]

    # Обработка событий
    logger.info("📊 Processing L3-Lite events...")
    for event_batch in events_sequence:
        for event in event_batch:
            l3_agg.on_l3_event(event)

        # Обновление книги после каждой партии событий
        current_time = event_batch[-1].ts_ms
        updated_book = BookSnapshot(
            ts_ms=current_time,
            bids=base_book.bids,
            asks=base_book.asks
        )
        l3_agg.on_book_update(updated_book)

    # Получение финальных метрик
    l3_features = l3_agg.build_features(get_ny_time_millis())

    if l3_features:
        logger.info("📈 L3-Lite Features Calculated:")
        logger.info(".2f")
        logger.info(".2f")
        logger.info(".2f")
        logger.info(".2f")
        logger.info(".2f")
        logger.info(".3f")
        logger.info(".2f")
        logger.info(".3f")
        logger.info(".3f")
        logger.info(".3f")
        logger.info(".3f")
        logger.info(".3f")
        logger.info(".3f")
        # Создание mock SignalContext
        class MockSignalContext:
            def __init__(self, features):
                self.symbol = symbol
                self.family = "crypto_orderflow"
                self.cancel_to_trade_bid_5s = features.cancel_to_trade_bid_5s
                self.cancel_to_trade_ask_5s = features.cancel_to_trade_ask_5s
                self.cancel_to_trade_bid_20s = features.cancel_to_trade_bid_20s
                self.cancel_to_trade_ask_20s = features.cancel_to_trade_ask_20s
                self.microprice_shift_bps_20 = features.microprice_shift_bps_20
                self.spread_bps = features.spread_bps
                self.obi_5 = features.obi_5
                self.obi_20 = features.obi_20
                self.obi_50 = features.obi_50
                self.obi_persistence_score = features.obi_persistence_score
                self.microprice_velocity_bps = features.microprice_velocity_bps
                self.queue_pressure_bid = features.queue_pressure_bid
                self.queue_pressure_ask = features.queue_pressure_ask
                self.market_depth_imbalance = features.market_depth_imbalance

        ctx = MockSignalContext(l3_features)

        # Расчет confidence
        confidence = conf_scorer(ctx, symbol)
        logger.info(".3f")
        # Запись в монитор качества
        quality_monitor.record_signal(
            signal_id=f"demo_{symbol}_{int(time.time())}",
            symbol=symbol,
            family="crypto_orderflow",
            ctx=ctx,
            raw_score=2.5,  # mock
            final_score=2.5 * (1.0 + confidence * 0.1),  # mock
        )

        # Имитация результата
        quality_monitor.record_result(
            signal_id=f"demo_{symbol}_{int(time.time())}",
            pnl_r=0.8  # прибыль
        )

    # Отчет о качестве
    logger.info("\n📊 Quality Report:")
    report = quality_monitor.get_quality_report(symbol=symbol)
    logger.info(report)

    alerts = quality_monitor.get_alerts()
    if alerts:
        logger.warning("🚨 Quality Alerts:")
        for alert in alerts:
            logger.warning(f"  {alert}")
    else:
        logger.info("✅ No quality alerts")

    return l3_features, confidence if l3_features else 0.0


def demonstrate_symbol_configs():
    """Демонстрация symbol-specific конфигураций."""
    logger.info("\n🔧 Symbol-Specific Configurations Demo")

    # Конфигурации для разных символов
    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "SOLUSDT"]

    for symbol in symbols:
        cfg = CryptoConfScorerConfig()
        symbol_cfg = cfg.get_symbol_config(symbol)

        logger.info(f"📊 {symbol}:")
        logger.info(f"  Spread OK: {symbol_cfg['l3_spread_max_ok_bps']} bps")
        logger.info(f"  Spread Hard: {symbol_cfg['l3_spread_hard_limit_bps']} bps")
        logger.info(f"  Cancel/Trade Soft: {symbol_cfg['l3_cancel_to_trade_soft']}")
        logger.info(f"  Cancel/Trade Hard: {symbol_cfg['l3_cancel_to_trade_hard']}")
        logger.info(f"  Microprice Drift: {symbol_cfg['l3_mp_drift_max_bps']} bps")


def main():
    """Главная демонстрация."""
    logger.info("🎯 L3-Lite Metrics System Demo")
    logger.info("=" * 50)

    # Демонстрация конфигураций
    demonstrate_symbol_configs()

    # Демонстрация BTCUSDT
    logger.info("\n🎲 Simulating BTCUSDT trading...")
    features, confidence = simulate_l3_stream("BTCUSDT")

    if features:
        logger.info("\n✨ Summary:")
        logger.info(".2f")
        logger.info(".3f")
        logger.info(".3f")
        logger.info(".3f")
    logger.info("\n🎉 Demo completed successfully!")


if __name__ == "__main__":
    main()
