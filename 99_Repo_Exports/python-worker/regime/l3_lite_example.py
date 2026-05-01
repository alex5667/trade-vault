from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Пример использования L3-Lite метрик в CryptoOrderFlowHandler.

Показывает, как интегрировать L3-Lite агрегатор в обработчик сигналов.
"""

import time
from regime import L3LiteMetricsAggregator, L3LiteEvent, BookSnapshot, CryptoConfScorer, CryptoConfScorerConfig


def simulate_l3_events():
    """Симуляция потока L3-Lite событий.""",
    events = [
        # Торги
        L3LiteEvent(ts_ms=get_ny_time_millis(), kind="trade", side="bid", price=50000.0, qty=0.1),
        L3LiteEvent(ts_ms=get_ny_time_millis() + 100, kind="trade", side="ask", price=50001.0, qty=0.05),

        # Отмены
        L3LiteEvent(ts_ms=get_ny_time_millis() + 200, kind="cancel", side="bid", price=49999.0, qty=0.2),
        L3LiteEvent(ts_ms=get_ny_time_millis() + 300, kind="cancel", side="ask", price=50002.0, qty=0.15)],
    return events,


def simulate_book_snapshot():
    """Симуляция снимка книги ордеров.""",
    return BookSnapshot(
        ts_ms=get_ny_time_millis(),
        bids=[
            (49999.0, 1.0),  # price, qty
            (49998.0, 2.0),
            (49997.0, 1.5),
            (49996.0, 3.0),
            (49995.0, 2.5)],
        asks=[
            (50001.0, 1.0),
            (50002.0, 2.0),
            (50003.0, 1.5),
            (50004.0, 3.0),
            (50005.0, 2.5)]
    )


def example_l3_integration():
    """Пример полной интеграции L3-Lite в обработчик сигналов."""
    print("🧪 L3-Lite Integration Example\n")

    # Инициализация компонентов
    l3_agg = L3LiteMetricsAggregator(
        microprice_horizon_sec=20,
        obi_persistence_sec=30,
    )

    conf_scorer = CryptoConfScorer(CryptoConfScorerConfig())

    # Симуляция потока данных
    events = simulate_l3_events()
    book_snap = simulate_book_snapshot()

    print("📊 Processing L3-Lite events...")
    for ev in events:
        l3_agg.on_l3_event(ev)
        print(f"  {ev.kind} {ev.side}: {ev.qty} @ {ev.price}")

    print("\n📊 Processing book snapshot...")
    l3_agg.on_book_update(book_snap)
    print(f"  Book: {len(book_snap.bids)} bids, {len(book_snap.asks)} asks")

    # Получение L3-метрик
    now_ms = get_ny_time_millis()
    l3_features = l3_agg.build_features(now_ms)

    if l3_features:
        print("\n📈 L3-Lite Features:")
        print(".2f")
        print(".2f")
        print(".2f")
        print(".2f")
        print(".1f")
        print(".2f")
        print(".3f")
        print(".3f")
        print(".3f")
        print(".3f")
        print(".3f")
    else:
        print("\n❌ No L3 features available")
        return

    # Создание mock SignalContext с L3-метриками
    class MockSignalContext:
        def __init__(self):
            # L3 fields
            self.cancel_to_trade_bid_5s = l3_features.cancel_to_trade_bid_5s
            self.cancel_to_trade_ask_5s = l3_features.cancel_to_trade_ask_5s
            self.cancel_to_trade_bid_20s = l3_features.cancel_to_trade_bid_20s
            self.cancel_to_trade_ask_20s = l3_features.cancel_to_trade_ask_20s
            self.microprice_shift_bps_20 = l3_features.microprice_shift_bps_20
            self.spread_bps = l3_features.spread_bps
            self.obi_5 = l3_features.obi_5
            self.obi_20 = l3_features.obi_20
            self.obi_50 = l3_features.obi_50
            self.obi_persistence_score = l3_features.obi_persistence_score

    ctx = MockSignalContext()

    # Расчет confidence score
    _confidence = conf_scorer(ctx)
    print(".3f")
    # Анализ вклада L3-terms
    print("\n🔍 L3-Terms Analysis:")

    _spread_term = conf_scorer._spread_ok_term(ctx)
    _obi_term = conf_scorer._obi_persistence_term(ctx)
    _cancel_term = conf_scorer._cancel_to_trade_term(ctx)
    _micro_term = conf_scorer._microprice_drift_term(ctx)

    print(".3f")
    print(".3f")
    print(".3f")
    print(".3f")
    print(".3f")
if __name__ == "__main__":
    example_l3_integration()
