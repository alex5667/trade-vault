# tests/test_realized_spread.py
"""
Unit tests для RealizedSpreadTracker.
"""
import pytest
from signals.realized_spread import (
    RealizedSpreadTracker,
    RealizedSpreadMetrics,
    create_tracker,
    interpret_metrics,
)


class TestRealizedSpreadTracker:
    """Тесты для RealizedSpreadTracker."""

    def test_initialization(self):
        """Тест инициализации трекера."""
        tracker = RealizedSpreadTracker(lag_ms=1000)
        
        metrics = tracker.get_metrics()
        assert metrics.realized_bps == 0.0
        assert metrics.realized_ema_bps == 0.0
        assert metrics.spread_bps == 0.0
        assert metrics.spread_ema_bps == 0.0
        assert metrics.adverse_ratio_ema == 0.0
        assert metrics.realized_count == 0

    def test_spread_calculation(self):
        """Тест расчета спреда."""
        tracker = RealizedSpreadTracker(lag_ms=1000)
        
        # bid=100, ask=100.1, mid=100.05
        # spread = (100.1 - 100) / 100.05 * 10000 ≈ 9.995 bps
        metrics = tracker.update(
            ts=1000,
            bid=100.0,
            ask=100.1,
            last=100.05,
            is_buyer_maker=None,
        )
        
        assert 9.9 < metrics.spread_bps < 10.1
        assert metrics.spread_ema_bps > 0

    def test_buy_aggression_momentum(self):
        """Тест buy aggression с momentum (цена пошла вверх)."""
        tracker = RealizedSpreadTracker(lag_ms=1000, ema_alpha=1.0)
        
        # T=0: buy aggression at 100
        tracker.update(
            ts=0,
            bid=99.95,
            ask=100.05,
            last=100.05,
            is_buyer_maker=False,  # buyer is taker => buy aggression
        )
        
        # T=1500: цена выросла до 100.5 (momentum)
        metrics = tracker.update(
            ts=1500,
            bid=100.45,
            ask=100.55,
            last=100.5,
            is_buyer_maker=None,
        )
        
        # realized = +1 * (100.5 - 100.05) / 100.05 * 10000 ≈ +44.97 bps
        assert metrics.realized_count == 1
        assert metrics.realized_bps > 40
        assert metrics.realized_ema_bps > 40
        assert metrics.adverse_ratio_ema == 0.0  # нет adverse

    def test_sell_aggression_momentum(self):
        """Тест sell aggression с momentum (цена пошла вниз)."""
        tracker = RealizedSpreadTracker(lag_ms=1000, ema_alpha=1.0)
        
        # T=0: sell aggression at 100
        tracker.update(
            ts=0,
            bid=99.95,
            ask=100.05,
            last=99.95,
            is_buyer_maker=True,  # buyer is maker => sell aggression
        )
        
        # T=1500: цена упала до 99.5 (momentum)
        metrics = tracker.update(
            ts=1500,
            bid=99.45,
            ask=99.55,
            last=99.5,
            is_buyer_maker=None,
        )
        
        # realized = -1 * (99.5 - 99.95) / 99.95 * 10000 ≈ +45.01 bps (положительный!)
        assert metrics.realized_count == 1
        assert metrics.realized_bps > 40
        assert metrics.adverse_ratio_ema == 0.0

    def test_buy_aggression_absorption(self):
        """Тест buy aggression с absorption (цена пошла вниз)."""
        tracker = RealizedSpreadTracker(lag_ms=1000, ema_alpha=1.0)
        
        # T=0: buy aggression at 100
        tracker.update(
            ts=0,
            bid=99.95,
            ask=100.05,
            last=100.05,
            is_buyer_maker=False,
        )
        
        # T=1500: цена упала до 99.5 (absorption)
        metrics = tracker.update(
            ts=1500,
            bid=99.45,
            ask=99.55,
            last=99.5,
            is_buyer_maker=None,
        )
        
        # realized = +1 * (99.5 - 100.05) / 100.05 * 10000 ≈ -54.97 bps (отрицательный!)
        assert metrics.realized_count == 1
        assert metrics.realized_bps < -50
        assert metrics.adverse_ratio_ema == 1.0  # 100% adverse

    def test_multiple_trades_mixed(self):
        """Тест нескольких сделок с mixed результатом."""
        tracker = RealizedSpreadTracker(lag_ms=1000, ema_alpha=1.0, adverse_ema_alpha=1.0)
        
        # Trade 1: buy at 100
        tracker.update(ts=0, bid=99.95, ask=100.05, last=100.05, is_buyer_maker=False)
        
        # Trade 2: sell at 100
        tracker.update(ts=100, bid=99.95, ask=100.05, last=99.95, is_buyer_maker=True)
        
        # T=1500: цена на 100.5 (первая сделка momentum, вторая absorption)
        metrics = tracker.update(
            ts=1500,
            bid=100.45,
            ask=100.55,
            last=100.5,
            is_buyer_maker=None,
        )
        
        assert metrics.realized_count == 2
        # Первая: +1 * (100.5 - 100.05) / 100.05 * 10000 ≈ +44.97 (momentum)
        # Вторая: -1 * (100.5 - 99.95) / 99.95 * 10000 ≈ -55.03 (absorption)
        # Среднее: около -5 bps
        assert -10 < metrics.realized_bps < 0
        assert metrics.adverse_ratio_ema == 0.5  # 50% adverse

    def test_lag_timing(self):
        """Тест что trades не матурятся раньше lag_ms."""
        tracker = RealizedSpreadTracker(lag_ms=2000)
        
        # Trade at T=0
        tracker.update(ts=0, bid=99.95, ask=100.05, last=100.05, is_buyer_maker=False)
        
        # T=1000 (< lag_ms): trade не должен матуриться
        metrics = tracker.update(ts=1000, bid=100.45, ask=100.55, last=100.5)
        assert metrics.realized_count == 0
        
        # T=2500 (> lag_ms): trade должен матуриться
        metrics = tracker.update(ts=2500, bid=100.45, ask=100.55, last=100.5)
        assert metrics.realized_count == 1

    def test_gap_detection(self):
        """Тест очистки pending при большом gap."""
        tracker = RealizedSpreadTracker(lag_ms=1000, max_gap_ms=5000)
        
        # Trade at T=0
        tracker.update(ts=0, bid=99.95, ask=100.05, last=100.05, is_buyer_maker=False)
        
        # T=10000 (gap > max_gap_ms): pending должен очиститься
        metrics = tracker.update(ts=10000, bid=100.45, ask=100.55, last=100.5)
        
        # Trade не должен матуриться, так как pending был очищен
        assert metrics.realized_count == 0

    def test_reset(self):
        """Тест сброса состояния."""
        tracker = RealizedSpreadTracker(lag_ms=1000)
        
        # Добавляем данные
        tracker.update(ts=0, bid=99.95, ask=100.05, last=100.05, is_buyer_maker=False)
        tracker.update(ts=1500, bid=100.45, ask=100.55, last=100.5)
        
        # Проверяем что есть данные
        metrics = tracker.get_metrics()
        assert metrics.realized_count > 0
        
        # Сбрасываем
        tracker.reset()
        
        # Проверяем что всё обнулилось
        metrics = tracker.get_metrics()
        assert metrics.realized_count == 0
        assert metrics.realized_ema_bps == 0.0
        assert metrics.adverse_ratio_ema == 0.0

    def test_ema_smoothing(self):
        """Тест EMA сглаживания."""
        tracker = RealizedSpreadTracker(lag_ms=1000, ema_alpha=0.5)
        
        # Trade 1: +50 bps
        tracker.update(ts=0, bid=99.95, ask=100.05, last=100.05, is_buyer_maker=False)
        metrics1 = tracker.update(ts=1500, bid=100.5, ask=100.6, last=100.55)
        
        # Trade 2: +10 bps
        tracker.update(ts=2000, bid=100.5, ask=100.6, last=100.6, is_buyer_maker=False)
        metrics2 = tracker.update(ts=3500, bid=100.65, ask=100.75, last=100.7)
        
        # EMA должна быть между первым и вторым значением
        assert metrics1.realized_ema_bps > metrics2.realized_ema_bps
        assert metrics2.realized_ema_bps > 10


class TestConvenienceFunctions:
    """Тесты для вспомогательных функций."""

    def test_create_tracker(self):
        """Тест create_tracker."""
        tracker = create_tracker(lag_ms=3000, ema_alpha=0.2)
        assert tracker.lag_ms == 3000
        assert tracker.ema_alpha == 0.2

    def test_interpret_metrics_warming_up(self):
        """Тест интерпретации: warming up."""
        metrics = RealizedSpreadMetrics(
            realized_bps=10.0,
            realized_ema_bps=10.0,
            spread_bps=5.0,
            spread_ema_bps=5.0,
            adverse_ratio_ema=0.2,
            realized_count=5,  # < 10
        )
        assert interpret_metrics(metrics) == "warming_up"

    def test_interpret_metrics_strong_momentum(self):
        """Тест интерпретации: strong momentum."""
        metrics = RealizedSpreadMetrics(
            realized_bps=5.0,
            realized_ema_bps=5.0,
            spread_bps=5.0,
            spread_ema_bps=5.0,
            adverse_ratio_ema=0.2,
            realized_count=20,
        )
        assert interpret_metrics(metrics) == "strong_momentum"

    def test_interpret_metrics_absorption(self):
        """Тест интерпретации: absorption."""
        metrics = RealizedSpreadMetrics(
            realized_bps=-5.0,
            realized_ema_bps=-5.0,
            spread_bps=5.0,
            spread_ema_bps=5.0,
            adverse_ratio_ema=0.7,
            realized_count=20,
        )
        assert interpret_metrics(metrics) == "absorption"

    def test_interpret_metrics_mixed(self):
        """Тест интерпретации: mixed."""
        metrics = RealizedSpreadMetrics(
            realized_bps=0.3,
            realized_ema_bps=0.3,
            spread_bps=5.0,
            spread_ema_bps=5.0,
            adverse_ratio_ema=0.45,
            realized_count=20,
        )
        assert interpret_metrics(metrics) == "mixed"


class TestEdgeCases:
    """Тесты граничных случаев."""

    def test_zero_prices(self):
        """Тест с нулевыми ценами."""
        tracker = RealizedSpreadTracker(lag_ms=1000)
        
        metrics = tracker.update(
            ts=1000,
            bid=0.0,
            ask=0.0,
            last=0.0,
            is_buyer_maker=None,
        )
        
        assert metrics.spread_bps == 0.0
        assert metrics.realized_count == 0

    def test_negative_timestamp(self):
        """Тест с отрицательным timestamp."""
        tracker = RealizedSpreadTracker(lag_ms=1000)
        
        metrics = tracker.update(
            ts=-1000,
            bid=100.0,
            ask=100.1,
            last=100.05,
            is_buyer_maker=None,
        )
        
        assert metrics.realized_count == 0

    def test_very_small_spread(self):
        """Тест с очень маленьким спредом."""
        tracker = RealizedSpreadTracker(lag_ms=1000)
        
        # Спред 0.001%
        metrics = tracker.update(
            ts=1000,
            bid=100.0,
            ask=100.001,
            last=100.0005,
            is_buyer_maker=None,
        )
        
        assert 0.09 < metrics.spread_bps < 0.11

    def test_inverted_bid_ask(self):
        """Тест с инвертированными bid/ask (некорректные данные)."""
        tracker = RealizedSpreadTracker(lag_ms=1000)
        
        # ask < bid (некорректно)
        metrics = tracker.update(
            ts=1000,
            bid=100.1,
            ask=100.0,
            last=100.05,
            is_buyer_maker=None,
        )
        
        # Должно обработаться без ошибок
        assert metrics.spread_bps < 0  # Отрицательный спред


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

