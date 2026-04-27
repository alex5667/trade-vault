import pytest
from contexts import BucketState
import sys
import os
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from handlers.data_processor import OrderFlowDataProcessor
from contexts import Tick


class MockConfig:
    """Mock config for testing"""
    def __init__(self):
        self.delta_window_ticks = 10
        self.delta_bucket_ms = 1000
        self.l2_stale_ms = 800
        self.l2_skew_tick_thr_ms = 300  # Correct parameter name
        self.family = "crypto_orderflow"
        self.venue = "binance_futures"
        self.timeframe_s = 60
        self.min_bucket_trades = 10
        self.min_bucket_notional_usd = 1000.0
        self.min_delta_z = 1.0
        self.min_obi_z = 0.5


class MockSpecs:
    """Mock specs for testing"""
    def __init__(self):
        self.price_precision = 2
        self.size_precision = 4


def create_tick(ts: int, bid: float = 100.0, ask: float = 100.1, last: float = 100.05,
                volume: float = 1.0, is_buyer_maker: bool = True) -> Tick:
    """Helper to create Tick"""
    return Tick(
        ts=ts,
        bid=bid,
        ask=ask,
        last=last,
        volume=volume,
        flags=0,
        is_buyer_maker=is_buyer_maker
    )


class TestDataProcessorStaleness:

    def test_l2_fresh_data(self):
        """Test L2 data is considered fresh when within threshold"""
        config = MockConfig()
        specs = MockSpecs()
        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Set L2 timestamp close to tick timestamp
        processor._bucket_state.l2_ts = 1000

        # Process tick with close timestamp
        tick = create_tick(ts=1100)  # 100ms difference
        processor._process_tick(tick)

        assert processor._bucket_state.l2_age_ms == 100
        assert processor._bucket_state.l2_is_stale is False
        assert processor._bucket_state.l2_skew_tick_flag is False

    def test_l2_stale_data(self):
        """Test L2 data is marked as stale when beyond threshold"""
        config = MockConfig()
        specs = MockSpecs()
        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Set L2 timestamp far from tick timestamp
        processor._bucket_state.l2_ts = 1000

        # Process tick with distant timestamp
        tick = create_tick(ts=2000)  # 1000ms difference > 800ms threshold
        processor._process_tick(tick)

        assert processor._bucket_state.l2_age_ms == 1000
        assert processor._bucket_state.l2_is_stale is True
        assert processor._bucket_state.l2_skew_flag is True  # 1000ms > 300ms skew threshold

    def test_l2_skew_only(self):
        """Test L2 skew flag when age > skew threshold but < stale threshold"""
        config = MockConfig()
        specs = MockSpecs()
        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Set L2 timestamp
        processor._bucket_state.l2_ts = 1000

        # Process tick with age > skew but < stale
        tick = create_tick(ts=1400)  # 400ms difference > 300ms skew, < 800ms stale
        processor._process_tick(tick)

        assert processor._bucket_state.l2_age_ms == 400
        assert processor._bucket_state.l2_is_stale is False  # 400 < 800
        assert processor._bucket_state.l2_skew_flag is True  # 400 > 300

    def test_no_l2_data_ever(self):
        """Test behavior when no L2 data has ever been received"""
        config = MockConfig()
        specs = MockSpecs()
        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Don't set l2_ts (remains 0)
        assert processor._bucket_state.l2_ts == 0

        # Process tick
        tick = create_tick(ts=1000)
        processor._process_tick(tick)

        assert processor._bucket_state.l2_age_ms == 10**9  # Large default value
        assert processor._bucket_state.l2_is_stale is True
        assert processor._bucket_state.l2_skew_tick_flag is False  # No L2 data to skew against

    def test_l2_becomes_available(self):
        """Test transition from no L2 data to available L2 data"""
        config = MockConfig()
        specs = MockSpecs()
        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # First tick with no L2 data
        tick1 = create_tick(ts=1000)
        processor._process_tick(tick1)

        assert processor._bucket_state.l2_is_stale is True

        # Simulate L2 data arrival (normally done in _process_book)
        processor._bucket_state.l2_ts = 1500

        # Second tick with L2 data available
        tick2 = create_tick(ts=1600)
        processor._process_tick(tick2)

        assert processor._bucket_state.l2_age_ms == 100  # 1600 - 1500
        assert processor._bucket_state.l2_is_stale is False
        assert processor._bucket_state.l2_skew_tick_flag is False

    def test_l2_staleness_with_different_thresholds(self):
        """Test staleness with custom config thresholds"""
        config = MockConfig()
        config.l2_stale_ms = 500  # Custom stale threshold
        config.l2_skew_tick_thr_ms = 100   # Custom skew threshold
        specs = MockSpecs()
        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        processor._bucket_state.l2_ts = 1000

        # Test at skew threshold boundary
        tick1 = create_tick(ts=1100)  # 100ms >= skew threshold
        processor._process_tick(tick1)
        assert processor._bucket_state.l2_skew_flag is True  # >= 100

        # Test at stale threshold boundary
        tick2 = create_tick(ts=1500)  # 500ms >= stale threshold
        processor._process_tick(tick2)
        assert processor._bucket_state.l2_is_stale is True  # >= 500

    def test_wall_confidence_modifier_buy_signal(self):
        """Test wall confidence modifier for BUY signals"""
        from contexts import OrderflowSignalContext

        config = MockConfig()
        config.wall_filter_persist_min = 0.7
        config.wall_filter_dist_max_bps = 4.0
        config.wall_confidence_penalty = 0.1
        specs = MockSpecs()
        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Test case 1: Проблемная ask стена - большой штраф
        ctx1 = OrderflowSignalContext(
            wall_ask_persist_ratio=0.8,  # высокая persistence
            wall_ask_suspicious=False,   # не suspicious
            wall_ask_dist_bps=2.0        # очень близко
        )
        modifier1 = processor._get_wall_confidence_modifier(ctx1, "buy")
        assert modifier1 == -0.2  # двойной штраф

        # Test case 2: Менее проблемная стена - меньший штраф
        ctx2 = OrderflowSignalContext(
            wall_ask_persist_ratio=0.6,  # средняя persistence
            wall_ask_suspicious=False,
            wall_ask_dist_bps=5.0        # не очень близко
        )
        modifier2 = processor._get_wall_confidence_modifier(ctx2, "buy")
        assert modifier2 == -0.05  # половинный штраф

        # Test case 3: Suspicious стена вдали - бонус
        ctx3 = OrderflowSignalContext(
            wall_ask_persist_ratio=0.4,
            wall_ask_suspicious=True,    # suspicious - хорошо!
            wall_ask_dist_bps=8.0        # далеко (> 4.0 * 1.5 = 6.0)
        )
        modifier3 = processor._get_wall_confidence_modifier(ctx3, "buy")
        assert modifier3 == 0.03  # бонус за выявленный спуф

        # Test case 4: Нет проблем - нет модификации
        ctx4 = OrderflowSignalContext(
            wall_ask_persist_ratio=0.0,
            wall_ask_suspicious=False,
            wall_ask_dist_bps=100.0
        )
        modifier4 = processor._get_wall_confidence_modifier(ctx4, "buy")
        assert modifier4 == 0.0

    def test_wall_confidence_modifier_sell_signal(self):
        """Test wall confidence modifier for SELL signals"""
        from contexts import OrderflowSignalContext

        config = MockConfig()
        config.wall_filter_persist_min = 0.7
        config.wall_filter_dist_max_bps = 4.0
        config.wall_confidence_penalty = 0.1
        specs = MockSpecs()
        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Проблемная bid стена для SELL сигнала
        ctx = OrderflowSignalContext(
            wall_bid_persist_ratio=0.8,
            wall_bid_suspicious=False,
            wall_bid_dist_bps=2.0
        )
        modifier = processor._get_wall_confidence_modifier(ctx, "sell")
        assert modifier == -0.2  # двойной штраф
