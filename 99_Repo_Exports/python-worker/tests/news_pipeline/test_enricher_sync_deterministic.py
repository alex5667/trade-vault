"""
Unit tests for NewsEnricherSync deterministic time handling
"""
from unittest.mock import Mock, patch

from contexts import OrderflowSignalContext
from news_pipeline.enricher_sync import NewsEnricherSync


class TestNewsEnricherSyncDeterministic:
    """Test deterministic time handling in NewsEnricherSync"""

    def setup_method(self):
        self.redis = Mock()
        self.enricher = NewsEnricherSync(redis=self.redis, per_symbol_cache_ms=1000)

    def test_attach_with_tick_time_calculates_tminus_correctly(self):
        """Test that tminus is calculated from event_ts_ms when now_ts_ms provided"""
        # Mock Redis responses
        tick_ts_ms = 1000000  # 1 second into epoch
        event_ts_ms = 1300000  # 1.3 seconds into epoch
        expected_tminus = 300  # 300ms difference

        mock_pipe = Mock()
        mock_pipe.execute.return_value = [
            {"risk_ema": "0.5", "news_grade_id": "3", "confidence": "0.8"},  # news
            {"event_ts_ms": str(event_ts_ms), "event_grade_id": "2"}       # calendar
        ]
        self.redis.pipeline.return_value = mock_pipe

        ctx = Mock(spec=OrderflowSignalContext)
        ctx.symbol = "EURUSD"
        ctx.news = None

        try:
            self.enricher.attach(ctx, now_ts_ms=tick_ts_ms, asset_class="crypto")
        except Exception as e:
            print(f"Exception during attach: {e}")
            raise

        # Verify tminus calculation: (1300000 - 1000000) / 1000 = 300
        assert ctx.news is not None, "ctx.news should not be None"
        assert ctx.news.event_tminus_sec == expected_tminus

    def test_attach_without_tick_time_fallback_to_wall_clock(self):
        """Test fallback to wall clock when now_ts_ms not provided"""

        tick_ts_ms = 1000000
        event_ts_ms = 1300000

        mock_pipe = Mock()
        mock_pipe.execute.return_value = [
            {"risk_ema": "0.5"},
            {"event_ts_ms": str(event_ts_ms), "event_grade_id": "2"}
        ]
        self.redis.pipeline.return_value = mock_pipe

        ctx = Mock(spec=OrderflowSignalContext)
        ctx.symbol = "EURUSD"
        ctx.news = None
        ctx.data_quality_flags = []

        with patch('time.time', return_value=tick_ts_ms / 1000.0):
            self.enricher.attach(ctx, asset_class="crypto")  # no now_ts_ms

        # Should append dq flag for wall clock fallback
        assert "time_fallback_wall_clock" in ctx.data_quality_flags

    def test_attach_forex_scope_normalized_to_fx(self):
        """Test that forex asset_class is normalized to fx for calendar lookup"""
        tick_ts_ms = 1000000

        mock_pipe = Mock()
        mock_pipe.execute.return_value = [
            {"risk_ema": "0.5"},
            {"event_ts_ms": "1300000", "event_grade_id": "2"}
        ]
        self.redis.pipeline.return_value = mock_pipe

        ctx = Mock(spec=OrderflowSignalContext)
        ctx.symbol = "EURUSD"
        ctx.news = None

        self.enricher.attach(ctx, asset_class="forex", now_ts_ms=tick_ts_ms)

        # Verify pipeline was called with normalized key
        pipeline = self.redis.pipeline.return_value
        calls = pipeline.hgetall.call_args_list
        assert any("calendar:agg:fx" in str(call) for call in calls)

    def test_attach_cache_bucketed_by_tick_time(self):
        """Test that cache is bucketed by tick time, not wall clock"""
        tick_ts_ms_1 = 1000000  # bucket 1000
        tick_ts_ms_2 = 1000500  # same bucket 1000

        mock_pipe = Mock()
        mock_pipe.execute.return_value = [
            {"risk_ema": "0.5"},
            {"event_ts_ms": "1300000", "event_grade_id": "2"}
        ]
        self.redis.pipeline.return_value = mock_pipe

        ctx1 = Mock(spec=OrderflowSignalContext)
        ctx1.symbol = "EURUSD"
        ctx1.news = None

        ctx2 = Mock(spec=OrderflowSignalContext)
        ctx2.symbol = "EURUSD"
        ctx2.news = None

        # First call
        self.enricher.attach(ctx1, now_ts_ms=tick_ts_ms_1, asset_class="crypto")
        assert ctx1.news is not None

        # Second call with different tick time but same bucket should hit cache
        self.redis.pipeline.reset_mock()
        self.enricher.attach(ctx2, now_ts_ms=tick_ts_ms_2, asset_class="crypto")

        # Pipeline should not be called (cache hit)
        assert not self.redis.pipeline.called
        assert ctx2.news is ctx1.news
