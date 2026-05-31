"""
Integration test for deterministic gate decisions in replay scenarios
"""
from unittest.mock import Mock, patch

from common.news_gate import NewsGate
from news_pipeline.enricher_sync import NewsEnricherSync


class TestGateDeterministicReplay:
    """Test that gate decisions are deterministic across replay"""

    def test_gate_decisions_identical_for_same_timestamp(self):
        """Test that identical timestamps produce identical decisions"""
        redis = Mock()

        # Setup calendar data
        redis.hgetall.return_value = {
            "event_ts_ms": "1005000",  # 5 seconds after tick
            "event_grade_id": "3",
            "updated_ts_ms": "1000000"
        }

        gate = NewsGate(redis_client=redis, window_sec=300, grade_min=4)

        tick_ts_ms = 1000000

        # Multiple calls with same timestamp
        decision1 = gate.decide(now_ts_ms=tick_ts_ms, news_risk=0.5, news_grade_id=2, confidence=0.8)
        decision2 = gate.decide(now_ts_ms=tick_ts_ms, news_risk=0.5, news_grade_id=2, confidence=0.8)
        decision3 = gate.decide(now_ts_ms=tick_ts_ms, news_risk=0.5, news_grade_id=2, confidence=0.8)

        assert decision1.risk_factor_bps == decision2.risk_factor_bps == decision3.risk_factor_bps
        assert decision1.hard_block == decision2.hard_block == decision3.hard_block
        assert decision1.soft_reasons == decision2.soft_reasons == decision3.soft_reasons

    def test_enricher_tminus_deterministic_across_calls(self):
        """Test that enricher produces identical tminus for same tick time"""
        redis = Mock()

        # Mock Redis responses for news and calendar
        mock_pipe = Mock()
        mock_pipe.execute.return_value = [
            {"risk_ema": "0.5", "news_grade_id": "3", "confidence": "0.8"},  # news
            {"event_ts_ms": "1300000", "event_grade_id": "2"}               # calendar
        ]
        redis.pipeline.return_value = mock_pipe

        enricher = NewsEnricherSync(redis=redis, per_symbol_cache_ms=1000)

        from contexts import OrderflowSignalContext

        # Create multiple contexts with same symbol and tick time
        contexts = []
        for i in range(3):
            ctx = Mock(spec=OrderflowSignalContext)
            ctx.symbol = "EURUSD"
            ctx.news = None
            ctx.data_quality_flags = []
            contexts.append(ctx)

        tick_ts_ms = 1000000  # Same timestamp for all

        # Attach to all contexts
        for ctx in contexts:
            enricher.attach(ctx, now_ts_ms=tick_ts_ms, asset_class="crypto")

        # All should have identical tminus: (1300000 - 1000000) / 1000 = 300
        expected_tminus = 300
        for ctx in contexts:
            assert ctx.news.event_tminus_sec == expected_tminus
            assert "time_fallback_wall_clock" not in ctx.data_quality_flags

    def test_replay_scenario_no_wall_clock_dependency(self):
        """Test that replay produces same results regardless of wall clock"""
        redis = Mock()

        # Calendar event 10 seconds in the future
        redis.hgetall.return_value = {
            "event_ts_ms": "1010000",  # 10 seconds after tick
            "event_grade_id": "2",
            "updated_ts_ms": "1000000"
        }

        gate = NewsGate(redis_client=redis, window_sec=300, grade_min=4)

        tick_ts_ms = 1000000

        with patch('time.time', side_effect=[2000000, 3000000, 4000000]):  # Different wall times
            decisions = []
            for i in range(3):
                decision = gate.decide(now_ts_ms=tick_ts_ms, news_risk=0.3, news_grade_id=1, confidence=0.6)
                decisions.append(decision)

            # All decisions should be identical despite different wall clock times
            for d in decisions[1:]:
                assert d.risk_factor_bps == decisions[0].risk_factor_bps
                assert d.hard_block == decisions[0].hard_block
                assert d.soft_reasons == decisions[0].soft_reasons
