"""
Unit tests for NewsGate decide() method with Hard Block + Soft Gate
"""
from unittest.mock import Mock

from common.news_gate import NewsGate


class TestNewsGateDecision:
    """Test NewsGate decide() method"""

    def setup_method(self):
        self.redis = Mock()
        self.gate = NewsGate(
            redis_client=self.redis,
            asset_class="crypto",
            window_sec=300,
            grade_min=4
        )

    def test_decide_no_timestamp_returns_no_ts(self):
        """Test invalid timestamp handling"""
        decision = self.gate.decide(now_ts_ms=0)
        assert decision.hard_block is False
        assert decision.hard_reason == "no_ts"
        assert decision.risk_factor_bps == 10000
        assert "no_ts" in decision.dq_flags

    def test_decide_manual_hard_block(self):
        """Test manual hard block override"""
        now_ts_ms = 1000000
        until_ts_ms = 2000000

        self.redis.get.return_value = '{"active":1,"until_ts_ms":2000000,"reason":"CPI"}'

        decision = self.gate.decide(now_ts_ms=now_ts_ms)

        assert decision.hard_block is True
        assert decision.hard_reason == "CPI"
        assert decision.until_ts_ms == until_ts_ms
        assert decision.risk_factor_bps == 0

    def test_decide_calendar_hard_block_high_grade_near_event(self):
        """Test calendar hard block for grade >= grade_min near event"""
        now_ts_ms = 1000000
        event_ts_ms = 1005000  # 5 seconds from now, within window
        tminus = 500  # within 300s window

        self.redis.hgetall.return_value = {
            "event_ts_ms": str(event_ts_ms),
            "event_grade_id": "4",  # high impact
            "event_tminus_sec": str(tminus)
        }

        decision = self.gate.decide(now_ts_ms=now_ts_ms)

        assert decision.hard_block is True
        assert decision.hard_reason == "calendar_hi_impact"
        assert decision.risk_factor_bps == 0
        assert decision.meta["cal_grade"] == 4

    def test_decide_calendar_soft_factor_grade_2_near_event(self):
        """Test calendar soft factor for grade 2 near event"""
        now_ts_ms = 1000000
        event_ts_ms = 1005000  # 5 seconds from now, within window

        self.redis.hgetall.return_value = {
            "event_ts_ms": str(event_ts_ms),
            "event_grade_id": "2",  # medium impact
            "event_tminus_sec": "500"
        }

        decision = self.gate.decide(now_ts_ms=now_ts_ms)

        assert decision.hard_block is False
        assert decision.risk_factor_bps == 5000  # reduced factor
        assert "soft_cal" in decision.soft_reasons

    def test_decide_news_soft_factor_with_features(self):
        """Test news soft factor calculation with provided features"""
        now_ts_ms = 1000000

        # Mock empty calendar
        self.redis.hgetall.return_value = {}

        decision = self.gate.decide(
            now_ts_ms=now_ts_ms,
            news_risk=0.8,
            news_grade_id=3,
            confidence=0.9
        )

        # Factor should be reduced due to news risk
        assert decision.hard_block is False
        assert decision.risk_factor_bps < 10000  # some reduction
        assert "soft_news" in decision.soft_reasons

    def test_decide_combined_soft_factors(self):
        """Test combination of calendar and news soft factors"""
        now_ts_ms = 1000000
        event_ts_ms = 1005000

        self.redis.hgetall.return_value = {
            "event_ts_ms": str(event_ts_ms),
            "event_grade_id": "2",
            "event_tminus_sec": "500"
        }

        decision = self.gate.decide(
            now_ts_ms=now_ts_ms,
            news_risk=0.6,
            news_grade_id=2,
            confidence=0.7
        )

        assert decision.hard_block is False
        assert decision.risk_factor_bps == 5000  # calendar factor is limiting
        assert "soft_cal" in decision.soft_reasons
        assert "soft_news" in decision.soft_reasons

    def test_check_backward_compatibility(self):
        """Test check() method maintains backward compatibility"""
        now_ts_ms = 1000000
        event_ts_ms = 1005000

        self.redis.hgetall.return_value = {
            "event_ts_ms": str(event_ts_ms),
            "event_grade_id": "4",
            "event_tminus_sec": "500"
        }

        block = self.gate.check(now_ts_ms=now_ts_ms)

        assert block.blocked is True
        assert block.reason == "calendar_hi_impact"
        assert block.until_ts_ms > 0
