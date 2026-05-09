from __future__ import annotations

import json

from common.news_gate import GateDecision, NewsGate


class R:
    def __init__(self):
        self.kv = {}
        self.hh = {}
    def get(self, k):
        return self.kv.get(k)
    def hgetall(self, k):
        return self.hh.get(k, {})


def test_manual_blocks_legacy():
    """Test legacy NewsBlock interface"""
    r = R()
    r.kv["news:hi:active"] = json.dumps({"active": 1, "until_ts_ms": 2000, "reason": "CPI", "symbols": ["BTCUSDT"]})
    g = NewsGate(redis_client=r)
    nb = g.check(now_ts_ms=1500, symbols=("BTCUSDT",))
    assert nb.blocked is True
    assert nb.until_ts_ms == 2000


def test_calendar_blocks_legacy():
    """Test legacy NewsBlock interface"""
    r = R()
    r.hh["calendar:agg:crypto"] = {"event_grade_id": "4", "event_tminus_sec": "100", "title": "FOMC"}
    g = NewsGate(redis_client=r)
    nb = g.check(now_ts_ms=1_000_000, symbols=("BTCUSDT",))
    assert nb.blocked is True


def test_calendar_not_block_if_outside_window_legacy():
    """Test legacy NewsBlock interface"""
    r = R()
    r.hh["calendar:agg:crypto"] = {"event_grade_id": "4", "event_tminus_sec": "10000", "title": "FOMC"}
    g = NewsGate(redis_client=r)
    nb = g.check(now_ts_ms=1_000_000, symbols=("BTCUSDT",))
    assert nb.blocked is False


def test_decide_method_basic():
    """Test new decide() method returns GateDecision"""
    r = R()
    g = NewsGate(redis_client=r, asset_class="crypto")
    decision = g.decide(now_ts_ms=1000)
    assert isinstance(decision, GateDecision)
    assert decision.hard_block is False
    assert decision.hard_reason == "ok"
    assert decision.risk_factor_bps == 10000
    # Should have dq_flag for missing event_ts_ms since no calendar data
    assert "calendar_missing_event_ts_ms" in decision.dq_flags
    assert "cal_key" in decision.meta


def test_decide_manual_hard_block():
    """Test decide() method with manual hard block"""
    r = R()
    r.kv["news:hi:active"] = json.dumps({"active": 1, "until_ts_ms": 2000, "reason": "CPI", "symbols": ["BTCUSDT"]})
    g = NewsGate(redis_client=r, asset_class="crypto")
    decision = g.decide(now_ts_ms=1500, symbols=("BTCUSDT",))
    assert decision.hard_block is True
    assert decision.hard_reason == "CPI"
    assert decision.until_ts_ms == 2000
    assert decision.risk_factor_bps == 0


def test_decide_calendar_hard_block():
    """Test decide() method with calendar hard block"""
    r = R()
    r.hh["calendar:agg:crypto"] = {"event_grade_id": "4", "event_ts_ms": "1600", "title": "FOMC"}
    g = NewsGate(redis_client=r, asset_class="crypto")
    decision = g.decide(now_ts_ms=1500)
    assert decision.hard_block is True
    assert decision.hard_reason == "calendar_hi_impact"
    assert decision.risk_factor_bps == 0


def test_decide_soft_gate_calendar():
    """Test decide() method with calendar soft gate"""
    r = R()
    # Grade 2 event 10 minutes before now_ts_ms
    r.hh["calendar:agg:crypto"] = {"event_grade_id": "2", "event_ts_ms": "1600", "title": "Event"}
    g = NewsGate(redis_client=r, asset_class="crypto", soft_enabled=True, soft_grade_min=2)
    decision = g.decide(now_ts_ms=1000)  # 600 seconds before event
    assert decision.hard_block is False
    assert decision.risk_factor_bps == 5000  # soft_grade2_bps
    assert "soft_calendar_bps" in decision.meta


def test_decide_soft_gate_news():
    """Test decide() method with news soft gate"""
    r = R()
    g = NewsGate(redis_client=r, asset_class="crypto", soft_enabled=True)
    decision = g.decide(
        now_ts_ms=1000,
        news_risk=0.8,
        news_grade_id=3,
        confidence=0.9
    )
    assert decision.hard_block is False
    assert decision.risk_factor_bps < 10000  # should be reduced
    assert "soft_news_bps" in decision.meta
    assert "soft_news_impact" in decision.meta


def test_gate_decision_no_block():
    """Test GateDecision with no blocking"""
    r = R()
    g = NewsGate(redis_client=r)
    decision = g.decide(now_ts_ms=1000)
    assert decision.hard_block is False
    assert decision.hard_reason == "ok"
    assert decision.risk_factor_bps == 10000
    assert decision.soft_reasons == []


def test_gate_decision_hard_block_manual():
    """Test GateDecision hard block from manual override"""
    r = R()
    r.kv["news:hi:active"] = json.dumps({"active": 1, "until_ts_ms": 2000, "reason": "CPI", "symbols": ["BTCUSDT"]})
    g = NewsGate(redis_client=r)
    decision = g.decide(now_ts_ms=1500, symbols=("BTCUSDT",))
    assert decision.hard_block is True
    assert decision.hard_reason == "CPI"
    assert decision.until_ts_ms == 2000
    assert decision.risk_factor_bps == 0  # max reduction on hard block


def test_gate_decision_hard_block_calendar():
    """Test GateDecision hard block from calendar event"""
    r = R()
    r.hh["calendar:agg:crypto"] = {
        "event_grade_id": "4",
        "event_tminus_sec": "100",
        "event_ts_ms": "1100000",
        "title": "FOMC",
        "updated_ts_ms": "1000000"
    }
    g = NewsGate(redis_client=r)
    decision = g.decide(now_ts_ms=1_000_000)
    assert decision.hard_block is True
    assert "calendar_hi_impact" in decision.hard_reason


def test_gate_decision_soft_risk_calendar():
    """Test GateDecision soft risk reduction from calendar proximity"""
    r = R()
    # Medium grade event, outside hard block window but in soft window
    r.hh["calendar:agg:crypto"] = {
        "event_grade_id": "2",
        "event_tminus_sec": "1800",  # 30 min before event
        "event_ts_ms": "1180000",
        "title": "GDP",
        "updated_ts_ms": "1000000"
    }
    g = NewsGate(redis_client=r)
    decision = g.decide(now_ts_ms=1_000_000)
    assert decision.hard_block is False
    assert decision.risk_factor_bps < 10000  # some risk reduction
    assert len(decision.soft_reasons) > 0
    assert "soft_cal" in decision.soft_reasons


def test_gate_decision_time_fallback():
    """Test GateDecision with time fallback"""
    r = R()
    g = NewsGate(redis_client=r)
    decision = g.decide(now_ts_ms=0)  # invalid timestamp
    assert decision.hard_block is False
    assert "no_ts" in decision.dq_flags


def test_gate_decision_stale_calendar():
    """Test GateDecision with stale calendar data"""
    r = R()
    # Make data older than 1 hour (3600 sec = 3,600,000 ms)
    # now_ts_ms = 1_000_000, updated_ts_ms should be < 1_000_000 - 3_600_000 = -2_600_000
    # Since Redis hgetall returns strings, and _i() converts strings to int
    stale_updated = str(1_000_000 - 4_000_000)  # "-3000000"
    r.hh["calendar:agg:crypto"] = {
        "event_grade_id": "3",
        "event_tminus_sec": "3600",
        "updated_ts_ms": stale_updated
    }
    g = NewsGate(redis_client=r)
    decision = g.decide(now_ts_ms=1_000_000)
    assert decision.hard_block is False
    assert "calendar_stale" in decision.dq_flags
    # Stale data should not affect risk factor
    assert decision.risk_factor_bps == 10000
