import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from services.orderflow.strategy import OrderFlowStrategy
from services.orderflow.runtime import SymbolRuntime
from core.burst_gate import BurstCandidateSelector
from core.pressure_tracker import PressureTracker


@pytest.fixture
def runtime():
    rt = SymbolRuntime("BTCUSDT", config={})
    rt.config = {
        "cooldown_reversal_sec": 30,
        "cooldown_continuation_sec": 15,
        "cooldown_min_ms": 1000,
        "cooldown_max_ms": 300000,
        "cooldown_reversal_dir_mul": 3.0,
        "cooldown_mul_thin": 1.6,
        "cooldown_mul_wide_spread": 1.4,
        "cooldown_mul_pressure_hi": 1.25,
        "cooldown_spread_hi_bp": 18.0,
    }
    rt.pressure = PressureTracker(window_ms=60000, ema_alpha=0.2)
    rt.burst = BurstCandidateSelector(window_ms=1000)
    return rt


@pytest.mark.asyncio
async def test_g11_cooldown_buffer_reversal(runtime):
    # Setup Strategy (mocking dependencies)
    redis_mock = AsyncMock()
    ticks_mock = AsyncMock()
    pub_mock = AsyncMock()
    of_engine = MagicMock()
    
    strategy = OrderFlowStrategy(redis_mock, ticks_mock, pub_mock, of_engine)
    
    # 1. First payload passes immediately (age > cooldown)
    payload1 = {
        "direction": "LONG",
        "confidence": 0.8,
        "indicators": {"strong_gate_scn": "reversal", "of_confirm_score": 0.8}
    }
    runtime.last_signal_ts = 0  # No previous signal
    
    res1 = await strategy._emit_payload(runtime, payload1, now_ms=100000)
    assert res1 == payload1, "First payload should pass (no cooldown active)"
    
    # Simulate first payload emission state update
    runtime.last_signal_ts = 100000
    runtime.last_emit_dir = "LONG"
    
    # 2. Second payload arrives during cooldown window (cooldown_reversal_sec = 30000ms)
    payload2 = {
        "direction": "LONG",
        "confidence": 0.9,
        "indicators": {"strong_gate_scn": "reversal", "of_confirm_score": 0.9} # Same direction
    }
    
    now_ms = 100000 + 10000  # 10 seconds later
    res2 = await strategy._emit_payload(runtime, payload2, now_ms=now_ms)
    
    assert res2 is None, "Payload should be buffered (G11 Cooldown Block)"
    assert runtime.pending_payload == payload2
    assert runtime.pending_score == 0.9

    # 3. Third payload arrives with higher score (overrides pending)
    payload3 = {
        "direction": "LONG",
        "confidence": 0.95,
        "indicators": {"strong_gate_scn": "reversal", "of_confirm_score": 0.95}
    }
    now_ms += 5000  # 15 seconds after 1st payload
    res3 = await strategy._emit_payload(runtime, payload3, now_ms=now_ms)
    
    assert res3 is None, "Payload should still be buffered"
    assert runtime.pending_payload == payload3
    assert runtime.pending_score == 0.95

    # 4. Fourth payload after cooldown passes, and emits pending first, then itself...
    # Wait, the logic passes the previous BEST buffered payload as the CURRENT result when cooldown expires,
    # OR if age >= cooldown, we use pending if it's better than current score.
    payload4 = {
        "direction": "LONG",
        "confidence": 0.5,
        "indicators": {"strong_gate_scn": "reversal", "of_confirm_score": 0.5}
    }
    now_ms = 100000 + 40000  # 40 seconds (cooldown = 30000)
    
    res4 = await strategy._emit_payload(runtime, payload4, now_ms=now_ms)
    
    # Because pending_score (0.95) >= cur_score (0.5), it should emit payload3!
    assert res4 == payload3, "G11 should emit the best-of-burst buffered payload"
    assert runtime.pending_payload is None, "Pending buffer should be cleared"


@pytest.mark.asyncio
async def test_g11_directional_reversal_penalty(runtime):
    redis_mock = AsyncMock()
    strategy = OrderFlowStrategy(redis_mock, AsyncMock(), AsyncMock(), MagicMock())
    
    # Base continuation cooldown is 15s.
    payload1 = {
        "direction": "LONG",
        "confidence": 0.8,
        "indicators": {"strong_gate_scn": "continuation", "of_confirm_score": 0.8}
    }
    runtime.last_signal_ts = 100000
    runtime.last_emit_dir = "SHORT"  # Previous emit was SHORT
    
    # Wait 20 seconds. Normally, 15s continuation cooldown would have passed.
    now_ms = 100000 + 20000
    
    # But because direction flipped (SHORT -> LONG), directional penalty applies:
    # cooldown = 15s * 3.0 = 45s.
    res1 = await strategy._emit_payload(runtime, payload1, now_ms=now_ms)
    
    assert res1 is None, "Directional reverse penalty should extend cooldown and block"
    assert runtime.pending_payload == payload1


@pytest.mark.asyncio
async def test_g11_pressure_hit_rate_metrics(runtime):
    redis_mock = AsyncMock()
    strategy = OrderFlowStrategy(redis_mock, AsyncMock(), AsyncMock(), MagicMock())
    
    runtime.last_signal_ts = 100000
    runtime.last_emit_dir = "LONG"
    
    # Hit cooldown 3 times
    for i in range(3):
        payload = {
            "direction": "LONG",
            "confidence": 0.8,
            "indicators": {"strong_gate_scn": "reversal"}
        }
        await strategy._emit_payload(runtime, payload, now_ms=105000 + i*1000)
    
    # Simulate pressure snapshot inside tick update
    for i in range(5):
        runtime.pressure.on_raw_trigger(ts_ms=105000 + i*1000)
        
    ps = runtime.pressure.snapshot(now_ms=108000)
    # n_raw = 5, n_cd = 3
    # rate = 3/5 = 0.6
    assert ps.cd_rate == 0.6
    # EMA follows rate
    assert ps.cd_rate_ema > 0.0

