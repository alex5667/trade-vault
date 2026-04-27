import pytest
import time
from types import SimpleNamespace
from unittest.mock import MagicMock
from services.orderflow.components.bar_processor import BarProcessor

class MockRuntime:
    def __init__(self):
        self.symbol = "BTCUSDT"
        self.config = {}
        self.last_regime = "na"
        self.last_regime_ts_ms = 0
        self.rsi_price_value = float("nan")
        self.rsi_price_prev_value = float("nan")
        self.last_rsi_ts_ms = 0
        self.last_swing_high = None
        self.last_swing_low = None
        self.cont_ctx_trend_dir = None

@pytest.fixture
def processor():
    # Pass MagicMock for all dependencies
    return BarProcessor(
        redis_client=MagicMock(), 
        ticks_client=MagicMock(), 
        signal_pipeline=MagicMock(), 
        atr_cache=MagicMock(), 
        atr_tf_selector=MagicMock(), 
        calib_svc=MagicMock()
    )

def test_compute_trend_bias_cont_ctx(processor):
    runtime = MockRuntime()
    bar = SimpleNamespace(close=100.0)
    
    runtime.cont_ctx_trend_dir = "LONG"
    bias, source, strength = processor._compute_trend_bias(runtime, bar)
    assert bias == "UP"
    assert source == "cont_ctx"
    assert strength == 1.0

def test_compute_trend_bias_breakout(processor):
    runtime = MockRuntime()
    bar = SimpleNamespace(close=105.0)
    runtime.last_swing_high = SimpleNamespace(price=100.0)
    
    bias, source, strength = processor._compute_trend_bias(runtime, bar)
    assert bias == "UP"
    assert source == "breakout"
    assert strength == 0.8

def test_compute_trend_bias_regime(processor):
    runtime = MockRuntime()
    bar = SimpleNamespace(close=100.0)
    runtime.config = {"bias_regime_enable": "1"}
    runtime.last_regime = "trending_bull"
    runtime.last_regime_ts_ms = int(time.time() * 1000)
    
    bias, source, strength = processor._compute_trend_bias(runtime, bar)
    assert bias == "UP"
    assert source == "regime"
    assert strength == 0.6

def test_compute_trend_bias_regime_ttl(processor):
    runtime = MockRuntime()
    bar = SimpleNamespace(close=100.0)
    runtime.config = {"bias_regime_enable": "1", "bias_regime_ttl_ms": "1000"}
    runtime.last_regime = "trending_bull"
    runtime.last_regime_ts_ms = int(time.time() * 1000) - 2000 # Expired
    
    bias, source, strength = processor._compute_trend_bias(runtime, bar)
    assert bias == "none"
    assert source == "none"

def test_compute_trend_bias_rsi(processor):
    runtime = MockRuntime()
    bar = SimpleNamespace(close=100.0)
    runtime.config = {"bias_rsi_enable": "1", "bias_rsi_hi": "60"}
    runtime.rsi_price_value = 70.0
    runtime.last_rsi_ts_ms = int(time.time() * 1000)
    
    bias, source, strength = processor._compute_trend_bias(runtime, bar)
    assert bias == "UP"
    assert source == "rsi"
    assert strength == 0.4

def test_compute_trend_bias_rsi_slope(processor):
    runtime = MockRuntime()
    bar = SimpleNamespace(close=100.0)
    runtime.config = {"bias_rsi_enable": "1", "bias_rsi_hi": "60", "bias_rsi_require_slope": "1"}
    runtime.rsi_price_value = 70.0
    runtime.rsi_price_prev_value = 75.0 # Downtick but above 60 -> slope FAIL for Long
    runtime.last_rsi_ts_ms = int(time.time() * 1000)
    
    bias, source, strength = processor._compute_trend_bias(runtime, bar)
    assert bias == "none"
    
    runtime.rsi_price_prev_value = 65.0 # Uptick -> slope OK
    bias, source, strength = processor._compute_trend_bias(runtime, bar)
    assert bias == "UP"

def test_infer_bias_from_divergence(processor):
    runtime = MockRuntime()
    runtime.config = {"div_infer_enable": "1", "div_infer_max_age_ms": "100000"}
    d = SimpleNamespace(kind="bullish_regular", ts_ms=1000, strength=0.5)
    bar_ts_ms = 2000
    
    bias, source, strength, inferred = processor._infer_bias_from_divergence(runtime, d, bar_ts_ms, "none")
    assert bias == "UP"
    assert source == "div_infer"
    assert inferred == 1

def test_infer_bias_from_divergence_disabled(processor):
    runtime = MockRuntime()
    runtime.config = {"div_infer_enable": "0"}
    d = SimpleNamespace(kind="bullish_regular", ts_ms=1000, strength=0.5)
    bar_ts_ms = 2000
    
    bias, source, strength, inferred = processor._infer_bias_from_divergence(runtime, d, bar_ts_ms, "none")
    assert bias == "none"
    assert inferred == 0
